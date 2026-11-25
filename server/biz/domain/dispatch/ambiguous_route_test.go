package dispatch

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
)

// TestResolveActivityRouteUndeclaredQueueAmbiguity 锁住没有声明队列时的选路：候选分布在互不相关的队列上、
// 又没有权重这样的显式分流时，不在两个服务之间静默二选一，而是返回 ErrActivityRouteUnavailable 让调度阻塞；
// 声明了队列的调用只在该队列里选，不受另一个服务影响。
func TestResolveActivityRouteUndeclaredQueueAmbiguity(t *testing.T) {
	type worker struct {
		identity, build string
		queues          []string
		isDefault       bool
		weight          int
	}
	catalog := func(t *testing.T, workers ...worker) *ActivityRouteCatalog {
		t.Helper()
		c := NewActivityRouteCatalog(clocktest.NewFake(time.Now()))
		for _, w := range workers {
			if err := c.RegisterActivityWorkerVersion(protocol.ActivityWorkerVersion{
				Namespace: "shop", Identity: w.identity, BuildID: w.build, TaskQueues: w.queues,
				ActivityTypes: []string{"notify.send"}, Default: w.isDefault, Weight: w.weight,
			}); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}
	undeclared := ActivityRouteRef{Namespace: "shop", ActivityID: "send", Type: "notify.send"}

	for _, test := range []struct {
		name      string
		workers   []worker
		ref       ActivityRouteRef
		wantQueue string
		ambiguous bool
	}{
		{
			name: "Happy Path: ten instances of one service share a queue",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders"}, true, 0},
				{"order-2", "order-v1", []string{"orders"}, true, 0},
				{"order-3", "order-v1", []string{"orders"}, true, 0},
			},
			ref: undeclared, wantQueue: "orders",
		},
		{
			name: "Happy Path: rolling deploy keeps both builds on one queue",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders"}, true, 0},
				{"order-2", "order-v2", []string{"orders"}, true, 0},
			},
			ref: undeclared, wantQueue: "orders",
		},
		{
			name: "Happy Path: one build polling two queues is one pool",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders", "orders-bulk"}, true, 0},
			},
			ref: undeclared, wantQueue: "orders",
		},
		{
			name: "Error Case: two services serve the type as default on unrelated queues",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders"}, true, 0},
				{"billing-1", "billing-v1", []string{"billing"}, true, 0},
			},
			ref: undeclared, ambiguous: true,
		},
		{
			name: "Error Case: neither default nor weight leaves the choice to sorting",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders"}, false, 0},
				{"billing-1", "billing-v1", []string{"billing"}, false, 0},
			},
			ref: undeclared, ambiguous: true,
		},
		{
			name: "Happy Path: a single default route wins over unrelated non-default routes",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders"}, true, 0},
				{"billing-1", "billing-v1", []string{"billing"}, false, 0},
			},
			ref: undeclared, wantQueue: "orders",
		},
		{
			name: "Happy Path: weights without defaults are an explicit split",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders"}, false, 90},
				{"billing-1", "billing-v1", []string{"billing"}, false, 10},
			},
			ref: undeclared,
		},
		{
			name: "Happy Path: a declared queue only looks at that queue",
			workers: []worker{
				{"order-1", "order-v1", []string{"orders"}, true, 0},
				{"billing-1", "billing-v1", []string{"billing"}, true, 0},
			},
			ref:       ActivityRouteRef{Namespace: "shop", ActivityID: "send", Type: "notify.send", TaskQueue: "billing"},
			wantQueue: "billing",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			route, err := catalog(t, test.workers...).ResolveActivityRoute(test.ref)
			if test.ambiguous {
				if !errors.Is(err, ErrActivityRouteUnavailable) || !strings.Contains(err.Error(), "declares no task queue") ||
					!strings.Contains(err.Error(), `"billing"`) || !strings.Contains(err.Error(), `"orders"`) {
					t.Fatalf("err = %v, want an ambiguity naming both queues", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if test.wantQueue != "" && route.TaskQueue != test.wantQueue {
				t.Fatalf("queue = %q, want %q", route.TaskQueue, test.wantQueue)
			}
		})
	}
}
