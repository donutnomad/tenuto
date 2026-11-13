package dispatch

import (
	"fmt"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func TestChooseActivityRoute(t *testing.T) {
	ref := ActivityRouteRef{Namespace: "tenant", ActivityID: "charge", Type: "charge", RoutingKey: "run-1:7"}
	for _, test := range []struct {
		name   string
		routes []ActivityRoute
		want   string
	}{
		{
			name:   "Happy Path: single route wins without default or weight",
			routes: []ActivityRoute{{BuildID: "v1"}},
			want:   "v1",
		},
		{
			name:   "Happy Path: default route beats weights",
			routes: []ActivityRoute{{BuildID: "v1", Weight: 100}, {BuildID: "v2", Default: true}},
			want:   "v2",
		},
		{
			name:   "Edge Case: several defaults resolve to the smallest build id",
			routes: []ActivityRoute{{BuildID: "v3", Default: true}, {BuildID: "v1"}, {BuildID: "v2", Default: true}},
			want:   "v2",
		},
		{
			name:   "Happy Path: the only weighted route takes every key",
			routes: []ActivityRoute{{BuildID: "v1"}, {BuildID: "v2", Weight: 5}, {BuildID: "v3"}},
			want:   "v2",
		},
		{
			name:   "Edge Case: negative weight counts as unweighted",
			routes: []ActivityRoute{{BuildID: "v1", Weight: -10}, {BuildID: "v2", Weight: 1}},
			want:   "v2",
		},
		{
			name:   "Edge Case: no default and no weight falls back to the first route",
			routes: []ActivityRoute{{BuildID: "v1"}, {BuildID: "v2", Weight: -1}},
			want:   "v1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := chooseActivityRoute(test.routes, ref).BuildID; got != test.want {
				t.Fatalf("build = %q, want %q", got, test.want)
			}
		})
	}
}

func TestChooseActivityRouteSpreadsKeysByWeight(t *testing.T) {
	routes := []ActivityRoute{{BuildID: "v1", Weight: 1}, {BuildID: "v2", Weight: 3}, {BuildID: "v3"}}
	const keys = 4000
	counts := map[string]int{}
	for i := range keys {
		ref := ActivityRouteRef{RoutingKey: fmt.Sprintf("run-%d:7", i)}
		first := chooseActivityRoute(routes, ref)
		if again := chooseActivityRoute(routes, ref); again.BuildID != first.BuildID {
			t.Fatalf("key %q moved from %q to %q", ref.RoutingKey, first.BuildID, again.BuildID)
		}
		counts[first.BuildID]++
	}
	if counts["v3"] != 0 {
		t.Fatalf("unweighted route received %d keys", counts["v3"])
	}
	// 权重 1:3，期望 1000:3000；FNV 哈希是确定的，留 10% 余量只为表达“按权重分布”。
	if counts["v1"] < 900 || counts["v1"] > 1100 || counts["v1"]+counts["v2"] != keys {
		t.Fatalf("distribution = %+v, want about 1000/3000", counts)
	}
}

func TestChooseActivityRouteHashPlacement(t *testing.T) {
	routes := []ActivityRoute{{BuildID: "v1", Weight: 1}, {BuildID: "v2", Weight: 1}}
	// 找两个落在不同版本上的 routing key，证明下面的断言能区分两条路由。
	byBuild := map[string]string{}
	for i := 0; len(byBuild) < 2 && i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		byBuild[chooseActivityRoute(routes, ActivityRouteRef{RoutingKey: key}).BuildID] = key
	}
	if len(byBuild) != 2 {
		t.Fatalf("100 keys all landed on one route: %+v", byBuild)
	}

	t.Run("Happy Path: routing key overrides the activity identity", func(t *testing.T) {
		for build, key := range byBuild {
			for _, ref := range []ActivityRouteRef{
				{RoutingKey: key},
				{RoutingKey: key, Namespace: "tenant", ActivityID: "charge", Type: "charge", TaskQueue: "payments"},
			} {
				if got := chooseActivityRoute(routes, ref).BuildID; got != build {
					t.Fatalf("ref %+v build = %q, want %q", ref, got, build)
				}
			}
		}
	})

	t.Run("Edge Case: default namespace and queue spell the same key", func(t *testing.T) {
		implicit := ActivityRouteRef{ActivityID: "charge", Type: "charge"}
		explicit := ActivityRouteRef{Namespace: protocol.DefaultNamespace, ActivityID: "charge", Type: "charge", TaskQueue: defaultActivityTaskQueue}
		if activityRouteSelectionKey(implicit) != activityRouteSelectionKey(explicit) {
			t.Fatalf("keys differ: %q vs %q", activityRouteSelectionKey(implicit), activityRouteSelectionKey(explicit))
		}
		if a, b := chooseActivityRoute(routes, implicit), chooseActivityRoute(routes, explicit); a.BuildID != b.BuildID {
			t.Fatalf("same activity placed on %q and %q", a.BuildID, b.BuildID)
		}
	})

	t.Run("Edge Case: activity identity separates keys without a routing key", func(t *testing.T) {
		base := ActivityRouteRef{Namespace: "tenant", ActivityID: "charge", Type: "charge", TaskQueue: "payments"}
		for name, other := range map[string]ActivityRouteRef{
			"namespace":  {Namespace: "other", ActivityID: "charge", Type: "charge", TaskQueue: "payments"},
			"step":       {Namespace: "tenant", ActivityID: "refund", Type: "charge", TaskQueue: "payments"},
			"type":       {Namespace: "tenant", ActivityID: "charge", Type: "refund", TaskQueue: "payments"},
			"task queue": {Namespace: "tenant", ActivityID: "charge", Type: "charge", TaskQueue: "batch"},
		} {
			if activityRouteSelectionKey(base) == activityRouteSelectionKey(other) {
				t.Fatalf("%s does not change the selection key", name)
			}
		}
	})
}
