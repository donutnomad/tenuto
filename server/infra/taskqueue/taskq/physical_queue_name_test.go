package taskq

import (
	"context"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// TestPhysicalQueueNameOverridePrecedence 验证物理名单一事实源的解析优先级：
// namespace 作用域键 > 裸 route 名 > activity: 前缀约定。
func TestPhysicalQueueNameOverridePrecedence(t *testing.T) {
	scoped := dispatch.ActivityTaskQueueKey("tenant-a", "payments")
	tests := []struct {
		name      string
		overrides map[string]string
		namespace string
		taskQueue string
		want      string
	}{
		{
			name:      "unconfigured route uses activity prefix",
			namespace: "tenant-a",
			taskQueue: "payments",
			want:      activityPhysicalQueuePrefix + scoped,
		},
		{
			name:      "bare route override wins over convention",
			overrides: map[string]string{"payments": "legacy-payments"},
			namespace: "tenant-a",
			taskQueue: "payments",
			want:      "legacy-payments",
		},
		{
			name:      "scoped override wins over bare route",
			overrides: map[string]string{scoped: "scoped-payments", "payments": "legacy-payments"},
			namespace: "tenant-a",
			taskQueue: "payments",
			want:      "scoped-payments",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhysicalQueueName(tt.overrides, tt.namespace, tt.taskQueue); got != tt.want {
				t.Fatalf("PhysicalQueueName = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRouterUsesPhysicalQueueNameSingleSourceOfTruth 是核心防漂移断言：router 运行时
// lazy-dial 用的物理名，必须与外部（如启动初始化）调 PhysicalQueueName 算出的完全一致。
func TestRouterUsesPhysicalQueueNameSingleSourceOfTruth(t *testing.T) {
	overrides := map[string]string{"payments": "legacy-payments"}
	cases := []struct{ namespace, taskQueue string }{
		{"tenant-a", "payments"},    // configured route -> override
		{"tenant-a", "brand-new-q"}, // unconfigured route -> activity: convention
	}
	for _, c := range cases {
		var dialed string
		router := newLazyActivityQueueRouter(overrides, func(_ context.Context, physical string) (*Queue, interface{ Close() }, error) {
			dialed = physical
			return NewQueue(&routingClientFake{}), &fakeCloser{}, nil
		})
		task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: c.namespace, RunID: "run-1", TaskQueue: c.taskQueue}
		if err := router.Enqueue(context.Background(), c.taskQueue, task); err != nil {
			router.Close()
			t.Fatalf("enqueue %s/%s: %v", c.namespace, c.taskQueue, err)
		}
		want := PhysicalQueueName(overrides, c.namespace, c.taskQueue)
		if dialed != want {
			router.Close()
			t.Fatalf("router dialed %q, PhysicalQueueName = %q (drift)", dialed, want)
		}
		router.Close()
	}
}
