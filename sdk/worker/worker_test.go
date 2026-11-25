package worker

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/sdk/activity"
)

type queueIn struct {
	ID string `json:"id"`
}

type queueOut struct {
	OK bool `json:"ok"`
}

func queueActivity(context.Context, queueIn) (queueOut, error) { return queueOut{OK: true}, nil }

// TestActivityTaskQueues 锁住 Worker 拉取队列的推导与校验。
func TestActivityTaskQueues(t *testing.T) {
	queued := func(t *testing.T) *activity.Registry {
		t.Helper()
		registry := activity.NewRegistry()
		if err := activity.RegisterActivityOnQueue(registry, "payments", "payments.charge", "v1", queueActivity); err != nil {
			t.Fatal(err)
		}
		if err := activity.RegisterActivityOnQueue(registry, "orders", "orders.notify", "v1", queueActivity); err != nil {
			t.Fatal(err)
		}
		return registry
	}

	t.Run("empty explicit uses registered queues", func(t *testing.T) {
		got, err := activityTaskQueues(nil, queued(t))
		if err != nil || !reflect.DeepEqual(got, []string{"orders", "payments"}) {
			t.Fatalf("queues = %v, err = %v", got, err)
		}
	})

	t.Run("unqueued activity adds default", func(t *testing.T) {
		registry := queued(t)
		if err := activity.RegisterActivity(registry, "audit.log", "v1", queueActivity); err != nil {
			t.Fatal(err)
		}
		got, err := activityTaskQueues(nil, registry)
		if err != nil || !reflect.DeepEqual(got, []string{"orders", "payments", "default"}) {
			t.Fatalf("queues = %v, err = %v", got, err)
		}
	})

	t.Run("explicit superset is kept", func(t *testing.T) {
		explicit := []string{"payments", "orders", "rerouted"}
		got, err := activityTaskQueues(explicit, queued(t))
		if err != nil || !reflect.DeepEqual(got, explicit) {
			t.Fatalf("queues = %v, err = %v", got, err)
		}
	})

	t.Run("explicit missing a registered queue fails", func(t *testing.T) {
		_, err := activityTaskQueues([]string{"payments"}, queued(t))
		if err == nil || !strings.Contains(err.Error(), `"orders.notify"`) || !strings.Contains(err.Error(), `task queue "orders"`) {
			t.Fatalf("err = %v, want missing orders queue", err)
		}
	})

	t.Run("no registry keeps explicit", func(t *testing.T) {
		got, err := activityTaskQueues(nil, nil)
		if err != nil || got != nil {
			t.Fatalf("queues = %v, err = %v", got, err)
		}
	})
}
