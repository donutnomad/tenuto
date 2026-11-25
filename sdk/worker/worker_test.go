package worker

import (
	"context"
	"reflect"
	"runtime/debug"
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

// TestResolveBuildID 锁住 BuildID 的取值：显式值优先，其次是二进制里的 vcs.revision（前 12 位，
// 有未提交改动时加 -dirty），两者都没有时返回错误（New 据此 panic）。
func TestResolveBuildID(t *testing.T) {
	info := func(settings ...debug.BuildSetting) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) { return &debug.BuildInfo{Settings: settings}, true }
	}
	revision := debug.BuildSetting{Key: "vcs.revision", Value: "0123456789abcdef0123"}
	for _, test := range []struct {
		name     string
		explicit string
		read     func() (*debug.BuildInfo, bool)
		want     string
	}{
		{name: "explicit wins over the revision", explicit: "orders-v1", read: info(revision), want: "orders-v1"},
		{name: "clean checkout uses the short revision", read: info(revision, debug.BuildSetting{Key: "vcs.modified", Value: "false"}), want: "0123456789ab"},
		{name: "uncommitted changes are marked dirty", read: info(revision, debug.BuildSetting{Key: "vcs.modified", Value: "true"}), want: "0123456789ab-dirty"},
		{name: "short revisions are kept whole", read: info(debug.BuildSetting{Key: "vcs.revision", Value: "r42"}), want: "r42"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveBuildID(test.explicit, test.read)
			if err != nil || got != test.want {
				t.Fatalf("build id = %q, err = %v, want %q", got, err, test.want)
			}
		})
	}
	for name, read := range map[string]func() (*debug.BuildInfo, bool){
		"no revision setting": info(),
		"no build info":       func() (*debug.BuildInfo, bool) { return nil, false },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveBuildID("", read); err == nil || !strings.Contains(err.Error(), "Options.BuildID is empty") {
				t.Fatalf("err = %v, want an error asking for Options.BuildID", err)
			}
		})
	}
}
