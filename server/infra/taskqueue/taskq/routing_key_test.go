package taskq

import (
	"slices"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func TestRoutingKeysAreStableScopedAndLengthSafe(t *testing.T) {
	sticky, err := stickyRoutingKey("worker-a")
	if err != nil {
		t.Fatal(err)
	}
	stickyAgain, err := stickyRoutingKey("worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if sticky != stickyAgain {
		t.Fatalf("sticky routing key changed: %q != %q", sticky, stickyAgain)
	}
	const stableWorkerAKey = "workflow-sticky:v1:amXiN65ExCiVtQSQMUZmFhc0afUTsAJA6duQ32aQakk"
	if sticky != stableWorkerAKey {
		t.Fatalf("sticky routing key = %q, want %q", sticky, stableWorkerAKey)
	}
	if sticky == activityRoutingKey("default", "worker-a") {
		t.Fatalf("sticky and activity routing keys collided: %q", sticky)
	}
	other, err := stickyRoutingKey("worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if sticky == other {
		t.Fatalf("different worker identities share routing key %q", sticky)
	}

	longIdentity := strings.Repeat("工作节点/", 1000)
	longKey, err := stickyRoutingKey(longIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if len(longKey) > 191 {
		t.Fatalf("long identity routing key has %d bytes", len(longKey))
	}
	defaultKey := activityRoutingKey("tenant-a", "payments")
	if defaultKey == "" {
		t.Fatal("namespace default activity routing key is empty")
	}
	if defaultKey == activityRoutingKey("tenant-b", "payments") {
		t.Fatal("activity routing key is shared across namespaces")
	}
	if defaultKey == activityRoutingKey("tenant-a", "emails") {
		t.Fatal("activity routing key is shared across task queues")
	}
	if defaultKey == targetedActivityRoutingKey("tenant-a", "payments", "") {
		t.Fatal("queue and targeted activity routing keys collided")
	}
	if _, err := stickyRoutingKey(""); err == nil {
		t.Fatal("empty sticky worker identity was accepted")
	}
}

// 投递的 routing key 只由队列与 Run 的目标 Worker 决定；Worker 接受队列的通用 key 与发给自己 Identity 的 key，
// 与它的 build 无关，所以旧 build 下线后任务由同队列的新 build 领取。
func TestActivityRoutingKeyTable(t *testing.T) {
	general := activityRoutingKey("tenant-a", "payments")
	targetA := targetedActivityRoutingKey("tenant-a", "payments", "worker-a")
	for _, test := range []struct {
		name string
		task protocol.Task
		want string
	}{
		{
			name: "task without a target uses the queue key",
			task: protocol.Task{Namespace: "tenant-a", TaskQueue: "payments"},
			want: general,
		},
		{
			name: "task of a run with a target worker uses that worker's key",
			task: protocol.Task{Namespace: "tenant-a", TaskQueue: "payments", TargetWorkerIdentity: "worker-a"},
			want: targetA,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := taskActivityRoutingKey(test.task); got != test.want {
				t.Fatalf("routing key = %q, want %q", got, test.want)
			}
		})
	}
	for _, test := range []struct {
		name     string
		identity string
		want     []string
	}{
		{name: "worker without identity accepts the queue key", want: []string{general}},
		{name: "worker accepts the queue key and its own target key", identity: "worker-a", want: []string{general, targetA}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := activityRoutingKeysForWorker("tenant-a", "payments", test.identity)
			if !slices.Equal(got, test.want) {
				t.Fatalf("accepted keys = %q, want %q", got, test.want)
			}
		})
	}
}
