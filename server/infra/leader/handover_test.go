package leaderinfra

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

type startedTask struct{ started chan time.Time }

func (t *startedTask) Name() string { return "handover" }

func (t *startedTask) Start(ctx context.Context) error {
	select {
	case t.started <- time.Now():
	default:
	}
	<-ctx.Done()
	return nil
}

// 正常停机（Manager.Stop）释放 tenuto_leader_lock：接任的副本在一个选主轮询间隔内开始运行，不等租约 TTL 到期。
func TestStoppedLeaderReleasesLock(t *testing.T) {
	db := mysqltest.NewDB(t)
	first := &startedTask{started: make(chan time.Time, 1)}
	a, err := StartScheduleDriver(first, db, "replica-a:7233")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.started:
	case <-time.After(10 * time.Second):
		t.Fatal("replica a never became leader")
	}
	a.Stop()
	stopped := time.Now()

	second := &startedTask{started: make(chan time.Time, 1)}
	b, err := StartScheduleDriver(second, db, "replica-b:7233")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	select {
	case at := <-second.started:
		gap := at.Sub(stopped)
		t.Logf("schedule driver handover after graceful stop took %s (lease TTL %s, heartbeat %s)", gap.Round(time.Millisecond), scheduleLeaseTTL, scheduleHeartbeat)
		if gap > 3*time.Second {
			t.Fatalf("leader lock was not released on Stop: successor waited %s for the lease to expire", gap.Round(time.Millisecond))
		}
	case <-time.After(40 * time.Second):
		t.Fatal("replica b never became leader")
	}
}
