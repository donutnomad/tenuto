package dispatch_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestDeliverRecordsQueueResultAndDeduplicates(t *testing.T) {
	store := memory.NewDispatchRepository()
	task := protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-1", ScheduledEventID: 7}
	var enqueued protocol.Task
	enqueueCalls := 0
	encode := func(task protocol.Task) (protocol.Task, error) {
		task.TaskToken = "signed"
		return task, nil
	}
	enqueue := func(_ context.Context, task protocol.Task) error {
		enqueueCalls++
		enqueued = task
		return nil
	}

	delivered, err := dispatch.Deliver(context.Background(), store, task, time.Minute, encode, enqueue)
	if err != nil || !delivered {
		t.Fatalf("first delivery: delivered=%t err=%v", delivered, err)
	}
	if enqueued.TaskToken != "signed" || enqueued.Attempt != 1 {
		t.Fatalf("queue task was not prepared: %+v", enqueued)
	}
	state, ok, err := store.GetTaskDispatch(task)
	if err != nil || !ok || state.Status != dispatch.StatusDispatched {
		t.Fatalf("dispatch state: state=%+v found=%t err=%v", state, ok, err)
	}
	delivered, err = dispatch.Deliver(context.Background(), store, task, time.Minute, encode, enqueue)
	if err != nil || delivered || enqueueCalls != 1 {
		t.Fatalf("duplicate delivery: delivered=%t calls=%d err=%v", delivered, enqueueCalls, err)
	}
}

func TestDeliverMarksFailuresForRecovery(t *testing.T) {
	for _, failure := range []string{"encode", "enqueue"} {
		t.Run(failure, func(t *testing.T) {
			store := memory.NewDispatchRepository()
			task := protocol.Task{Type: protocol.TaskTypeWorkflow, RunID: "run-1", ScheduledEventID: 7}
			wantErr := errors.New(failure + " failed")
			encode := func(task protocol.Task) (protocol.Task, error) {
				if failure == "encode" {
					return protocol.Task{}, wantErr
				}
				return task, nil
			}
			enqueue := func(context.Context, protocol.Task) error { return wantErr }
			delivered, err := dispatch.Deliver(context.Background(), store, task, time.Minute, encode, enqueue)
			if delivered || !errors.Is(err, wantErr) {
				t.Fatalf("delivery: delivered=%t err=%v", delivered, err)
			}
			state, ok, err := store.GetTaskDispatch(task)
			if err != nil || !ok || state.Status != dispatch.StatusFailed || state.LastError != wantErr.Error() {
				t.Fatalf("dispatch state: state=%+v found=%t err=%v", state, ok, err)
			}
		})
	}
}

// markFailingRepository 让「标记已派发」失败，其余操作交给内存仓储。
type markFailingRepository struct {
	*memory.DispatchRepository
}

func (markFailingRepository) MarkTaskDispatchDispatched(protocol.Task, string) error {
	return errors.New("database unavailable")
}

func TestDeliverReportsSuccessWhenOnlyMarkDispatchedFails(t *testing.T) {
	// 任务已入队后标记失败：投递按成功返回，调用方不把已入队的任务当作失败重投。
	store := markFailingRepository{memory.NewDispatchRepository()}
	task := protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-1", ScheduledEventID: 7}
	enqueueCalls := 0
	delivered, err := dispatch.Deliver(context.Background(), store, task, time.Minute,
		func(task protocol.Task) (protocol.Task, error) { return task, nil },
		func(context.Context, protocol.Task) error { enqueueCalls++; return nil })
	if err != nil || !delivered || enqueueCalls != 1 {
		t.Fatalf("delivery: delivered=%t calls=%d err=%v", delivered, enqueueCalls, err)
	}
}

func TestServiceStampsEnqueueTimeFromClock(t *testing.T) {
	// Happy Path: 入队时间取自注入的时钟，schedule-to-start 指标按它计算。
	clk := clocktest.NewFake(time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC))
	activities := memory.NewActivityQueueRouter(1)
	service := dispatch.NewService(memory.NewDispatchRepository(), memory.NewTaskQueue(1), activities,
		func(task protocol.Task) (protocol.Task, error) { return task, nil },
		func(string, time.Time) (dispatch.StickyAssignment, bool) { return dispatch.StickyAssignment{}, false },
		time.Minute, clk)
	delivered, err := service.DispatchActivity(context.Background(), protocol.Task{RunID: "run-1", ScheduledEventID: 7, TaskQueue: "default"})
	if err != nil || !delivered {
		t.Fatalf("dispatch: delivered=%t err=%v", delivered, err)
	}
	lease, err := activities.Dequeue(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if !lease.Task.EnqueuedAt.Equal(clk.Now()) {
		t.Fatalf("EnqueuedAt = %v, want %v", lease.Task.EnqueuedAt, clk.Now())
	}
}

// 已投递却没有被领取的记录：等待时长从 base 起按派发次数翻倍，最长 MaxRedeliveryWait；其他状态不由它重投。
func TestRedeliveryDueBacksOffByDispatchCount(t *testing.T) {
	base := 30 * time.Second
	sent := time.Date(2026, 11, 12, 6, 19, 17, 0, time.UTC)
	cases := []struct {
		name     string
		state    dispatch.State
		elapsed  time.Duration
		expected bool
	}{
		{"first delivery within base", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 1, LastDispatchedAt: sent}, base, false},
		{"first delivery after base", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 1, LastDispatchedAt: sent}, base + time.Millisecond, true},
		{"third delivery waits 4x", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 3, LastDispatchedAt: sent}, 4 * base, false},
		{"third delivery after 4x", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 3, LastDispatchedAt: sent}, 4*base + time.Millisecond, true},
		{"seventh delivery still doubles past 16x", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 7, LastDispatchedAt: sent}, 64 * base, false},
		{"sixth delivery after 32x", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 6, LastDispatchedAt: sent}, 32*base + time.Millisecond, true},
		{"wait is capped at an hour", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 40, LastDispatchedAt: sent}, dispatch.MaxRedeliveryWait, false},
		{"capped wait elapses", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 1 << 30, LastDispatchedAt: sent}, dispatch.MaxRedeliveryWait + time.Millisecond, true},
		{"leased record is not redelivered here", dispatch.State{Status: dispatch.StatusLeased, Attempts: 1, LastDispatchedAt: sent}, time.Hour, false},
		{"failed record is not redelivered here", dispatch.State{Status: dispatch.StatusFailed, Attempts: 1, LastDispatchedAt: sent}, time.Hour, false},
		{"dispatched without a delivery time", dispatch.State{Status: dispatch.StatusDispatched, Attempts: 1}, time.Hour, false},
	}
	for _, tc := range cases {
		if got := tc.state.RedeliveryDue(sent.Add(tc.elapsed), base); got != tc.expected {
			t.Errorf("%s: RedeliveryDue = %v, want %v", tc.name, got, tc.expected)
		}
	}
}
