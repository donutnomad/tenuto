package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	taskqfrontend "github.com/donutnomad/taskq/frontend"
)

// 真实 TaskQ + MySQL（TENUTO_TEST_MYSQL=1）：poll 的期限反复到期时，没有投递隐身到可见性超时。
//
// Server 的 Workflow Task 长轮询每秒以新的期限重新 poll 一次（sticky 期限扫描的节拍）。owner 在期限到期前
// 签发、应答还在途中的租约，曾被到期的调用方丢掉，任务在 TaskQ 里是已投递，60s 可见性超时后才再出现；无故障
// 压测里 8 个任务由 reconciler 的兜底约 56 秒后补投。这里把期限缩短到毫秒级放大这个窗口。
//
// 两种结果分开判定。机器满载时 5–26ms 的期限可能短于一次 RPC 往返，15s 内领不完的任务若在库里仍可见，是期限
// 本身做不到（饥饿），不是正确性问题：停下短期限消费者，换 1s 期限的消费者排空 5s，可见的任务必须全部领到。
// 丢失的租约（已投递、无人持有）在 60s 可见性超时之前一直隐藏，排空之后仍缺任务即失败，失败信息列出每个任务
// 在库里的状态与 attempt。
func TestTaskQShortDeadlinePollsLoseNoDelivery(t *testing.T) {
	const (
		queueName = "race-tasks"
		tasks     = 300
	)
	h := newTaskQHarness(t, map[string]int{queueName: 2})
	t.Cleanup(h.close)
	producer := h.frontend(t, queueName)

	received := make(chan string, tasks*2)
	startConsumers := func(count int, deadline func(consumer int) time.Duration) (stop func()) {
		ctx, cancel := context.WithCancel(context.Background())
		var consumers sync.WaitGroup
		for consumer := range count {
			fe := h.frontend(t, queueName)
			consumers.Go(func() {
				for ctx.Err() == nil {
					pollCtx, pollCancel := context.WithTimeout(ctx, deadline(consumer))
					lease, err := fe.DequeueWithOptions(pollCtx, taskqfrontend.PollOptions{})
					pollCancel()
					if err != nil {
						continue
					}
					if err := fe.Ack(context.Background(), lease.Token); err != nil && !errors.Is(err, context.Canceled) {
						t.Errorf("ack: %v", err)
					}
					received <- string(lease.Data)
				}
			})
		}
		return func() {
			cancel()
			consumers.Wait()
		}
	}

	stopShort := startConsumers(8, func(consumer int) time.Duration { return time.Duration(5+consumer*3) * time.Millisecond })
	for i := range tasks {
		if err := producer.Enqueue(context.Background(), fmt.Appendf(nil, "task-%d", i)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(i%3) * time.Millisecond)
	}

	seen := make(map[string]bool, tasks)
	collectTaskQDeliveries(received, seen, tasks, 15*time.Second)
	stopShort()
	collectTaskQDeliveries(received, seen, tasks, 0)
	if len(seen) == tasks {
		return
	}
	t.Logf("short-deadline consumers received %d of %d tasks within 15s; %s", len(seen), tasks, describeUndeliveredTaskQTasks(t, h, queueName, seen))

	stopDrain := startConsumers(2, func(int) time.Duration { return time.Second })
	collectTaskQDeliveries(received, seen, tasks, 5*time.Second)
	stopDrain()
	collectTaskQDeliveries(received, seen, tasks, 0)
	if len(seen) < tasks {
		t.Fatalf("received %d of %d tasks after draining with 1s-deadline consumers; %s", len(seen), tasks, describeUndeliveredTaskQTasks(t, h, queueName, seen))
	}
}

// collectTaskQDeliveries 把 received 里的任务记入 seen，直到收齐 tasks 个或等满 wait；wait 为 0 时只取走已到达的。
func collectTaskQDeliveries(received <-chan string, seen map[string]bool, tasks int, wait time.Duration) {
	timeout := time.After(wait)
	for len(seen) < tasks {
		select {
		case data := <-received:
			seen[data] = true
			continue
		default:
		}
		if wait == 0 {
			return
		}
		select {
		case data := <-received:
			seen[data] = true
		case <-timeout:
			return
		}
	}
}

// describeUndeliveredTaskQTasks 从 taskq_tasks 列出没收到的任务：dispatched 是已投递但无人持有（租约丢失，可见性超时
// 之前一直隐藏），visible 是可见但没被领到（饥饿）。调用方已停下所有消费者；被放弃的租约在后台归还，最多等 2s 让
// 已投递的行回到可见。
func describeUndeliveredTaskQTasks(t *testing.T, h *taskQHarness, queueName string, seen map[string]bool) string {
	t.Helper()
	type taskRow struct {
		TaskID       int64
		Data         []byte
		Attempt      int64
		DispatchedAt *time.Time
	}
	var dispatched, visible []string
	for settle := time.Now().Add(2 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		var rows []taskRow
		if err := h.store.DB().Raw("SELECT task_id, data, attempt, dispatched_at FROM taskq_tasks WHERE queue_name = ? ORDER BY task_id", queueName).Scan(&rows).Error; err != nil {
			t.Fatal(err)
		}
		dispatched, visible = nil, nil
		for _, row := range rows {
			if seen[string(row.Data)] {
				continue
			}
			entry := fmt.Sprintf("%s(task_id=%d attempt=%d)", row.Data, row.TaskID, row.Attempt)
			if row.DispatchedAt != nil {
				dispatched = append(dispatched, entry)
			} else {
				visible = append(visible, entry)
			}
		}
		if len(dispatched) == 0 || time.Now().After(settle) {
			break
		}
	}
	return fmt.Sprintf("dispatched but held by no one (lost lease): %v; visible but never claimed (starvation): %v", dispatched, visible)
}
