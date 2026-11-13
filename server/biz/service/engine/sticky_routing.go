package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

const stickyFallbackScanInterval = time.Second

// stickyFallbackScanLimit 给单轮定向扫描封顶，避免大批量 sticky 同时过期时单轮无界。
// 未处理完的剩余部分由下一轮（1 秒节流）继续。
const stickyFallbackScanLimit = 256

func (s *Server) FallbackExpiredStickyWorkflowTasks(ctx context.Context) error {
	s.mu.Lock()
	s.stickyFallbackLast = time.Time{}
	s.mu.Unlock()
	return s.fallbackExpiredStickyAssignments(ctx, s.clock.Now())
}

// dequeueWorkflowTask 长轮询一个 Workflow Task，每次开始轮询前按节拍做一次 sticky 期限扫描。
//
// 队列自己在 routing deadline 把定向投递交给普通 poller 时（TaskQ，HandlesStickyRoutingDeadline），期限扫描只清除
// 过期的 sticky 会话，投递不依赖它：长轮询按调用方与队列的期限进行，不再每秒以新期限重新开始。此前每个 poller
// 每秒重新 poll 一次，每次都向每个 owner 发一次快速扫描与一次长轮询。期限扫描仍随轮询开始触发，副本上任何一个
// poller 开始轮询都会让到期的扫描运行。其它队列（内存队列）的过期投递由扫描重新派发：每秒重新开始轮询。
func (s *Server) dequeueWorkflowTask(ctx context.Context, workerIdentity string) (protocol.TaskLease, error) {
	for {
		if err := s.fallbackExpiredStickyAssignments(ctx, s.clock.Now()); err != nil {
			return protocol.TaskLease{}, err
		}
		if s.wfQueue.HandlesStickyRoutingDeadline() {
			lease, err := s.wfQueue.DequeueForWorker(ctx, workerIdentity)
			if err != nil && ctx.Err() != nil {
				return protocol.TaskLease{}, ctx.Err()
			}
			return lease, err
		}
		pollCtx, cancel := context.WithTimeout(ctx, stickyFallbackScanInterval)
		lease, err := s.wfQueue.DequeueForWorker(pollCtx, workerIdentity)
		cancel()
		if err == nil {
			return lease, nil
		}
		if ctx.Err() != nil {
			return protocol.TaskLease{}, ctx.Err()
		}
		if pollCtx.Err() == context.DeadlineExceeded {
			continue
		}
		return protocol.TaskLease{}, err
	}
}

func (s *Server) fallbackExpiredStickyAssignments(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	if now.Sub(s.stickyFallbackLast) < stickyFallbackScanInterval {
		s.mu.Unlock()
		return nil
	}
	s.stickyFallbackLast = now
	s.mu.Unlock()

	candidates, err := s.mgr.ListExpiredStickyExecutions(now, stickyFallbackScanLimit)
	if err != nil {
		return err
	}
	for _, state := range candidates {
		runID := state.RunID
		// 已被 Worker 领取的 Workflow Task 留给它完成：重投会签发新的 task token，激活 ID 随之改变，
		// 新任务过不了激活 fence，原 Worker 的提交又被清除 sticky 后的代数 fence 拒绝。
		if state.WorkflowTask.StartedScheduledEventID == state.OpenWorkflowTaskEventID {
			continue
		}
		handledByQueue := s.wfQueue.HandlesStickyRoutingDeadline()
		// trace context 只是 sideband 元数据，取不到就带空的继续。
		var traceContext map[string]string
		if scheduled, ok, err := s.mgr.GetWorkflowTaskScheduledEvent(runID, state.OpenWorkflowTaskEventID); !handledByQueue && err == nil && ok {
			traceContext = cloneStringMap(scheduled.WorkflowTaskScheduled.TraceContext)
		}
		task := protocol.Task{
			// 手上只有 execution state（没有 *Run），用 runNamespace 反查；只对真正过期的 run 发生。
			Namespace:        s.runNamespace(runID),
			Type:             protocol.TaskTypeWorkflow,
			RunID:            runID,
			ScheduledEventID: state.OpenWorkflowTaskEventID,
			TraceContext:     traceContext,
		}
		// 先释放派发记录，成功之后才清除 sticky：旧记录还在时重投会被去重门控挡掉。释放失败时
		// sticky 保持原状，这个 Run 留在下一轮扫描的候选里重试。
		if !handledByQueue {
			if err := s.dispatches.ReleaseTaskDispatch(task, "sticky schedule-to-start timeout"); err != nil {
				s.logger.Error("release sticky task dispatch failed; fallback retries on the next scan",
					slog.String("namespace", task.Namespace),
					slog.String("run_id", task.RunID),
					slog.Int64("scheduled_event_id", task.ScheduledEventID),
					slog.Any("err", err),
				)
				return err
			}
		}
		_, cleared, err := s.mgr.ClearStickyExecution(runID, state.ExecutionGeneration, state.StickyGeneration)
		if err != nil {
			return err
		}
		// 没清掉说明 sticky 会话已被别处更替；已释放的派发记录由 reconciler 按 open WFT 补投。
		if !cleared || handledByQueue {
			continue
		}
		if _, err := s.dispatchWorkflowTask(context.WithoutCancel(ctx), task); err != nil {
			return err
		}
	}
	return nil
}
