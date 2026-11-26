package engine

import (
	"context"
	"sync"
	"time"
)

const (
	// DefaultReconcileInterval 是周期补偿的默认间隔。
	DefaultReconcileInterval = 30 * time.Second
	// DefaultReconcileFullScanInterval 是全量补偿的默认间隔。
	DefaultReconcileFullScanInterval = 10 * time.Minute
)

// Reconciler 是周期补偿循环：每隔 Interval 做一次 Reconcile（lookahead 为两个间隔，补投任务、把 lookahead 内到期的
// 定时器挂到本进程，并接管持久化状态已过期超过 DefaultOverdueTimerGrace 的定时器），每隔 FullScanInterval 做一次
// 全量补偿（DispatchPendingTasks）。两者各用一个 goroutine：全量补偿运行期间周期补偿照常执行。Server 应用（MySQL
// 后端经 leader 选主）与内嵌的 assembly.Engine 共用它。
type Reconciler struct {
	Server           *Server
	Interval         time.Duration
	FullScanInterval time.Duration
}

func (Reconciler) Name() string { return "workflow-reconciler" }

// Lookahead 是周期补偿挂定时器的窗口，也是启动恢复的窗口：不短于两次周期补偿的间隔，定时器在到期前至少被一轮挂上。
func (r Reconciler) Lookahead() time.Duration { return 2 * r.Interval }

// Start 运行两个循环，ctx 取消且两者都返回后返回。
func (r Reconciler) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Go(func() { runEvery(ctx, r.Interval, r.reconcile) })
	wg.Go(func() { runEvery(ctx, r.FullScanInterval, r.reconcileAll) })
	wg.Wait()
	return nil
}

// runEvery 每隔 interval 调用一次 fn，直到 ctx 取消；fn 运行超过 interval 时跳过错过的节拍。
func runEvery(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}

// reconcile 处理有到期或待处理事项的 Running Run。单个 Run 的失败不影响其余 Run。
func (r Reconciler) reconcile(ctx context.Context) {
	logger := r.Server.logger
	result, overdue, err := r.Server.Reconcile(ctx, DefaultOverdueTimerGrace, r.Lookahead())
	if err != nil {
		logger.Error("workflow reconciliation incomplete", "err", err)
	}
	if result.WorkflowTasks > 0 || result.ActivityTasks > 0 {
		logger.Info("workflow dispatch reconciliation completed", "workflow_tasks", result.WorkflowTasks, "activity_tasks", result.ActivityTasks)
	}
	if overdue != (OverdueTimerResult{}) {
		logger.Warn("workflow overdue timers taken over",
			"run_timeouts", overdue.RunTimeouts,
			"durable_timers", overdue.DurableTimers,
			"activity_timeouts", overdue.ActivityTimeouts,
			"workflow_task_timeouts", overdue.WorkflowTaskTimeouts,
			"workflow_tasks", overdue.WorkflowTasks,
			"activity_tasks", overdue.ActivityTasks)
	}
}

// reconcileAll 逐页批量检查全部 Running Run，兜底没有到期索引的崩溃窗口（子 Run 补建、
// 终态写回、父关闭策略、子 Run 取消请求补发），再删除到期超过一天的 Activity Worker 路由。
func (r Reconciler) reconcileAll(ctx context.Context) {
	started := r.Server.clock.Now()
	result, err := r.Server.DispatchPendingTasks(ctx)
	if err != nil {
		r.Server.logger.Error("workflow full reconciliation incomplete", "err", err)
	}
	if purged, err := r.Server.PurgeExpiredActivityWorkerVersions(); err != nil {
		r.Server.logger.Error("purge expired activity worker versions failed", "err", err)
	} else if purged > 0 {
		r.Server.logger.Info("expired activity worker versions purged", "rows", purged)
	}
	r.Server.logger.Info("workflow full reconciliation finished",
		"duration", r.Server.clock.Now().Sub(started),
		"workflow_tasks", result.WorkflowTasks,
		"activity_tasks", result.ActivityTasks)
}
