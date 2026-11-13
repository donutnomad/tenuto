// Package retention 编排保留期回收：到期整树删除与孤儿兜底扫描。
package retention

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

const (
	// DefaultScanInterval 是扫描器两轮之间的间隔。
	DefaultScanInterval = 30 * time.Second
	// DefaultBatch 是每次读取到期任务的条数。一轮扫描一批批读取，直到没有到期任务或用满 ScanInterval。
	DefaultBatch = 100
	// MaxAttempts 是失败退避的告警阈值。超过它仍不放弃（不静默丢数据），
	// 但推得很远，避免坏任务持续占用扫描窗口。
	MaxAttempts = 10
	// parkDuration 是超过阈值后的推后时长。
	parkDuration = 24 * time.Hour
	// notTerminalDefer 是「整树尚有 run 未终态」时的推后时长。这不算失败，
	// 不计入 attempts —— run 还活着是完全正常的状态。
	notTerminalDefer = 10 * time.Minute
	// DefaultScavengeInterval 是兜底扫描两轮之间的间隔。
	//
	// 比主扫描（30s）低三个数量级：兜底扫描处理的是**结构上够不着**的残留，不是常规
	// 回收路径。它的查询要对 tenuto_events 做 GROUP BY / NOT EXISTS，比主路径的
	// 「按 fire_at 索引取 N 条」重得多，跑太勤会白白压 DB。孤儿是罕见的中断产物，
	// 每天清一次绰绰有余。对应 Temporal history scavenger 的低频定位。
	DefaultScavengeInterval = 24 * time.Hour
	// DefaultScavengeMinAge 是兜底扫描的年龄下界：只有最后活动时间早于
	// now - minAge 的数据才会被回收。
	//
	// 这是**防误删的核心防线**：判定孤儿靠的是「关联行查不到」，而在途写入天然存在
	// 短暂的不一致窗口（事件已落、run 行还没提交可见之类）。7 天远大于任何写入窗口，
	// 落进窗口的新数据一律不碰。对应 Temporal 的 HistoryScannerDataMinAge（默认 60 天）。
	DefaultScavengeMinAge = 7 * 24 * time.Hour
	// DefaultScavengeBatch 是单轮单类孤儿的处理上限，防止一轮扫描长时间占住连接。
	DefaultScavengeBatch = 200
)

// Config 配置扫描器。
type Config struct {
	// ScanInterval 是两轮扫描的间隔，也是一轮扫描的时长上限。
	ScanInterval time.Duration
	// Batch 是每次读取到期任务的条数。
	Batch int
	// ScavengeInterval 是兜底扫描的间隔（默认每天一次）。
	ScavengeInterval time.Duration
	// ScavengeMinAge 是兜底扫描的年龄下界：只回收比它更老的数据。这是防误删的核心
	// 防线，不是性能参数。
	ScavengeMinAge time.Duration
	// ScavengeBatch 是单轮单类孤儿的处理上限。
	ScavengeBatch int
}

func (c Config) withDefaults() Config {
	if c.ScanInterval <= 0 {
		c.ScanInterval = DefaultScanInterval
	}
	if c.Batch <= 0 {
		c.Batch = DefaultBatch
	}
	if c.ScavengeInterval <= 0 {
		c.ScavengeInterval = DefaultScavengeInterval
	}
	if c.ScavengeMinAge <= 0 {
		c.ScavengeMinAge = DefaultScavengeMinAge
	}
	if c.ScavengeBatch <= 0 {
		c.ScavengeBatch = DefaultScavengeBatch
	}
	return c
}

// Task 的方法集满足 dbleader 的 LeaderTask，只有抢到 leader 的实例才跑扫描循环。
//
// ⚠️ 选主不能替代幂等：dbleader 是租约制，切换瞬间可能双 leader。互斥最终仍靠幂等
// 兜底（DELETE 天然幂等，DeleteBranch 已幂等化），选主只是把重复工作降到罕见。
type Task struct {
	store   execution.RetentionGCStore
	locker  WorkflowLocker
	metrics Metrics
	clock   clock.Clock
	cfg     Config
	logger  *slog.Logger
}

// WorkflowLocker 是回收期间与本副本上用户操作互斥用到的执行域能力。消费方是本包的 Task。
type WorkflowLocker interface {
	// LockWorkflow 取 (namespace, workflowID) 的进程内互斥锁，与本副本上该 workflow 的写操作共用同一把。
	// ctx 取消时返回错误；成功时返回释放函数。
	LockWorkflow(ctx context.Context, namespace, workflowID string) (release func(), err error)
}

// Metrics 记录回收指标。消费方是本包的 Task。
type Metrics interface {
	// RecordGCReclaimed 记录回收掉的对象数，object 是对象类型维度。
	RecordGCReclaimed(ctx context.Context, object string, count int)
	// RecordGCFailure 记录一次回收失败，outcome 取 deferred 或 parked。
	RecordGCFailure(ctx context.Context, outcome string)
	// RecordGCBacklog 记录一轮扫描之后到期未处理的任务数与其中最早任务的等待时长。
	RecordGCBacklog(ctx context.Context, overdue int, oldestAge time.Duration)
}

func NewTask(store execution.RetentionGCStore, locker WorkflowLocker, metrics Metrics, clk clock.Clock, cfg Config, logger *slog.Logger) *Task {
	return &Task{store: store, locker: locker, metrics: metrics, clock: clk, cfg: cfg.withDefaults(), logger: logger}
}

func (t *Task) Name() string { return "workflow-retention-gc" }

// Start 在**同一个 leader 任务里**跑两个节奏不同的循环：主扫描（默认 30s）与兜底扫描
// （默认 24h）。
//
// **为什么共用一个 dbleader 而不是各起一个**：
//  1. 两者删的是同一批表。分开选主意味着两个实例可能同时删同一份数据，把「罕见的重复
//     工作」变成常态 —— 而 GC 的互斥本就靠幂等兜底，没必要再增加一个竞争源。
//  2. dbleader 的每个 lock_name 都要独立的租约续期与 CAS 写，多一个就是多一份心跳
//     负载和一处可独立失效的组件，换不来任何并行度（兜底扫描每天才跑一次）。
//  3. 兜底扫描跑在同一个 goroutine 树下，继承同一套生命周期与取消语义。
//
// 两个循环用各自的 ticker 但共享 ctx：leader 一旦失去租约，ctx 取消，两者一起停。
func (t *Task) Start(ctx context.Context) error {
	ticker := time.NewTicker(t.cfg.ScanInterval)
	defer ticker.Stop()
	scavengeTicker := time.NewTicker(t.cfg.ScavengeInterval)
	defer scavengeTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			t.RunOnce(ctx)
		case <-scavengeTicker.C:
			t.ScavengeOnce(ctx)
		}
	}
}

// ScavengeOnce 执行一轮兜底扫描。导出供测试直接驱动，无需等 24h 的真实 ticker。
//
// 失败只记日志不重试：兜底扫描是尽力而为的清理，下一轮（默认 24h 后）自然会再来。
// 它没有任何「必须成功否则数据泄漏」的语义 —— 那是主路径 deletion task 的职责。
func (t *Task) ScavengeOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	result, err := t.store.ScavengeOrphans(t.clock.Now(), t.cfg.ScavengeMinAge, t.cfg.ScavengeBatch)
	if err != nil {
		t.logger.Error("retention gc: orphan scavenge failed", "err", err)
		return
	}
	if result.Empty() {
		return
	}
	t.metrics.RecordGCReclaimed(ctx, "orphan_event_run", result.OrphanEventRuns)
	t.metrics.RecordGCReclaimed(ctx, "orphan_tree_row", result.OrphanTreeRows)
	t.logger.Info("retention gc: orphan scavenge reclaimed data",
		"orphan_event_runs", result.OrphanEventRuns,
		"orphan_tree_rows", result.OrphanTreeRows)
}

// RunOnce 执行一轮扫描：按 fire_at 升序一批批取到期任务并处理，直到没有到期任务、本轮用满 ScanInterval
// 或 ctx 结束，最后记录积压。导出供测试直接驱动，无需等真实 ticker。
//
// 一轮不止处理一批：每轮只处理 Batch 个时，回收速度被钉在 Batch / ScanInterval（缺省每分钟 200 棵），
// 创建更快时到期任务无界堆积。每批之间检查 ctx，leader 失去租约时立即停下。
//
// 每棵 tree 在一轮里至多处理一次：删除或推后之后它不再到期；推后写入失败时它仍排在最前，
// 再取到的批次里没有新任务就结束本轮，不在同一批坏任务上空转。
func (t *Task) RunOnce(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	started := t.clock.Now()
	deadline := started.Add(t.cfg.ScanInterval)
	seen := make(map[string]bool)
	deleted := 0
	for ctx.Err() == nil && t.clock.Now().Before(deadline) {
		tasks, err := t.store.ListDueDeletionTasks(t.clock.Now(), t.cfg.Batch)
		if err != nil {
			t.logger.Error("retention gc: list due deletion tasks failed", "err", err)
			return
		}
		fresh := 0
		for _, task := range tasks {
			if ctx.Err() != nil {
				break
			}
			if seen[task.TreeID] {
				continue
			}
			seen[task.TreeID] = true
			fresh++
			if t.processTask(ctx, task) {
				deleted++
			}
		}
		if len(tasks) < t.cfg.Batch || fresh == 0 {
			break
		}
	}
	if deleted > 0 {
		t.logger.Info("retention gc: history trees deleted", "trees", deleted, "elapsed", t.clock.Now().Sub(started))
	}
	t.recordBacklog(ctx)
}

// recordBacklog 在一轮扫描之后记录到期未处理的任务数与最早任务的等待时长。最早的任务已等待超过一个
// ScanInterval 时记 WARN：回收速度低于到期速度（或删除持续失败），积压在增长。
func (t *Task) recordBacklog(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	now := t.clock.Now()
	backlog, err := t.store.DeletionBacklog(now)
	if err != nil {
		t.logger.Error("retention gc: read deletion backlog failed", "err", err)
		return
	}
	var oldestAge time.Duration
	if backlog.Overdue > 0 {
		oldestAge = max(now.Sub(backlog.OldestFireAt), 0)
	}
	t.metrics.RecordGCBacklog(ctx, backlog.Overdue, oldestAge)
	if oldestAge > t.cfg.ScanInterval {
		t.logger.Warn("retention gc: deletion backlog is growing; overdue trees are waiting longer than one scan interval",
			"overdue_tasks", backlog.Overdue, "oldest_overdue_age", oldestAge, "scan_interval", t.cfg.ScanInterval, "batch", t.cfg.Batch)
	}
}

// processTask 回收单棵 tree。
//
// 数据的一致性由存储的 DeleteTree 事务保证：一棵 tree 的行在同一个事务里删除，任何副本都读不到删了一半的状态。
// workflow 锁是进程内的：它让本副本上该 workflow 的写操作在删除期间等待，不覆盖其他副本上的操作。
//
// 一棵 tree 可能横跨多个 workflowID（child workflow / CAN），故按 key 逐一取锁。
// 取锁顺序由 TreeWorkflowKeys 的稳定排序保证，避免交叉持锁死锁。
//
// 返回值报告 tree 是否已删除。
func (t *Task) processTask(ctx context.Context, task execution.DeletionTask) bool {
	keys, err := t.store.TreeWorkflowKeys(task.TreeID)
	if err != nil {
		t.deferTask(ctx, task, err)
		return false
	}

	releases := make([]func(), 0, len(keys))
	defer func() {
		// 逆序释放，与获取顺序对称。
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for _, key := range keys {
		release, err := t.locker.LockWorkflow(ctx, key.Namespace, key.WorkflowID)
		if err != nil {
			// ctx 取消：本轮放弃，任务留在表里，下轮再来。
			return false
		}
		releases = append(releases, release)
	}

	if err := t.store.DeleteTree(task.TreeID); err != nil {
		if errors.Is(err, execution.ErrTreeNotTerminal) {
			// 树上还有活着的 run（含 reset 复活）。不是失败，不计 attempts，推后重来。
			if deferErr := t.store.DeferDeletionTask(task.TreeID, t.clock.Now().Add(notTerminalDefer), false, nil); deferErr != nil {
				t.logger.Error("retention gc: defer non-terminal tree failed", "tree_id", task.TreeID, "err", deferErr)
			}
			return false
		}
		t.deferTask(ctx, task, err)
		return false
	}
	t.metrics.RecordGCReclaimed(ctx, "tree", 1)
	t.logger.Debug("retention gc: history tree deleted", "tree_id", task.TreeID, "namespace", task.Namespace)
	return true
}

// deferTask 把失败任务推后。超过阈值时告警并推得很远，但**不静默删除**任务 ——
// 静默删除会把「删不掉的数据」变成「没人知道删不掉的数据」。
func (t *Task) deferTask(ctx context.Context, task execution.DeletionTask, cause error) {
	attempts := task.Attempts + 1
	delay := backoff(attempts)
	if attempts >= MaxAttempts {
		delay = parkDuration
		t.metrics.RecordGCFailure(ctx, "parked")
		t.logger.Error("retention gc: tree deletion keeps failing, parking task",
			"tree_id", task.TreeID, "attempts", attempts, "park_for", delay, "err", cause)
	} else {
		t.metrics.RecordGCFailure(ctx, "deferred")
		t.logger.Warn("retention gc: tree deletion failed, retrying later",
			"tree_id", task.TreeID, "attempts", attempts, "retry_in", delay, "err", cause)
	}
	if err := t.store.DeferDeletionTask(task.TreeID, t.clock.Now().Add(delay), true, cause); err != nil {
		t.logger.Error("retention gc: defer failed task failed", "tree_id", task.TreeID, "err", err)
	}
}

// backoff 是指数退避，上界 1h。
func backoff(attempts int) time.Duration {
	delay := time.Minute
	for range attempts - 1 {
		delay *= 2
		if delay >= time.Hour {
			return time.Hour
		}
	}
	return delay
}

// ErrUnsupported 在给不支持 GC 的存储后端启用 retention 时返回。
var ErrUnsupported = errors.New("retention GC is not supported by this storage backend")
