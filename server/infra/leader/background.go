package leaderinfra

import (
	"fmt"
	"time"

	leader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/tenuto/server/infra/db"
)

// 后台任务共用 tenuto_leader_lock 表，各用一个 lock_name。
const (
	retentionGCLeaderLockName = "workflow:gc-scanner"
	retentionGCLeaseTTL       = 30 * time.Second
	retentionGCHeartbeat      = 10 * time.Second

	reconcilerLeaderLockName = "workflow:reconciler"
	reconcilerLeaseTTL       = 30 * time.Second
	reconcilerHeartbeat      = 10 * time.Second

	batchDriverLeaderLockName = "workflow:batch-driver"
	batchDriverLeaseTTL       = 15 * time.Second
	batchDriverHeartbeat      = 5 * time.Second
)

// StartRetentionGC 启动 retention GC 扫描器（仅 gorm 模式调用）。
// task 是应用入口构造的回收任务，只有 leader 实例运行。
//
// 用 dbleader 选主而非行锁抢占（SELECT ... FOR UPDATE SKIP LOCKED）：GC 是低频后台清理，
// 不需要多副本并行度，单一 leader 更简单。
func StartRetentionGC(task leader.LeaderTask, db db.DB, nodeAddr string) (*leader.Manager, error) {
	return startLeaderTask(retentionGCLeaderLockName, task, db, nodeAddr, retentionGCLeaseTTL, retentionGCHeartbeat)
}

// StartReconciler 启动周期补偿（仅 gorm 模式调用）：只有 leader 实例运行 task。
func StartReconciler(task leader.LeaderTask, db db.DB, nodeAddr string) (*leader.Manager, error) {
	return startLeaderTask(reconcilerLeaderLockName, task, db, nodeAddr, reconcilerLeaseTTL, reconcilerHeartbeat)
}

// StartBatchDriver 启动批量操作驱动（仅 gorm 模式调用）：只有 leader 实例处理批量操作。
// 租约切换瞬间可能有两个 leader，批量操作记录的 version 乐观锁让落后的一方在保存进度时停下。
func StartBatchDriver(task leader.LeaderTask, db db.DB, nodeAddr string) (*leader.Manager, error) {
	return startLeaderTask(batchDriverLeaderLockName, task, db, nodeAddr, batchDriverLeaseTTL, batchDriverHeartbeat)
}

// startLeaderTask 以 lockName 选主并启动 task，返回已 Start() 的 *leader.Manager。调用方在停止 Server 之前
// 调用它的 Stop()：释放 leader 锁，其他节点不必等锁的 TTL 到期。
// nodeAddr 是本节点唯一的 host:port；空串会让多个实例的 CAS 无法区分彼此。
func startLeaderTask(lockName string, task leader.LeaderTask, db db.DB, nodeAddr string, leaseTTL, heartbeat time.Duration) (*leader.Manager, error) {
	if nodeAddr == "" {
		return nil, fmt.Errorf("%s: node address must be a unique host:port", lockName)
	}
	lockStore := leader.NewMysqlLockStore(db, scheduleLeaderLockTable)
	mgr := leader.NewManager(lockStore, lockName, nodeAddr, []leader.LeaderTask{task},
		leader.WithLeaseTTL(leaseTTL), leader.WithHeartbeat(heartbeat))
	mgr.Start()
	return mgr, nil
}
