package leaderinfra

import (
	"time"

	leader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/tenuto/server/infra/db"
)

const (
	scheduleLeaderLockTable = "tenuto_leader_lock"
	scheduleLeaderLockName  = "workflow:schedule-driver"
	scheduleLeaseTTL        = 15 * time.Second
	scheduleHeartbeat       = 5 * time.Second
)

// StartScheduleDriver 启动 cron 调度驱动（仅 gorm 模式调用）。
// 通过 dbleader 选举，只有 leader 实例运行 task 的 tick 循环，多实例下不会重复触发。
func StartScheduleDriver(task leader.LeaderTask, db db.DB, nodeAddr string) (*leader.Manager, error) {
	return startLeaderTask(scheduleLeaderLockName, task, db, nodeAddr, scheduleLeaseTTL, scheduleHeartbeat)
}
