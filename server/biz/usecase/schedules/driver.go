package schedules

import (
	"context"
	"time"
)

const tickInterval = 1 * time.Second

// DriverTask 周期调用 UseCase.FireDue。它的方法集满足 dbleader 的 LeaderTask，
// 多副本下交给 leader 管理器，只有 leader 实例运行。
type DriverTask struct {
	useCase *UseCase
}

func NewDriverTask(useCase *UseCase) *DriverTask {
	return &DriverTask{useCase: useCase}
}

func (t *DriverTask) Name() string { return "workflow-schedule-driver" }

func (t *DriverTask) Start(ctx context.Context) error {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			t.useCase.FireDue(ctx)
		}
	}
}

// LocalDriver 在本进程内运行 DriverTask，单实例部署使用。
type LocalDriver struct {
	cancel context.CancelFunc
}

func (d *LocalDriver) Stop() { d.cancel() }

func StartLocalDriver(task *DriverTask) *LocalDriver {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = task.Start(ctx) }()
	return &LocalDriver{cancel: cancel}
}
