package batches

import (
	"context"
	"time"
)

const tickInterval = 1 * time.Second

// DriverTask 周期调用 UseCase.ProcessActive。它的方法集满足 dbleader 的 LeaderTask，
// 多副本下交给 leader 管理器，只有 leader 实例运行。
type DriverTask struct {
	useCase *UseCase
}

func NewDriverTask(useCase *UseCase) *DriverTask {
	return &DriverTask{useCase: useCase}
}

func (t *DriverTask) Name() string { return "workflow-batch-driver" }

// Start 每个 tick 处理一轮；ctx 取消时正在处理的页不保存，返回后由下一任驱动从游标继续。
func (t *DriverTask) Start(ctx context.Context) error {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			t.useCase.ProcessActive(ctx)
		}
	}
}

// LocalDriver 在本进程内运行 DriverTask，单实例部署使用。Stop 等驱动返回。
type LocalDriver struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (d *LocalDriver) Stop() {
	d.cancel()
	<-d.done
}

func StartLocalDriver(task *DriverTask) *LocalDriver {
	ctx, cancel := context.WithCancel(context.Background())
	driver := &LocalDriver{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(driver.done)
		_ = task.Start(ctx)
	}()
	return driver
}
