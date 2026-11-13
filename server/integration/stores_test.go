package integration

import (
	"os"
	"testing"

	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/persistence"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

// TestMain 负责在全部测试结束后销毁 mysqltest 的共享容器（只在 TENUTO_TEST_MYSQL=1 且没有
// TENUTO_TEST_MYSQL_DSN 时启动）。
func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

// newMySQLWorkflowStore 返回独立 MySQL 库上的 GORM Store；没有开启 MySQL 后端测试时跳过 t。
func newMySQLWorkflowStore(t *testing.T) *persistence.Store {
	t.Helper()
	return openWorkflowStore(t, mysqltest.NewDB(t), persistence.Config{})
}

func openWorkflowStore(t *testing.T, db *gorm.DB, cfg persistence.Config) *persistence.Store {
	t.Helper()
	store, err := persistence.New(db, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// newWorkflowStores 返回一套 Workflow 仓储：默认是内存实现，TENUTO_TEST_MYSQL=1 时是独立 MySQL 库上的
// GORM Store。同一套仓储交给先后两个 Server，模拟进程重启后读回已持久化的数据。
func newWorkflowStores(t *testing.T) assembly.Stores {
	t.Helper()
	return newReopenableWorkflowStores(t)()
}

// newReopenableWorkflowStores 返回的函数每次调用都给出一组读写同一份数据的仓储：MySQL 上每次在同一个库上
// 新建 Store，内存实现上返回同一组实例。
func newReopenableWorkflowStores(t *testing.T) func() assembly.Stores {
	t.Helper()
	if mysqltest.Enabled() {
		db := mysqltest.NewDB(t)
		return func() assembly.Stores {
			return assembly.Persistent(openWorkflowStore(t, db, persistence.Config{}))
		}
	}
	stores := assembly.Stores{
		Executions:     memory.NewExecutionManager(),
		Dispatches:     memory.NewDispatchRepository(),
		WorkerVersions: memory.NewWorkerVersionRepository(),
		Schedules:      memory.NewScheduleRepository(),
		Batches:        memory.NewBatchRepository(),
	}
	return func() assembly.Stores { return stores }
}
