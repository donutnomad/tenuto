package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch/dispatchtest"
)

// 派发租约与 Worker 版本仓储的契约与内存实现共用 dispatchtest；gorm 特有的行级断言在
// dispatchrepo 包，并发与 MySQL 行为在本包的 mysql_dispatch_* 测试里。

func TestDispatchRepositoryContract(t *testing.T) {
	dispatchtest.RunRepositoryContract(t, func(t *testing.T) dispatch.Repository { return newTestStore(t).Dispatches })
}

func TestWorkerVersionRepositoryContract(t *testing.T) {
	dispatchtest.RunWorkerVersionRepositoryContract(t, func(t *testing.T) dispatch.WorkerVersionRepository { return newTestStore(t).Dispatches })
}
