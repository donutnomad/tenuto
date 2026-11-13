package memory_test

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch/dispatchtest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 内存后端的 Activity 队列遵守与 TaskQ 后端相同的投递约定。
func TestMemoryActivityRoutingContract(t *testing.T) {
	dispatchtest.RunActivityRoutingContract(t, func(*testing.T) dispatch.ActivityTaskQueueRouter {
		return memory.NewActivityQueueRouter(8)
	})
}
