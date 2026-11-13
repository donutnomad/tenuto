package taskq

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch/dispatchtest"
)

// TaskQ 后端的 Router 遵守共享的投递约定。客户端是按 routing key 匹配的 fake，
// 不连接数据库；真实 TaskQ 的 routing key 匹配由 MySQL 集成测试覆盖。
func TestTaskQActivityRoutingContract(t *testing.T) {
	dispatchtest.RunActivityRoutingContract(t, func(t *testing.T) dispatch.ActivityTaskQueueRouter {
		return newStaticActivityQueueRouter(t, map[string]*Queue{
			dispatchtest.QueuePayments: NewQueue(&routingClientFake{}),
			dispatchtest.QueueEmails:   NewQueue(&routingClientFake{}),
		})
	})
}
