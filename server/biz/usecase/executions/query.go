// Package executions 放执行域用例消费的端口。
package executions

import (
	"context"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
)

// QueryEvaluator 把一段历史只读重放到最新状态，再对名为 name 的 Query handler 求值一次。
// 返回结果、是否被拒绝、拒绝原因；重放失败、handler 未注册与 handler 出错都表达为拒绝。
// 消费方是服务端的 Query 用例，实现由 Worker 侧的解释器提供。
type QueryEvaluator interface {
	EvaluateQuery(ctx context.Context, workflow *program.WorkflowConfig, events []history.Event, name string, input map[string]any) (map[string]any, bool, string)
}
