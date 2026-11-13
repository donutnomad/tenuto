package interpreter

import (
	"slices"

	coroutine2 "github.com/donutnomad/tenuto/internal/executor/coroutine"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/sdk/workflow"
	"github.com/donutnomad/wlang/types"
)

// workflowContext 是 workflow.Context 在解释器里的值：lane 路径加取消作用域。
//
// lanePath 随协程作用域携带：主协程为 ""，workflow.Go 的第 i 条 lane 为 parent+"/"+i。Activity、
// Timer、子 Workflow 的缺省 ID 由 lanePath 加 lane 内序号组成。scope 是 ctx 所属的取消作用域，
// WithCancel 与 NewDisconnectedContext 换成新的作用域，workflow.Go 的 lane 沿用父 ctx 的作用域。
type workflowContext struct {
	lanePath string
	scope    *cancelScope
}

// withLanePath 返回换成 lanePath 的副本，作用域不变。
func (c workflowContext) withLanePath(lanePath string) workflowContext {
	c.lanePath = lanePath
	return c
}

// Err 在作用域未取消时返回 nil，取消后返回 workflow.ErrCanceled（对标 Temporal cancelCtx.Err）。
func (c workflowContext) Err() error {
	if c.scope == nil || !c.scope.canceled {
		return nil
	}
	return workflow.ErrCanceled
}

// Done 返回作用域取消时关闭的 Channel（对标 Temporal cancelCtx.Done）。
func (c workflowContext) Done() types.Value {
	return types.NewValue("workflow.Channel", channelHandle{executor: c.scope.executor, ch: c.scope.doneChannel()})
}

// canceled 报告 ctx 是否已取消。
func (c workflowContext) canceled() bool {
	return c.scope != nil && c.scope.canceled
}

// onCancel 在 ctx 的作用域上登记一个取消回调；作用域已取消时立即执行。
func (c workflowContext) onCancel(op cancelOp) {
	if c.scope != nil {
		c.scope.register(op)
	}
}

// cancelScope 是一个取消作用域，对标 Temporal cancelCtx。根作用域在 Run 收到取消请求时取消，
// WithCancel 的作用域随 parent 或自己的 cancel 取消，NewDisconnectedContext 的作用域只随自己的 cancel 取消。
//
// 取消在协作式调度的单一执行线上同步完成，顺序固定：先按登记顺序执行本作用域的取消回调，再按创建顺序
// 取消子作用域（对标 Temporal 的 SDKFlagOrderedChildCancel）。登记与创建都按程序顺序发生，所以重放时
// 产生的取消命令与首次执行相同。
type cancelScope struct {
	executor *WorkflowExecutor
	canceled bool
	done     *coroutine2.Channel
	children []*cancelScope
	ops      []cancelOp
}

// cancelOp 是在作用域上发起、尚未结束的一个操作。finished 为 true 的操作不再取消，登记新操作时被清理。
type cancelOp struct {
	finished func() bool
	cancel   func()
}

func (e *WorkflowExecutor) newCancelScope(parent *cancelScope) *cancelScope {
	scope := &cancelScope{executor: e}
	if parent != nil {
		if parent.canceled {
			scope.canceled = true
		} else {
			parent.children = append(parent.children, scope)
		}
	}
	return scope
}

func (s *cancelScope) doneChannel() *coroutine2.Channel {
	if s.done == nil {
		s.done = coroutine2.NewChannel()
		if s.canceled {
			s.done.Close()
		}
	}
	return s.done
}

func (s *cancelScope) register(op cancelOp) {
	if s.canceled {
		op.cancel()
		return
	}
	s.ops = slices.DeleteFunc(s.ops, func(pending cancelOp) bool { return pending.finished() })
	s.ops = append(s.ops, op)
}

// cancel 取消作用域及其子作用域；重复调用没有效果。
func (s *cancelScope) cancel() {
	if s.canceled {
		return
	}
	s.canceled = true
	if s.done != nil {
		s.done.Close()
	}
	ops := s.ops
	s.ops = nil
	for _, op := range ops {
		if !op.finished() {
			op.cancel()
		}
	}
	children := s.children
	s.children = nil
	for _, child := range children {
		child.cancel()
	}
	// 在协程里调用 cancel 时让 RunBatch 再转一轮，等待 Done 与取消结果的协程在本批重查。
	s.executor.progressed()
}

// WithCancel 是 workflow.WithCancel 的 host 方法：返回 parent 作用域下的新作用域与它的 cancel。
func (api *runtimeWorkflowAPI) WithCancel(ctx workflowContext) (types.Value, types.Value) {
	return api.derivedContext(ctx, ctx.scope)
}

// NewDisconnectedContext 是 workflow.NewDisconnectedContext 的 host 方法：新作用域不随 parent 取消。
func (api *runtimeWorkflowAPI) NewDisconnectedContext(ctx workflowContext) (types.Value, types.Value) {
	return api.derivedContext(ctx, nil)
}

func (api *runtimeWorkflowAPI) derivedContext(ctx workflowContext, parent *cancelScope) (types.Value, types.Value) {
	scope := api.executor.newCancelScope(parent)
	ctx.scope = scope
	return types.NewValue("workflow.Context", ctx), types.NewValue("workflow.CancelFunc", func() { scope.cancel() })
}

// NewCanceledError 是 workflow.NewCanceledError 的 host 方法：details 按 registry 编码。
func (api *runtimeWorkflowAPI) NewCanceledError(details ...any) error {
	return api.newApplicationError("canceled", apperror.CanceledType, nil, true, details)
}

// IsCanceledError 是 workflow.IsCanceledError 的 host 方法。
func (api *runtimeWorkflowAPI) IsCanceledError(err error) bool {
	return apperror.IsCanceled(err)
}

// withdrawCommand 撤回本批产出、尚未提交的一条命令：操作在发出前就被取消时不再发给 Server，
// 对标 Temporal 命令状态机里 created 状态被取消的命令。重放批只在本批的重放命令里查找。
func (e *WorkflowExecutor) withdrawCommand(match func(history.Command) bool) bool {
	if e.isReplay {
		batch := e.replayCommands[e.replayBatchStart:]
		if i := slices.IndexFunc(batch, match); i >= 0 {
			e.replayCommands = slices.Delete(e.replayCommands, e.replayBatchStart+i, e.replayBatchStart+i+1)
			return true
		}
		return false
	}
	if i := slices.IndexFunc(e.commands, match); i >= 0 {
		e.commands = slices.Delete(e.commands, i, i+1)
		return true
	}
	return false
}
