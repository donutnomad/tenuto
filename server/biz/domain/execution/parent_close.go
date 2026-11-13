package execution

import "github.com/donutnomad/tenuto/protocol"

// ParentCloseReason 是父关闭策略终止或取消子 Workflow 时记录的原因。
const ParentCloseReason = "by parent close policy"

type ParentCloseAction uint8

const (
	ParentCloseActionNone ParentCloseAction = iota
	ParentCloseActionTerminate
	ParentCloseActionRequestCancel
)

// EffectiveParentClosePolicy 返回这个子关联的父关闭策略；没有记录时是默认的 Terminate。
func (s *ChildWorkflowState) EffectiveParentClosePolicy() protocol.ParentClosePolicy {
	if s.ParentClosePolicy == "" {
		return protocol.ParentClosePolicyTerminate
	}
	return s.ParentClosePolicy
}

// ParentCloseCandidates 选出关闭的父 Run 要按策略处理的子关联：子 Run 已建、子关联未终结、策略不是 Abandon。
// 子关联终结表示子 Workflow 已向父 Run 报告结束。
func ParentCloseCandidates(states []*ChildWorkflowState) []*ChildWorkflowState {
	var out []*ChildWorkflowState
	for _, state := range states {
		if state != nil && !state.IsTerminal() && state.ChildRunID != "" &&
			state.EffectiveParentClosePolicy() != protocol.ParentClosePolicyAbandon {
			out = append(out, state)
		}
	}
	return out
}

// ParentCloseActionFor 决定父 Run 关闭后对子 Workflow 当前 Run 的处理，对标 Temporal 的 parent close policy。
// parent 是父 Run 的 execution state，link 是父 Run 上的子关联，child 是子 Workflow 当前 Run 的 execution state。
//
// 父 Run 以任何方式关闭都适用，包括 continue-as-new、失败后重试与 reset 终止：子 Workflow 属于关闭的那个 Run，
// 不随接续的 Run 迁移。父 Run 未关闭、子关联已终结、子 Run 已关闭或策略为 Abandon 时不处理；
// RequestCancel 对已请求过取消的子 Run 不再重复请求。
func ParentCloseActionFor(parent *WorkflowExecutionState, link *ChildWorkflowState, child *WorkflowExecutionState) ParentCloseAction {
	if parent == nil || !parent.IsTerminal() || link == nil || link.IsTerminal() || child == nil || child.IsTerminal() {
		return ParentCloseActionNone
	}
	switch link.EffectiveParentClosePolicy() {
	case protocol.ParentClosePolicyAbandon:
		return ParentCloseActionNone
	case protocol.ParentClosePolicyRequestCancel:
		if child.CancelRequested {
			return ParentCloseActionNone
		}
		return ParentCloseActionRequestCancel
	default:
		return ParentCloseActionTerminate
	}
}
