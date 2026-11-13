package protocol

// ParentClosePolicy 决定父 Run 关闭时仍在运行的子 Workflow 如何处理，在启动子 Workflow 时按子选择。
type ParentClosePolicy string

const (
	// ParentClosePolicyTerminate 终止子 Workflow，是未指定时的默认值。
	ParentClosePolicyTerminate ParentClosePolicy = "terminate"
	// ParentClosePolicyRequestCancel 向子 Workflow 请求取消，由子 Workflow 自己决定如何结束。
	ParentClosePolicyRequestCancel ParentClosePolicy = "request_cancel"
	// ParentClosePolicyAbandon 让子 Workflow 继续运行，它的结果不再传给父 Run。
	ParentClosePolicyAbandon ParentClosePolicy = "abandon"
)

// NormalizeParentClosePolicy 校验子 Workflow 的父关闭策略：空值表示 Terminate。
func NormalizeParentClosePolicy(policy ParentClosePolicy) (ParentClosePolicy, error) {
	switch policy {
	case "":
		return ParentClosePolicyTerminate, nil
	case ParentClosePolicyTerminate, ParentClosePolicyRequestCancel, ParentClosePolicyAbandon:
		return policy, nil
	}
	return "", InvalidRequestf("unknown parent close policy %q: use %q, %q or %q",
		policy, ParentClosePolicyTerminate, ParentClosePolicyRequestCancel, ParentClosePolicyAbandon)
}
