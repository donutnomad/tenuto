package protocol

import "context"

type workflowTaskTokenKey struct{}

// WithWorkflowTaskToken 把已租到的 workflow task token 带到完成上报；空 token 时原样返回 ctx。
func WithWorkflowTaskToken(ctx context.Context, token TaskToken) context.Context {
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, workflowTaskTokenKey{}, token)
}

// WorkflowTaskTokenFrom 取出 WithWorkflowTaskToken 放入的 token。
func WorkflowTaskTokenFrom(ctx context.Context) TaskToken {
	token, _ := ctx.Value(workflowTaskTokenKey{}).(TaskToken)
	return token
}
