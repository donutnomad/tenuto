package protocol

import (
	"context"
	"strings"
)

const DefaultNamespace = "default"

func NormalizeNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return DefaultNamespace
	}
	return namespace
}

type namespaceContextKey struct{}

// WithNamespace 把规范化后的 namespace 放进 ctx，供队列与 runtime 边界读取。
func WithNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, namespaceContextKey{}, NormalizeNamespace(namespace))
}

// NamespaceFromContext 返回 ctx 里的 namespace，未设置时返回默认 namespace。
func NamespaceFromContext(ctx context.Context) string {
	namespace, _ := ctx.Value(namespaceContextKey{}).(string)
	return NormalizeNamespace(namespace)
}
