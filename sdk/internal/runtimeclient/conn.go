package runtimeclient

import (
	"net/http"

	tenutotel "github.com/donutnomad/tenuto/telemetry"
)

// Conn 是一个 Server 地址上的三组端点，由 sdk/client 创建，sdk/worker 复用。
type Conn struct {
	Namespace     string
	Identity      string
	Telemetry     tenutotel.Providers
	Workflow      *WorkflowRuntimeHTTPClient
	WorkflowTasks *WorkflowTaskRuntimeHTTPClient
	Activities    *ActivityRuntimeHTTPClient
}

// NewConn 用同一个 *http.Client 连接 baseURL 上的全部端点；options 的遥测与重试策略作用于全部端点。
func NewConn(baseURL string, client *http.Client, namespace, identity string, options RuntimeHTTPClientOptions) *Conn {
	return &Conn{
		Namespace:     namespace,
		Identity:      identity,
		Telemetry:     options.Telemetry,
		Workflow:      NewWorkflowRuntimeHTTPClient(baseURL, client, options),
		WorkflowTasks: NewWorkflowTaskRuntimeHTTPClient(baseURL, client, options),
		Activities:    NewActivityRuntimeHTTPClient(baseURL, client, options),
	}
}

// ConnProvider 由 sdk/client 的 Client 实现，sdk/worker 经它取得连接。方法返回本包类型，
// 包外代码无法声明这个接口，因此它不是公开 API。
type ConnProvider interface {
	RuntimeConn() *Conn
}
