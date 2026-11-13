package protocol

import "time"

const (
	// DefaultActivityTaskQueue 是 Activity 未指定队列时使用的队列名。
	DefaultActivityTaskQueue = "default"
	// DefaultActivityWorkerVersionTTL 是 Worker 登记未声明心跳 TTL 时采用的值。
	DefaultActivityWorkerVersionTTL = 30 * time.Second
	// DefaultWorkflowTaskTimeout 是 Workflow Task 未指定超时时采用的值。
	DefaultWorkflowTaskTimeout = 10 * time.Second
)

// ActivityWorkerVersion 是 Activity Worker 向 Server 登记的版本声明：它承接哪些队列与 Activity 类型，
// 以及每个类型的契约。
type ActivityWorkerVersion struct {
	Namespace       string             `json:"namespace,omitempty"`
	App             string             `json:"app,omitempty"`
	Identity        string             `json:"identity,omitempty"`
	BuildID         string             `json:"build_id,omitempty"`
	TaskQueues      []string           `json:"task_queues,omitempty"`
	ActivityTypes   []string           `json:"activity_types,omitempty"`
	Contracts       []ActivityContract `json:"contracts,omitempty"`
	Default         bool               `json:"default,omitempty"`
	Weight          int                `json:"weight,omitempty"`
	HeartbeatTTL    string             `json:"heartbeat_ttl,omitempty"`
	LastHeartbeatAt time.Time          `json:"last_heartbeat_at,omitempty"`
	ExpiresAt       time.Time          `json:"expires_at,omitempty"`
}
