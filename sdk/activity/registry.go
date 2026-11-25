package activity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/donutnomad/tenuto/internal/runtimectx"
	"github.com/donutnomad/tenuto/protocol"
)

// Contract 是一个 Activity 实现的 wire 契约：类型名、版本、输入与输出的结构指纹。
type Contract = protocol.ActivityContract

// Registry 保存一个 Worker 进程能执行的 Activity：每个类型一个实现、一份契约和它所在的 Task Queue。
// Worker 登记时把全部契约上报给 Server，Server 用它对照 Workflow 侧的声明；
// worker.Options.ActivityTaskQueues 为空时，Worker 拉取的队列由 Queues 推导。
type Registry struct {
	entries map[string]registered
}

type registered struct {
	fn       protocol.ActivityFunc
	contract Contract
	queue    string // 空表示登记时没有指定队列
}

// NewRegistry 返回一个空的 Registry。
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]registered)}
}

// Register 登记一个以 map[string]any 收发的 Activity。契约的输入输出指纹取自 map[string]any，
// 版本为空；Workflow 侧声明了版本的 Activity 用 RegisterActivity 登记。
func (r *Registry) Register(activityType string, fn protocol.ActivityFunc) error {
	contract, err := ContractOf[map[string]any, map[string]any](activityType, "")
	if err != nil {
		return err
	}
	return r.add(contract, "", fn)
}

func (r *Registry) add(contract Contract, queue string, fn protocol.ActivityFunc) error {
	if contract.Type == "" {
		return errors.New("activity type is empty")
	}
	if fn == nil {
		return fmt.Errorf("activity %q: implementation is nil", contract.Type)
	}
	if _, exists := r.entries[contract.Type]; exists {
		return fmt.Errorf("activity type %q is already registered", contract.Type)
	}
	r.entries[contract.Type] = registered{fn: fn, contract: contract, queue: queue}
	return nil
}

// Get 返回 activityType 的实现。
func (r *Registry) Get(activityType string) (protocol.ActivityFunc, error) {
	entry, ok := r.entries[activityType]
	if !ok {
		return nil, fmt.Errorf("activity %s not found", activityType)
	}
	return entry.fn, nil
}

// Types 返回按名称排序的 Activity 类型。
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.entries))
	for activityType := range r.entries {
		types = append(types, activityType)
	}
	sort.Strings(types)
	return types
}

// Queues 返回登记时指定的 Task Queue，去重并排序。
func (r *Registry) Queues() []string {
	seen := map[string]bool{}
	queues := []string{}
	for _, entry := range r.entries {
		if entry.queue != "" && !seen[entry.queue] {
			seen[entry.queue] = true
			queues = append(queues, entry.queue)
		}
	}
	sort.Strings(queues)
	return queues
}

// Unqueued 返回登记时没有指定 Task Queue 的 Activity 类型，按名称排序。
func (r *Registry) Unqueued() []string {
	types := []string{}
	for _, activityType := range r.Types() {
		if r.entries[activityType].queue == "" {
			types = append(types, activityType)
		}
	}
	return types
}

// QueueOf 返回 activityType 登记时指定的 Task Queue；没有登记或没有指定时返回空串。
func (r *Registry) QueueOf(activityType string) string {
	return r.entries[activityType].queue
}

// Contracts 返回按类型名排序的全部契约，Worker 登记时原样上报。
func (r *Registry) Contracts() []Contract {
	types := r.Types()
	contracts := make([]Contract, 0, len(types))
	for _, activityType := range types {
		contracts = append(contracts, r.entries[activityType].contract)
	}
	return contracts
}

// RegisterActivity 登记一个强类型 Activity，并从 I、O 算出它的契约；不指定 Task Queue，
// Worker 在 ActivityTaskQueues 列出的队列上执行它（为空时是 "default"）。知道队列时用 RegisterActivityOnQueue。
//
// Activity 的输入与输出在线上是 JSON object，所以 I 与 O 必须是 struct 或 map[string]any。
// version 是实现的契约版本，与 Workflow 侧 @Activity(version=...) 的值逐字对照。
func RegisterActivity[I, O any](reg *Registry, activityType, version string, fn func(context.Context, I) (O, error)) error {
	return RegisterActivityOnQueue(reg, "", activityType, version, fn)
}

// RegisterActivityOnQueue 与 RegisterActivity 相同，并记录 Activity 所在的 Task Queue，
// 与 Workflow 侧 @Activity(queue=...) 的值一致。生成的 RegisterActivities 用它登记。
// worker.Options.ActivityTaskQueues 为空时 Worker 拉取这些队列；显式设置时必须包含它们。
func RegisterActivityOnQueue[I, O any](reg *Registry, queue, activityType, version string, fn func(context.Context, I) (O, error)) error {
	if err := requireObject[I](activityType, "input"); err != nil {
		return err
	}
	if err := requireObject[O](activityType, "output"); err != nil {
		return err
	}
	contract, err := ContractOf[I, O](activityType, version)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("activity %q: implementation is nil", activityType)
	}
	return reg.add(contract, queue, func(ctx context.Context, input map[string]any) (map[string]any, error) {
		var in I
		if err := mapToStruct(input, &in); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		return structToMap(out)
	})
}

// requireObject 要求 T 编码成 JSON object：struct 或 map[string]any。
func requireObject[T any](activityType, side string) error {
	rt := reflect.TypeOf((*T)(nil)).Elem()
	if rt.Kind() == reflect.Struct || rt == reflect.TypeOf(map[string]any(nil)) {
		return nil
	}
	return fmt.Errorf("activity %q: %s type %s is not a JSON object; use a struct or map[string]any (wrap a single value in a struct field)", activityType, side, rt)
}

// ErrPending 由异步完成的 Activity 返回：本次执行不上报结果，
// 之后由外部用 Task Token 调 Server 的完成接口。
var ErrPending = errors.New("activity result pending")

// Info 是当前这次 Activity attempt 的信息：所属 Workflow Run、Activity ID、attempt 序号、时间点与
// 异步完成用的 Task Token。
type Info = runtimectx.ActivityInfo

// GetInfo 返回当前 Activity attempt 的信息；ctx 不来自 Worker 时返回零值。
func GetInfo(ctx context.Context) Info {
	return runtimectx.ActivityInfoFrom(ctx)
}

// ErrWorkerStopping 由察觉 Worker 停止（GetWorkerStopChannel 关闭）而提前结束的 Activity 返回：
// Worker 不上报失败，把这次 attempt 交还 Server，Server 以同一个 attempt 重新派发给其他 Worker。
var ErrWorkerStopping = errors.New("activity stopped because the worker is stopping")

// GetWorkerStopChannel 返回 Worker 开始停止时关闭的 channel。停止的宽限期内 ctx 不会被取消，
// 长时间运行的 Activity 监听它，保存进度（例如记一次心跳）后返回 ErrWorkerStopping。
// ctx 不来自 Worker 时返回 nil channel，永远不会就绪。
func GetWorkerStopChannel(ctx context.Context) <-chan struct{} {
	return runtimectx.WorkerStopFrom(ctx)
}

// RecordHeartbeat 上报一次心跳并附带进度详情。Workflow 请求取消这个 Activity、Run 已关闭、
// 这次 attempt 已超时或被重置时 ctx 被取消。取消只经心跳送达：不心跳的 Activity 只能在返回或超时时结束。
// Workflow 请求取消后，Activity 返回 ctx.Err() 或 apperror.NewCanceled(details...) 即确认取消
// （ActivityCancelled）；返回成功仍按完成上报。
func RecordHeartbeat(ctx context.Context, details map[string]any) {
	if record := runtimectx.HeartbeatFrom(ctx); record != nil {
		record(details)
	}
}

// GetLastHeartbeatDetails 返回 Server 记录的最后一次心跳详情：重试的 attempt 读到上一次 attempt 的，
// Worker 停止后以同一 attempt 重新派发时读到交还前的。首次执行返回 nil。重试时用它恢复进度。
func GetLastHeartbeatDetails(ctx context.Context) map[string]any {
	return runtimectx.LastHeartbeatFrom(ctx)
}
