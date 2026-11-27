package interpreter

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/apperror"
	wlregistry "github.com/donutnomad/wlang/registry"
	wlruntime "github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"
)

// spawnUpdateHandlers 为「已 accepted 且未 spawn」的 update 起 handler 协程（照 Temporal ProcessMessage
// 按 eventID 交错投递 → defaultUpdateHandler → scheduler.Spawn）。批间调用（applyBatch 后、RunBatch 前）：handler 在其
// UpdateAccepted 所属批的视界执行 → handler 内前缀一致性成立。
//
// 调度顺序对标 Temporal Go SDK 的 SDKPriorityUpdateHandling：name 已登记 handler 的 update 以高优先级协程起，
// 本批先于 main 与已有协程运行；name 尚未登记的进 queuedUpdates，SetUpdateHandler 登记时立即起
// （eager，排在登记它的协程之后），本批收敛后仍未登记的由 runDecisionBatch 以 UnknownUpdate 失败。
//
// 确定性铁律：按 AcceptedEventID（历史全局序）升序处理 → 同一批多个 update 的 handler 按事件顺序运行，每次 replay 相同。
func (e *WorkflowExecutor) spawnUpdateHandlers() {
	if e.spawnedUpdates == nil {
		e.spawnedUpdates = make(map[string]bool)
	}
	var fresh []history.WorkflowUpdateRecord
	for _, recs := range e.updateAccepted {
		for _, rec := range recs {
			if !e.spawnedUpdates[rec.UpdateID] {
				fresh = append(fresh, rec)
			}
		}
	}
	slices.SortFunc(fresh, func(a, b history.WorkflowUpdateRecord) int {
		return cmp.Compare(a.AcceptedEventID, b.AcceptedEventID)
	})
	for _, rec := range fresh {
		e.spawnedUpdates[rec.UpdateID] = true
		if _, registered := e.updateHandler(rec.Name); registered {
			e.sched.SpawnPriority("update:"+rec.UpdateID, e.updateHandlerCoroutine(rec))
		} else {
			e.queuedUpdates = append(e.queuedUpdates, rec)
		}
	}
}

// startQueuedUpdates 在 name 的 handler 登记时立即起排队的同名 update 的 handler 协程，按事件顺序排在当前协程之后。
func (e *WorkflowExecutor) startQueuedUpdates(name string) {
	e.queuedUpdates = slices.DeleteFunc(e.queuedUpdates, func(rec history.WorkflowUpdateRecord) bool {
		if rec.Name != name {
			return false
		}
		e.sched.SpawnEager("update:"+rec.UpdateID, e.updateHandlerCoroutine(rec))
		return true
	})
}

// updateHandler 返回 name 当前登记的 update handler。
func (e *WorkflowExecutor) updateHandler(name string) (registeredUpdateHandler, bool) {
	if e.activeAPI == nil {
		return registeredUpdateHandler{}, false
	}
	handler, ok := e.activeAPI.updateHandlers[name]
	return handler, ok
}

// updateHandlerCoroutine 返回单个 update 的 handler 协程体（runner 照 Temporal defaultUpdateHandler
// 的批模型等价物）。确定性键：lanePath = "update:"+UpdateID
// （UpdateID 历史锚定、全局唯一；不占用户 lane 的 spawnSeq 游标）——handler 内
// ExecuteActivity 的 laneKey 落 update:<id>#aN 专属桶，不再混用 main root 桶。
func (e *WorkflowExecutor) updateHandlerCoroutine(rec history.WorkflowUpdateRecord) func(yield func()) {
	return func(yield func()) {
		defer func() {
			if r := recover(); r != nil {
				e.recordCoroutinePanic(r)
			}
		}()
		// 本批收敛后 name 仍没有 handler → 以 UnknownUpdate 失败完成这次 update。
		// replay 批同样走到这里并产出同一条命令，与历史 WorkflowUpdateCompleted 配对。
		handler, registered := e.updateHandler(rec.Name)
		e.progressed()
		if !registered {
			e.emitUpdateHandlerFailure(rec, &protocol.Failure{
				Message: fmt.Sprintf("no update handler registered for %q", rec.Name),
				Type:    unknownUpdateFailureType,
			})
			return
		}
		ctxVal := types.NewValue("workflow.Context", workflowContext{lanePath: "update:" + rec.UpdateID, scope: e.rootScope})
		businessArgs, err := executor.DecodeUpdateHandlerArguments(handler.Registry, handler.ABI.BusinessParamTypes, rec.Args)
		if err != nil {
			e.emitUpdateHandlerFailure(rec, &protocol.Failure{Message: err.Error(), Type: "DataConversionError"})
			return
		}
		invokeArgs := make([]types.Value, 1, len(businessArgs)+1)
		invokeArgs[0] = ctxVal
		invokeArgs = append(invokeArgs, businessArgs...)
		out, err := e.activeWLExec.InvokeClosure(handler.Closure, invokeArgs)
		if err != nil {
			// 错误路由：非确定性与 panic（越界、nil 解引用、显式 panic，经 wlang safeCall 上浮，LangError.Cause
			// 保留错误链）不降级为 update 失败：升级为 executor 级错误 → Execute 返回错误 → Worker 以
			// non_deterministic_error / workflow_panic 让 Workflow Task 失败，与主流程一致（Temporal Go SDK 的
			// update handler panic 同样按 WorkflowPanicPolicy 处理，缺省 BlockWorkflow）。本 update 不产完成命令，
			// 修复代码后重放时重新执行 handler。handler 显式返回的 error 走 completeUpdateHandlerResult，仍是 update 失败。
			if executor.WorkflowTaskBlockedCause(err) != "" {
				e.evalErr = err
				return
			}
			e.emitUpdateHandlerFailure(rec, &protocol.Failure{Message: err.Error(), Type: "HandlerExecutionError"})
			return
		}
		outcome, failure := completeUpdateHandlerResult(handler, out)
		if failure != nil {
			e.emitUpdateHandlerFailure(rec, failure)
			return
		}
		// 完成命令经 emitCommand 分流（replay 批 → replayCommands 与历史 WorkflowUpdateCompleted
		// 按 UpdateID 配对；live 批 → 上报）。幂等跳过已删除——replay
		// 批重放 handler 全程（照 Temporal），server AppendIdempotent 按 UpdateID 去重仍是纵深防线。
		// 成功与失败都只走 Outcome（成功=Success payload，失败=Failure）；没有字符串通道。
		e.emitCommand(history.Command{
			Type: history.CmdCompleteWorkflowUpdate, Name: rec.Name, UpdateID: rec.UpdateID,
			UpdateOutcome: outcome,
		})
	}
}

// emitUpdateHandlerFailure 用统一的 Outcome.Failure 表达失败（指针非 nil 即失败），
// 与 handler 的 Program 版本无关：不存在只带字符串的失败通道。
func (e *WorkflowExecutor) emitUpdateHandlerFailure(rec history.WorkflowUpdateRecord, failure *protocol.Failure) {
	e.emitCommand(history.Command{
		Type: history.CmdCompleteWorkflowUpdate, Name: rec.Name, UpdateID: rec.UpdateID,
		UpdateOutcome: &protocol.UpdateOutcome{Failure: failure},
	})
}

// unknownUpdateFailureType 是「该 name 没有注册 handler」的失败类型。
const unknownUpdateFailureType = "UnknownUpdate"

// registeredUpdateHandler 是 SetUpdateHandler 登记的一个 handler 闭包（只登记，不执行；执行由
// update handler 协程在其 UpdateAccepted 所属批完成）。
type registeredUpdateHandler struct {
	Closure   *wlruntime.Closure
	Signature wlruntime.FunctionSignature
	ABI       executor.UpdateHandlerABI
	Registry  *wlregistry.Registry
}

func (api *runtimeWorkflowAPI) SetUpdateHandler(ctx workflowContext, name string, handler *wlruntime.Closure) error {
	if name == "" {
		return fmt.Errorf("workflow.SetUpdateHandler update name is empty")
	}
	if handler == nil {
		return fmt.Errorf("workflow.SetUpdateHandler handler is nil")
	}
	signature := handler.Signature()
	abi, err := executor.ClassifyUpdateHandlerSignature(signature, api.registry)
	if err != nil {
		return fmt.Errorf("workflow.SetUpdateHandler %q: %w", name, err)
	}
	if api.updateHandlers == nil {
		api.updateHandlers = make(map[string]registeredUpdateHandler)
	}
	api.updateHandlers[name] = registeredUpdateHandler{
		Closure: handler, Signature: signature, ABI: abi, Registry: api.registry,
	}
	api.executor.startQueuedUpdates(name)
	return nil
}

// SetQueryHandler 登记一个 query handler 闭包。仅登记到 queryHandlers，
// 不 spawn 协程、不参与 spawnUpdateHandlers/matchReplayWithHistory——query 只读、
// 不产命令、不参与历史比对。重复 name 后注册覆盖前者（与 SetUpdateHandler 覆盖语义一致）。
// 求值由 QueryWorkflow 在 ReplayForQuery 收敛后按 name 查表 + InvokeClosure 一次完成。
//
// 登记时校验 name 非空、handler 是 func(workflow.Context, map[string]any) map[string]any；
// 形状不对的 handler 在这里报错，不留到第一次 query 才暴露。
func (api *runtimeWorkflowAPI) SetQueryHandler(ctx workflowContext, name string, handler *wlruntime.Closure) error {
	if name == "" {
		return fmt.Errorf("workflow.SetQueryHandler query name is empty")
	}
	if handler == nil {
		return fmt.Errorf("workflow.SetQueryHandler %q: handler is nil", name)
	}
	signature := handler.Signature()
	if !slices.Equal(signature.Params, []string{"workflow.Context", queryPayloadType}) ||
		!slices.Equal(signature.Returns, []string{queryPayloadType}) {
		return fmt.Errorf("workflow.SetQueryHandler %q: handler must be func(ctx workflow.Context, input map[string]any) map[string]any, got func(%s) (%s)",
			name, strings.Join(signature.Params, ", "), strings.Join(signature.Returns, ", "))
	}
	if api.queryHandlers == nil {
		api.queryHandlers = make(map[string]*wlruntime.Closure)
	}
	api.queryHandlers[name] = handler
	return nil
}

// queryPayloadType 是 query handler 入参与返回值的 wlang 类型名。
const queryPayloadType = "map<string,any>"

// queryHandlerFailureKey 是 query handler 单返回值 map 中约定的失败键：值为非空字符串时，
// query 求值判定为被拒（QueryRejected），对齐 Temporal QueryFailedError。
// query 失败不经 emitCommand、不落历史。
const queryHandlerFailureKey = "__query_error"

// closureOutputMap 把 handler 闭包的单返回值 types.Value 解析回 map[string]any。
// handler 返回类型为 map<string,any>，其 .Go() 即 map[string]any；null/其他形态兜底为 nil。
func closureOutputMap(v types.Value) map[string]any {
	if m, ok := v.Go().(map[string]any); ok {
		return m
	}
	return nil
}

func completeUpdateHandlerResult(handler registeredUpdateHandler, result types.Value) (*protocol.UpdateOutcome, *protocol.Failure) {
	encoder := apperror.Encoder(func(detail any) (protocol.Payload, error) {
		return encodeUpdatePayloadAny(handler.Registry, detail)
	})
	switch handler.ABI.Kind {
	case executor.UpdateHandlerABITemporalError:
		if err := updateHandlerReturnedError(result); err != nil {
			failure := updateFailureFromError(encoder, err)
			return nil, &failure
		}
		payload, err := protocol.NewJSONPayload(nil)
		if err != nil {
			return nil, &protocol.Failure{Message: err.Error(), Type: "DataConversionError"}
		}
		return &protocol.UpdateOutcome{Success: &payload}, nil
	case executor.UpdateHandlerABITemporalResultError:
		values, ok := types.UnpackTuple(result)
		if !ok || len(values) != 2 {
			return nil, &protocol.Failure{Message: "update handler returned an invalid result tuple", Type: "HandlerExecutionError"}
		}
		if err := updateHandlerReturnedError(values[1]); err != nil {
			failure := updateFailureFromError(encoder, err)
			return nil, &failure
		}
		payload, err := executor.EncodeUpdatePayloadValue(handler.Registry, values[0])
		if err != nil {
			return nil, &protocol.Failure{Message: err.Error(), Type: "DataConversionError"}
		}
		return &protocol.UpdateOutcome{Success: &payload}, nil
	default:
		return nil, &protocol.Failure{Message: fmt.Sprintf("unsupported update handler ABI %q", handler.ABI.Kind), Type: "HandlerExecutionError"}
	}
}

func updateHandlerReturnedError(value types.Value) error {
	if value.Go() == nil {
		return nil
	}
	err, ok := value.Go().(error)
	if !ok {
		return fmt.Errorf("update handler error result has type %T", value.Go())
	}
	return err
}

// updateFailureFromError 把 handler 返回的 Go error 转成共享 Failure。普通 error 的 Type 保持
// 为空（untyped），只有编码本身失败时才退化成 ApplicationError 带上原因文本。
func updateFailureFromError(encode apperror.Encoder, err error) protocol.Failure {
	failure, encodeErr := apperror.Encode(err, encode)
	if encodeErr != nil {
		return protocol.Failure{Message: err.Error(), Type: "ApplicationError"}
	}
	return failure
}

// IsApplicationError 报告 err 是否应用错误且 Type 完全相等（大小写敏感）。沿 Unwrap 链查找，
// 因此 workflow 内包装过的错误同样可判定。
func (api *runtimeWorkflowAPI) IsApplicationError(err error, errorType string) bool {
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return false
	}
	return appErr.Type() == errorType
}

// TimeoutTypeOf 返回超时错误的超时种类；不是超时错误时返回空串。workflow.TimeoutType 在 WLang 里
// 是 string：返回值与 TimeoutType 常量都以 string 表示，才能与常量或字符串字面量比较。
func (api *runtimeWorkflowAPI) TimeoutTypeOf(err error) string {
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return ""
	}
	return string(appErr.TimeoutType())
}

// ApplicationErrorDetailCount 返回 details 个数（非应用错误为 0）。
func (api *runtimeWorkflowAPI) ApplicationErrorDetailCount(err error) int {
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return 0
	}
	return len(appErr.RawDetails())
}

// ApplicationErrorDetail 把第 index 个 detail 解码到 out（out 由 registry 按目标类型提供，
// 与 Future.Get 的 out 参数同机制）。所有失败路径都返回明确错误，out 只在成功时被改写。
func (api *runtimeWorkflowAPI) ApplicationErrorDetail(err error, index int, out any) error {
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return fmt.Errorf("%w: %w", apperror.ErrNoDetails, err)
	}
	payloads := appErr.RawDetails()
	if len(payloads) == 0 {
		return apperror.ErrNoDetails
	}
	if index < 0 || index >= len(payloads) {
		return fmt.Errorf("application error detail index %d out of range (have %d): %w",
			index, len(payloads), apperror.ErrNoDetails)
	}
	target := reflect.ValueOf(out)
	if out == nil || target.Kind() != reflect.Pointer || target.IsNil() {
		return apperror.ErrDetailTarget
	}
	payload := payloads[index]
	if payload.Encoding != protocol.PayloadEncodingJSONPlainV1 {
		return fmt.Errorf("decode application error detail %d: unsupported payload encoding %q",
			index, payload.Encoding)
	}
	// 解码到独立临时目标，成功后再写回 out：解码失败时 out 保持原值，不留下部分字段。
	decoded := reflect.New(target.Elem().Type())
	if err := json.Unmarshal(payload.Data, decoded.Interface()); err != nil {
		return fmt.Errorf("decode application error detail %d: %w", index, err)
	}
	target.Elem().Set(decoded.Elem())
	return nil
}

// registryEncoder 是解释器侧的 detail 编码器：typed record 走注册表，其余走 JSON payload。
func (api *runtimeWorkflowAPI) registryEncoder() apperror.Encoder {
	return func(detail any) (protocol.Payload, error) {
		return encodeUpdatePayloadAny(api.registry, detail)
	}
}

func (api *runtimeWorkflowAPI) NewApplicationError(message, errorType string, details ...any) error {
	return api.newApplicationError(message, errorType, nil, false, details)
}

func (api *runtimeWorkflowAPI) NewApplicationErrorWithCause(message, errorType string, cause error, details ...any) error {
	return api.newApplicationError(message, errorType, cause, false, details)
}

func (api *runtimeWorkflowAPI) NewNonRetryableApplicationError(message, errorType string, cause error, details ...any) error {
	return api.newApplicationError(message, errorType, cause, true, details)
}

// newApplicationError 是三个 workflow 侧构造器的唯一实现：details 经 registry 编码，
// 错误值本身是共享的 *apperror.Error，因此 Update / Workflow 终态 / 客户端读取用同一份载荷。
func (api *runtimeWorkflowAPI) newApplicationError(message, errorType string, cause error, nonRetryable bool, details []any) error {
	payloads := make(protocol.Payloads, len(details))
	for i, detail := range details {
		payload, err := encodeUpdatePayloadAny(api.registry, detail)
		if err != nil {
			return apperror.NewEncoded(err.Error(), "DataConversionError", nil, apperror.Options{})
		}
		payloads[i] = payload
	}
	return apperror.NewEncoded(message, errorType, payloads, apperror.Options{
		NonRetryable: nonRetryable,
		Cause:        cause,
	})
}

// encodeUpdatePayloadAny 把任意 host 值编成 update Payload：typed record 走 registry
// （否则裸 json.Marshal 会静默产出 "{}"，见 recordToMap 注释），其余走 NewJSONPayload。
//
// 服务于 ApplicationError detail 的编码。
func encodeUpdatePayloadAny(reg *wlregistry.Registry, detail any) (protocol.Payload, error) {
	if record, ok := detail.(*types.Record); ok {
		return executor.EncodeUpdatePayloadValue(reg, types.NewValue(record.TypeName(), record))
	}
	return protocol.NewJSONPayload(detail)
}
