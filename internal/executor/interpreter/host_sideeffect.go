package interpreter

import (
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"reflect"
	"runtime"
	"slices"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol/history"
	wlruntime "github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"
	"github.com/google/uuid"

	"github.com/donutnomad/tenuto/sdk/workflow"
)

// SideEffect 照 Temporal，marker 三态与 RandomUUID 相同：
// 闭包在 SideEffect() host 内执行（非 Get 内，对齐 Temporal f() 在 env.SideEffect 里）。
//   - marker 命中（replay 读回 / 同轮就地写）→ shadowMarker + future 只存 id（Get 纯读）。
//   - replay 缺 marker → panicNonDeterministic（代码在已决策位置新增 SideEffect 调用点）。
//   - 非 replay 首见 → InvokeClosure 执行闭包 → recordMarker（{"value": markerValue(...)} 包装，就地写 markers）。
//
// 闭包返回值经 markerValue 编码成可持久化的形态；replay 一致性由 Get 的 assignAny JSON round-trip 保证。
func (api *runtimeWorkflowAPI) SideEffect(ctx workflowContext, f any) SideEffectFuture {
	e := api.executor
	id := api.laneKey(ctx, "se")
	if existing, ok := e.marker("SideEffect", id); ok {
		e.shadowMarker("SideEffect", id, existing)
		return SideEffectFuture{executor: e, id: id}
	}
	if e.isReplay {
		panicNonDeterministic("no cached marker for SideEffect id=" + id)
	}
	cl, ok := f.(*wlruntime.Closure)
	if !ok {
		e.evalErr = fmt.Errorf("workflow.SideEffect f must be an inline closure, got %T", f)
		runtime.Goexit()
	}
	ctxVal := types.NewValue("workflow.Context", ctx)
	out, err := api.wlExec.InvokeClosure(cl, []types.Value{ctxVal})
	if err != nil {
		e.evalErr = err
		runtime.Goexit()
	}
	e.recordMarker("SideEffect", id, map[string]any{"value": api.markerValue(out.Go())})
	return SideEffectFuture{executor: e, id: id}
}

// markerValue 把闭包返回值编码成 marker 里持久化的形态：typed record 经 registry 按字段编码
// （裸 json.Marshal(*types.Record) 产出 "{}"，见 RecordToMap），其余值原样返回。
func (api *runtimeWorkflowAPI) markerValue(v any) any {
	record, ok := v.(*types.Record)
	if !ok {
		return v
	}
	encoded, err := RecordToMap(api.registry, record)
	if err != nil {
		api.executor.evalErr = err
		runtime.Goexit()
	}
	return encoded
}

// MutableSideEffect 照 Temporal：per-id callCounter + equals dedup。
// 与 SideEffect 差异：只在【值变化】时记新 marker（equals 相等则复用旧 marker，不产新命令）。
//
// replay 判据（方案 A.2，与 Temporal command 序等价）：我们 marker 按 callCount 编号（id_cc），first-run
// 只有 record 轮次产 marker、dedup 轮次不产 → replay 时 marker(id_cc) 命中 ⟺ 该 callCount 在 first-run
// 是 record 轮次。callCounter 两侧同序递增（确定性 replay），故 record/dedup 决策逐轮复现。
func (api *runtimeWorkflowAPI) MutableSideEffect(ctx workflowContext, id string, f any, equals any) SideEffectFuture {
	e := api.executor
	e.mseCallCounter[id]++
	callCount := e.mseCallCounter[id]

	prevCallCount, prevValue, hasPrev := e.mseLookup(id, callCount)
	if hasPrev {
		if e.isReplay {
			if existing, ok := e.marker("MutableSideEffect", mseMarkerID(id, callCount)); ok {
				e.shadowMarker("MutableSideEffect", mseMarkerID(id, callCount), existing)
				e.mseRecord(id, callCount, existing)
				return SideEffectFuture{executor: e, id: mseMarkerID(id, callCount), name: "MutableSideEffect"}
			}
			// 该轮 first-run 是 dedup 轮次（无 marker）→ 复用上一个 record 值，不产命令。
			return SideEffectFuture{executor: e, id: mseMarkerID(id, prevCallCount), name: "MutableSideEffect"}
		}
		newVal := api.invokeMSEClosure(f, ctx)
		if api.mseEquals(equals, prevValue["value"], newVal) {
			return SideEffectFuture{executor: e, id: mseMarkerID(id, prevCallCount), name: "MutableSideEffect"}
		}
		wrapped := map[string]any{"value": api.markerValue(newVal)}
		e.recordMarker("MutableSideEffect", mseMarkerID(id, callCount), wrapped)
		e.mseRecord(id, callCount, wrapped)
		return SideEffectFuture{executor: e, id: mseMarkerID(id, callCount), name: "MutableSideEffect"}
	}
	if e.isReplay {
		// 无历史条目（mseHistory 惰性重建，首次调用为空）。若该 callCount 有 marker → 是 first-run 的首次
		// record 正在 replay（照 Temporal handleMarkerRecorded 预载）→ 读回、shadow、录入历史。
		if existing, ok := e.marker("MutableSideEffect", mseMarkerID(id, callCount)); ok {
			e.shadowMarker("MutableSideEffect", mseMarkerID(id, callCount), existing)
			e.mseRecord(id, callCount, existing)
			return SideEffectFuture{executor: e, id: mseMarkerID(id, callCount), name: "MutableSideEffect"}
		}
		panicNonDeterministic("no cached marker for MutableSideEffect id=" + id)
	}
	newVal := api.invokeMSEClosure(f, ctx)
	wrapped := map[string]any{"value": api.markerValue(newVal)}
	e.recordMarker("MutableSideEffect", mseMarkerID(id, callCount), wrapped)
	e.mseRecord(id, callCount, wrapped)
	return SideEffectFuture{executor: e, id: mseMarkerID(id, callCount), name: "MutableSideEffect"}
}

// mseMarkerID 对齐 Temporal 的 id_callCount marker key 后缀。
func mseMarkerID(id string, cc int) string {
	return fmt.Sprintf("%s_%d", id, cc)
}

// mseLookup 照 lookupMutableSideEffect：查 ≤callCount 的最大已记录 callCount（因 dedup 后
// 不是每个 callCount 都有条目），并 GC 掉 ≤callCount 的非命中旧条目（历史只留最近一条，防无限增长）。
func (e *WorkflowExecutor) mseLookup(id string, callCount int) (int, map[string]any, bool) {
	hist := e.mseHistory[id]
	if len(hist) == 0 {
		return 0, nil, false
	}
	best := -1
	for cc := range hist {
		if cc <= callCount && cc > best {
			best = cc
		}
	}
	if best < 0 {
		return 0, nil, false
	}
	for cc := range hist {
		if cc <= callCount && cc != best {
			delete(hist, cc)
		}
	}
	return best, hist[best], true
}

// mseRecord 写 mseHistory[id][cc]=val（惰性建内层 map）。
func (e *WorkflowExecutor) mseRecord(id string, cc int, val map[string]any) {
	if e.mseHistory[id] == nil {
		e.mseHistory[id] = map[int]map[string]any{}
	}
	e.mseHistory[id][cc] = val
}

// invokeMSEClosure 执行 f 闭包（单参 ctx，复用 SideEffect 形态）取原生返回值；写入 marker 前经 markerValue 编码。
func (api *runtimeWorkflowAPI) invokeMSEClosure(f any, ctx workflowContext) any {
	cl, ok := f.(*wlruntime.Closure)
	if !ok {
		api.executor.evalErr = fmt.Errorf("workflow.MutableSideEffect f must be an inline closure, got %T", f)
		runtime.Goexit()
	}
	out, err := api.wlExec.InvokeClosure(cl, []types.Value{types.NewValue("workflow.Context", ctx)})
	if err != nil {
		api.executor.evalErr = err
		runtime.Goexit()
	}
	return out.Go()
}

// mseEquals 照 Temporal isEqualValue+ decodeValue：
// 归一——把 old JSON round-trip 到 newVal 的【动态类型】（reflect.New(TypeOf(newVal))），消除 int/int64 类型差；
// 再调用户 equals(new, old)（★ new 在前 equals(newValue, oldValue)）。
// 只在【非 replay】执行（replay 分支按历史重建，不跑 equals）。nil 特判：new/old 均 nil 视相等、单侧 nil 视不等。
func (api *runtimeWorkflowAPI) mseEquals(equals any, old, newVal any) bool {
	if newVal == nil || old == nil {
		return newVal == nil && old == nil
	}
	blob, err := json.Marshal(old)
	if err != nil {
		return false
	}
	var oldNorm any
	if record, ok := newVal.(*types.Record); ok {
		// typed record 的旧值在 marker 里是按字段编码的 map，经 registry 解回同类型的 record。
		decoded, err := api.registry.DecodeJSONValue(record.TypeName(), blob)
		if err != nil {
			return false
		}
		oldNorm = decoded.Go()
	} else {
		oldPtr := reflect.New(reflect.TypeOf(newVal)).Interface()
		if err := json.Unmarshal(blob, oldPtr); err != nil {
			return false // 归一失败视为不等（保守，产新 marker）
		}
		oldNorm = reflect.ValueOf(oldPtr).Elem().Interface()
	}
	cl, ok := equals.(*wlruntime.Closure)
	if !ok {
		api.executor.evalErr = fmt.Errorf("workflow.MutableSideEffect equals must be an inline closure, got %T", equals)
		runtime.Goexit()
	}
	out, err := api.wlExec.InvokeClosure(cl, []types.Value{
		equalsArg(newVal),  // a = newVal（new 在前）
		equalsArg(oldNorm), // b = oldNorm
	})
	if err != nil {
		api.executor.evalErr = err
		runtime.Goexit()
	}
	b, ok := out.Go().(bool)
	return ok && b
}

// equalsArg 把一侧的值包成 equals 闭包的实参：typed record 带自己的类型名，a.(T) 断言才成立。
func equalsArg(v any) types.Value {
	if record, ok := v.(*types.Record); ok {
		return types.NewValue(record.TypeName(), record)
	}
	return types.NewValue("any", v)
}

// GetVersion 照 Temporal workflowEnvironmentImpl.GetVersion：同一 changeID 已确定版本 → 校验后返回；
// replay 首见 → DefaultVersion；非 replay 首见 → maxSupported + marker + 版本搜索属性。
// 三条路径都校验支持范围，并把结果记入 changeVersions，供同一 Run 的后续调用复用。
func (api *runtimeWorkflowAPI) GetVersion(ctx workflowContext, changeID string, minSupported, maxSupported any) int64 {
	e := api.executor
	minVersion, maxVersion := int64FromAny(minSupported), int64FromAny(maxSupported)
	if existing, ok := e.marker(executor.VersionMarkerName, changeID); ok {
		e.shadowMarker(executor.VersionMarkerName, changeID, existing)
	}
	if version, ok := e.changeVersions[changeID]; ok {
		validateVersion(changeID, version, minVersion, maxVersion)
		return version
	}
	version := DefaultVersion
	if !e.isReplay {
		version = maxVersion
		e.recordMarker(executor.VersionMarkerName, changeID, map[string]any{"version": version})
		e.upsertChangeVersion(changeID, version)
	}
	validateVersion(changeID, version, minVersion, maxVersion)
	e.changeVersions[changeID] = version
	return version
}

// upsertChangeVersion 照 Temporal createSearchAttributesForChangeVersion：写入本次版本和
// 本 Run 已确定的全部版本，超过大小上限时只记录告警，版本语义由 marker 保证。
func (e *WorkflowExecutor) upsertChangeVersion(changeID string, version int64) {
	existing := make([]string, 0, len(e.changeVersions))
	for id, value := range e.changeVersions {
		existing = append(existing, changeVersionValue(id, value))
	}
	slices.Sort(existing)
	encoded := history.EncodeChangeVersions(append([]string{changeVersionValue(changeID, version)}, existing...))
	if len(encoded) >= changeVersionSearchAttrSizeLimit {
		slog.Warn("skipping change version search attribute: value exceeds size limit",
			slog.String("change_id", changeID),
			slog.Int("size", len(encoded)),
			slog.Int("limit", changeVersionSearchAttrSizeLimit),
		)
		return
	}
	e.emitCommand(history.Command{
		Type:  history.CmdUpsertSearchAttributes,
		Input: map[string]any{history.ChangeVersionSearchAttribute: encoded},
	})
}

func changeVersionValue(changeID string, version int64) string {
	return fmt.Sprintf("%s-%d", changeID, version)
}

// RandomUUID 照 Temporal SideEffect：marker 命中 → 读回；replay 缺 marker → panic
// 非确定性；非 replay 首见 → 算值 + recordMarker（就地写 markers）+ 同步返回。
func (api *runtimeWorkflowAPI) RandomUUID(ctx workflowContext) string {
	id := api.laneKey(ctx, "ru")
	if existing, ok := api.executor.marker("RandomUUID", id); ok {
		api.executor.shadowMarker("RandomUUID", id, existing)
		return stringFromMap(existing, "value")
	}
	if api.executor.isReplay {
		panicNonDeterministic("no cached marker for RandomUUID id=" + id)
	}
	value := uuid.NewString()
	api.executor.recordMarker("RandomUUID", id, map[string]any{"value": value})
	return value
}

// RandomInt 照 Temporal SideEffect：marker 命中 → 读回；replay 缺 marker → panic
// 非确定性；非 replay 首见 → 算值 + recordMarker（就地写 markers）+ 同步返回。
func (api *runtimeWorkflowAPI) RandomInt(ctx workflowContext, max any) int64 {
	id := api.laneKey(ctx, "ri")
	if existing, ok := api.executor.marker("RandomInt", id); ok {
		api.executor.shadowMarker("RandomInt", id, existing)
		return int64FromMap(existing, "value")
	}
	if api.executor.isReplay {
		panicNonDeterministic("no cached marker for RandomInt id=" + id)
	}
	limit := int64FromAny(max)
	var value int64
	if limit > 0 {
		randomValue, err := cryptorand.Int(cryptorand.Reader, big.NewInt(limit))
		if err != nil {
			panic(fmt.Errorf("workflow.RandomInt: %w", err))
		}
		value = randomValue.Int64()
	}
	api.executor.recordMarker("RandomInt", id, map[string]any{"value": value})
	return value
}

// DefaultVersion 是 workflow.DefaultVersion 在运行时的 int64 形态：replay 首见 GetVersion
// （history 缺 marker）返回此值，表示这段历史由加入 GetVersion 之前的代码写出。
const DefaultVersion int64 = workflow.DefaultVersion

const (
	// changeVersionSearchAttrSizeLimit 对齐 Temporal changeVersionSearchAttrSizeLimit。
	changeVersionSearchAttrSizeLimit = 2048
)

// validateVersion 照 Temporal validateVersion：GetVersion 命中
// 已存 version marker 时校验 min/max 边界。越界即静默非确定性——用户提高 minSupported（废弃旧版本分支）
// 后，老 run 的旧 marker version 会走本该报错的旧分支。DefaultVersion(-1) 不豁免，与 Temporal 一致：
// "add version" 场景调用方自身传 minSupported=DefaultVersion，故 version==-1 不会 < min。经
// panicNonDeterministic → recover 成 nonDeterministicError → 本轮 WFT 失败（Block），而非静默前进。
func validateVersion(changeID string, version, minSupported, maxSupported int64) {
	if version < minSupported {
		panicNonDeterministic(fmt.Sprintf("workflow code removed support of version %d for changeID %q. the oldest supported version is %d",
			version, changeID, minSupported))
	}
	if version > maxSupported {
		panicNonDeterministic(fmt.Sprintf("workflow code is too old to support version %d for changeID %q. the maximum supported version is %d",
			version, changeID, maxSupported))
	}
}
