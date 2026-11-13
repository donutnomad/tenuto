package interpreter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	wlregistry "github.com/donutnomad/wlang/registry"
	"github.com/donutnomad/wlang/types"
)

func anyToMap(v any) map[string]any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	data, err := json.Marshal(v)
	if err != nil {
		return map[string]any{"value": v}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return map[string]any{"value": v}
	}
	return out
}

// RecordToMap 是持久化边界上 anyToMap 的 registry-aware 替代。
//
// 为什么必须有它：types.Record 的三个字段全部未导出且它没有 MarshalJSON，
// 所以 json.Marshal(record) 产出 "{}" 且 **err == nil**——anyToMap 会静默返回
// 空 map，不报错、不进 {"value": v} 兜底，数据完全消失且无任何信号。
//
// 值里任何位置有 record（顶层，或 map、slice 的元素里）时走 registry.EncodeJSONValue，
// 按类型表把每个 record 的字段编出来（JSON tag 生效）。
//
// reg 为 nil 或值里没有 record 时，行为与 anyToMap 逐字节一致——调用点可以无脑替换。
func RecordToMap(reg *wlregistry.Registry, v any) (map[string]any, error) {
	if reg == nil || !containsRecord(v) {
		return anyToMap(v), nil
	}
	raw, err := reg.EncodeJSONValue(types.NewValue(types.TAny, v))
	if err != nil {
		return nil, fmt.Errorf("encode workflow value: %w", err)
	}
	// UseNumber 保大整数精度：事件字段会被持久化并跨进程回传，
	// float64 化会让 int64 在 2^53 之外悄悄失真。
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var out any
	if err := decoder.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode workflow value: %w", err)
	}
	if object, ok := out.(map[string]any); ok {
		return object, nil
	}
	return map[string]any{"value": out}, nil
}

// containsRecord 判断值里是否有 record。
func containsRecord(v any) bool {
	switch current := v.(type) {
	case *types.Record:
		return true
	case []any:
		for _, item := range current {
			if containsRecord(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range current {
			if containsRecord(item) {
				return true
			}
		}
	}
	return false
}

// mustRecordToMap 给不返回 error 的 host 方法用（ExecuteActivity /
// ExecuteChildWorkflow / ContinueAsNew 的签名都不带 error 返回值）。
//
// 编码失败即 panic：body 任意深处的 panic 都会被 Execute 主协程的 recover 接住
// 转成 execErr → 本轮 WFT 失败，所以「没有错误通道」并不成立。
//
// 为什么不能静默回落：record 编码失败意味着 descriptor 与值不匹配，回落到
// anyToMap 会写入一个空 map 并当作成功继续推进——activity 拿到空输入、
// 历史里留下空载荷，而没有任何信号。宁可让这一轮 WFT 失败。
func mustRecordToMap(reg *wlregistry.Registry, v any) map[string]any {
	out, err := RecordToMap(reg, v)
	if err != nil {
		panic(fmt.Errorf("encode workflow payload: %w", err))
	}
	return out
}

func durationFromAny(value any) (time.Duration, error) {
	switch v := value.(type) {
	case time.Duration:
		return v, nil
	case int:
		return time.Duration(v), nil
	case int8:
		return time.Duration(v), nil
	case int16:
		return time.Duration(v), nil
	case int32:
		return time.Duration(v), nil
	case int64:
		return time.Duration(v), nil
	case uint:
		return time.Duration(v), nil
	case uint8:
		return time.Duration(v), nil
	case uint16:
		return time.Duration(v), nil
	case uint32:
		return time.Duration(v), nil
	case uint64:
		if v > uint64(math.MaxInt64) {
			return 0, fmt.Errorf("workflow sleep duration overflows int64: %d", v)
		}
		return time.Duration(v), nil
	case float32:
		return time.Duration(v), nil
	case float64:
		return time.Duration(v), nil
	case string:
		return time.ParseDuration(v)
	default:
		return 0, fmt.Errorf("unsupported workflow sleep duration type: %T", value)
	}
}

func int64FromMap(value map[string]any, key string) int64 {
	if value == nil {
		return 0
	}
	return int64FromAny(value[key])
}

func int64FromAny(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v > uint64(math.MaxInt64) {
			return math.MaxInt64
		}
		return int64(v)
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return 0
}

func stringFromMap(value map[string]any, key string) string {
	if value == nil {
		return ""
	}
	raw := value[key]
	if s, ok := raw.(string); ok {
		return s
	}
	return fmt.Sprint(raw)
}
