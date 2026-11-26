package executor

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	wlregistry "github.com/donutnomad/wlang/registry"
	wltypes "github.com/donutnomad/wlang/types"
)

// DecodeTypedJSON 按类型 typeName 解码载荷 raw，解码之前用 NormalizeTypedPayload 规范化，结果与 Go 的
// encoding/json 解码进同一类型相同。
func DecodeTypedJSON(reg *wlregistry.Registry, typeName string, raw json.RawMessage) (wltypes.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err == nil {
		if normalized, err := json.Marshal(NormalizeTypedPayload(reg, typeName, payload)); err == nil {
			raw = normalized
		}
	}
	return reg.DecodeJSONValue(typeName, raw)
}

// NormalizeTypedPayload 按类型描述符遍历 value，返回交给 wlang 解码之后与 Go 的 encoding/json 结果一致的副本：
//
//   - 去掉结构体里为 null 的 slice、map 字段：wlang 把 null 解成没有载体的值，len、range、读 map 会报 E_TYPE；
//     字段缺省时它取类型的零值（nil slice、nil map），与 Go 解码 null 的结果相同，workflow.IsNil 仍为 true。
//     map 的元素与 slice 的元素保留 null：Go 里它们是存在的条目。
//   - 结构体里声明类型是 any 的位置（any 字段、map[string]any 字段的值、[]any 字段的元素，及其内部）里的数字
//     写成带小数点的字面量：wlang 把不带小数点的 JSON 数字解成 int64，Go 解成 float64；带小数点时它同样解成
//     float64，取值、断言、比较与再编码都与 Go 相同。载荷本身是 map[string]any 时不改，与 Workflow 输入、
//     Signal 的 map[string]any 载荷一致（数字是 json.Number，workflow.Equal 按 float64 比较）。
//
// 只能用于解码成 typeID 的载荷：解码目标是 map[string]any 时 null 的键在 Go 里存在，不能去掉。
func NormalizeTypedPayload(reg *wlregistry.Registry, typeID string, value any) any {
	return normalizeTyped(reg, typeID, value, false)
}

// normalizeTyped 是 NormalizeTypedPayload 的遍历；inRecord 表示 value 位于某个结构体之内。
func normalizeTyped(reg *wlregistry.Registry, typeID string, value any, inRecord bool) any {
	if value == nil {
		return nil
	}
	if typeID == wltypes.TAny {
		if inRecord {
			return floatNumbers(value)
		}
		return value
	}
	if elem, ok := genericCollectionElem(typeID); ok {
		return mapCollection(reg, elem, value, inRecord)
	}
	descriptor, ok := reg.Descriptor(typeID)
	if !ok {
		return value
	}
	switch descriptor.Kind {
	case wltypes.TypeKindAlias:
		return normalizeTyped(reg, descriptor.Underlying, value, inRecord)
	case wltypes.TypeKindPointer:
		return normalizeTyped(reg, descriptor.Elem, value, inRecord)
	case wltypes.TypeKindSlice, wltypes.TypeKindArray, wltypes.TypeKindMap:
		return mapCollection(reg, descriptor.Elem, value, inRecord)
	case wltypes.TypeKindRecord:
		object, ok := value.(map[string]any)
		if !ok {
			return value
		}
		fields := make(map[string]wltypes.FieldDescriptor, len(descriptor.Fields))
		for _, field := range descriptor.Fields {
			fields[field.GoName] = field
			if field.JSONName != "" {
				fields[field.JSONName] = field
			}
		}
		out := make(map[string]any, len(object))
		for key, item := range object {
			field, known := fields[key]
			if !known {
				out[key] = item
				continue
			}
			if item == nil && isCollectionType(reg, field.TypeID) {
				continue
			}
			out[key] = normalizeTyped(reg, field.TypeID, item, true)
		}
		return out
	}
	return value
}

// floatNumbers 把 any 位置里的数字换成带小数点的 JSON 字面量（json.Number），值与 Go 解码成的 float64 相同。
func floatNumbers(value any) any {
	switch current := value.(type) {
	case json.Number:
		return floatLiteral(current.String(), value)
	case float64:
		return floatLiteral(strconv.FormatFloat(current, 'g', -1, 64), value)
	case int:
		return floatLiteral(strconv.Itoa(current), value)
	case int64:
		return floatLiteral(strconv.FormatInt(current, 10), value)
	case []any:
		out := make([]any, len(current))
		for i, item := range current {
			out[i] = floatNumbers(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(current))
		for key, item := range current {
			out[key] = floatNumbers(item)
		}
		return out
	}
	return value
}

// floatLiteral 是 raw 按 float64 解析后带小数点的写法；超出 float64 范围时原样返回 original。
func floatLiteral(raw string, original any) any {
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return original
	}
	text := strconv.FormatFloat(parsed, 'g', -1, 64)
	if !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return json.Number(text)
}

func mapCollection(reg *wlregistry.Registry, elem string, value any, inRecord bool) any {
	switch collection := value.(type) {
	case []any:
		out := make([]any, len(collection))
		for i, item := range collection {
			out[i] = normalizeTyped(reg, elem, item, inRecord)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(collection))
		for key, item := range collection {
			out[key] = normalizeTyped(reg, elem, item, inRecord)
		}
		return out
	}
	return value
}

func isCollectionType(reg *wlregistry.Registry, typeID string) bool {
	if _, ok := genericCollectionElem(typeID); ok {
		return true
	}
	descriptor, ok := reg.Descriptor(typeID)
	if !ok {
		return false
	}
	switch descriptor.Kind {
	case wltypes.TypeKindAlias:
		return isCollectionType(reg, descriptor.Underlying)
	case wltypes.TypeKindSlice, wltypes.TypeKindMap:
		return true
	}
	return false
}

// genericCollectionElem 解析没有描述符的集合类型名 array<T> 与 map<string,T>，返回元素类型。
func genericCollectionElem(typeID string) (string, bool) {
	if !strings.HasSuffix(typeID, ">") {
		return "", false
	}
	if elem, ok := strings.CutPrefix(typeID, "array<"); ok {
		return strings.TrimSuffix(elem, ">"), true
	}
	if elem, ok := strings.CutPrefix(typeID, "map<string,"); ok {
		return strings.TrimSuffix(elem, ">"), true
	}
	return "", false
}
