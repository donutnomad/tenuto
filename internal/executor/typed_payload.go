package executor

import (
	"bytes"
	"encoding/json"
	"strings"

	wlregistry "github.com/donutnomad/wlang/registry"
	wltypes "github.com/donutnomad/wlang/types"
)

// DecodeTypedJSON 按类型 typeName 解码载荷 raw。解码之前去掉结构体里值为 null 的 slice、map 字段：
// wlang 把 null 解成没有载体的值，len、range、读 map 都会报 E_TYPE；字段缺省时它取类型的零值（nil slice、
// nil map），与 Go 的 encoding/json 解码 null 的结果相同，workflow.IsNil 仍判为 true。
func DecodeTypedJSON(reg *wlregistry.Registry, typeName string, raw json.RawMessage) (wltypes.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err == nil {
		if normalized, err := json.Marshal(DropNullCollectionFields(reg, typeName, payload)); err == nil {
			raw = normalized
		}
	}
	return reg.DecodeJSONValue(typeName, raw)
}

// DropNullCollectionFields 按类型描述符遍历 value，返回去掉结构体里 null 的 slice、map 字段之后的副本。
// map 的元素与 slice 的元素保留 null：Go 里它们是存在的条目。只能用于解码成 typeID 的载荷：解码目标是
// map[string]any 时 null 的键在 Go 里存在，不能去掉。
func DropNullCollectionFields(reg *wlregistry.Registry, typeID string, value any) any {
	if value == nil {
		return nil
	}
	if elem, ok := genericCollectionElem(typeID); ok {
		return mapCollection(reg, elem, value)
	}
	descriptor, ok := reg.Descriptor(typeID)
	if !ok {
		return value
	}
	switch descriptor.Kind {
	case wltypes.TypeKindAlias:
		return DropNullCollectionFields(reg, descriptor.Underlying, value)
	case wltypes.TypeKindPointer:
		return DropNullCollectionFields(reg, descriptor.Elem, value)
	case wltypes.TypeKindSlice, wltypes.TypeKindArray, wltypes.TypeKindMap:
		return mapCollection(reg, descriptor.Elem, value)
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
			out[key] = DropNullCollectionFields(reg, field.TypeID, item)
		}
		return out
	}
	return value
}

func mapCollection(reg *wlregistry.Registry, elem string, value any) any {
	switch collection := value.(type) {
	case []any:
		out := make([]any, len(collection))
		for i, item := range collection {
			out[i] = DropNullCollectionFields(reg, elem, item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(collection))
		for key, item := range collection {
			out[key] = DropNullCollectionFields(reg, elem, item)
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
