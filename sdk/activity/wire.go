package activity

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/donutnomad/tenuto/protocol"
)

// WireFingerprintOf 从 Go 类型计算 wire 结构指纹。I/O 类型必须是 codec
// 支持的子集：基础值、record、指针、slice/array、string-key map。
func WireFingerprintOf[T any]() (string, error) {
	schema, err := wireTypeOfGo(reflect.TypeOf((*T)(nil)).Elem(), nil)
	if err != nil {
		return "", err
	}
	return protocol.WireFingerprint(schema), nil
}

// ContractOf 为独立部署的 Activity 生成契约元数据：Type/Version 显式声明，
// Input/Output 使用与 Workflow 侧同算法计算的 wire 指纹。
func ContractOf[I, O any](activityType, version string) (Contract, error) {
	input, err := WireFingerprintOf[I]()
	if err != nil {
		return Contract{}, fmt.Errorf("activity %s input: %w", activityType, err)
	}
	output, err := WireFingerprintOf[O]()
	if err != nil {
		return Contract{}, fmt.Errorf("activity %s output: %w", activityType, err)
	}
	return Contract{Type: activityType, Version: version, Input: input, Output: output}, nil
}

// wireTypeOfGo 把 Go 类型转换为 wire 结构。stack 是正在展开的具名类型；递归类型再次遇到其中一个时
// 写成 recursive 回指，与 program.WLangWireFingerprint 的描述符侧一致（具名类型对应 record 与 alias 描述符）。
func wireTypeOfGo(rt reflect.Type, stack []reflect.Type) (protocol.WireType, error) {
	if rt == nil {
		return protocol.WireType{}, fmt.Errorf("nil type")
	}
	if rt.Name() != "" && rt.PkgPath() != "" {
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i] == rt {
				return protocol.WireType{Kind: protocol.WireKindRecursive, Length: int64(len(stack) - i)}, nil
			}
		}
		stack = append(stack[:len(stack):len(stack)], rt)
	}
	if rt.Kind() == reflect.Pointer {
		schema, err := wireTypeOfGo(rt.Elem(), stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindPointer, Elem: &schema, Optional: true}, nil
	}
	if rt.Kind() == reflect.Interface {
		// 与 descriptor 侧 "any" 内置标量对齐（只允许空接口，其它接口无法描述）。
		if rt.NumMethod() == 0 {
			return scalarWireType("any"), nil
		}
		return protocol.WireType{}, fmt.Errorf("interface type %s: only empty interface is wire-compatible", rt)
	}
	if rt.Kind() == reflect.Uint {
		return protocol.WireType{}, fmt.Errorf("type %s: platform-dependent uint is not supported; use uint8/16/32/64", rt)
	}
	if rt.Kind() == reflect.Slice && rt.Elem().Kind() == reflect.Uint8 {
		return protocol.WireType{}, fmt.Errorf("type %s: []byte encodes as base64 string and is not a supported wire type", rt)
	}
	if implementsCustomJSON(rt) {
		return protocol.WireType{}, fmt.Errorf("type %s: custom JSON/text codec is not a supported wire type", rt)
	}
	switch rt.Kind() {
	case reflect.String:
		return scalarWireType("string"), nil
	case reflect.Bool:
		return scalarWireType("boolean"), nil
	case reflect.Int:
		// WLang 把 Go int 归一化为 int64（go2wlang goTypeToWlang）。
		return scalarWireType("int64"), nil
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return scalarWireType(rt.Kind().String()), nil
	case reflect.Float32, reflect.Float64:
		return scalarWireType(rt.Kind().String()), nil
	case reflect.Slice:
		elem, err := wireTypeOfGo(rt.Elem(), stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindSlice, Elem: &elem}, nil
	case reflect.Array:
		elem, err := wireTypeOfGo(rt.Elem(), stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindArray, Elem: &elem, Length: int64(rt.Len())}, nil
	case reflect.Map:
		if rt.Key().Kind() != reflect.String {
			return protocol.WireType{}, fmt.Errorf("map type %s: only string keys are supported", rt)
		}
		key, err := wireTypeOfGo(rt.Key(), stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		elem, err := wireTypeOfGo(rt.Elem(), stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindMap, Key: &key, Elem: &elem}, nil
	case reflect.Struct:
		fields := make([]protocol.WireField, 0, rt.NumField())
		for i := 0; i < rt.NumField(); i++ {
			field := rt.Field(i)
			if field.PkgPath != "" {
				continue // unexported
			}
			if field.Anonymous {
				return protocol.WireType{}, fmt.Errorf("field %s: embedded fields are not a supported wire type", field.Name)
			}
			name, omitEmpty, skip, err := jsonFieldName(field)
			if err != nil {
				return protocol.WireType{}, fmt.Errorf("field %s: %w", field.Name, err)
			}
			if skip {
				continue
			}
			fieldType, err := wireTypeOfGo(field.Type, stack)
			if err != nil {
				return protocol.WireType{}, fmt.Errorf("field %s: %w", field.Name, err)
			}
			fields = append(fields, protocol.WireField{JSON: name, Type: fieldType, OmitEmpty: omitEmpty})
		}
		return protocol.WireType{Kind: protocol.WireKindRecord, Fields: fields}, nil
	default:
		return protocol.WireType{}, fmt.Errorf("type %s: unsupported kind %s", rt, rt.Kind())
	}
}

func scalarWireType(name string) protocol.WireType {
	return protocol.WireType{Kind: protocol.WireKindScalar, Name: name}
}

func jsonFieldName(field reflect.StructField) (name string, omitEmpty bool, skip bool, err error) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false, true, nil
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	if name == "" {
		name = field.Name
	}
	for _, option := range parts[1:] {
		switch option {
		case "":
		case "omitempty":
			omitEmpty = true
		default:
			return "", false, false, fmt.Errorf("json option %q is not a supported wire type", option)
		}
	}
	return name, omitEmpty, false, nil
}

func implementsCustomJSON(rt reflect.Type) bool {
	jsonMarshaler := reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	textMarshaler := reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	for _, candidate := range []reflect.Type{rt, reflect.PointerTo(rt)} {
		if candidate.Implements(jsonMarshaler) || candidate.Implements(textMarshaler) {
			return true
		}
	}
	return false
}
