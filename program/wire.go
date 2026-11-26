package program

import (
	"github.com/donutnomad/tenuto/protocol"
)

// WLangWireFingerprint 把 WLang type descriptor ID 转成与 Go 包名/类型名无关的
// wire 结构指纹。Workflow 声明侧（Program.Types）与 Worker 上报侧
// （activity.ContractOf / activity.WireFingerprintOf）使用同一算法，跨模块独立
// DTO 只要 JSON 字段名、结构、类型与可选性一致就匹配。
func WLangWireFingerprint(typeID string, types map[string]WLangTypeDescriptor) (string, error) {
	if typeID == "" {
		return "", nil
	}
	schema, err := WLangWireType(typeID, types)
	if err != nil {
		return "", err
	}
	return protocol.WireFingerprint(schema), nil
}

// WLangWireType 把 WLang type descriptor ID 转成 wire 结构，与 activity.ContractOf 从 Go 类型得到的结构同构。
func WLangWireType(typeID string, types map[string]WLangTypeDescriptor) (protocol.WireType, error) {
	return resolveWireType(typeID, types, nil)
}

// resolveWireType 把描述符 ID 转成 wire 结构。stack 是正在展开的具名类型（record 与 alias 描述符），
// 递归类型再次遇到其中一个时写成 recursive 回指，与 activity.WireFingerprintOf 的 Go 类型侧一致。
func resolveWireType(typeID string, types map[string]WLangTypeDescriptor, stack []string) (protocol.WireType, error) {
	if scalar, ok := builtinWireScalar(typeID); ok {
		return scalar, nil
	}
	descriptor, ok := types[typeID]
	if !ok {
		return protocol.WireType{}, protocol.InvalidRequestf("type descriptor %q not found", typeID)
	}
	if descriptor.Kind == "alias" || descriptor.Kind == "record" {
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i] == typeID {
				return protocol.WireType{Kind: protocol.WireKindRecursive, Length: int64(len(stack) - i)}, nil
			}
		}
		stack = append(stack[:len(stack):len(stack)], typeID)
	}
	switch descriptor.Kind {
	case "alias":
		return resolveWireType(descriptor.Underlying, types, stack)
	case "record":
		fields := make([]protocol.WireField, 0, len(descriptor.Fields))
		for _, field := range descriptor.Fields {
			if field.JSONName == "-" {
				continue // json:"-"：wire 上不存在该字段
			}
			wire, err := resolveWireType(field.TypeID, types, stack)
			if err != nil {
				return protocol.WireType{}, protocol.InvalidRequestf("field %s: %w", field.JSONName, err)
			}
			fields = append(fields, protocol.WireField{JSON: field.JSONName, Type: wire, OmitEmpty: field.OmitEmpty})
		}
		return protocol.WireType{Kind: protocol.WireKindRecord, Fields: fields}, nil
	case "pointer":
		elem, err := resolveWireType(descriptor.Elem, types, stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindPointer, Elem: &elem, Optional: true}, nil
	case "slice":
		elem, err := resolveWireType(descriptor.Elem, types, stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindSlice, Elem: &elem}, nil
	case "array":
		elem, err := resolveWireType(descriptor.Elem, types, stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindArray, Elem: &elem, Length: descriptor.Length}, nil
	case "map":
		key, err := resolveWireType(descriptor.Key, types, stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		elem, err := resolveWireType(descriptor.Elem, types, stack)
		if err != nil {
			return protocol.WireType{}, err
		}
		return protocol.WireType{Kind: protocol.WireKindMap, Key: &key, Elem: &elem}, nil
	default:
		return protocol.WireType{}, protocol.InvalidRequestf("type descriptor %q has unsupported kind %q (not a wire-compatible type)", descriptor.ID, descriptor.Kind)
	}
}

func builtinWireScalar(typeID string) (protocol.WireType, bool) {
	switch typeID {
	case "string", "boolean", "any", "error", "bigInt", "bigDecimal",
		"int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64",
		"float32", "float64":
		return protocol.WireType{Kind: "scalar", Name: typeID}, true
	}
	return protocol.WireType{}, false
}
