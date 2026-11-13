package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// WireType 是一个与 Go 包名、类型名无关的 JSON wire 结构描述。
//
// 契约对照必须基于 wire 结构：调用方 `checkout.ChargeInput` 与远端
// `payments.PayRequest` 的 Go 身份不同，只要 JSON 字段名、结构、类型和
// 可选性一致就兼容。字段顺序不参与比较（按 JSON 字段名排序）。
type WireType struct {
	Kind     string      `json:"kind"`
	Name     string      `json:"name,omitempty"`
	Length   int64       `json:"length,omitempty"`
	Fields   []WireField `json:"fields,omitempty"`
	Elem     *WireType   `json:"elem,omitempty"`
	Key      *WireType   `json:"key,omitempty"`
	Optional bool        `json:"optional,omitempty"`
}

// WireField 是 record 的一个字段。
type WireField struct {
	JSON      string   `json:"json"`
	Type      WireType `json:"type"`
	OmitEmpty bool     `json:"omitempty,omitempty"`
}

// WireKind 是 WireType.Kind 的取值。
const (
	WireKindScalar  = "scalar"
	WireKindRecord  = "record"
	WireKindSlice   = "slice"
	WireKindArray   = "array"
	WireKindMap     = "map"
	WireKindPointer = "pointer"
	// WireKindRecursive 是递归类型的回指：Length 是从当前位置向外数第几个正在展开的具名类型（1 是最近的一个）。
	// 与 Go 包名、类型名无关，两侧对同一递归结构得到同一指纹。
	WireKindRecursive = "recursive"
)

// WireFingerprint 返回稳定的 wire 结构指纹（不包含 Go 类型身份）。
func WireFingerprint(schema WireType) string {
	normalized := normalizeWireType(schema)
	raw, _ := json.Marshal(normalized)
	sum := sha256.Sum256(raw)
	return "wire:" + hex.EncodeToString(sum[:16])
}

func normalizeWireType(schema WireType) WireType {
	switch schema.Kind {
	case WireKindRecord:
		fields := append([]WireField(nil), schema.Fields...)
		sort.Slice(fields, func(i, j int) bool { return fields[i].JSON < fields[j].JSON })
		for i := range fields {
			fields[i].Type = normalizeWireType(fields[i].Type)
		}
		schema.Fields = fields
	case WireKindSlice, WireKindArray, WireKindPointer:
		if schema.Elem != nil {
			elem := normalizeWireType(*schema.Elem)
			schema.Elem = &elem
		}
	case WireKindMap:
		if schema.Key != nil {
			key := normalizeWireType(*schema.Key)
			schema.Key = &key
		}
		if schema.Elem != nil {
			elem := normalizeWireType(*schema.Elem)
			schema.Elem = &elem
		}
	}
	return schema
}

// WireIncompatibility 是旧 wire 结构的一个位置在新结构下读不回原值的原因。Path 是 JSON 路径：字段用 `.` 连接，
// 切片与数组元素写 `[]`，map 的值写 `[*]`；根位置为空。
type WireIncompatibility struct {
	Path   string
	Reason string
}

func (i WireIncompatibility) String() string {
	if i.Path == "" {
		return i.Reason
	}
	return i.Path + ": " + i.Reason
}

// CompareWireTypes 判断按 old 写出的 JSON 能否被 new 无损读回，返回全部不兼容的位置；nil 表示兼容。
//
// 规则（保守）：
//   - record：old 的每个字段在 new 里以同一 JSON 名存在且类型兼容；new 新增的字段允许（旧载荷里取零值）。
//     字段删除或改名都表现为 old 的字段在 new 里不存在。
//   - 标量：名字相同；或无损放宽（有符号整数变宽、无符号整数变宽或转成更宽的有符号整数、32 位以内整数转 float64、
//     16 位以内整数转 float32、float32 转 float64）。其余改动不兼容，包括 int64 与 float64 互转。
//   - new 是 any 时接受一切；old 是 any 时旧载荷的结构未知，不判为不兼容。old 是值类型为 any 的 map、new 是 record
//     时同理。
//   - 指针：T 改成 *T 兼容；*T 改成 T 不兼容（null 与零值无法区分）。
//   - 切片、数组、map 递归比较元素（map 还比较键）；数组改成切片兼容，切片改成数组或数组长度改变不兼容。
//   - 递归回指只与同一深度的递归回指兼容。
func CompareWireTypes(old, new WireType) []WireIncompatibility {
	var issues []WireIncompatibility
	compareWire(old, new, "", &issues)
	return issues
}

func compareWire(old, new WireType, path string, issues *[]WireIncompatibility) {
	report := func(reason string) {
		*issues = append(*issues, WireIncompatibility{Path: path, Reason: reason})
	}
	if isWireAny(new) || isWireAny(old) {
		return
	}
	if new.Kind == WireKindPointer && new.Elem != nil {
		if old.Kind == WireKindPointer && old.Elem != nil {
			compareWire(*old.Elem, *new.Elem, path, issues)
			return
		}
		compareWire(old, *new.Elem, path, issues)
		return
	}
	if old.Kind == WireKindPointer {
		report("optional value became required (null can no longer be told apart from the zero value)")
		return
	}
	switch {
	case old.Kind == WireKindScalar && new.Kind == WireKindScalar:
		if !scalarWidens(old.Name, new.Name) {
			report(fmt.Sprintf("type changed from %s to %s", describeWire(old), describeWire(new)))
		}
	case old.Kind == WireKindRecord && new.Kind == WireKindRecord:
		fields := make(map[string]WireType, len(new.Fields))
		for _, field := range new.Fields {
			fields[field.JSON] = field.Type
		}
		for _, field := range sortedWireFields(old.Fields) {
			child := joinWirePath(path, field.JSON)
			newType, ok := fields[field.JSON]
			if !ok {
				*issues = append(*issues, WireIncompatibility{Path: child, Reason: "field is missing in the new type (removed or renamed)"})
				continue
			}
			compareWire(field.Type, newType, child, issues)
		}
	case old.Kind == WireKindRecord && new.Kind == WireKindMap && new.Elem != nil:
		if new.Key == nil || !isWireString(*new.Key) {
			report(fmt.Sprintf("type changed from %s to %s", describeWire(old), describeWire(new)))
			return
		}
		for _, field := range sortedWireFields(old.Fields) {
			compareWire(field.Type, *new.Elem, joinWirePath(path, field.JSON), issues)
		}
	case old.Kind == WireKindMap && new.Kind == WireKindRecord && old.Elem != nil && isWireAny(*old.Elem):
		// 值为 any 的 map：旧载荷有哪些键未知，不判定。
	case old.Kind == WireKindMap && new.Kind == WireKindMap && old.Key != nil && new.Key != nil && old.Elem != nil && new.Elem != nil:
		compareWire(*old.Key, *new.Key, path+"[key]", issues)
		compareWire(*old.Elem, *new.Elem, path+"[*]", issues)
	case (old.Kind == WireKindSlice || old.Kind == WireKindArray) && new.Kind == WireKindSlice && old.Elem != nil && new.Elem != nil:
		compareWire(*old.Elem, *new.Elem, path+"[]", issues)
	case old.Kind == WireKindArray && new.Kind == WireKindArray && old.Elem != nil && new.Elem != nil:
		if old.Length != new.Length {
			report(fmt.Sprintf("array length changed from %d to %d", old.Length, new.Length))
			return
		}
		compareWire(*old.Elem, *new.Elem, path+"[]", issues)
	case old.Kind == WireKindRecursive && new.Kind == WireKindRecursive:
		if old.Length != new.Length {
			report("recursive structure changed")
		}
	default:
		report(fmt.Sprintf("type changed from %s to %s", describeWire(old), describeWire(new)))
	}
}

func isWireAny(schema WireType) bool {
	return schema.Kind == WireKindScalar && schema.Name == "any"
}

func isWireString(schema WireType) bool {
	return schema.Kind == WireKindScalar && schema.Name == "string"
}

func joinWirePath(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
}

func sortedWireFields(fields []WireField) []WireField {
	sorted := append([]WireField(nil), fields...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].JSON < sorted[j].JSON })
	return sorted
}

// describeWire 是错误消息里的类型写法。
func describeWire(schema WireType) string {
	switch schema.Kind {
	case WireKindScalar:
		return schema.Name
	case WireKindRecord:
		return "object"
	case WireKindPointer:
		if schema.Elem != nil {
			return "*" + describeWire(*schema.Elem)
		}
	case WireKindSlice:
		if schema.Elem != nil {
			return "[]" + describeWire(*schema.Elem)
		}
	case WireKindArray:
		if schema.Elem != nil {
			return fmt.Sprintf("[%d]%s", schema.Length, describeWire(*schema.Elem))
		}
	case WireKindMap:
		if schema.Key != nil && schema.Elem != nil {
			return "map[" + describeWire(*schema.Key) + "]" + describeWire(*schema.Elem)
		}
	}
	return schema.Kind
}

// scalarWidens 报告 old 标量的每个 JSON 值能否被 new 标量无损读回。
func scalarWidens(old, new string) bool {
	if old == new {
		return true
	}
	oldBits, oldSigned, oldInt := integerScalar(old)
	newBits, newSigned, newInt := integerScalar(new)
	switch {
	case oldInt && newInt:
		if oldSigned == newSigned {
			return newBits >= oldBits
		}
		return !oldSigned && newSigned && newBits > oldBits
	case oldInt && new == "float64":
		return oldBits <= 32
	case oldInt && new == "float32":
		return oldBits <= 16
	case old == "float32" && new == "float64":
		return true
	}
	return false
}

func integerScalar(name string) (bits int, signed bool, ok bool) {
	switch name {
	case "int8":
		return 8, true, true
	case "int16":
		return 16, true, true
	case "int32":
		return 32, true, true
	case "int64":
		return 64, true, true
	case "uint8":
		return 8, false, true
	case "uint16":
		return 16, false, true
	case "uint32":
		return 32, false, true
	case "uint64":
		return 64, false, true
	}
	return 0, false, false
}
