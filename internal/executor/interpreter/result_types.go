package interpreter

import (
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	wlregistry "github.com/donutnomad/wlang/registry"
	wltypes "github.com/donutnomad/wlang/types"
)

// payloadNormalizer 在 Signal、Activity 结果与子 Workflow 结果进入解释器之前，按 Program 解码它们用的类型规范化
// 载荷，使 Workflow 代码读到的值与 Go 的 encoding/json 解码进同一类型的结果相同。
//
// 宿主在 Get 时不知道 out 的类型：Get 的 out 形参是 *map[string]any，out 是结构体时 wlang 传入一个新建的 map，
// Get 返回后在 wlang 内部按结构体类型解码；out 是 map[string]any 时传入变量自己的 map。两种情况在宿主看来相同，
// 所以按 program.PayloadTypes 在载荷进入解释器时决定（每个 Signal 名、Activity type、子 Workflow Type 的载荷被
// Get 进的 out 变量的类型）：
//
//   - 所有解码目标都是 map[string]any、[]any 或 any：数字换成 float64（executor.GoJSONNumbers），与 Go 解码进
//     map[string]any 相同；
//   - 所有解码目标是同一个结构体类型：按它规范化（executor.NormalizeTypedPayload）：去掉 null 的 slice、map
//     字段，any 位置的数字按 float64 解码，int64 等字段的整数保持精确；
//   - 其余情况（几处目标类型不同、有一处类型无法确定、目标是其他类型）载荷原样交给解释器：解进结构体时整数保持
//     精确；解进 map[string]any 时整数是 int64，null 的集合字段原样交给 wlang。
//
// 位置无法确定的解码处按它可能对应的全部位置处理。
type payloadNormalizer struct {
	reg *wlregistry.Registry
	// targets 是每个位置、每个种类（Name 为空）与全部载荷（Kind 为空）各自的解码目标：结构体的类型名，
	// map[string]any、[]any、any 记为 untypedTarget，其他类型记为类型名。
	targets map[program.PayloadPosition][]string
	// unknown 是有类型无法确定的解码目标的位置，键的含义与 targets 相同。
	unknown map[program.PayloadPosition]bool
	// records 是 targets 里的结构体类型名。
	records map[string]bool
}

// untypedTarget 表示解码目标是 map[string]any、[]any 或 any：数字按 Go 的规则是 float64。
const untypedTarget = "<untyped>"

func newPayloadNormalizer(wf *program.WorkflowConfig) *payloadNormalizer {
	normalizer := &payloadNormalizer{
		targets: map[program.PayloadPosition][]string{},
		unknown: map[program.PayloadPosition]bool{},
		records: map[string]bool{},
	}
	all := program.PayloadPosition{}
	if wf == nil {
		normalizer.unknown[all] = true
		return normalizer
	}
	normalizer.reg = wlregistry.New()
	types, err := program.PayloadTypes(wf)
	if err != nil || normalizer.reg.BindDescriptors(wf.Program.Types) != nil {
		normalizer.unknown[all] = true
		return normalizer
	}
	for position, decoded := range types.Positions {
		for _, payload := range decoded {
			normalizer.add(position, payload.Name)
		}
	}
	for _, site := range types.Unresolved {
		normalizer.add(site.Position, site.Type)
	}
	return normalizer
}

func (n *payloadNormalizer) add(position program.PayloadPosition, typeName string) {
	switch position.Kind {
	case "", program.PayloadSignal, program.PayloadActivityResult, program.PayloadChildResult:
	default:
		return
	}
	switch {
	case typeName == "":
		n.unknown[position] = true
		return
	case typeName == wltypes.TAny || typeName == "map<string,any>" || typeName == "array<any>":
		typeName = untypedTarget
	case n.isRecord(typeName):
		n.records[typeName] = true
	}
	n.targets[position] = append(n.targets[position], typeName)
}

// isRecord 报告 typeName（可以是别名或指针）是否为结构体。
func (n *payloadNormalizer) isRecord(typeName string) bool {
	for range 16 {
		descriptor, ok := n.reg.Descriptor(typeName)
		if !ok {
			return false
		}
		switch descriptor.Kind {
		case wltypes.TypeKindRecord:
			return true
		case wltypes.TypeKindAlias:
			typeName = descriptor.Underlying
		case wltypes.TypeKindPointer:
			typeName = descriptor.Elem
		default:
			return false
		}
	}
	return false
}

// normalize 返回 kind 位置 name 的载荷规范化之后的副本；解码目标有歧义时原样返回。
func (n *payloadNormalizer) normalize(kind program.PayloadPositionKind, name string, payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	target := ""
	for _, scope := range []program.PayloadPosition{{}, {Kind: kind}, {Kind: kind, Name: name}} {
		if n.unknown[scope] {
			return payload
		}
		for _, typeName := range n.targets[scope] {
			if target != "" && typeName != target {
				return payload
			}
			target = typeName
		}
	}
	switch {
	case target == untypedTarget:
		return executor.GoJSONNumbers(payload).(map[string]any)
	case n.records[target]:
		normalized, _ := executor.NormalizeTypedPayload(n.reg, target, payload).(map[string]any)
		return normalized
	}
	return payload
}

// normalizePayload 规范化 Signal、Activity 结果与子 Workflow 结果；normalizer 在第一次用到时按 Program 构造。
func (e *WorkflowExecutor) normalizePayload(kind program.PayloadPositionKind, name string, payload map[string]any) map[string]any {
	if e.payloadNormalizer == nil {
		e.payloadNormalizer = newPayloadNormalizer(e.workflow)
	}
	return e.payloadNormalizer.normalize(kind, name, payload)
}
