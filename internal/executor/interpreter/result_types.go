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
// 解码类型来自 program.PayloadTypes：每个 Signal 名、Activity type、子 Workflow Type 的载荷被 Get 进的 out 变量的
// 类型。载荷进入解释器时还不知道哪一处 Get 会读它：
//
//   - 所有可能的解码目标是同一个结构体类型时，按它规范化（executor.NormalizeTypedPayload）：去掉 null 的
//     slice、map 字段，any 位置的数字按 float64 解码，int 等字段的整数保持精确；
//   - 其余情况（目标是 map[string]any、几处目标类型不同、类型无法确定）载荷里的数字换成 float64
//     （executor.GoJSONNumbers），与 Go 解码进 map[string]any 相同，null 的键保留。这时解进结构体的载荷，
//     int 字段里超过 2^53 的整数会丢失精度，any 字段里的整数解成 int64，null 的集合字段原样交给 wlang。
//
// 位置无法确定的解码处按它可能对应的全部位置处理。
type payloadNormalizer struct {
	reg *wlregistry.Registry
	// records 是每个位置、每个种类（Name 为空）与全部载荷（Kind 为空）各自的结构体解码类型。
	records map[program.PayloadPosition][]string
	// untyped 是有不是结构体（或类型无法确定）的解码目标的位置，键的含义与 records 相同。
	untyped map[program.PayloadPosition]bool
}

func newPayloadNormalizer(wf *program.WorkflowConfig) *payloadNormalizer {
	normalizer := &payloadNormalizer{records: map[program.PayloadPosition][]string{}, untyped: map[program.PayloadPosition]bool{}}
	all := program.PayloadPosition{}
	if wf == nil {
		normalizer.untyped[all] = true
		return normalizer
	}
	normalizer.reg = wlregistry.New()
	types, err := program.PayloadTypes(wf)
	if err != nil || normalizer.reg.BindDescriptors(wf.Program.Types) != nil {
		normalizer.untyped[all] = true
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
	if typeName == "" || !n.isRecord(typeName) {
		n.untyped[position] = true
		return
	}
	n.records[position] = append(n.records[position], typeName)
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

// normalize 返回 kind 位置 name 的载荷规范化之后的副本。
func (n *payloadNormalizer) normalize(kind program.PayloadPositionKind, name string, payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	target := ""
	for _, scope := range []program.PayloadPosition{{}, {Kind: kind}, {Kind: kind, Name: name}} {
		if n.untyped[scope] {
			return executor.GoJSONNumbers(payload).(map[string]any)
		}
		for _, typeName := range n.records[scope] {
			if target != "" && typeName != target {
				return executor.GoJSONNumbers(payload).(map[string]any)
			}
			target = typeName
		}
	}
	if target == "" {
		return executor.GoJSONNumbers(payload).(map[string]any)
	}
	normalized, _ := executor.NormalizeTypedPayload(n.reg, target, payload).(map[string]any)
	return normalized
}

// normalizePayload 规范化 Signal、Activity 结果与子 Workflow 结果；normalizer 在第一次用到时按 Program 构造。
func (e *WorkflowExecutor) normalizePayload(kind program.PayloadPositionKind, name string, payload map[string]any) map[string]any {
	if e.payloadNormalizer == nil {
		e.payloadNormalizer = newPayloadNormalizer(e.workflow)
	}
	return e.payloadNormalizer.normalize(kind, name, payload)
}
