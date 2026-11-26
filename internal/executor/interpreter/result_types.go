package interpreter

import (
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	wlregistry "github.com/donutnomad/wlang/registry"
	wltypes "github.com/donutnomad/wlang/types"
)

// resultNormalizer 在 Activity 结果与子 Workflow 结果进入解释器之前，按 Program 解码它们用的类型去掉结构体里
// 为 null 的 slice、map 字段（executor.DropNullCollectionFields）：Go 的 encoding/json 把 nil slice、nil map
// 编码成 null，wlang 把 null 解成没有载体的值，len、range 会报 E_TYPE；字段缺省时它取 nil 容器，与 Go 相同。
//
// 解码类型来自 program.PayloadTypes：每个 Activity type、子 Workflow Type 的结果被 Get 进的 out 变量的类型。
// 一个位置只要有一处解码目标不是结构体（例如 map[string]any，null 的键在 Go 里存在）或类型无法确定，
// 这个位置的结果原样交给解释器；位置无法确定的解码处按它可能对应的全部位置处理。
type resultNormalizer struct {
	reg *wlregistry.Registry
	// records 是每个位置、每个种类（Name 为空）与全部结果（Kind 为空）各自要套用的结构体类型。
	records map[program.PayloadPosition][]string
	// blocked 是不能去掉 null 字段的位置，键的含义与 records 相同。
	blocked map[program.PayloadPosition]bool
}

func newResultNormalizer(wf *program.WorkflowConfig) *resultNormalizer {
	normalizer := &resultNormalizer{records: map[program.PayloadPosition][]string{}, blocked: map[program.PayloadPosition]bool{}}
	all := program.PayloadPosition{}
	if wf == nil {
		normalizer.blocked[all] = true
		return normalizer
	}
	normalizer.reg = wlregistry.New()
	types, err := program.PayloadTypes(wf)
	if err != nil || normalizer.reg.BindDescriptors(wf.Program.Types) != nil {
		normalizer.blocked[all] = true
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

func (n *resultNormalizer) add(position program.PayloadPosition, typeName string) {
	if position.Kind != "" && position.Kind != program.PayloadActivityResult && position.Kind != program.PayloadChildResult {
		return
	}
	if typeName == "" || !n.isRecord(typeName) {
		n.blocked[position] = true
		return
	}
	n.records[position] = append(n.records[position], typeName)
}

// isRecord 报告 typeName（可以是别名或指针）是否为结构体。
func (n *resultNormalizer) isRecord(typeName string) bool {
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

// normalize 返回 kind 位置 name 的结果去掉 null 的 slice、map 字段之后的副本；不能去掉时原样返回。
func (n *resultNormalizer) normalize(kind program.PayloadPositionKind, name string, output map[string]any) map[string]any {
	if output == nil {
		return nil
	}
	scopes := []program.PayloadPosition{{}, {Kind: kind}, {Kind: kind, Name: name}}
	for _, scope := range scopes {
		if n.blocked[scope] {
			return output
		}
	}
	var value any = output
	for _, scope := range scopes {
		for _, typeName := range n.records[scope] {
			value = executor.DropNullCollectionFields(n.reg, typeName, value)
		}
	}
	normalized, _ := value.(map[string]any)
	return normalized
}

// normalizeResult 规范化 Activity 结果与子 Workflow 结果；normalizer 在第一次用到时按 Program 构造。
func (e *WorkflowExecutor) normalizeResult(kind program.PayloadPositionKind, name string, output map[string]any) map[string]any {
	if e.resultNormalizer == nil {
		e.resultNormalizer = newResultNormalizer(e.workflow)
	}
	return e.resultNormalizer.normalize(kind, name, output)
}
