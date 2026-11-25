// Package programtest 提供测试用的最小 Workflow 配置。
package programtest

import (
	"strings"

	"github.com/donutnomad/tenuto/program"
	wlangtypes "github.com/donutnomad/wlang/types"
)

// Noop 返回立即以 nil error 结束的 WLang 程序。只验证服务端语义、由测试直接回报命令的用例用它登记配置。
func Noop() program.WLangProgram {
	return program.NewWLangProgram([]any{
		map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
	})
}

// Workflow 返回只含 Noop 程序的配置。
func Workflow(id string) *program.WorkflowConfig {
	return &program.WorkflowConfig{Type: id, Program: Noop()}
}

// Field 是 Record 的一个字段：JSON 名与类型 ID（内置标量或另一个描述符的 ID）。
type Field struct {
	JSON string
	Type string
}

// Record 返回 record 描述符：语言名是 languageName（如 order.Approval），ID 是 pkgPath + "." + 类型名部分，
// 同一个语言名可以放在不同的包路径下，模拟改包名。指纹按 WLang 的规则计算。
func Record(pkgPath, languageName string, fields ...Field) program.WLangTypeDescriptor {
	_, typeName, _ := strings.Cut(languageName, ".")
	descriptor := program.WLangTypeDescriptor{
		ID: pkgPath + "." + typeName, LanguageName: languageName, Kind: wlangtypes.TypeKindRecord,
	}
	for _, field := range fields {
		descriptor.Fields = append(descriptor.Fields, wlangtypes.FieldDescriptor{GoName: goName(field.JSON), JSONName: field.JSON, TypeID: field.Type})
	}
	descriptor.Fingerprint = wlangtypes.DescriptorFingerprint(descriptor)
	return descriptor
}

// Types 把描述符按 ID 组成 Program.Types。
func Types(descriptors ...program.WLangTypeDescriptor) map[string]program.WLangTypeDescriptor {
	out := make(map[string]program.WLangTypeDescriptor, len(descriptors))
	for _, descriptor := range descriptors {
		out[descriptor.ID] = descriptor
	}
	return out
}

// goName 把 JSON 字段名转成导出的 Go 字段名：order_id → OrderId。
func goName(jsonName string) string {
	var out strings.Builder
	upper := true
	for _, char := range jsonName {
		if char == '_' {
			upper = true
			continue
		}
		if upper {
			out.WriteString(strings.ToUpper(string(char)))
			upper = false
			continue
		}
		out.WriteRune(char)
	}
	return out.String()
}

// PayloadTypeNames 是 PayloadWorkflow 在各载荷位置解码用的类型（语言名）。
var PayloadTypeNames = []string{
	"order.Input", "order.Signal", "order.Later", "order.ChargeInput", "order.ChargeOutput",
	"order.ChildOutput", "order.UpdateArg", "order.SideValue", "order.FlagValue",
	"order.Picked", "order.Named", "order.Dynamic",
}

// PayloadWorkflow 返回在每类载荷位置各解码一次的 Workflow，types 必须含 PayloadTypeNames 的全部描述符：
//   - Workflow 输入 order.Input；
//   - Signal "approve" 直接 Get 到 order.Signal，Signal "later" 先存进 future 变量再 Get 到 order.Later；
//   - Activity "orders.charge"（符号 order.Charge）的输入 order.ChargeInput、结果 order.ChargeOutput；
//   - 子 Workflow "child-flow" 的结果 order.ChildOutput；
//   - Update "approve-update" 的参数 order.UpdateArg；
//   - SideEffect 的值 order.SideValue，MutableSideEffect "flag" 的值 order.FlagValue；
//   - Signal "pick" 在 Selector 的 AddSignalFuture 回调里 Get 到 order.Picked；
//   - Signal "named-signal" 的名字是变量与字面量的 `+` 拼接，Get 到 order.Named；
//   - 名字取自输入字段 in.OrderID 的 Signal Get 到 order.Dynamic：名字无法静态确定，是未确定的解码处。
func PayloadWorkflow(id string, types map[string]program.WLangTypeDescriptor) *program.WorkflowConfig {
	pkg := map[string]any{"pkg": "workflow"}
	ctx := map[string]any{"var": "ctx"}
	str := func(value string) map[string]any {
		return map[string]any{"literal": map[string]any{"type": "string", "value": value}}
	}
	zero := func(name, typeName string) map[string]any {
		return map[string]any{"let": map[string]any{name: map[string]any{"zero": typeName}}}
	}
	get := func(future any, out string) map[string]any {
		return map[string]any{"set": map[string]any{"err": map[string]any{"Get": []any{future, ctx, map[string]any{"out": out}}}}}
	}
	call := func(name string, args ...any) map[string]any {
		return map[string]any{name: append([]any{pkg, ctx}, args...)}
	}
	fn := func(params []any, returns ...any) map[string]any {
		return map[string]any{"fn": map[string]any{"params": params, "returns": returns, "do": []any{
			map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
		}}}
	}
	ctxParam := []any{"ctx", "workflow.Context"}
	var entryInput string
	for typeID, descriptor := range types {
		if descriptor.LanguageName == "order.Input" {
			entryInput = typeID
		}
	}
	statements := []any{
		map[string]any{"let": map[string]any{"err": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}}},
		zero("approval", "order.Signal"),
		get(call("WaitSignal", str("approve")), "approval"),
		map[string]any{"let": map[string]any{"laterFuture": call("WaitSignal", str("later"))}},
		zero("later", "order.Later"),
		get(map[string]any{"var": "laterFuture"}, "later"),
		zero("charged", "order.ChargeOutput"),
		get(call("ExecuteActivity", map[string]any{"symbol": "order.Charge"},
			map[string]any{"struct": []any{"order.ChargeInput", map[string]any{}}},
			map[string]any{"struct": []any{"workflow.ActivityOptions", map[string]any{}}}), "charged"),
		zero("child", "order.ChildOutput"),
		get(call("ExecuteChildWorkflow", str("child-flow"), map[string]any{"var": "in"}), "child"),
		zero("side", "order.SideValue"),
		get(call("SideEffect", fn([]any{ctxParam}, "order.SideValue")), "side"),
		zero("flag", "order.FlagValue"),
		get(call("MutableSideEffect", str("flag"), fn([]any{ctxParam}, "order.FlagValue"), fn([]any{}, "boolean")), "flag"),
		map[string]any{"expr": call("SetUpdateHandler", str("approve-update"),
			fn([]any{ctxParam, []any{"arg", "order.UpdateArg"}}, "map<string,any>", "error"))},
		zero("picked", "order.Picked"),
		map[string]any{"let": map[string]any{"pickFuture": call("WaitSignal", str("pick"))}},
		map[string]any{"let": map[string]any{"selector": call("NewSelector")}},
		map[string]any{"expr": map[string]any{"AddSignalFuture": []any{map[string]any{"var": "selector"}, ctx, map[string]any{"var": "pickFuture"},
			map[string]any{"fn": map[string]any{"params": []any{[]any{"f", "workflow.SignalFuture"}}, "returns": []any{}, "do": []any{
				map[string]any{"expr": map[string]any{"Get": []any{map[string]any{"var": "f"}, ctx, map[string]any{"out": "picked"}}}},
			}}}}}},
		map[string]any{"expr": map[string]any{"Select": []any{map[string]any{"var": "selector"}, ctx}}},
		map[string]any{"let": map[string]any{"prefix": str("named-")}},
		zero("named", "order.Named"),
		get(call("WaitSignal", map[string]any{"+": []any{map[string]any{"var": "prefix"}, str("signal")}}), "named"),
		zero("dynamic", "order.Dynamic"),
		get(call("WaitSignal", map[string]any{"var": "in.OrderID"}), "dynamic"),
		map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
	}
	wlang := program.NewWLangProgram(statements)
	wlang.Types = types
	wlang.Entry = program.WLangEntryDescriptor{InputVar: "in", Input: entryInput}
	return &program.WorkflowConfig{
		Type: id, Program: wlang,
		Activities: map[string]program.ActivityRef{
			"order.Charge": {Symbol: "order.Charge", Type: "orders.charge", TaskQueue: "orders"},
		},
	}
}

// PayloadTypes 返回 PayloadWorkflow 用的全部描述符，放在包路径 pkgPath 下：每个类型只有一个 order_id 字段；
// override 按语言名替换其中一些。
func PayloadTypes(pkgPath string, override ...program.WLangTypeDescriptor) map[string]program.WLangTypeDescriptor {
	byName := map[string]program.WLangTypeDescriptor{}
	for _, name := range PayloadTypeNames {
		byName[name] = Record(pkgPath, name, Field{JSON: "order_id", Type: "string"})
	}
	for _, descriptor := range override {
		byName[descriptor.LanguageName] = descriptor
	}
	descriptors := make([]program.WLangTypeDescriptor, 0, len(byName))
	for _, descriptor := range byName {
		descriptors = append(descriptors, descriptor)
	}
	return Types(descriptors...)
}
