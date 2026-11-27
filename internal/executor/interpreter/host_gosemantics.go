package interpreter

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/donutnomad/wlang/types"
)

// 生成器把 WLang 运算符与 Go 不一致的写法改写成下面的 workflow 包调用（见 compiler/sdkgen 的 lowerTyped）：
// 操作数含 any 的 == / != 与 any 上的 switch 改写成 Equal，slice、map 与 nil 的比较改写成 IsNil，
// 两侧都是指针的比较改写成 SamePointer，
// 目标是 []any、map[string]any 的类型断言改写成 TypeAssert、TypeAssertOK、
// TypeIs（解释器里这两种值的类型名与断言目标不同，WLang 的断言永远不成立）。
//
// 这些函数的参数是 types.Value：保存在变量与形参里的值带着 WLang 类型名（Go 里接口值的动态类型），
// var x any = []Order{} 的 x 是 array<Order>，装着 nil 指针的 any 是 *T。值存进 []any、map[string]any 或
// 结构体的 any 字段之后只剩 Go 载体，类型名是 any，只能按载体判断（go-subset 文档列为差异）。

// Equal 按 Go 的接口比较规则比较 a 与 b：动态类型与值都相等时为 true；动态类型相同但不可比较
// （slice、map、函数，或含这些字段的 struct）时与 Go 一样 panic。
func (api *runtimeWorkflowAPI) Equal(a, b types.Value) bool {
	if nilA, nilB := isNilInterface(a), isNilInterface(b); nilA || nilB {
		return nilA && nilB
	}
	equal, err := goEqual(a.Go(), b.Go())
	if err != nil {
		panic(err)
	}
	return equal
}

// isNilInterface 报告 v 作为 Go 接口值是否为 nil：没有动态类型（null），或类型名已丢失（any）且载体是 nil。
// 带类型名的 nil 载体（nil 指针 *T、nil slice、nil map）是非 nil 的接口值。
func isNilInterface(v types.Value) bool {
	switch v.TypeName() {
	case types.TNull:
		return true
	case types.TAny:
		return v.Go() == nil
	}
	return false
}

// SamePointer 是 Go 的指针 a == b，a、b 指向 struct：指向同一个对象时为 true。解释器里指向结构体的指针的载体是
// 共享的 *types.Record，`p := q`、经 map、slice、字段传递的指针都是同一个载体，载体相同即地址相同。生成器只对
// 指向 struct 的指针改写成这个调用；其他指针的载体是指向的值本身（slice、map 不可比较）。
func (api *runtimeWorkflowAPI) SamePointer(a, b any) bool {
	return a == b
}

// IsNil 报告 v 是否为 nil 值。nil slice（var xs []T、JSON 缺省或 null 的字段）与 nil map 是 nil，
// []T{}、make([]T, 0) 与空 map 不是。
func (api *runtimeWorkflowAPI) IsNil(v types.Value) bool {
	if v.Go() == nil {
		return true
	}
	switch value := reflect.ValueOf(v.Go()); value.Kind() {
	case reflect.Slice, reflect.Map, reflect.Pointer, reflect.Func, reflect.Interface, reflect.Chan:
		return value.IsNil()
	}
	return false
}

// TypeAssert 是 Go 的 v.(T)，T 是 []any（target "array<any>"）或 map[string]any（"map<string,any>"）：
// 动态类型不是 T 时与 Go 一样 panic（Workflow Task 以 workflow_panic 失败）。
func (api *runtimeWorkflowAPI) TypeAssert(v types.Value, target string) types.Value {
	value, ok := assertAnyContainer(v, target)
	if !ok {
		panic(fmt.Sprintf("interface conversion: interface {} is %s, not %s", dynamicTypeName(v), goContainerName(target)))
	}
	return value
}

// TypeAssertOK 是 Go 的 x, ok := v.(T)：动态类型不是 T 时得到 T 的零值（nil）与 false。
func (api *runtimeWorkflowAPI) TypeAssertOK(v types.Value, target string) (types.Value, bool) {
	return assertAnyContainer(v, target)
}

// TypeIs 报告 v 的动态类型是否为 T，用于 type switch 的 case。
func (api *runtimeWorkflowAPI) TypeIs(v types.Value, target string) bool {
	_, ok := assertAnyContainer(v, target)
	return ok
}

// assertAnyContainer 判断 v 的动态类型是否为 target 对应的 []any 或 map[string]any，返回带 WLang 类型名的值；
// 不是时返回 nil 值与 false。带类型名的值按类型名判断（array<Order> 不是 array<any>，与 Go 相同）。类型名是
// any 的值（取自 []any、map[string]any 或 any 字段）只剩载体：元素不是基础类型的类型化 slice（[]Order、
// [][]any、[]map[string]any）与 map（map[string]Order）的载体同样是 []any、map[string]any，这里判为成立，
// Go 里不成立。文档列为差异。
func assertAnyContainer(value types.Value, target string) (types.Value, bool) {
	v := value.Go()
	if name := value.TypeName(); name != types.TAny && name != types.TNull && name != target {
		v = nil
	}
	switch target {
	case "array<any>":
		if items, ok := v.([]any); ok {
			return types.NewValue(target, items), true
		}
		return types.NewNilArray(types.TAny), false
	case "map<string,any>":
		if entries, ok := v.(map[string]any); ok {
			return types.NewValue(target, entries), true
		}
		return types.NewValue(target, map[string]any(nil)), false
	}
	panic(fmt.Sprintf("workflow type assertion target %q is not []any or map[string]any", target))
}

func goContainerName(target string) string {
	if target == "array<any>" {
		return "[]interface {}"
	}
	return "map[string]interface {}"
}

// dynamicTypeName 是 Go 的 panic 消息里 v 的动态类型名。
func dynamicTypeName(value types.Value) string {
	if name := value.TypeName(); name != types.TAny && name != types.TNull {
		return name
	}
	v := value.Go()
	if v == nil {
		return "nil"
	}
	if record, ok := v.(*types.Record); ok {
		return record.TypeName()
	}
	return fmt.Sprintf("%T", v)
}

// goEqual 是 Go 的 a == b（a、b 是接口值）在解释器值上的实现。
//
// 解释器里的值与 Go 的对应关系：int 与 int64 都是 int64；struct 是 *types.Record，类型名与各字段相等时相等；
// 载荷里的数字在进入解释器时已换成与 Go 相同的 float64（executor.GoJSONNumbers、NormalizeTypedPayload）；
// 仍是 json.Number 的值按 float64 比较。
func goEqual(a, b any) (bool, error) {
	a, b = normalizeDynamic(a), normalizeDynamic(b)
	if a == nil || b == nil {
		return a == nil && b == nil, nil
	}
	recordA, isRecordA := a.(*types.Record)
	recordB, isRecordB := b.(*types.Record)
	if isRecordA || isRecordB {
		if !isRecordA || !isRecordB || recordA.TypeName() != recordB.TypeName() {
			return false, nil
		}
		fieldsA, fieldsB := recordA.Fields(), recordB.Fields()
		equal := true
		for _, name := range recordA.SortedFieldNames() {
			fieldEqual, err := goEqual(fieldsA[name].Go(), fieldsB[name].Go())
			if err != nil {
				return false, err
			}
			equal = equal && fieldEqual
		}
		return equal, nil
	}
	typeA, typeB := reflect.TypeOf(a), reflect.TypeOf(b)
	if typeA != typeB {
		return false, nil
	}
	if !typeA.Comparable() {
		return false, fmt.Errorf("runtime error: comparing uncomparable type %s", typeA)
	}
	return a == b, nil
}

// normalizeDynamic 把解释器的载体换成 Go 里同一个值的动态类型。
func normalizeDynamic(v any) any {
	switch value := v.(type) {
	case json.Number:
		if f, err := value.Float64(); err == nil {
			return f
		}
	case types.Value:
		return normalizeDynamic(value.Go())
	}
	return v
}
