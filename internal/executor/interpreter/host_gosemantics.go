package interpreter

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/donutnomad/wlang/types"
)

// 生成器把 WLang 运算符与 Go 不一致的写法改写成下面的 workflow 包调用（见 compiler/sdkgen 的 lowerTyped）：
// 操作数含 any 的 == / != 与 any 上的 switch 改写成 Equal，slice、map 与 nil 的比较改写成 IsNil，
// 带容量的 make 改写成 make 加 SetCap。

// Equal 按 Go 的接口比较规则比较 a 与 b：动态类型与值都相等时为 true；动态类型相同但不可比较
// （slice、map、函数，或含这些字段的 struct）时与 Go 一样 panic。
func (api *runtimeWorkflowAPI) Equal(a, b any) bool {
	equal, err := goEqual(a, b)
	if err != nil {
		panic(err)
	}
	return equal
}

// IsNil 报告 v 是否为 nil 值。nil slice（var xs []T、JSON 缺省或 null 的字段）与 nil map 是 nil，
// []T{}、make([]T, 0) 与空 map 不是。
func (api *runtimeWorkflowAPI) IsNil(v any) bool {
	if v == nil {
		return true
	}
	switch value := reflect.ValueOf(v); value.Kind() {
	case reflect.Slice, reflect.Map, reflect.Pointer, reflect.Func, reflect.Interface, reflect.Chan:
		return value.IsNil()
	}
	return false
}

// SetCap 把输出参数指向的 slice 换成元素相同、容量为 capacity 的新 slice。
func (api *runtimeWorkflowAPI) SetCap(out any, capacity int64) error {
	target := reflect.ValueOf(out)
	if target.Kind() != reflect.Pointer || target.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("workflow.SetCap expects a pointer to a slice, got %T", out)
	}
	target = target.Elem()
	if capacity < int64(target.Len()) {
		panic("makeslice: cap out of range")
	}
	grown := reflect.MakeSlice(target.Type(), target.Len(), int(capacity))
	reflect.Copy(grown, target)
	target.Set(grown)
	return nil
}

// goEqual 是 Go 的 a == b（a、b 是接口值）在解释器值上的实现。
//
// 解释器里的值与 Go 的对应关系：int 与 int64 都是 int64；struct 是 *types.Record，类型名与各字段相等时相等；
// History 载荷里 map[string]any 的数字是 json.Number，Go 的 encoding/json 把它解码成 float64，这里按 float64 比较。
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
