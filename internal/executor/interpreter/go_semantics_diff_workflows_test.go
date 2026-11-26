package interpreter

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// 差分测试的 Workflow 函数（TestGoSemanticsDifferential）：同一份源码在原生 Go 里直接调用，也经生成器翻译后
// 由解释器执行，两边的结果必须相同。函数只用原生 Go 语法，不调用 workflow 包的函数。

type DiffOut struct {
	Sum   int
	Count int
	Text  string
	Flag  bool
	Value any
}

type DiffItem struct {
	Name string
	Qty  int
}

type DiffInput struct {
	V        any
	M        map[string]any
	Name     string
	Tags     []string
	Attrs    map[string]string
	Children []DiffInput
	Lookup   map[string]*DiffItem
}

func DiffAnyEqualsTrue(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	return DiffOut{Flag: input["approved"] == true}, nil
}

func DiffAnyEqualsString(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	if input["action"] == "cancel" {
		return DiffOut{Text: "cancel"}, nil
	}
	return DiffOut{Text: "other"}, nil
}

func DiffAnyNotEqualsString(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	if input["status"] != "approved" {
		return DiffOut{Text: "rejected-branch"}, nil
	}
	return DiffOut{Text: "approved-branch"}, nil
}

func DiffStringEqualsAny(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	return DiffOut{Flag: "approved" == input["status"]}, nil
}

// JSON 数字在 Go 的 map[string]any 里是 float64：与整数常量比较为 false，与浮点常量比较为 true。
func DiffAnyNumberFromJSON(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	text := "no"
	if input["n"] == 3.0 {
		text = "float"
	}
	return DiffOut{Flag: input["n"] == 3, Text: text}, nil
}

func DiffAnyLocalInt(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	m := map[string]any{"n": 3, "s": "x"}
	return DiffOut{Flag: m["n"] == 3 && m["s"] != 3 && m["missing"] == nil}, nil
}

func DiffAnyEqualsAny(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var a any = "x"
	var b any = "x"
	var c any = 1
	return DiffOut{Flag: a == b && a != c && input["a"] == input["b"]}, nil
}

func DiffAnyStructEquals(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var v any = DiffItem{Name: "a", Qty: 1}
	return DiffOut{Flag: v == DiffItem{Name: "a", Qty: 1} && v != DiffItem{Name: "a", Qty: 2}}, nil
}

func DiffSwitchOnAny(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	switch input["kind"] {
	case "a":
		return DiffOut{Text: "A"}, nil
	case "b", "c":
		return DiffOut{Text: "BC"}, nil
	case nil:
		return DiffOut{Text: "nil"}, nil
	}
	return DiffOut{Text: "none"}, nil
}

func DiffSwitchOnAnyWithInit(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	count := 0
	switch kind := input["kind"]; kind {
	case true:
		count = 1
	case "b":
		count = 2
	default:
		count = 3
	}
	return DiffOut{Count: count}, nil
}

// init 声明的 k 只在 switch 里可见，外层的 k 不变。
func DiffSwitchOnAnyInitShadow(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	k := 10
	inner := 0
	switch k := 1; input["a"] {
	case "x":
		inner = k
	default:
		inner = k + 1
	}
	return DiffOut{Count: k, Sum: inner}, nil
}

func DiffNilSlice(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var s []int
	e := []int{}
	m := make([]int, 0)
	return DiffOut{Flag: s == nil && e != nil && m != nil && nil != e, Sum: len(s)}, nil
}

func DiffNilMap(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	m := map[string]int{}
	var n map[string]int
	return DiffOut{Flag: m != nil && n == nil}, nil
}

func DiffInputNilFields(ctx workflow.Context, in DiffInput) (DiffOut, error) {
	text := ""
	if in.Tags == nil {
		text += "tags-nil "
	}
	if in.Attrs == nil {
		text += "attrs-nil"
	}
	count := len(in.Tags) + len(in.Attrs)
	for _, tag := range in.Tags {
		text += " " + tag
	}
	for _, child := range in.Children {
		count += len(child.Tags) + len(child.Attrs)
		if child.Tags == nil {
			text += " child-tags-nil"
		}
	}
	for key, item := range in.Lookup {
		if item == nil {
			text += " lookup-nil:" + key
		}
	}
	return DiffOut{Text: text, Count: count, Sum: len(in.Children) + len(in.Lookup)}, nil
}

func DiffInitNilMap(ctx workflow.Context, in DiffInput) (DiffOut, error) {
	attrs := in.Attrs
	if attrs == nil {
		attrs = map[string]string{}
	}
	attrs["k"] = "v"
	return DiffOut{Count: len(attrs)}, nil
}

func DiffMissingKeyStruct(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	m := map[string]DiffItem{"a": {Name: "a", Qty: 2}}
	v := m["x"]
	w := m["a"]
	return DiffOut{Sum: v.Qty + 1, Text: v.Name + w.Name, Count: w.Qty}, nil
}

func DiffMissingKeyStructCommaOK(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	m := map[string]DiffItem{"a": {Name: "a", Qty: 2}}
	v, ok := m["x"]
	w, found := m["a"]
	return DiffOut{Sum: v.Qty + w.Qty, Text: v.Name, Flag: !ok && found}, nil
}

func DiffMissingKeyStructArgument(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	m := map[string]DiffItem{}
	items := []DiffItem{}
	items = append(items, m["x"])
	first := items[0]
	return DiffOut{Count: len(items), Sum: first.Qty, Text: first.Name}, nil
}

func DiffSliceWithinCap(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	a := make([]int, 0, 10)
	b := a[:3]
	b[2] = 7
	return DiffOut{Sum: len(b), Count: cap(a) + b[2]}, nil
}

func DiffSizedSliceWithCap(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	n := 2
	a := make([]int, n, n+3)
	a = append(a, 5)
	b := a[:5]
	return DiffOut{Sum: len(a) + b[2], Count: cap(a)}, nil
}

func DiffSliceBeyondCap(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	a := make([]int, 0, 2)
	n := 3
	b := a[:n]
	return DiffOut{Sum: len(b)}, nil
}

func DiffAnyUncomparable(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var a any = []int{1}
	var b any = []int{1}
	return DiffOut{Flag: a == b}, nil
}

// DiffResult 是 Activity 与子 Workflow 的结果：Go 的 encoding/json 把 nil slice、nil map 字段编码成 null。
type DiffResult struct {
	V     any
	M     map[string]any
	Tags  []string
	Attrs map[string]string
	Items []DiffItem
	Inner *DiffResult
}

// summarizeResult 在原生 Go 与解释器里对同一个结果做 nil 判断、len、range 与 map 读取。
func summarizeResult(r DiffResult) DiffOut {
	text := ""
	if r.Tags == nil {
		text += "tags-nil "
	}
	if r.Attrs == nil {
		text += "attrs-nil "
	}
	count := len(r.Tags) + len(r.Attrs) + len(r.Items)
	for _, tag := range r.Tags {
		text += tag + " "
	}
	for _, item := range r.Items {
		count += item.Qty
	}
	text += r.Attrs["k"]
	if r.Inner != nil {
		count += len(r.Inner.Tags) + len(r.Inner.Attrs)
	}
	return DiffOut{Text: text, Count: count, Sum: anyNumberFlags(r.V, r.M), Value: r.M}
}

// anyNumberFlags 对 any 字段与 map[string]any 字段里来自 JSON 的数字做比较、断言与算术：Go 的 encoding/json
// 把它们解成 float64。每一项占一位。
func anyNumberFlags(v any, m map[string]any) int {
	flags := 0
	if v == 1 {
		flags += 1
	}
	if v == 1.0 {
		flags += 2
	}
	if f, ok := v.(float64); ok && f*2 > 1.5 {
		flags += 4
	}
	if _, ok := v.(int); ok {
		flags += 8
	}
	if m["n"] == 1 {
		flags += 16
	}
	if m["n"] == 1.0 {
		flags += 32
	}
	if n, ok := m["n"].(float64); ok && n+1 > 1.5 {
		flags += 64
	}
	if _, ok := m["n"].(int64); ok {
		flags += 128
	}
	return flags
}

func DiffInputAny(ctx workflow.Context, in DiffInput) (DiffOut, error) {
	return DiffOut{Sum: anyNumberFlags(in.V, in.M), Value: in.M}, nil
}

// update 参数是结构体：any 字段与 map[string]any 字段里的数字按 float64 解码。
func DiffUpdateAny(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	if err := workflow.SetUpdateHandler(ctx, "probe", func(ctx workflow.Context, in DiffInput) (DiffOut, error) {
		return DiffOut{Sum: anyNumberFlags(in.V, in.M), Value: in.M}, nil
	}); err != nil {
		return DiffOut{}, err
	}
	return DiffOut{}, workflow.Await(ctx, func() bool { return false })
}

func DiffActivityResult(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var r DiffResult
	if err := workflow.ExecuteActivity(ctx, "load-result", input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &r); err != nil {
		return DiffOut{}, err
	}
	return summarizeResult(r), nil
}

func DiffChildResult(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var r DiffResult
	if err := workflow.ExecuteChildWorkflow(ctx, "child-result", input).Get(ctx, &r); err != nil {
		return DiffOut{}, err
	}
	return summarizeResult(r), nil
}

// 结果解进 map[string]any 时，null 的键在 Go 里存在，不能去掉。
func DiffActivityRawResult(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var d map[string]any
	if err := workflow.ExecuteActivity(ctx, "load-raw", input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &d); err != nil {
		return DiffOut{}, err
	}
	return summarizeRawResult(d), nil
}

// summarizeMapNumbers 对 map[string]any 载荷里来自 JSON 的数字做比较、断言、算术与再编码：Go 的
// encoding/json 把它们解成 float64。每一项占一位。
func summarizeMapNumbers(d map[string]any) DiffOut {
	flags := 0
	if d["amount"] == 1 {
		flags += 1
	}
	if d["amount"] == 1.0 {
		flags += 2
	}
	text := "not-float"
	if f, ok := d["amount"].(float64); ok {
		text = "float"
		if f*3 > 2.5 {
			flags += 4
		}
	}
	if _, ok := d["amount"].(int64); ok {
		flags += 8
	}
	if _, ok := d["amount"].(int); ok {
		flags += 16
	}
	switch d["amount"] {
	case 1:
		flags += 128
	case 1.0:
		flags += 256
	}
	return DiffOut{Sum: flags, Text: text, Value: d}
}

func DiffMapInputNumbers(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	return summarizeMapNumbers(input), nil
}

func DiffSignalNumbers(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var d map[string]any
	if err := workflow.WaitSignal(ctx, "pay").Get(ctx, &d); err != nil {
		return DiffOut{}, err
	}
	return summarizeMapNumbers(d), nil
}

func DiffUpdateMapNumbers(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	if err := workflow.SetUpdateHandler(ctx, "probe", func(ctx workflow.Context, in map[string]any) (DiffOut, error) {
		return summarizeMapNumbers(in), nil
	}); err != nil {
		return DiffOut{}, err
	}
	return DiffOut{}, workflow.Await(ctx, func() bool { return false })
}

func DiffActivityMapNumbers(ctx workflow.Context, input map[string]any) (DiffOut, error) {
	var d map[string]any
	if err := workflow.ExecuteActivity(ctx, "load-map", input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &d); err != nil {
		return DiffOut{}, err
	}
	return summarizeMapNumbers(d), nil
}

func summarizeRawResult(d map[string]any) DiffOut {
	_, ok := d["Tags"]
	return DiffOut{Flag: ok && d["Tags"] == nil, Count: len(d)}
}
