package interpreter

// 翻译器接受的 Go 写法在解释器里的结果与原生 Go 一致。期望值是原生 Go 的结果。

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/protocol/history"
)

const goProbeHeader = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)
var _ = time.Second
type Out struct {
	Sum   int
	Count int
	Text  string
	Flag  bool
	Stamp int64
}
type Item struct {
	Name string
	Qty  int
}
type Outer struct {
	In   Item
	Tags []string
}
`

type goProbe struct {
	name string
	body string
	want map[string]any
}

func runGoProbe(t *testing.T, body string) (map[string]any, string) {
	result, err := sdkgen.GenerateFromSource([]byte(goProbeHeader+body), sdkgen.Options{WorkflowType: "probe", FuncName: "W", Filename: "w.go"})
	if err != nil {
		return nil, "generate: " + err.Error()
	}
	started := history.NewWorkflowStartedEvent(nil, "", "", "", "", 0)
	started.ID = 1
	started.Timestamp = time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
	commands, err := NewWorkflowExecutor(result.Workflow, []history.Event{started}).Execute()
	if err != nil {
		return nil, "execute: " + err.Error()
	}
	for _, command := range commands {
		if command.Type == history.CmdCompleteWorkflow {
			return command.Output, ""
		}
		if command.Type == history.CmdFailWorkflow {
			return nil, "workflow failed: " + command.Error
		}
	}
	return nil, "no terminal command"
}

func normProbeValue(v any) any {
	if number, ok := v.(interface{ Int64() (int64, error) }); ok {
		n, _ := number.Int64()
		return n
	}
	switch x := v.(type) {
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return v
}

func TestGoSemanticsMatchNativeGo(t *testing.T) {
	probes := []goProbe{
		{"struct assign copies", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := Item{Name: "x", Qty: 1}
	b := a
	b.Qty = 5
	return Out{Sum: a.Qty, Count: b.Qty}, nil
}`, map[string]any{"Sum": int64(1), "Count": int64(5)}},
		{"slice element read copies struct", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	xs := []Item{{Name: "a", Qty: 1}}
	y := xs[0]
	y.Qty = 9
	for _, it := range xs {
		it.Qty = 7
	}
	return Out{Sum: xs[0].Qty, Count: y.Qty}, nil
}`, map[string]any{"Sum": int64(1), "Count": int64(9)}},
		{"slice element field write", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	xs := []Item{{Name: "a", Qty: 1}, {Name: "b", Qty: 2}}
	xs[1].Qty = 20
	for i := range xs {
		xs[i].Qty++
	}
	return Out{Sum: xs[0].Qty + xs[1].Qty}, nil
}`, map[string]any{"Sum": int64(23)}},
		{"slice aliasing", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := []int{1, 2, 3}
	b := a
	b[0] = 9
	c := a[:2]
	c = append(c, 100)
	return Out{Sum: a[0], Count: a[2]}, nil
}`, map[string]any{"Sum": int64(9), "Count": int64(100)}},
		{"append to shared backing array", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := make([]int, 3, 10)
	b := append(a, 1)
	c := append(a, 2)
	return Out{Sum: b[3], Count: c[3] + len(a)}, nil
}`, map[string]any{"Sum": int64(2), "Count": int64(5)}},
		{"map is a reference", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	m := map[string]int{}
	n := m
	n["x"] = 1
	return Out{Sum: len(m), Count: m["x"]}, nil
}`, map[string]any{"Sum": int64(1), "Count": int64(1)}},
		{"struct in map is a copy", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	m := map[string]Item{"a": {Name: "a", Qty: 1}}
	v := m["a"]
	v.Qty = 3
	return Out{Sum: m["a"].Qty, Count: v.Qty}, nil
}`, map[string]any{"Sum": int64(1), "Count": int64(3)}},
		{"nested struct copy", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	o1 := Outer{In: Item{Name: "a", Qty: 1}, Tags: []string{"x"}}
	o2 := o1
	o2.In.Qty = 5
	o2.Tags[0] = "y"
	return Out{Sum: o1.In.Qty, Text: o1.Tags[0]}, nil
}`, map[string]any{"Sum": int64(1), "Text": "y"}},
		{"struct param is a copy", `func bump(it Item) int {
	it.Qty += 10
	return it.Qty
}
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := Item{Name: "x", Qty: 1}
	r := bump(a)
	return Out{Sum: a.Qty, Count: r}, nil
}`, map[string]any{"Sum": int64(1), "Count": int64(11)}},
		{"struct captured by closure is shared", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := Item{Name: "x", Qty: 1}
	f := func() { a.Qty = 42 }
	f()
	return Out{Sum: a.Qty}, nil
}`, map[string]any{"Sum": int64(42)}},
		{"negative division and modulo", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a, b := -7, 2
	return Out{Sum: a / b, Count: a % b}, nil
}`, map[string]any{"Sum": int64(-3), "Count": int64(-1)}},
		{"sized integer wraps", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var u uint8 = 250
	u += 10
	var i int32 = 2147483647
	i++
	return Out{Sum: int(u), Stamp: int64(i)}, nil
}`, map[string]any{"Sum": int64(4), "Stamp": int64(-2147483648)}},
		{"int64 beyond 2^53", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var x int64 = 9007199254740993
	x += 2
	return Out{Stamp: x, Flag: x%2 == 1}, nil
}`, map[string]any{"Stamp": int64(9007199254740995), "Flag": true}},
		{"int64 overflow wraps", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var x int64 = 9223372036854775807
	x++
	return Out{Flag: x < 0}, nil
}`, map[string]any{"Flag": true}},
		{"float to int truncates", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	f := -2.7
	g := 7.0 / 2.0
	return Out{Sum: int(f), Count: int(g), Stamp: int64(7 / 2)}, nil
}`, map[string]any{"Sum": int64(-2), "Count": int64(3), "Stamp": int64(3)}},
		{"string bytes and runes", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	s := "héllo"
	n := 0
	for range s {
		n++
	}
	return Out{Sum: len(s), Count: n, Text: s[1:3]}, nil
}`, map[string]any{"Sum": int64(6), "Count": int64(5), "Text": "é"}},
		{"inner := shadows", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	total := 0
	x := 1
	if x > 0 {
		x := 2
		total += x
	}
	for i := 0; i < 3; i++ {
		total, ok := i*100, true
		_ = ok
		_ = total
	}
	return Out{Sum: total, Count: x}, nil
}`, map[string]any{"Sum": int64(2), "Count": int64(1)}},
		{"closures capture per-iteration var", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	fs := []func() int{}
	for i := 0; i < 3; i++ {
		fs = append(fs, func() int { return i * 10 })
	}
	sum := 0
	for _, f := range fs {
		sum += f()
	}
	return Out{Sum: sum}, nil
}`, map[string]any{"Sum": int64(30)}},
		{"defer order and named result", `func calc() (n int) {
	defer func() { n *= 2 }()
	defer func() { n += 3 }()
	return 4
}
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	return Out{Sum: calc()}, nil
}`, map[string]any{"Sum": int64(14)}},
		{"short circuit", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	calls := 0
	hit := func() bool { calls++; return true }
	a := false && hit()
	b := true || hit()
	return Out{Count: calls, Flag: a || !b}, nil
}`, map[string]any{"Count": int64(0), "Flag": false}},
		{"array is a value", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := [3]int{1, 2, 3}
	b := a
	b[0] = 9
	return Out{Sum: a[0], Count: b[0]}, nil
}`, map[string]any{"Sum": int64(1), "Count": int64(9)}},
		{"labeled continue and switch break", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	sum := 0
outer:
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			switch {
			case j == 1:
				continue outer
			case i == 2:
				break outer
			}
			sum += 10*i + j
		}
	}
	return Out{Sum: sum}, nil
}`, map[string]any{"Sum": int64(10)}},
		{"range evaluates slice once", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	xs := []int{1, 2, 3}
	n := 0
	for _, v := range xs {
		if v < 3 {
			xs = append(xs, 10)
		}
		n++
	}
	return Out{Count: n, Sum: len(xs)}, nil
}`, map[string]any{"Count": int64(3), "Sum": int64(5)}},
		{"nil map read and zero values", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var m map[string]int
	var xs []int
	var it Item
	v, ok := m["a"]
	xs = append(xs, 1)
	return Out{Sum: v + it.Qty + len(xs), Flag: ok, Text: it.Name + "|"}, nil
}`, map[string]any{"Sum": int64(1), "Flag": false, "Text": "|"}},
		{"multi-assign swap with index", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	xs := []int{1, 2, 3}
	i := 0
	i, xs[i] = 2, 9
	xs[0], xs[2] = xs[2], xs[0]
	return Out{Sum: xs[0]*100 + xs[1]*10 + xs[2], Count: i}, nil
}`, map[string]any{"Sum": int64(329), "Count": int64(2)}},
		{"string compare and concat", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a, b := "apple", "Banana"
	s := ""
	for i := 0; i < 3; i++ {
		s += string(rune('a' + i))
	}
	return Out{Flag: a < b, Text: s}, nil
}`, map[string]any{"Flag": false, "Text": "abc"}},
		{"integer shifts and bit ops", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	x := 5
	y := x << 3 >> 1
	z := (x &^ 1) | (x ^ 3)
	return Out{Sum: y, Count: z}, nil
}`, map[string]any{"Sum": int64(20), "Count": int64(6)}},
		{"method value receiver copy", `func (it Item) Doubled() Item {
	it.Qty *= 2
	return it
}
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := Item{Name: "x", Qty: 3}
	b := a.Doubled()
	return Out{Sum: a.Qty, Count: b.Qty}, nil
}`, map[string]any{"Sum": int64(3), "Count": int64(6)}},
		{"copy and sub-slice", `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := []int{1, 2, 3, 4}
	b := make([]int, 2)
	n := copy(b, a[1:])
	b[0] = 50
	return Out{Sum: a[1], Count: n*100 + b[0] + b[1]}, nil
}`, map[string]any{"Sum": int64(2), "Count": int64(253)}},
		{"struct comparison and returned struct copy", `func mk() Item { return Item{Name: "a", Qty: 1} }
func W(ctx workflow.Context, input map[string]any) (Out, error) {
	a := mk()
	b := mk()
	items := []Item{a}
	items = append(items, a)
	items[0].Qty = 100
	return Out{Flag: a == b, Sum: a.Qty + items[1].Qty}, nil
}`, map[string]any{"Flag": true, "Sum": int64(2)}},
	}
	// 这些写法在生成期被拒绝（带 file:line），不会以与 Go 不同的语义运行。
	rejected := map[string]string{
		"slice element read copies struct":           "field access on a computed value",
		"slice element field write":                  "assignment to a field of a computed value",
		"append to shared backing array":             "append assignment requires =",
		"struct in map is a copy":                    "field access on a computed value",
		"nested struct copy":                         "assignment to an element of a computed value",
		"sized integer wraps":                        "conversion from uint8 to int",
		"float to int truncates":                     "conversion from float64 to int",
		"string bytes and runes":                     "range over string",
		"array is a value":                           "定长数组",
		"string compare and concat":                  "类型转换 string(...)",
		"integer shifts and bit ops":                 "operator >> is not supported",
		"struct comparison and returned struct copy": "assignment to a field of a computed value",
	}
	for _, probe := range probes {
		output, problem := runGoProbe(t, probe.body)
		if reason, ok := rejected[probe.name]; ok {
			if !strings.Contains(problem, "generate: w.go:") || !strings.Contains(problem, reason) {
				t.Errorf("%s: want generation error containing %q, got %q", probe.name, reason, problem)
			}
			continue
		}
		if problem != "" {
			t.Errorf("%s: %s", probe.name, problem)
			continue
		}
		for field, want := range probe.want {
			if got := normProbeValue(output[field]); got != want {
				t.Errorf("%s: %s = %#v, Go gives %#v (output %+v)", probe.name, field, output[field], want, output)
			}
		}
	}
}
