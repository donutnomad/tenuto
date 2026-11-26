package sdkgen

import (
	"strings"
	"testing"
)

const lowerHeader = `package flows

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

var (
	_ = rand.Int
	_ = http.MethodGet
	_ = os.Getenv
	_ sync.Mutex
	_ = time.Second
	_ = uuid.NewString
	_ = errors.New
	_ = fmt.Sprint
	_ = strings.ToUpper
)

type Out struct {
	Sum  int
	Text string
}

const retryLimit = 3
const pollInterval = 2 * time.Second
const typedTimeout time.Duration = 5 * time.Minute

func helper(x int) int { return x * 2 }

`

func generateLowered(body string) (Result, error) {
	src := lowerHeader + "func W(ctx workflow.Context, input map[string]any) (Out, error) {\n" + body + "\n}\n"
	return GenerateFromSource([]byte(src), Options{WorkflowType: "lower", FuncName: "W", Filename: "w.go"})
}

// 第 3 项与第 2 项：没有确定性语义的写法在生成期报错，错误信息带行号与替代写法。
func TestLowerRejectsNonDeterministicConstructs(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []string
	}{
		{"go statement", `go func() {}(); return Out{}, nil`, []string{"w.go:41", "go 语句", "workflow.Go", "workflow.NewWaitGroup"}},
		{"chan type", `var ch chan int; _ = ch; return Out{}, nil`, []string{"原生 chan", "workflow.NewChannel"}},
		{"make chan", `ch := make(chan int, 1); _ = ch; return Out{}, nil`, []string{"原生 chan", "workflow.NewBufferedChannel"}},
		{"select", `select {}`, []string{"select 语句", "workflow.NewSelector"}},
		{"sync.WaitGroup", `var wg sync.WaitGroup; wg.Wait(); return Out{}, nil`, []string{"sync.WaitGroup", "workflow.NewWaitGroup"}},
		{"sync.Mutex", `var mu sync.Mutex; mu.Lock(); return Out{}, nil`, []string{"sync.Mutex", "协作式调度"}},
		{"math/rand", `n := rand.Intn(3); return Out{Sum: n}, nil`, []string{"rand.Intn", "workflow.RandomInt"}},
		{"uuid", `id := uuid.NewString(); return Out{Text: id}, nil`, []string{"uuid.NewString", "workflow.RandomUUID"}},
		{"os", `v := os.Getenv("X"); return Out{Text: v}, nil`, []string{"os.Getenv", "Activity"}},
		{"net/http", `_, err := http.Get("http://x"); return Out{}, err`, []string{"http.Get", "Activity"}},
		{"time.Now", `t := time.Now(); _ = t; return Out{}, nil`, []string{"time.Now", "workflow.Now(ctx)"}},
		{"time.Sleep", `time.Sleep(time.Second); return Out{}, nil`, []string{"time.Sleep", "workflow.Sleep(ctx, d)"}},
		{"time.After", `_ = time.After(time.Second); return Out{}, nil`, []string{"time.After", "workflow.Sleep(ctx, d)"}},
		{"time.Since", `s := workflow.Now(ctx); _ = time.Since(s); return Out{}, nil`, []string{"time.Since", "workflow.Now(ctx).Sub(t)"}},
		{"time.Parse", `_, err := time.Parse(time.RFC3339, "x"); return Out{}, err`, []string{"time.Parse", "workflow.Now(ctx)"}},
		{"float conversion", `x := 3; y := float64(x); _ = y; return Out{}, nil`, []string{"类型转换 float64(...)"}},
		{"local function value", `f := helper; return Out{Sum: f(2)}, nil`, []string{"w.go:", "本包函数 helper 不能作为值使用", "闭包"}},
		{"errors package", `err := errors.New("x"); return Out{}, err`, []string{"errors.New", "fmt.Errorf", "workflow.NewApplicationError"}},
		{"unbound strings function", `return Out{Text: strings.Title("x")}, nil`, []string{"w.go:", "strings.Title is not supported", "bound functions of strings: Contains"}},
		{"strings.Builder", `var b strings.Builder; b.WriteString("x"); return Out{Text: b.String()}, nil`, []string{"w.go:", "type strings.Builder is not supported"}},
		{"Sprintf of a struct", `return Out{Text: fmt.Sprintf("%v", Out{})}, nil`, []string{"w.go:", "fmt operand of type"}},
		{"non-host package call", `return Out{Text: uuid.New().String()}, nil`, []string{"uuid.New", "workflow.RandomUUID"}},
		{"assertion field in condition", `var a any = Out{}; if a.(Out).Sum > 0 { return Out{}, nil }; return Out{}, nil`, []string{"a.(Out).Sum", "先把类型断言赋值给变量"}},
		{"sized make as argument", `n := 2; x := len(make([]int, n)); return Out{Sum: x}, nil`, []string{"make([]int, n)", "独立的赋值语句"}},
		{"for without init and condition", "for {\n\t}\n\treturn Out{}, nil", []string{"w.go:41", "for requires an init statement and a condition"}},
		{"make with capacity as argument", `x := len(make([]int, 0, 4)); return Out{Sum: x}, nil`, []string{"make([]int, 0, 4)", "独立的赋值语句"}},
		{"pointer alias", `a := Out{Sum: 1}; p := &a; p.Sum = 5; return a, nil`, []string{"w.go:41", "&a 只能直接作为调用实参", "直接使用值 a"}},
		{"pointer to field returned", `a := Out{}; q := &a.Sum; _ = q; return a, nil`, []string{"&a.Sum 只能直接作为调用实参"}},
		{"labeled switch on any with init", `var k any = 1
L:
	switch n := 2; k {
	case n:
		break L
	}
	return Out{}, nil`, []string{"w.go:43", "带标签的 switch", "n := 2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := generateLowered(tc.body)
			if err == nil {
				t.Fatal("want generation error")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

// 定长数组在 Go 里是值，翻译成 WLang 后变成引用语义的 slice；生成期拒绝，报 file:line 并提示改用 slice。
func TestGenerateRejectsFixedArrays(t *testing.T) {
	const header = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type Grid [3]int

type Box struct {
	Cells [2]int
}

type Out struct {
	Sum int
}

func sum(cells [2]int) int { return cells[0] + cells[1] }

`
	cases := []struct {
		name, body, want string
	}{
		{"array literal", "a := [3]int{1, 2, 3}\n\tb := a\n\tb[0] = 9\n\treturn Out{Sum: a[0]}, nil", "w.go:18"},
		{"ellipsis literal", "a := [...]int{1, 2}\n\treturn Out{Sum: a[0]}, nil", "w.go:18"},
		{"named array type", "var g Grid\n\treturn Out{Sum: g[0]}, nil", "w.go:18"},
		{"array field", "box := Box{}\n\treturn Out{Sum: box.Cells[0]}, nil", "w.go:19"},
		{"helper parameter", "return Out{Sum: sum([2]int{1, 2})}, nil", "w.go:18"},
		{"slice of arrays", "gs := []Grid{}\n\treturn Out{Sum: len(gs)}, nil", "w.go:18"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := header + "func W(ctx workflow.Context, input map[string]any) (Out, error) {\n\t" + tc.body + "\n}\n"
			_, err := GenerateFromSource([]byte(src), Options{WorkflowType: "arrays", FuncName: "W", Filename: "w.go"})
			if err == nil {
				t.Fatal("want generation error")
			}
			for _, want := range []string{tc.want, "定长数组", "改用 slice"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

// 第 2 项：time 常量、time.Duration 转换与本文件的包级常量在生成期折叠，产物不带 time 符号。
func TestLowerFoldsTimeConstantsAndPackageConsts(t *testing.T) {
	result, err := generateLowered(`
	var wait time.Duration
	wait = typedTimeout + pollInterval*time.Duration(retryLimit)
	layout := time.RFC3339
	_ = layout
	return Out{Sum: int(wait / time.Millisecond)}, workflow.Sleep(ctx, 15*time.Minute)`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	for _, banned := range []string{`"symbol": "time.`, `"pkg": "time"`, `retryLimit`, `pollInterval`, `typedTimeout`, `"int"`} {
		if strings.Contains(program, banned) {
			t.Fatalf("program still contains %s:\n%s", banned, program)
		}
	}
	for _, want := range []string{`"value": "900000000000"`, `"value": "2006-01-02T15:04:05Z07:00"`, `"value": "306000000000"`} {
		if !strings.Contains(program, want) {
			t.Fatalf("program misses %s:\n%s", want, program)
		}
	}
	if _, ok := result.Workflow.Program.Imports["time"]; ok {
		t.Fatalf("imports still list time: %v", result.Workflow.Program.Imports)
	}
}

// `_ = items[i]` 与 Go 一样求值右侧：生成的程序保留这次索引，越界时运行时报错（engine 的
// TestWorkflowCodePanicBlocksRun 覆盖执行结果）。`_ = x` 与 `_ = pkg.Func` 保持省略。
func TestLowerEvaluatesBlankAssignedExpression(t *testing.T) {
	result, err := generateLowered(`
	items := []int{1, 2, 3}
	n := len(input) + 5
	_ = items[n]
	_ = n
	_ = strings.ToUpper
	var out *Out
	_ = out.Sum
	return Out{}, nil`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	if !strings.Contains(program, "zzDiscard1") || !strings.Contains(program, "zzDiscard2") || strings.Contains(program, "zzDiscard3") {
		t.Fatalf("program should evaluate only the discarded index and field expressions:\n%s", program)
	}
}

// update handler 的结果是 map[string]any 时，Workflow 里带初始值的 map[string]any 字面量照常生成：
// wlang 运行时的 any 接受任意值。
func TestGenerateAcceptsMapLiteralWithMapTypedUpdateHandler(t *testing.T) {
	if _, err := generateLowered(`
	err := workflow.SetUpdateHandler(ctx, "approve", func(ctx workflow.Context, n int) (map[string]any, error) {
		return map[string]any{"handled": true}, nil
	})
	return Out{}, err`); err != nil {
		t.Fatalf("map handler result with map literal must generate: %v", err)
	}
}

// 操作数含 any 的比较、any 上的 switch、slice/map 与 nil 的比较改写成 workflow.Equal / workflow.IsNil；
// 元素是 struct 的 map 读取改写成立即调用的闭包；带容量的 make 不再调用宿主（容量只是提示）。
func TestLowerTypedRewritesGoSemantics(t *testing.T) {
	result, err := generateLowered(`
	var xs []int
	m := map[string]Out{}
	v := m["x"]
	w, ok := m["y"]
	buf := make([]int, 0, 8)
	switch input["kind"] {
	case "a", "b":
		v.Text = "ab"
	}
	if input["status"] == "ok" || xs == nil || m != nil || ok {
		return Out{Sum: v.Sum + w.Sum + len(buf)}, nil
	}
	return v, nil`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	for _, want := range []string{`"Equal"`, `"IsNil"`, `"zzTag1"`, `"zzZero1"`, `"zzZero2"`, `"zzMakeCap1"`} {
		if !strings.Contains(program, want) {
			t.Fatalf("program misses %s:\n%s", want, program)
		}
	}
	if strings.Contains(program, `"SetCap"`) {
		t.Fatalf("make with capacity must not call the host:\n%s", program)
	}
}

// 元素是指针或接口的 map 读取、指针与 nil 的比较、不含 any 的比较保持 WLang 运算。
func TestLowerTypedKeepsComparableForms(t *testing.T) {
	result, err := generateLowered(`
	m := map[string]*Out{}
	var p *Out
	s := "x"
	return Out{Text: s}, func() error { if p == nil && m["a"] == nil && s == "x" { return nil }; return nil }()`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	if strings.Contains(program, `"Equal"`) || strings.Contains(program, `"IsNil"`) || strings.Contains(program, "zzZero") {
		t.Fatalf("program should keep plain comparisons and map reads:\n%s", program)
	}
}

// 没有导入 workflow 包的文件被改写成 workflow.Equal 时补一条 import，写在 package 子句同一行，
// 之后的诊断行号不变。
func TestLowerTypedInjectsWorkflowImportOnSameLine(t *testing.T) {
	src := "package flows\n\nfunc W(input map[string]any) int {\n\ta := make([]int, 0, 4)\n\tif input[\"k\"] == \"v\" {\n\t\treturn cap(a)\n\t}\n\treturn 0\n}\n"
	result, err := GenerateFromSource([]byte(src), Options{WorkflowType: "inject", FuncName: "W", Filename: "inject.go"})
	if err != nil {
		t.Fatal(err)
	}
	if program := string(result.ProgramJSON); !strings.Contains(program, `"Equal"`) {
		t.Fatalf("program misses workflow calls:\n%s", program)
	}
	bad := strings.Replace(src, "return 0", "return len(\"ab\"[0:1])", 1)
	_, err = GenerateFromSource([]byte(bad), Options{WorkflowType: "inject", FuncName: "W", Filename: "inject.go"})
	if err == nil || !strings.Contains(err.Error(), "inject.go:8") {
		t.Fatalf("err = %v, want a diagnostic at inject.go:8", err)
	}
}

// 每个循环体开头插入一条计数语句，每轮执行都计入 Workflow Task 的步数；循环体在 Go 里非空、翻译后为空
// （`_ = i` 整句省略）的循环同样计数。多轮改写只插入一次。
func TestLowerLoopBodyGetsCountedStatement(t *testing.T) {
	result, err := generateLowered(`
	for range 3 {
	}
	for i := 0; i < 1; {
		_ = i
	}
	for i := 0; i < 2; i++ {
		n := make([]int, i)
		_ = n
	}
	return Out{}, nil`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	// 三个循环，加上 make([]int, i) 展开出的追加零值循环。
	for _, name := range []string{`"zzLoopStep1"`, `"zzLoopStep2"`, `"zzLoopStep3"`, `"zzLoopStep4"`} {
		if strings.Count(program, name) != 1 {
			t.Fatalf("program should declare %s once:\n%s", name, program)
		}
	}
	if strings.Contains(program, "zzLoopStep5") {
		t.Fatalf("program should count steps once per loop:\n%s", program)
	}
}

// 类型检查没有得到参与比较、switch 或 map 读取的表达式的类型时（这里导入无法解析），生成失败，不静默按缺省翻译。
func TestLowerTypedFailsWithoutTypeInformation(t *testing.T) {
	src := `package flows

import (
	"example.com/missing/dto"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Out struct {
	Flag bool
}

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	var v dto.Thing
	return Out{Flag: v.Kind == "refund"}, nil
}
`
	_, err := GenerateFromSource([]byte(src), Options{WorkflowType: "missing", FuncName: "W", Filename: "w.go"})
	if err == nil || !strings.Contains(err.Error(), "w.go:15") || !strings.Contains(err.Error(), "类型检查没有得到 v.Kind 的类型") {
		t.Fatalf("err = %v, want a type information error at w.go:15", err)
	}
}

// 目标是 []any、map[string]any 的类型断言（含 comma-ok 与 type switch 的 case）改写成 workflow 包的宿主函数；
// 其他目标保持 WLang 的断言。
func TestFoldRewritesAnyContainerAssertions(t *testing.T) {
	result, err := generateLowered(`
	a := input["a"].([]any)
	b, ok := input["b"].(map[string]any)
	switch v := input["c"].(type) {
	case []any:
		return Out{Sum: len(v)}, nil
	case string:
		return Out{Text: v}, nil
	}
	s, _ := input["s"].(string)
	return Out{Sum: len(a) + len(b), Text: s}, func() error { if ok { return nil }; return nil }()`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	for _, want := range []string{`"TypeAssert"`, `"TypeAssertOK"`, `"TypeIs"`, `"type.assert.ok"`, `"type.is"`} {
		if !strings.Contains(program, want) {
			t.Fatalf("program misses %s:\n%s", want, program)
		}
	}
	if strings.Contains(program, `"type.assert": [`+"\n") && strings.Contains(program, `"value": "array<any>"`) {
		t.Fatalf("program still asserts array<any> with WLang type.assert:\n%s", program)
	}
}

// 类型是 slice、map 的 nil 改写成该类型的零值；指向 struct 的指针之间的比较改写成 workflow.SamePointer，其他指针
// 与和 nil 的比较不改。
func TestLowerTypedNilAndPointerComparison(t *testing.T) {
	result, err := generateLowered(`
	xs := []string{"a"}
	xs = nil
	var m map[string]int
	m = nil
	var p, q *Out
	var i, j *int
	var xs1, xs2 *[]int
	same := p == q && i != j && xs1 == xs2
	if p == nil || xs == nil {
		return Out{Sum: len(m)}, nil
	}
	return Out{Text: strings.Join(xs, ","), Sum: len(m)}, func() error { if same { return nil }; return nil }()`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	for _, want := range []string{`"zzNil1"`, `"zzNil2"`, `"SamePointer"`, `"IsNil"`} {
		if !strings.Contains(program, want) {
			t.Fatalf("program misses %s:\n%s", want, program)
		}
	}
	if n := strings.Count(program, `"SamePointer"`); n != 1 {
		t.Fatalf("only the struct pointer comparison is rewritten, got %d SamePointer calls:\n%s", n, program)
	}
	if strings.Contains(program, `"zzNil3"`) {
		t.Fatalf("nil compared with a pointer or returned as error must stay nil:\n%s", program)
	}
}

// 表示不变的转换（具名基础类型与底层类型互转）可以写；改变表示的转换在生成期报错，外层转换先报。
func TestLowerTypedConversions(t *testing.T) {
	src := lowerHeader + `type Status string
type Amount float64

func W(ctx workflow.Context, input map[string]any) (Out, error) {
	s := Status("paid")
	a := Amount(1.5)
	return Out{Text: string(s) + strings.ToUpper(string(s)), Sum: int(len(string(s)))}, func() error { _ = float64(a); return nil }()
}
`
	if _, err := GenerateFromSource([]byte(src), Options{WorkflowType: "conv", FuncName: "W", Filename: "w.go"}); err != nil {
		t.Fatalf("representation-preserving conversions must generate: %v", err)
	}
	bad := lowerHeader + `func W(ctx workflow.Context, input map[string]any) (Out, error) {
	n := 65
	return Out{Text: string(rune(n))}, nil
}
`
	_, err := GenerateFromSource([]byte(bad), Options{WorkflowType: "conv", FuncName: "W", Filename: "w.go"})
	if err == nil || !strings.Contains(err.Error(), "类型转换 string(...)") {
		t.Fatalf("err = %v, want the outer string conversion rejected", err)
	}
}

// 带容量的 make 按 make([]T, n) 生成：容量照常求值，小于长度时与 Go 一样 panic，不调用宿主。
func TestLowerMakeWithCapacityIsHint(t *testing.T) {
	result, err := generateLowered(`
	n := 2
	xs := make([]int, n, n+3)
	xs = append(xs, 1)
	return Out{Sum: len(xs)}, nil`)
	if err != nil {
		t.Fatal(err)
	}
	program := string(result.ProgramJSON)
	if strings.Contains(program, `"SetCap"`) || !strings.Contains(program, "makeslice: cap out of range") {
		t.Fatalf("make with capacity must check the capacity without a host call:\n%s", program)
	}
}
