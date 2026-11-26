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
