package sdkgen

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

// rewriteFixture 构造一份最小的重写输入：同包一个本地契约、跨包一个本地契约、
// 跨包一个 remote 契约。
func rewriteFixture() (map[string][]Contract, string) {
	const ownPath = "example.com/app/orders"
	const inventoryPath = "example.com/app/inventory"
	const paymentsPath = "example.com/app/payments"
	structType := mustTypeExpr("ChargeInput")
	structs := map[string]bool{"ChargeInput": true}
	return map[string][]Contract{
		ownPath: {{
			PkgDir: "/repo/app/orders", PkgName: "orders", ImportPath: ownPath,
			Type: "orders.charge", Queue: "orders", Version: "v1",
			Input: "ChargeInput", Output: "ChargeOutput",
			InputExpr: structType, OutputExpr: mustTypeExpr("ChargeOutput"),
			CallName: "CallCharge", MarkerName: "Charge", Pos: "workflow.go:9",
			Structs: structs,
		}},
		inventoryPath: {{
			PkgDir: "/repo/app/inventory", PkgName: "inventory", ImportPath: inventoryPath,
			Type: "inventory.reserve", Queue: "inventory", Version: "v1",
			Input: "ReserveInput", Output: "ReserveOutput",
			InputExpr: mustTypeExpr("ReserveInput"), OutputExpr: mustTypeExpr("ReserveOutput"),
			CallName: "CallReserve", MarkerName: "Reserve", Pos: "inventory.go:7",
		}},
		paymentsPath: {{
			PkgDir: "/repo/app/payments", PkgName: "payments", ImportPath: paymentsPath,
			Type: "payments.settle", Queue: "payments", Version: "v1", Remote: true,
			Input: "SettleInput", Output: "SettleOutput",
			InputExpr: mustTypeExpr("SettleInput"), OutputExpr: mustTypeExpr("SettleOutput"),
			CallName: "CallSettle", MarkerName: "SettleRemoteActivity",
			Pos: "payments.go:7",
		}},
	}, ownPath
}

// mustTypeExpr 解析一个类型表达式，供测试构造 Contract。
func mustTypeExpr(source string) ast.Expr {
	expr, err := parser.ParseExpr(source)
	if err != nil {
		panic(err)
	}
	return expr
}

// rewriteSource 跑一次重写并返回结果源码。
func rewriteSource(t *testing.T, body string) (string, rewriteResult) {
	t.Helper()
	contracts, ownPath := rewriteFixture()
	imports := `import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/inventory"
	"example.com/app/payments"
)`
	source := `package orders

` + imports + `

type ChargeInput struct {
	OrderID       string
	ReservationID string
}

type ChargeOutput struct {
	ChargeID string
}

type ReserveInput struct {
	OrderID string
}

type ReserveOutput struct {
	ReservationID string
}

type SettleInput struct {
	OrderID string
}

type SettleOutput struct {
	Receipt string
}

func Charge(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) { return ChargeOutput{}, nil }
func CallCharge(ctx workflow.Context, in ChargeInput, opts ...workflow.ActivityOptions) (ChargeOutput, error) {
	panic("bridge")
}

func W(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) {
` + body + `
}
`
	result, err := rewriteWorkflowCalls([]byte(source), "workflow.go", "W", ownPath, contracts)
	if err != nil {
		t.Fatalf("重写失败: %v", err)
	}
	return string(result.Source), result
}

// rewriteError 期望重写报错。
func rewriteError(t *testing.T, body string) error {
	t.Helper()
	contracts, ownPath := rewriteFixture()
	source := `package orders

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/inventory"
)

func W(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) {
` + body + `
}
`
	_, err := rewriteWorkflowCalls([]byte(source), "workflow.go", "W", ownPath, contracts)
	if err == nil {
		t.Fatal("期望重写报错，实际通过")
	}
	return err
}

// TestRewriteSupportedStatementForms 覆盖三种受支持的语句形态与 opts 透传。
func TestRewriteSupportedStatementForms(t *testing.T) {
	t.Run("out, err := 同包桥", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	out, err := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ChargeOutput{}, err
	}
	return out, nil`)
		// 结构体变量入参按 json tag 展开成 map 载荷（record 无法绑定 ExecuteActivity 的 any 参数）。
		want := `var out ChargeOutput; err := workflow.ExecuteActivity(ctx, Charge, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)`
		if !strings.Contains(source, want) {
			t.Fatalf("重写结果缺少 %q\n%s", want, source)
		}
	})

	t.Run("out, err = 跨包桥", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	var out ChargeOutput
	var err error
	out, err = inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	return out, err`)
		want := "err = workflow.ExecuteActivity(ctx, inventory.Reserve, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)"
		if !strings.Contains(source, want) {
			t.Fatalf("重写结果缺少 %q\n%s", want, source)
		}
		if strings.Contains(source, "var out ChargeOutput; err = ") {
			t.Fatalf("`=` 形态不应补 var 声明\n%s", source)
		}
	})

	t.Run("return 桥调用", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	return inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})`)
		for _, want := range []string{
			"var bridgeOut1 inventory.ReserveOutput",
			"bridgeErr1 := workflow.ExecuteActivity(ctx, inventory.Reserve, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &bridgeOut1)",
			"return bridgeOut1, bridgeErr1",
		} {
			if !strings.Contains(source, want) {
				t.Fatalf("重写结果缺少 %q\n%s", want, source)
			}
		}
	})

	t.Run("同一函数内第二次 out, err := 用 = 保留 Go 语义", func(t *testing.T) {
		// 重写把输出变量提成 var 声明；第二次 `x, err :=` 里 err 已在作用域中，
		// 若仍写 `:=` 就没有新变量，整包不再通过类型检查（跨文件 marker 会因此退化）。
		source, _ := rewriteSource(t, `
	first, err := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ChargeOutput{}, err
	}
	second, err := CallCharge(ctx, ChargeInput{OrderID: first.ChargeID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ChargeOutput{}, err
	}
	return second, nil`)
		for _, want := range []string{
			"var first ChargeOutput; err := workflow.ExecuteActivity",
			"var second ChargeOutput; err = workflow.ExecuteActivity",
		} {
			if !strings.Contains(source, want) {
				t.Fatalf("重写结果缺少 %q\n%s", want, source)
			}
		}
		if strings.Contains(source, "var second ChargeOutput; err := ") {
			t.Fatalf("err 已在作用域时不应再用 :=\n%s", source)
		}
	})

	t.Run("输出变量已声明时不再补 var", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	var out ChargeOutput
	out, err := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ChargeOutput{}, err
	}
	return out, nil`)
		if strings.Contains(source, "var out ChargeOutput; err") {
			t.Fatalf("输出变量已声明，不应重复 var\n%s", source)
		}
		if !strings.Contains(source, "err := workflow.ExecuteActivity") {
			t.Fatalf("新声明的 err 应保留 :=\n%s", source)
		}
	})

	t.Run("opts 透传", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	out, err := payments.CallSettle(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, TaskQueue: "payments"})
	return out, err`)
		want := `workflow.ExecuteActivity(ctx, payments.SettleRemoteActivity, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, TaskQueue: "payments"}).Get(ctx, &out)`
		if !strings.Contains(source, want) {
			t.Fatalf("重写结果缺少 %q\n%s", want, source)
		}
	})

	t.Run("结构体字面量输入原样传递", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	out, err := CallCharge(ctx, ChargeInput{OrderID: in.OrderID, ReservationID: in.ReservationID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	return out, err`)
		want := `workflow.ExecuteActivity(ctx, Charge, ChargeInput{OrderID: in.OrderID, ReservationID: in.ReservationID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})`
		if !strings.Contains(source, want) {
			t.Fatalf("重写结果缺少 %q\n%s", want, source)
		}
	})

	t.Run("if 初始化里的 _ 输出位提升临时变量", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	if _, err := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return ChargeOutput{}, err
	}
	return ChargeOutput{}, nil`)
		for _, want := range []string{
			`var bridgeOut1 ChargeOutput; if err := workflow.ExecuteActivity(ctx, Charge, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &bridgeOut1); err != nil {`,
		} {
			if !strings.Contains(source, want) {
				t.Fatalf("重写结果缺少 %q\n%s", want, source)
			}
		}
	})

	t.Run("未导入 workflow 时注入 import", func(t *testing.T) {
		contracts, ownPath := rewriteFixture()
		source := `package orders

import (
	"time"
	"example.com/app/inventory"
)

func W(ctx Context, in ReserveInput) (ReserveOutput, error) {
	return inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
}
`
		result, err := rewriteWorkflowCalls([]byte(source), "workflow.go", "W", ownPath, contracts)
		if err != nil {
			t.Fatalf("重写失败: %v", err)
		}
		if !strings.Contains(string(result.Source), `workflow "github.com/donutnomad/tenuto/sdk/workflow"`) {
			t.Fatalf("没有注入 workflow import\n%s", result.Source)
		}
	})

	t.Run("非桥调用保持不变", func(t *testing.T) {
		source, _ := rewriteSource(t, `
	other := Charge
	_ = other
	return ChargeOutput{}, nil`)
		if strings.Contains(source, "ExecuteActivity") {
			t.Fatalf("非桥调用不应被重写\n%s", source)
		}
	})
}

// TestRewriteRejectsUnsupportedShapes 覆盖「其他形态不猜、报 file:line」。
func TestRewriteRejectsUnsupportedShapes(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "单返回值赋值",
			body:    "\terr := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})\n\treturn ChargeOutput{}, err",
			wantErr: "必须赋值给两个变量 (out, err)",
		},
		{
			name:    "输出与错误都丢弃",
			body:    "\t_, _ = CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})\n\treturn ChargeOutput{}, nil",
			wantErr: "输出与错误都被丢弃",
		},
		{
			name:    "返回值被丢弃",
			body:    "\tCallCharge(ctx, in)\n\treturn ChargeOutput{}, nil",
			wantErr: "返回值被丢弃",
		},
		{
			name:    "嵌在函数调用里",
			body:    "\t_ = now(CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}))\n\treturn ChargeOutput{}, nil",
			wantErr: "只能用作",
		},
		{
			name:    "多返回值重组",
			body:    "\treturn ChargeOutput{}, errorOf(CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}))",
			wantErr: "只能用作",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := rewriteError(t, testCase.body)
			if !strings.Contains(err.Error(), testCase.wantErr) {
				t.Fatalf("错误 %q 缺少 %q", err.Error(), testCase.wantErr)
			}
			if !strings.Contains(err.Error(), "workflow.go:") {
				t.Fatalf("错误缺少 file:line: %v", err)
			}
		})
	}
}

// TestRewriteReportsMissingContract 保证「被调用桥没有对应契约」时报错。
func TestRewriteReportsMissingContract(t *testing.T) {
	contracts, ownPath := rewriteFixture()
	source := `package orders

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func W(ctx workflow.Context) error {
	out, err := missing.CallGhost(ctx, nil, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	_ = out
	return err
}
`
	_, err := rewriteWorkflowCalls([]byte(source), "workflow.go", "W", ownPath, contracts)
	if err != nil {
		t.Fatalf("未知包调用应原样保留而不是报错: %v", err)
	}
}

// TestQualifyTypeRendersAcrossPackages 覆盖跨包类型限定。
func TestQualifyTypeRendersAcrossPackages(t *testing.T) {
	rewriter := &rewriter{pathAlias: map[string]string{"example.com/app/payments": "payments"}}
	contract := Contract{Pos: "payments.go:7", Type: "payments.settle", Imports: map[string]string{"payments": "example.com/app/payments"}}
	cases := []struct {
		expr    string
		want    string
		qualify string
	}{
		{"SettleOutput", "payments.SettleOutput", "payments"},
		{"*SettleOutput", "*payments.SettleOutput", "payments"},
		{"[]SettleOutput", "[]payments.SettleOutput", "payments"},
		{"map[string]SettleOutput", "map[string]payments.SettleOutput", "payments"},
		{"SettleOutput", "SettleOutput", ""},
		{"payments.SettleOutput", "payments.SettleOutput", "payments"},
	}
	for _, testCase := range cases {
		got, err := rewriter.qualifyType(mustTypeExpr(testCase.expr), contract, testCase.qualify)
		if err != nil {
			t.Fatalf("qualifyType(%s): %v", testCase.expr, err)
		}
		if got != testCase.want {
			t.Fatalf("qualifyType(%s, %q) = %q，期望 %q", testCase.expr, testCase.qualify, got, testCase.want)
		}
	}
}

// TestExportedNameDerivesBridgeNames 锁住桥名派生规则。
func TestExportedNameDerivesBridgeNames(t *testing.T) {
	cases := []struct{ activityType, want string }{
		{"payments.charge", "CallCharge"},
		{"lifecycle.charge-v2", "CallChargeV2"},
		{"orders.reserve_inventory", "CallReserveInventory"},
	}
	for _, testCase := range cases {
		got, err := bridgeCallName(testCase.activityType, "x.go:1")
		if err != nil {
			t.Fatalf("bridgeCallName(%s): %v", testCase.activityType, err)
		}
		if got != testCase.want {
			t.Fatalf("bridgeCallName(%s) = %s，期望 %s", testCase.activityType, got, testCase.want)
		}
	}
	if _, err := bridgeCallName("orders.9charge", "x.go:1"); err == nil {
		t.Fatal("非法 activity type 应报错")
	}
}

// TestAnnotationValueParsing 覆盖注解值的引号与逗号规则。
func TestAnnotationValueParsing(t *testing.T) {
	values, err := parseAnnotationValues(`type="lifecycle-demo",name="Lifecycle, Demo",policy=auto_upgrade,version=v2`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"type":    "lifecycle-demo",
		"name":    "Lifecycle, Demo",
		"policy":  "auto_upgrade",
		"version": "v2",
	}
	for key, expected := range want {
		if values[key] != expected {
			t.Fatalf("%s = %q，期望 %q", key, values[key], expected)
		}
	}
	if _, err := parseAnnotationValues(`id="unterminated`); err == nil {
		t.Fatal("未闭合引号应报错")
	}
	if _, err := parseAnnotationValues(`id=a,id=b`); err == nil {
		t.Fatal("重复 key 应报错")
	}
}

// 自然的 `if workflow.IsApplicationError(err, "T")` 守卫必须被重写成「临时变量 + 变量守卫」，
// 且作者源码形态（if/else、!）继续可用。
func TestRewriteHoistsApplicationErrorGuard(t *testing.T) {
	out, _ := rewriteSource(t, `
	err := ChargeErr()
	if workflow.IsApplicationError(err, "PaymentDeclined") {
		return ChargeOutput{ChargeID: "compensated"}, nil
	} else {
		return ChargeOutput{ChargeID: "manual"}, nil
	}`)
	if !strings.Contains(out, `applicationError1 := workflow.IsApplicationError(err, "PaymentDeclined")`) {
		t.Fatalf("缺少提升后的临时变量声明:\n%s", out)
	}
	if !strings.Contains(out, `if applicationError1 {`) {
		t.Fatalf("守卫没有改读临时变量:\n%s", out)
	}
	if !strings.Contains(out, `} else {`) {
		t.Fatalf("else 分支被破坏:\n%s", out)
	}
}

// ! 取反形态：调用提升为变量，守卫保留取反。
func TestRewriteHoistsNegatedApplicationErrorGuard(t *testing.T) {
	out, _ := rewriteSource(t, `
	err := ChargeErr()
	if !workflow.IsApplicationError(err, "PaymentDeclined") {
		return ChargeOutput{ChargeID: "manual"}, nil
	}
	return ChargeOutput{ChargeID: "compensated"}, nil`)
	if !strings.Contains(out, `applicationError1 := workflow.IsApplicationError(err, "PaymentDeclined")`) {
		t.Fatalf("缺少提升后的临时变量声明:\n%s", out)
	}
	if !strings.Contains(out, `if !applicationError1 {`) {
		t.Fatalf("取反守卫没有改读临时变量:\n%s", out)
	}
}

// 作者自己先赋值变量的形态保持原样：不重复提升、不改写。
func TestRewriteKeepsAuthorVariableApplicationErrorGuard(t *testing.T) {
	out, _ := rewriteSource(t, `
	err := ChargeErr()
	declined := workflow.IsApplicationError(err, "PaymentDeclined")
	if declined {
		return ChargeOutput{ChargeID: "compensated"}, nil
	}
	return ChargeOutput{ChargeID: "manual"}, nil`)
	if strings.Contains(out, "applicationError1") {
		t.Fatalf("变量形态不应再提升:\n%s", out)
	}
	if !strings.Contains(out, `if declined {`) {
		t.Fatalf("变量守卫被改写:\n%s", out)
	}
}

// 不支持的形态必须给 file:line 诊断，不能静默改条件求值时机。
func TestRewriteRejectsUnsupportedApplicationErrorGuards(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "if init 声明变量后条件引用",
			body: `
	if chargeErr := ChargeErr(); workflow.IsApplicationError(chargeErr, "PaymentDeclined") {
		return ChargeOutput{ChargeID: "compensated"}, nil
	}
	return ChargeOutput{}, nil`,
			want: "if 初始化语句",
		},
		{
			name: "else-if 形态",
			body: `
	err := ChargeErr()
	if err == nil {
		return ChargeOutput{ChargeID: "ok"}, nil
	} else if workflow.IsApplicationError(err, "PaymentDeclined") {
		return ChargeOutput{ChargeID: "compensated"}, nil
	}
	return ChargeOutput{}, nil`,
			want: "else-if",
		},
		{
			name: "组合条件",
			body: `
	err := ChargeErr()
	if workflow.IsApplicationError(err, "PaymentDeclined") && true {
		return ChargeOutput{ChargeID: "compensated"}, nil
	}
	return ChargeOutput{}, nil`,
			want: "与其它条件组合",
		},
		{
			name: "for 条件",
			body: `
	err := ChargeErr()
	for workflow.IsApplicationError(err, "PaymentDeclined") {
		break
	}
	return ChargeOutput{}, nil`,
			want: "for 条件",
		},
		{
			name: "switch 条件",
			body: `
	err := ChargeErr()
	switch workflow.IsApplicationError(err, "PaymentDeclined") {
	case true:
		return ChargeOutput{ChargeID: "compensated"}, nil
	}
	return ChargeOutput{}, nil`,
			want: "switch 条件",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			contracts, ownPath := rewriteFixture()
			source := `package orders

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func ChargeErr() error { return nil }

func W(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) {
` + test.body + `
}
`
			_, err := rewriteWorkflowCalls([]byte(source), "workflow.go", "W", ownPath, contracts)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want 包含 %q", err, test.want)
			}
			if !regexp.MustCompile(`workflow\.go:\d+`).MatchString(err.Error()) {
				t.Fatalf("诊断缺少 file:line: %v", err)
			}
		})
	}
}

// TestRewriteStructVariableInput：结构体变量（含跨包契约的结构体）原样传给 ExecuteActivity；
// record 值绑定到 any 形参，Worker 按类型表编码成 JSON。
func TestRewriteStructVariableInput(t *testing.T) {
	contracts, ownPath := rewriteFixture()
	source := `package orders

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/inventory"
)

func W(ctx workflow.Context, in map[string]any) error {
	var order inventory.ReserveInput
	reserved, err := inventory.CallReserve(ctx, order, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return err
	}
	_ = reserved
	var charge ChargeInput
	charged, err := CallCharge(ctx, charge, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return err
	}
	_ = charged
	return nil
}
`
	result, err := rewriteWorkflowCalls([]byte(source), "workflow.go", "W", ownPath, contracts)
	if err != nil {
		t.Fatalf("重写失败: %v", err)
	}
	rewritten := string(result.Source)
	for _, want := range []string{
		`workflow.ExecuteActivity(ctx, inventory.Reserve, order, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserved)`,
		`workflow.ExecuteActivity(ctx, Charge, charge, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &charged)`,
	} {
		if !strings.Contains(rewritten, want) {
			t.Fatalf("未生成期望调用\nwant 片段: %s\n实际:\n%s", want, rewritten)
		}
	}
}

// TestRewriteRequiresActivityTimeout 要求每个 Activity 调用点设置 StartToCloseTimeout 或 ScheduleToCloseTimeout。
func TestRewriteRequiresActivityTimeout(t *testing.T) {
	for name, body := range map[string]string{
		"没有选项":       `return CallCharge(ctx, in)`,
		"选项缺少超时":     `return CallCharge(ctx, in, workflow.ActivityOptions{TaskQueue: "payments"})`,
		"超时为零":       `return CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 0})`,
		"直接 Execute": "var out ChargeOutput\n\terr := workflow.ExecuteActivity(ctx, Charge, in).Get(ctx, &out)\n\treturn out, err",
	} {
		t.Run(name, func(t *testing.T) {
			err := rewriteError(t, body)
			if !strings.Contains(err.Error(), "workflow.go:") || !strings.Contains(err.Error(), "StartToCloseTimeout 或 ScheduleToCloseTimeout") {
				t.Fatalf("错误没有指出调用点与缺少的超时: %v", err)
			}
		})
	}
	for name, body := range map[string]string{
		"StartToClose":    `return CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})`,
		"ScheduleToClose": `return CallCharge(ctx, in, workflow.ActivityOptions{ScheduleToCloseTimeout: 5 * time.Minute})`,
		// 选项是变量时值在生成期不可知，由 Server 在调度时校验。
		"选项变量": "opts := workflow.ActivityOptions{}\n\treturn CallCharge(ctx, in, opts)",
	} {
		t.Run(name, func(t *testing.T) {
			rewriteSource(t, body)
		})
	}
}

// 同一个块里两条 `if out, err := CallX(...)` 用同名变量：各自提升的 out 不能在同一作用域里重复声明。
func TestRewriteIfInitBridgeKeepsOutScoped(t *testing.T) {
	source, _ := rewriteSource(t, `
	if out, err := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return out, err
	}
	if out, err := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return out, err
	}
	return ChargeOutput{}, nil`)
	file, err := parser.ParseFile(token.NewFileSet(), "workflow.go", source, 0)
	if err != nil {
		t.Fatalf("parse rewritten source: %v\n%s", err, source)
	}
	fn := findFuncDecl(file, "W")
	declared := map[string]int{}
	for _, stmt := range fn.Body.List {
		decl, ok := stmt.(*ast.DeclStmt)
		if !ok {
			continue
		}
		for _, spec := range decl.Decl.(*ast.GenDecl).Specs {
			for _, name := range spec.(*ast.ValueSpec).Names {
				declared[name.Name]++
			}
		}
	}
	if declared["out"] > 1 {
		t.Fatalf("out declared %d times in the function block\n%s", declared["out"], source)
	}
}
