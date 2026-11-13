package sdkgen

import (
	"strings"
	"testing"
)

// ============================================
// root 层 If/Switch Init 内 activity 的符号收集与 used 标记
// ============================================
// 测试策略:
// - T-S1  Happy Path: If-Init 内联 Get-guard（body `return err`，走 scanStmt IfStmt 分支）
// - T-S1b Happy Path: If-Init 内联 Get-guard（body `return nil`，走 scanStmtList early-return 特判）
// - T-S2  Happy Path: If-Init 变量 f.Get guard（两形态），used 置位、无 "must be awaited" 误报
// - T-S3  Happy Path: Switch-Init 同型
// 背景: scanStmt 的 IfStmt/SwitchStmt 分支与 scanStmtList 的 early-return 特判都不扫 x.Init，
// 而 root 层 guard 惯用形态的 ExecuteActivity 恰在 Init 里（.trash/zzz_probe_prefix_consistency_test.go
// 当年运行时 E_SYMBOL 实锤）。
// ============================================

const initScanHeader = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

`

func generateInitScan(t *testing.T, body string) (Result, error) {
	t.Helper()
	src := initScanHeader + "func W(ctx workflow.Context, input map[string]any) error {\n" + body + "\n}\n"
	return GenerateFromSource([]byte(src), Options{
		WorkflowID: "init-scan",
		FuncName:   "W",
		Filename:   "w.go",
	})
}

// T-S1 Happy Path: If-Init 内联 Get-guard（return err 形态，走 scanStmt 的 IfStmt 分支）
// → 翻译成功且 activities.CheckC 进 Activities 表（否则运行时 E_SYMBOL）。
func TestInitScan_IfInit_InlineGetGuard_ReturnErr(t *testing.T) {
	result, err := generateInitScan(t, `
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.CheckC, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return err
	}
	return nil`)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.CheckC"]; !ok {
		t.Fatalf("If-Init 内联 activity 符号应进 Activities 表, got %+v", result.Workflow.Activities)
	}
}

// T-S1b Happy Path: If-Init 内联 Get-guard（return nil 形态，走 scanStmtList 的 early-return 特判路径）。
func TestInitScan_IfInit_InlineGetGuard_EarlyReturn(t *testing.T) {
	result, err := generateInitScan(t, `
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.CheckD, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return nil
	}
	return nil`)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.CheckD"]; !ok {
		t.Fatalf("early-return 路径的 If-Init 内联 activity 符号应进 Activities 表, got %+v", result.Workflow.Activities)
	}
}

// T-S2 Happy Path: If-Init 变量 f.Get guard → used 置位（无 "must be awaited with Get" 误报）。
func TestInitScan_IfInit_FutureVarGetGuard(t *testing.T) {
	for name, ret := range map[string]string{"ReturnErr": "return err", "EarlyReturnNil": "return nil"} {
		t.Run(name, func(t *testing.T) {
			result, err := generateInitScan(t, `
	f := workflow.ExecuteActivity(ctx, activities.CheckE, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var out map[string]any
	if err := f.Get(ctx, &out); err != nil {
		`+ret+`
	}
	return nil`)
			if err != nil {
				t.Fatalf("If-Init 内 f.Get 应置 used、不报误报, got err: %v", err)
			}
			if _, ok := result.Workflow.Activities["activities.CheckE"]; !ok {
				t.Fatalf("future 变量的 activity 符号应进 Activities 表, got %+v", result.Workflow.Activities)
			}
		})
	}
}

// T-S3 Happy Path: Switch-Init 同型（走 scanStmt 的 SwitchStmt 分支）。
func TestInitScan_SwitchInit_FutureVarGetGuard(t *testing.T) {
	result, err := generateInitScan(t, `
	f := workflow.ExecuteActivity(ctx, activities.CheckF, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var out map[string]any
	switch err := f.Get(ctx, &out); err {
	case nil:
		return nil
	default:
		return err
	}`)
	if err != nil {
		t.Fatalf("Switch-Init 内 f.Get 应置 used、不报误报, got err: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.CheckF"]; !ok {
		t.Fatalf("Switch-Init 的 activity 符号应进 Activities 表, got %+v", result.Workflow.Activities)
	}
}

// 对照（现状已工作的形态不回归）: body 内赋值形式的 guard。
func TestInitScan_BodyAssignForm_StillWorks(t *testing.T) {
	result, err := generateInitScan(t, `
	var out map[string]any
	err := workflow.ExecuteActivity(ctx, activities.CheckG, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
	if err != nil {
		return err
	}
	return nil`)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.CheckG"]; !ok {
		t.Fatalf("对照形态符号应在表中, got %+v", result.Workflow.Activities)
	}
}

// T-S1c Happy Path（回归锚点）: If-Init 内联 Get-guard 的 activity 输入是含运行时变量的
// 复合字面量（`map[string]any{"which": which}`，非可翻译的 input ident）。
// Init 扫描只能收集符号 + 标 used，绝不能把 Init 的 .Get() lower 成展示 Step——否则展示 Step
// 的 Input 携带运行时变量 → rejectUnsupportedWorkflowExpressions 报 "unsupported workflow
// expression ... input"（正是 prefixSelectorSrc 撞出的回归）。符号仍须进 Activities 表。
func TestInitScan_IfInit_InlineGetGuard_RuntimeVarInput(t *testing.T) {
	result, err := generateInitScan(t, `
	which := "B"
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.CheckH, map[string]any{"which": which}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return err
	}
	return nil`)
	if err != nil {
		t.Fatalf("Init 扫描不得 lower .Get() 成展示 Step（运行时变量输入应由 go2wlang 执行态承载）, got err: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.CheckH"]; !ok {
		t.Fatalf("含运行时变量输入的 Init activity 符号仍应进 Activities 表, got %+v", result.Workflow.Activities)
	}
}

// T-S1c early-return 变体：body `return nil`（走 scanStmtList 特判路径），同样不得 lower。
func TestInitScan_IfInit_InlineGetGuard_RuntimeVarInput_EarlyReturn(t *testing.T) {
	result, err := generateInitScan(t, `
	which := "B"
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.CheckI, map[string]any{"which": which}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return nil
	}
	return nil`)
	if err != nil {
		t.Fatalf("early-return 路径 Init 扫描不得 lower .Get() 成展示 Step, got err: %v", err)
	}
	if _, ok := result.Workflow.Activities["activities.CheckI"]; !ok {
		t.Fatalf("含运行时变量输入的 Init activity 符号仍应进 Activities 表, got %+v", result.Workflow.Activities)
	}
}

// 保底：翻译产物中 Init 语句仍在（go2wlang 执行态不受展示态扫描影响）。
func TestInitScan_ProgramJSONContainsInit(t *testing.T) {
	result, err := generateInitScan(t, `
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.CheckC, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
		return err
	}
	return nil`)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(string(result.ProgramJSON), "activities.CheckC") {
		t.Fatalf("执行态 ProgramJSON 应含 Init 内的 activity 调用")
	}
}
