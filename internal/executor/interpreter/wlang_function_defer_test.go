package interpreter

// ============================================
// WLang defer 函数级语义：executor 级集成回归
// ============================================
// 背景：WLang 的 defer 原先归属于「当前块 scope」，`if`/`for`/`foreach`/`select` 块退出就执行，
// 与 Go 的函数级语义相反。Saga 的补偿写法（注册在 if / 循环里、函数返回时 LIFO 执行）在旧
// 语义下会提前执行或整段漏补偿。
//
// 本文件锁住 executor 级可见行为：
//   - Reserve 成功 → Pay 成功 → Ship 失败 → Refund → Release（LIFO），每个 activity 单独跨 WFT；
//   - 补偿注册分别在 if 块与 for 块内 → 只在函数返回时生效。旧块级语义下 if-defer 会在
//     Pay 之前、for-defer 会在 Ship 之前就 drain（守卫看到 err == nil 直接跳过，等于漏补偿），
//     因此「Ship 失败后的下一轮才首次出现补偿调度」这条断言会转红；
//   - 每轮一个 WFT 的真实 Event history 多轮 Execute()；终态轮 replay 不再重复调度；
//   - 补偿自身失败时同一函数 frame 的 drain 继续（后登记的 Refund 失败不影响先登记的
//     Release），业务失败通道保留原 typed Failure，运行时报错通道不吸收补偿错误。
//
// 证据边界：只有 interpreter/executor 层（真实 sdkgen 生成 WorkflowConfig →
// NewWorkflowExecutor.Execute），不启动 server/worker，不连真实 DB；
// activity 结果由 Event history 注入，不是真实外部服务调用。
// ============================================

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
)

// ============================================
// 生成与驱动 helper
// ============================================

// generateDeferProgram 用真实 sdkgen 从 Go 源码生成 Program（不手写 WorkflowConfig）。
func generateDeferProgram(t *testing.T, src string) sdkgen.Result {
	t.Helper()
	result, err := sdkgen.GenerateFromSource([]byte(src), sdkgen.Options{
		WorkflowType: "wlang-function-defer",
		FuncName:     "W",
		Filename:     "w.go",
	})
	if err != nil {
		t.Fatalf("sdkgen.GenerateFromSource: %v", err)
	}
	return result
}

// deferSagaRound 是一轮 WFT 的观察结果。
type deferSagaRound struct {
	scheduled []string             // 本轮新调度的 activity type（按命令顺序）
	commands  []historypkg.Command // 本轮全部命令，失败时打印用
}

// driveDeferSaga 用真实 Event history 逐轮驱动 executor：一轮 = 一个 WFT。
//
// 每轮对当前 history 跑一次 Execute()；本轮新调度的 activity 按剧本落成 ActivityScheduled +
// ActivityCompleted/ActivityFailed 事件，历史里追加 WorkflowTaskCompleted 关闭该批——下一轮从
// 批次边界重新 replay。出现 CmdFailWorkflow / CmdCompleteWorkflow 即收敛。
//
// failures 按 activity type 指定失败剧本（缺省正常完成）。任何一轮 Execute() 返回非 nil error
// 都直接失败：运行时报错通道不得混入业务失败/补偿失败。
func driveDeferSaga(t *testing.T, nodes *programpkg.WorkflowConfig, failures map[string]*protocol.Failure) ([]deferSagaRound, []historypkg.Command) {
	t.Helper()
	history := []historypkg.Event{historypkg.NewWorkflowStartedEvent(map[string]any{"order_id": "o-1"}, "", "", "", "", 0)}
	nextID := int64(2)
	var rounds []deferSagaRound
	for round := 1; round <= 16; round++ {
		commands, err := NewWorkflowExecutor(nodes, history).Execute()
		if err != nil {
			t.Fatalf("第 %d 轮 Execute 返回运行时报错（业务/补偿失败不得走这条通道）: %v，命令 %+v", round, err, commands)
		}
		observation := deferSagaRound{commands: commands}
		var (
			terminal bool
			outcomes []historypkg.Event
		)
		for _, command := range commands {
			switch command.Type {
			case historypkg.CmdScheduleActivity:
				observation.scheduled = append(observation.scheduled, command.ActivityType)
				scheduledEventID := nextID
				outcomes = append(outcomes, historypkg.Event{
					ID: nextID, Timestamp: time.Now(),
					ActivityScheduled: &historypkg.ActivityScheduledEvent{ActivityID: command.ActivityID, ActivityType: command.ActivityType},
				})
				nextID++
				if failure := failures[command.ActivityType]; failure != nil {
					outcomes = append(outcomes, historypkg.Event{
						ID: nextID, Timestamp: time.Now(),
						ActivityFailed: &historypkg.ActivityFailedEvent{
							ScheduledEventID: scheduledEventID,
							Error:            failure.Message,
							Failure:          failure,
						},
					})
				} else {
					outcomes = append(outcomes, historypkg.Event{
						ID: nextID, Timestamp: time.Now(),
						ActivityCompleted: &historypkg.ActivityCompletedEvent{
							ScheduledEventID: scheduledEventID,
							Output:           map[string]any{"ok": true},
						},
					})
				}
				nextID++
			case historypkg.CmdFailWorkflow, historypkg.CmdCompleteWorkflow:
				terminal = true
			}
		}
		rounds = append(rounds, observation)
		if terminal {
			return rounds, commands
		}
		history = append(history, historypkg.Event{ID: nextID, Timestamp: time.Now(), WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{}})
		nextID++
		history = append(history, outcomes...)
	}
	t.Fatalf("16 轮内未收敛，轮次: %+v", rounds)
	return nil, nil
}

// assertFunctionLevelDeferSaga 断言「Ship 失败后按函数级 LIFO 补偿」的完整调度轨迹，
// 外加「终态保留原 typed Failure 且 message 为空」。
//
// steps 是五步 activity type 的期望顺序：正向三步 + LIFO 补偿两步；failType 是终态
// CmdFailWorkflow 应保留的业务失败类型。
func assertFunctionLevelDeferSaga(t *testing.T, rounds []deferSagaRound, steps []string, failType string) {
	t.Helper()
	failure := assertDeferSagaTrace(t, rounds, steps)
	if failure.WorkflowFailure == nil {
		t.Fatalf("终态必须携带 typed Failure，实际 %+v（Error=%q）", failure, failure.Error)
	}
	if failure.WorkflowFailure.Type != failType {
		t.Fatalf("终态 Failure.Type = %q，期望保留原业务失败 %q（补偿失败不得覆盖）: %+v",
			failure.WorkflowFailure.Type, failType, failure.WorkflowFailure)
	}
	if failure.WorkflowFailure.Message != "" || failure.Error != "" {
		t.Fatalf("终态 Failure.Message = %q、Command.Error = %q，期望原 typed Failure 的空 message",
			failure.WorkflowFailure.Message, failure.Error)
	}
}

// assertDeferSagaTrace 断言跨轮调度轨迹：序列、每轮一个 activity、每个 activity 只调度一次、
// 终态只出现在最后一轮、终态轮不再调度；返回唯一的终态 CmdFailWorkflow。
func assertDeferSagaTrace(t *testing.T, rounds []deferSagaRound, steps []string) historypkg.Command {
	t.Helper()
	if got := deferScheduledSequence(rounds); !equalDeferSequence(got, steps) {
		t.Fatalf("跨轮调度序列 = %v，期望 %v\n轮次: %+v", got, steps, rounds)
	}
	// 每轮恰好一个 activity：正向三步各一轮，两个补偿各跨一轮，最后是终态轮。
	if len(rounds) != len(steps)+1 {
		t.Fatalf("轮次数 = %d，期望 %d（五步各一轮 + 终态轮），轮次: %+v", len(rounds), len(steps)+1, rounds)
	}
	for index, want := range steps {
		if len(rounds[index].scheduled) != 1 || rounds[index].scheduled[0] != want {
			t.Fatalf("第 %d 轮调度 = %v，期望恰好 [%s]（块级 defer 语义会在这里提前/漏补偿）", index+1, rounds[index].scheduled, want)
		}
	}
	// 终态只能出现在最后一轮：业务失败后的补偿要跨 WFT drain，任何提前的 Fail/Complete 都是错的。
	for index := 0; index < len(rounds)-1; index++ {
		if hasFailWorkflowCommand(rounds[index].commands) {
			t.Fatalf("第 %d 轮不应产出 CmdFailWorkflow（补偿还没 drain 完），命令 %+v", index+1, rounds[index].commands)
		}
		if hasCompleteWorkflow(rounds[index].commands) {
			t.Fatalf("第 %d 轮不应产出 CmdCompleteWorkflow，命令 %+v", index+1, rounds[index].commands)
		}
	}
	// 不重复调度：每个 step 全程只出现一次（replay 轮只产 shadow）。
	seen := map[string]int{}
	for _, round := range rounds {
		for _, activityType := range round.scheduled {
			seen[activityType]++
		}
	}
	for _, step := range steps {
		if seen[step] != 1 {
			t.Fatalf("%s 调度次数 = %d，期望 1（replay 重复调度）", step, seen[step])
		}
	}

	if len(rounds[len(rounds)-1].scheduled) != 0 {
		t.Fatalf("终态轮不应再调度 activity，实际 %v", rounds[len(rounds)-1].scheduled)
	}
	failures := failWorkflowCommands(rounds[len(rounds)-1].commands)
	if len(failures) != 1 {
		t.Fatalf("终态应恰好一条 CmdFailWorkflow，实际 %+v", rounds[len(rounds)-1].commands)
	}
	return failures[0]
}

// deferScheduledSequence 展开每轮新调度的 activity type。
func deferScheduledSequence(rounds []deferSagaRound) []string {
	var sequence []string
	for _, round := range rounds {
		sequence = append(sequence, round.scheduled...)
	}
	return sequence
}

// equalDeferSequence 逐项比较两个调度序列。
func equalDeferSequence(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// hasFailWorkflowCommand 判断某轮是否已产失败终态。
func hasFailWorkflowCommand(commands []historypkg.Command) bool {
	return len(failWorkflowCommands(commands)) > 0
}

// failWorkflowCommands 过滤出 CmdFailWorkflow。
func failWorkflowCommands(commands []historypkg.Command) []historypkg.Command {
	var failures []historypkg.Command
	for _, command := range commands {
		if command.Type == historypkg.CmdFailWorkflow {
			failures = append(failures, command)
		}
	}
	return failures
}

// ============================================
// Happy Path：直接 ExecuteActivity + Get 形态（上一轮 probe 的对照写法）
// ============================================

// 补偿注册刻意放在 if 块与 for 块内：函数级语义下两者都攒到函数返回时 LIFO 执行；
// 块级语义下 if-defer 在 Pay 之前、for-defer 在 Ship 之前就 drain（守卫看到 err == nil，
// 补偿被整段跳过），调度序列断言会转红。
//
// 业务错误与补偿错误分开存：`err` 是入口命名返回值（业务通道），`compensationError` 只记录
// 补偿失败，不回写 `err`——回写会把补偿结果变成工作流终态错误（上一轮 probe 实测过这个坑）。
const wlangFunctionDeferDirectSrc = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) (err error) {
	var compensationError error
	var reservedOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Reserve, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reservedOut)
	if err != nil {
		return err
	}
	reserved := true
	if reserved {
		defer func() {
			if err == nil {
				return
			}
			var out map[string]any
			compErr := workflow.ExecuteActivity(ctx, activities.Release, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
			if compErr != nil {
				compensationError = compErr
			}
		}()
	}
	for round := 0; round < 1; round++ {
		var paidOut map[string]any
		err = workflow.ExecuteActivity(ctx, activities.Pay, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &paidOut)
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				return
			}
			var out map[string]any
			compErr := workflow.ExecuteActivity(ctx, activities.Refund, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
			if compErr != nil {
				compensationError = compErr
			}
		}()
	}
	var shippedOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Ship, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &shippedOut)
	if err != nil {
		return err
	}
	// 正常路径上两处补偿都因 err != nil 守卫而跳过，compensationError 恒为 nil；
	// 这里读一次，明确「补偿失败独立记录、绝不覆盖业务错误」的意图。
	if compensationError != nil {
		return compensationError
	}
	return nil
}
`

// Happy Path：Reserve/Pay 成功、Ship 失败 → Refund（后登记）先于 Release（先登记）执行，
// 终态保留 Ship 的 typed Failure 且 message 为空。
func TestWLangFunctionDeferSagaDirectActivity(t *testing.T) {
	result := generateDeferProgram(t, wlangFunctionDeferDirectSrc)
	nodes := result.Workflow

	rounds, _ := driveDeferSaga(t, nodes, map[string]*protocol.Failure{
		"activities.Ship": {Type: "ShipRejected"},
	})
	assertFunctionLevelDeferSaga(t, rounds, []string{
		"activities.Reserve",
		"activities.Pay",
		"activities.Ship",
		"activities.Refund",
		"activities.Release",
	}, "ShipRejected")
}

// ============================================
// Happy Path：defer 闭包内用生成的 CallX（当前正式作者用法）
// ============================================

// callXActivitiesSrc 是契约包（有 @Activity 注解），生成器从这里产出 CallX 桥。
const callXActivitiesSrc = `package activities

import "context"

type OrderInput struct {
	OrderID string ` + "`json:\"order_id\"`" + `
}

type OrderOutput struct {
	OK bool ` + "`json:\"ok\"`" + `
}

// @Activity(type="saga.reserve",queue="saga")
func Reserve(_ context.Context, in OrderInput) (OrderOutput, error) { return OrderOutput{OK: true}, nil }

// @Activity(type="saga.pay",queue="saga")
func Pay(_ context.Context, in OrderInput) (OrderOutput, error) { return OrderOutput{OK: true}, nil }

// @Activity(type="saga.ship",queue="saga")
func Ship(_ context.Context, in OrderInput) (OrderOutput, error) { return OrderOutput{OK: true}, nil }

// @Activity(type="saga.refund",queue="saga")
func Refund(_ context.Context, in OrderInput) (OrderOutput, error) { return OrderOutput{OK: true}, nil }

// @Activity(type="saga.release",queue="saga")
func Release(_ context.Context, in OrderInput) (OrderOutput, error) { return OrderOutput{OK: true}, nil }
`

// __ACTIVITIES__ 运行时替换成临时目录的 import path。
const callXWorkflowSrc = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"__ACTIVITIES__"
)

// @Workflow(type="wlang-function-defer-callx",name="WLang Function Defer CallX",policy=auto_upgrade,version=v1)
func W(ctx workflow.Context, input map[string]any) (err error) {
	var compensationError error
	order := activities.OrderInput{OrderID: "o-1"}
	var reserved activities.OrderOutput
	reserved, err = activities.CallReserve(ctx, order, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return err
	}
	if reserved.OK {
		defer func() {
			if err == nil {
				return
			}
			var compErr error
			_, compErr = activities.CallRelease(ctx, order, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
			if compErr != nil {
				compensationError = compErr
			}
		}()
	}
	for round := 0; round < 1; round++ {
		var paid activities.OrderOutput
		paid, err = activities.CallPay(ctx, order, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if err != nil {
			return err
		}
		if paid.OK {
			defer func() {
				if err == nil {
					return
				}
				var compErr error
				_, compErr = activities.CallRefund(ctx, order, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
				if compErr != nil {
					compensationError = compErr
				}
			}()
		}
	}
	var shipped activities.OrderOutput
	shipped, err = activities.CallShip(ctx, order, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	_ = shipped // Ship 的输出不参与判断（生成器要求接收输出，错误单独判断）
	if err != nil {
		return err
	}
	if compensationError != nil {
		return compensationError
	}
	return nil
}
`

// Happy Path：defer 闭包体里的 CallX 被真实生成器重写成 ExecuteActivity + Get 后仍能调度，
// 且 LIFO / 跨 WFT / 终态 typed Failure 与直接形态一致。
//
// 输入走当前正式作者写法：同一个结构体变量 `order` 传给五个 CallX（跨包结构体变量必须被
// 展开成 json tag 命名的 map 载荷，否则运行时 ExecuteActivity 绑定 record 会报 E_TYPE；
// 重写侧回归见 sdkgen/rewrite_test.go 的 TestRewriteStructVariableInput）。
//
// CallX 需要磁盘上的契约包才能派生桥（Rewrite 阶段的 marker 与注解 type/queue 都来自包扫描），
// 因此这条用例走 Scan → 写桥 → Generate；临时目录建在 testdata 下并在 Cleanup 里删除，
// 因为 Scan 要求 package 落在本模块内（t.TempDir() 在模块外，找不到 go.mod）。
func TestWLangFunctionDeferSagaCallX(t *testing.T) {
	fixtureDir := writeCallXFixture(t)
	plan, err := sdkgen.Scan(fixtureDir)
	if err != nil {
		t.Fatalf("sdkgen.Scan: %v", err)
	}
	if len(plan.Bridges) != 1 {
		t.Fatalf("期望生成一份 CallX 桥，实际 %d 份", len(plan.Bridges))
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatalf("写桥文件 %s: %v", bridge.Path, err)
		}
	}
	result, err := sdkgen.Generate(plan)
	if err != nil {
		t.Fatalf("sdkgen.Generate: %v", err)
	}
	nodes := result.Workflow

	// 注解契约只有经桥（CallX → ExecuteActivity(marker)）才会带上注解里的 type/queue；
	// 直调形态的 ActivityType 会是 "activities.Reserve"、queue 为空。
	rounds, _ := driveDeferSaga(t, nodes, map[string]*protocol.Failure{
		"saga.ship": {Type: "ShipRejected"},
	})
	assertFunctionLevelDeferSaga(t, rounds, []string{
		"saga.reserve",
		"saga.pay",
		"saga.ship",
		"saga.refund",
		"saga.release",
	}, "ShipRejected")
	for _, round := range rounds {
		for _, command := range round.commands {
			if command.Type != historypkg.CmdScheduleActivity {
				continue
			}
			if command.TaskQueue != "saga" {
				t.Fatalf("activity %s 的 TaskQueue = %q，期望注解队列 saga（CallX 未被生成器重写）",
					command.ActivityType, command.TaskQueue)
			}
		}
	}
}

// writeCallXFixture 在 testdata 下建一个临时契约包 + workflow 包，返回 workflow 所在目录。
func writeCallXFixture(t *testing.T) string {
	t.Helper()
	moduleRoot, modulePath := deferModule(t)
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatalf("MkdirAll testdata: %v", err)
	}
	dir, err := os.MkdirTemp(filepath.Join("testdata"), "tmp-defer-callx-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	absolute, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	relative, err := filepath.Rel(moduleRoot, absolute)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}
	activitiesImport := modulePath + "/" + filepath.ToSlash(relative) + "/activities"
	if err := os.MkdirAll(filepath.Join(dir, "activities"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "activities", "activities.go"), []byte(callXActivitiesSrc), 0o644); err != nil {
		t.Fatalf("写契约包: %v", err)
	}
	workflowSource := strings.ReplaceAll(callXWorkflowSrc, "__ACTIVITIES__", activitiesImport)
	if err := os.WriteFile(filepath.Join(dir, "workflow.go"), []byte(workflowSource), 0o644); err != nil {
		t.Fatalf("写 workflow 包: %v", err)
	}
	return dir
}

// deferModule 从当前包目录向上找 go.mod，返回模块根与 module path。
func deferModule(t *testing.T) (string, string) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	for {
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
					return dir, strings.TrimSpace(rest)
				}
			}
			t.Fatalf("%s/go.mod 缺少 module 声明", dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("向上找不到 go.mod")
		}
		dir = parent
	}
}

// ============================================
// Error Case：补偿自身失败
// ============================================

// Error Case：后登记的 Refund 补偿失败 → 同一函数 frame 的 drain 继续，先登记的 Release 仍执行；
// 终态仍是 Ship 的原 typed Failure（不是 RefundUnavailable），Execute 不返回运行时报错。
func TestWLangFunctionDeferCompensationFailureKeepsBusinessFailure(t *testing.T) {
	result := generateDeferProgram(t, wlangFunctionDeferDirectSrc)
	nodes := result.Workflow

	rounds, final := driveDeferSaga(t, nodes, map[string]*protocol.Failure{
		"activities.Ship":   {Type: "ShipRejected"},
		"activities.Refund": {Type: "RefundUnavailable", Message: "refund-boom"},
	})
	assertFunctionLevelDeferSaga(t, rounds, []string{
		"activities.Reserve",
		"activities.Pay",
		"activities.Ship",
		"activities.Refund",
		"activities.Release",
	}, "ShipRejected")

	// drain 继续的直接证据：Refund 失败那一轮之后仍然调度了 Release。
	refundRound, releaseRound := -1, -1
	for index, round := range rounds {
		for _, activityType := range round.scheduled {
			switch activityType {
			case "activities.Refund":
				refundRound = index
			case "activities.Release":
				releaseRound = index
			}
		}
	}
	if refundRound < 0 || releaseRound <= refundRound {
		t.Fatalf("Release 应在 Refund 失败之后的轮次执行，refundRound=%d releaseRound=%d，轮次: %+v",
			refundRound, releaseRound, rounds)
	}
	failure := failWorkflowCommands(final)[0].WorkflowFailure
	if failure.Type == "RefundUnavailable" || failure.Cause != nil {
		t.Fatalf("补偿失败不得覆盖业务 Failure，实际 %+v", failure)
	}
}

// ============================================
// Error Case：补偿错误可查询 + named err 在补偿 WFT 之后写回
// ============================================

// 与 wlangFunctionDeferDirectSrc 的正向流程完全相同，唯一差别是补偿失败时的处理：
// 不再只记到独立变量，而是把「业务失败 + 补偿失败」包成 CompensationFailed，并把原业务
// 失败挂成 cause、补偿错误消息放进 details，写回入口命名返回值 err。
//
// 关键时序：Refund 的 Get 在补偿 WFT 才拿到失败事件，写回发生在那里；整个函数 frame 的
// defer drain 结束（Release 也跑完）之后，WLang 才解析命名返回值，所以终态 Failure 里
// 必须同时看到 CompensationFailed + 原 ShipRejected cause + refund-boom detail。
const wlangFunctionDeferCompensationWrapSrc = `package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func W(ctx workflow.Context, input map[string]any) (err error) {
	var reservedOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Reserve, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reservedOut)
	if err != nil {
		return err
	}
	reserved := true
	if reserved {
		defer func() {
			if err == nil {
				return
			}
			var out map[string]any
			compErr := workflow.ExecuteActivity(ctx, activities.Release, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
			if compErr != nil {
				err = workflow.NewApplicationErrorWithCause("workflow failed and compensation failed", "CompensationFailed", err, compErr.Error())
			}
		}()
	}
	for round := 0; round < 1; round++ {
		var paidOut map[string]any
		err = workflow.ExecuteActivity(ctx, activities.Pay, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &paidOut)
		if err != nil {
			return err
		}
		defer func() {
			if err == nil {
				return
			}
			var out map[string]any
			compErr := workflow.ExecuteActivity(ctx, activities.Refund, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
			if compErr != nil {
				err = workflow.NewApplicationErrorWithCause("workflow failed and compensation failed", "CompensationFailed", err, compErr.Error())
			}
		}()
	}
	var shippedOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Ship, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &shippedOut)
	if err != nil {
		return err
	}
	return nil
}
`

// Error Case：Refund 补偿失败 → 写回 CompensationFailed（cause = 原 ShipRejected、details 带
// refund-boom）→ 同一 frame 的 drain 继续调度 Release → 终态 CmdFailWorkflow 携带完整结构化失败。
func TestWLangFunctionDeferCompensationFailureIsQueryable(t *testing.T) {
	result := generateDeferProgram(t, wlangFunctionDeferCompensationWrapSrc)
	nodes := result.Workflow

	rounds, _ := driveDeferSaga(t, nodes, map[string]*protocol.Failure{
		"activities.Ship":   {Type: "ShipRejected"},
		"activities.Refund": {Type: "RefundUnavailable", Message: "refund-boom"},
	})
	// 补偿顺序仍是 LIFO：Refund 先执行并失败，Release 在它之后的轮次仍然执行。
	failure := assertDeferSagaTrace(t, rounds, []string{
		"activities.Reserve",
		"activities.Pay",
		"activities.Ship",
		"activities.Refund",
		"activities.Release",
	})

	workflowFailure := failure.WorkflowFailure
	if workflowFailure == nil {
		t.Fatalf("终态必须携带 typed Failure，实际 %+v（Error=%q）", failure, failure.Error)
	}
	// 写回生效：终态 type 是补偿包装后的 type，不再是原 ShipRejected。
	if workflowFailure.Type != "CompensationFailed" {
		t.Fatalf("终态 Failure.Type = %q，期望补偿包装后的 CompensationFailed（named err 写回失败）: %+v",
			workflowFailure.Type, workflowFailure)
	}
	if workflowFailure.Message != "workflow failed and compensation failed" {
		t.Fatalf("终态 Failure.Message = %q，期望补偿包装写入的 message", workflowFailure.Message)
	}
	// 原 typed Failure 保留为 cause（递归结构化，可查询）。
	if workflowFailure.Cause == nil || workflowFailure.Cause.Type != "ShipRejected" {
		t.Fatalf("终态 Failure.Cause 应保留原 ShipRejected，实际 %+v", workflowFailure.Cause)
	}
	if workflowFailure.Cause.Message != "" {
		t.Fatalf("cause 应是原 typed Failure（message 为空），实际 %+v", workflowFailure.Cause)
	}
	// 补偿错误消息进 details，可按 index 解码（API 侧 workflow.ApplicationErrorDetail 同源）。
	if len(workflowFailure.Details) == 0 {
		t.Fatalf("终态 Failure.Details 为空，期望包含补偿错误消息: %+v", workflowFailure)
	}
	var detail string
	if err := json.Unmarshal(workflowFailure.Details[0].Data, &detail); err != nil {
		t.Fatalf("解码 details[0] 失败: %v（原始 %q）", err, workflowFailure.Details[0].Data)
	}
	if !strings.Contains(detail, "refund-boom") {
		t.Fatalf("details[0] = %q，期望包含补偿错误消息 refund-boom", detail)
	}
	t.Logf("终态 Failure = type=%q cause.type=%q details[0]=%q（补偿 WFT 之后写回 named err）",
		workflowFailure.Type, workflowFailure.Cause.Type, detail)
}
