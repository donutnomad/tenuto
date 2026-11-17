package versionedworkflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/artifact"
	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/docs/examples/versioned-workflow/activities"
	"github.com/donutnomad/tenuto/docs/examples/versioned-workflow/remote"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
)

// activityImpl 把一次 activity 调度变成一次真实的 Go 函数调用。
type activityImpl func(context.Context, map[string]any) (map[string]any, error)

// lifecycleActivities 是示例 worker 注册的实现表，按 activity type 索引。
func lifecycleActivities(t *testing.T) map[string]activityImpl {
	t.Helper()
	return map[string]activityImpl{
		"lifecycle.reserve":   wrap[activities.ReserveInput, activities.ReserveOutput](activities.Reserve),
		"lifecycle.charge":    wrap[activities.ChargeInput, activities.ChargeOutput](activities.Charge),
		"lifecycle.charge-v2": wrap[activities.ChargeInput, activities.ChargeOutput](activities.ChargeV2),
		"lifecycle.notify":    wrap[activities.NotifyInput, activities.NotifyOutput](activities.Notify),
		// remote 契约 payments.settle：实现在 worker 侧，测试里用同一份确定性实现。
		"payments.settle": func(_ context.Context, input map[string]any) (map[string]any, error) {
			var in remote.SettleInput
			if err := decode(input, &in); err != nil {
				return nil, err
			}
			return encode(remote.SettleOutput{Receipt: fmt.Sprintf("receipt-%s-%d", in.OrderID, in.Amount), Settled: true})
		},
	}
}

// wrap 把强类型 activity 实现适配成「map 进、map 出」的 worker 形态。
func wrap[I, O any](fn func(context.Context, I) (O, error)) activityImpl {
	return func(ctx context.Context, input map[string]any) (map[string]any, error) {
		var in I
		if err := decode(input, &in); err != nil {
			return nil, err
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		return encode(out)
	}
}

func decode(input map[string]any, target any) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func encode(value any) (map[string]any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// generate 走真实的注解生成路径（扫描 → 重写 → 翻译）。
func generate(t *testing.T, dir string) sdkgen.PackageResult {
	t.Helper()
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatalf("Scan(%s): %v", dir, err)
	}
	result, err := sdkgen.Generate(plan)
	if err != nil {
		t.Fatalf("Generate(%s): %v", dir, err)
	}
	return result
}

// nodesOf 把生成出的 WorkflowConfig 解析成执行节点。
func nodesOf(t *testing.T, result sdkgen.PackageResult) *program.WorkflowConfig {
	t.Helper()
	nodes := result.Workflow
	return nodes
}

// runResult 是一次完整执行的产物。
type runResult struct {
	History []historypkg.Event
	Output  map[string]any
	// Executed 是本次实际执行过的 activity type（按执行顺序）。
	Executed []string
	// Queues 是调度命令上的 task queue（与 Executed 一一对应）。
	Queues []string
}

// runWorkflow 用解释器把程序跑到终态，activity 由本地实现真实执行。
//
// 这是最小引擎：每轮 Execute 产出的命令转成历史事件，activity 命令交给
// lifecycleActivities 求值，然后补 WorkflowTaskCompleted 进入下一轮。
func runWorkflow(t *testing.T, nodes *program.WorkflowConfig, input map[string]any) runResult {
	t.Helper()
	impls := lifecycleActivities(t)
	history := []historypkg.Event{
		historypkg.NewWorkflowStartedEvent(input, "", "", "lifecycle-demo", "Lifecycle Demo", 0),
	}
	history[0].ID = 1
	result := runResult{}
	for round := 0; round < 50; round++ {
		commands, err := interpreter.NewWorkflowExecutor(nodes, history).Execute()
		if err != nil {
			t.Fatalf("第 %d 轮执行失败: %v", round, err)
		}
		if len(commands) == 0 {
			t.Fatalf("第 %d 轮没有命令也没有终态", round)
		}
		// 历史顺序照 engine 落库顺序：本轮以 WorkflowTaskCompleted 收尾，
		// 随后才是本轮产出的命令对应事件（scheduled/completed/marker）。
		history = append(history, historypkg.Event{
			ID:                    int64(len(history) + 1),
			Timestamp:             time.Unix(0, int64(len(history))),
			WorkflowTaskCompleted: &historypkg.WorkflowTaskCompletedEvent{},
		})
		terminal := false
		for _, command := range commands {
			switch command.Type {
			case historypkg.CmdScheduleActivity:
				scheduledID := int64(len(history) + 1)
				history = append(history, historypkg.Event{
					ID:        scheduledID,
					Timestamp: time.Unix(0, scheduledID),
					ActivityScheduled: &historypkg.ActivityScheduledEvent{
						ActivityID:   command.ActivityID,
						ActivityType: command.ActivityType,
						TaskQueue:    command.TaskQueue,
						Input:        command.Input,
					},
				})
				impl, ok := impls[command.ActivityType]
				if !ok {
					t.Fatalf("没有 activity %q 的实现", command.ActivityType)
				}
				result.Executed = append(result.Executed, command.ActivityType)
				result.Queues = append(result.Queues, command.TaskQueue)
				output, err := impl(context.Background(), command.Input)
				if err != nil {
					t.Fatalf("activity %s 执行失败: %v", command.ActivityType, err)
				}
				history = append(history, historypkg.Event{
					ID:        int64(len(history) + 1),
					Timestamp: time.Unix(0, scheduledID),
					ActivityCompleted: &historypkg.ActivityCompletedEvent{
						ActivityID:       command.ActivityID,
						ScheduledEventID: scheduledID,
						ActivityType:     command.ActivityType,
						Output:           output,
					},
				})
			case historypkg.CmdRecordMarker:
				history = append(history, historypkg.Event{
					ID:        int64(len(history) + 1),
					Timestamp: time.Unix(0, 0),
					MarkerRecorded: &historypkg.MarkerRecordedEvent{
						MarkerName: command.MarkerName,
						MarkerID:   command.MarkerID,
						Value:      command.Input,
					},
				})
			case historypkg.CmdUpsertSearchAttributes:
				history = append(history, historypkg.Event{
					ID:                       int64(len(history) + 1),
					Timestamp:                time.Unix(0, 0),
					SearchAttributesUpserted: &historypkg.SearchAttributesUpsertedEvent{Attributes: command.Input},
				})
			case historypkg.CmdCompleteWorkflow:
				result.Output = command.Output
				terminal = true
			case historypkg.CmdFailWorkflow:
				t.Fatalf("workflow 失败: %s", command.Error)
			default:
				t.Fatalf("不支持的命令 %v", command.Type)
			}
		}
		if terminal {
			result.History = history
			return result
		}
	}
	t.Fatalf("50 轮内没有到达终态")
	return runResult{}
}

// TestSourceVersionCompatibility 把 v1 历史交给 v2 Program，验证 GetVersion 旧分支。
func TestSourceVersionCompatibility(t *testing.T) {
	v1 := generate(t, "v1")
	v2 := generate(t, "v2")

	firstRun := runWorkflow(t, nodesOf(t, v1), map[string]any{"order_id": "order-1"})
	if got := firstRun.Output["note"]; got != "legacy" {
		t.Fatalf("v1 新执行应走 legacy 分支，note=%v", got)
	}
	if want := []string{"lifecycle.reserve", "lifecycle.charge", "lifecycle.notify"}; !equalStrings(firstRun.Executed, want) {
		t.Fatalf("v1 执行顺序 = %v，期望 %v", firstRun.Executed, want)
	}
	historyHints := v1HistorySummary(firstRun.History)

	// 同一份 v1 历史交给 v2 Program：GetVersion 读回 4，保留旧扣款分支。
	replayed := replay(t, nodesOf(t, v2), firstRun.History, map[string]any{"order_id": "order-1"})
	if got := replayed.Output["policy"]; got != "legacy" {
		t.Fatalf("v1 历史在 v2 Program 下应走 legacy 分支，policy=%v（%s）", got, historyHints)
	}
	if got := replayed.Output["charge_id"]; got != "charge-order-1" {
		t.Fatalf("v1 历史应继续用 lifecycle.charge，charge_id=%v", got)
	}
	for _, executed := range replayed.Executed {
		if executed == "lifecycle.charge-v2" || executed == "payments.settle" {
			t.Fatalf("旧历史不应调度新 activity，实际执行 %v", replayed.Executed)
		}
	}

	// 新执行记录 6：走 ChargeV2 + remote 清算。
	freshRun := runWorkflow(t, nodesOf(t, v2), map[string]any{"order_id": "order-2", "amount": 4200})
	if got := freshRun.Output["policy"]; got != "v2" {
		t.Fatalf("v2 新执行应走 v2 分支，policy=%v", got)
	}
	if got := freshRun.Output["receipt"]; got != "receipt-order-2-4200" {
		t.Fatalf("v2 新执行应调用 remote 清算，receipt=%v", got)
	}
	wantExecuted := []string{"lifecycle.reserve", "lifecycle.charge-v2", "payments.settle", "lifecycle.notify"}
	if !equalStrings(freshRun.Executed, wantExecuted) {
		t.Fatalf("v2 执行顺序 = %v，期望 %v", freshRun.Executed, wantExecuted)
	}
	// 干跑的调度命令必须落到注解声明的队列：@Workflow(queue=lifecycle) 作为默认，
	// payments.settle 用调用点显式的 workflow.ActivityOptions{TaskQueue: "payments"}。
	wantQueues := []string{"lifecycle", "lifecycle", "payments", "lifecycle"}
	if !equalStrings(freshRun.Queues, wantQueues) {
		t.Fatalf("v2 调度队列 = %v，期望 %v", freshRun.Queues, wantQueues)
	}
}

// replay 只重放给定历史，不再新增 activity，用于确认旧历史在 v2 Program 下能跑完。
// TestMarkerlessHistoryIsOutsideSupportedRange 锁住 v2 源码注释里的第三条：历史越过 GetVersion
// 却没有记录时得到 DefaultVersion(-1)，不在 [3, 6] 内，重放以非确定性错误失败。
func TestMarkerlessHistoryIsOutsideSupportedRange(t *testing.T) {
	v2 := generate(t, "v2")
	started := historypkg.NewWorkflowStartedEvent(map[string]any{"order_id": "order-0"}, "", "", "", "", 0)
	taskCompleted := historypkg.NewWorkflowTaskCompletedEvent(0, "")
	_, err := interpreter.NewWorkflowExecutor(nodesOf(t, v2), []historypkg.Event{started, taskCompleted}).Execute()
	if err == nil {
		t.Fatal("没有 GetVersion 记录的历史应当重放失败")
	}
	for _, want := range []string{"charge-policy", "removed support of version -1", "oldest supported version is 3"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误 %q 不包含 %q", err.Error(), want)
		}
	}
}

func replay(t *testing.T, nodes *program.WorkflowConfig, history []historypkg.Event, input map[string]any) runResult {
	t.Helper()
	commands, err := interpreter.NewWorkflowExecutor(nodes, history).Execute()
	if err != nil {
		t.Fatalf("重放 v1 历史失败: %v", err)
	}
	result := runResult{}
	for _, command := range commands {
		switch command.Type {
		case historypkg.CmdCompleteWorkflow:
			result.Output = command.Output
		case historypkg.CmdFailWorkflow:
			t.Fatalf("重放 v1 历史后 workflow 失败: %s", command.Error)
		case historypkg.CmdScheduleActivity:
			// 旧历史里已经完成的 activity 在重放时命中历史事实，只补 shadow，不会重新调度。
			result.Executed = append(result.Executed, command.ActivityType)
		default:
			t.Fatalf("重放出现非预期命令 %v", command.Type)
		}
	}
	if result.Output == nil {
		t.Fatalf("重放 v1 历史没有到达完成态，命令 %v", commands)
	}
	return result
}

// v1HistorySummary 给出历史里 marker 的摘要，失败时帮助定位。
func v1HistorySummary(history []historypkg.Event) string {
	summary := ""
	for _, event := range history {
		if event.MarkerRecorded != nil {
			summary += fmt.Sprintf("%s=%v ", event.MarkerRecorded.MarkerID, event.MarkerRecorded.Value)
		}
	}
	return "markers: " + summary
}

func equalStrings(left, right []string) bool {
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

// TestGeneratedArtifactsAreStable 守住二次运行字节稳定：重新生成必须与仓库里的
// JSON 完全一致，否则说明存在时间戳、map 顺序或随机命名泄漏。
func TestGeneratedArtifactsAreStable(t *testing.T) {
	for _, dir := range []string{"v1", "v2"} {
		t.Run(dir, func(t *testing.T) {
			plan, err := sdkgen.Scan(dir)
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			first, err := sdkgen.Generate(plan)
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			second, err := sdkgen.Generate(plan)
			if err != nil {
				t.Fatalf("Generate（第二次）: %v", err)
			}
			for _, file := range []string{"workflow.json", "activities.json"} {
				if string(artifactBytes(t, first, file)) != string(artifactBytes(t, second, file)) {
					t.Fatalf("两次生成的 %s 不同", file)
				}
				committed, err := os.ReadFile(filepath.Join(dir, file))
				if err != nil {
					t.Fatalf("读取已提交产物 %s: %v", file, err)
				}
				generated := artifactBytes(t, first, file)
				if string(committed) != string(generated) {
					t.Fatalf("%s/%s 与重新生成的结果不一致，需要重新运行 tenuto-workflow-gen", dir, file)
				}
			}
			for _, bridge := range first.Bridges {
				committed, err := os.ReadFile(bridge.Path)
				if err != nil {
					t.Fatalf("读取已提交桥文件 %s: %v", bridge.Path, err)
				}
				if string(committed) != string(bridge.Content) {
					t.Fatalf("%s 与重新生成的桥文件不一致", bridge.Path)
				}
			}
		})
	}
}

// artifactBytes 把生成结果序列化成与 CLI 写盘相同的字节。
func artifactBytes(t *testing.T, result sdkgen.PackageResult, name string) []byte {
	t.Helper()
	var value any
	switch name {
	case "workflow.json":
		value = result.Workflow
	case "activities.json":
		value = artifact.Manifest(result.Activities)
	default:
		t.Fatalf("未知产物 %s", name)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("序列化 %s: %v", name, err)
	}
	return append(data, '\n')
}

// TestRemoteContractFreshGeneration 锁住 remote 契约的完整链路：
// CallSettle → 生成器 AST 重写 → ExecuteActivity(remote.SettleRemoteActivity)+Get。
//
// 用例从一份**没有任何 zz_*_contract_gen.go / JSON** 的副本开始，验证首次生成成功、
// 二次生成字节稳定、删掉生成文件后仍能重建同样的产物。
func TestRemoteContractFreshGeneration(t *testing.T) {
	temp := copyExampleTree(t, "v1", "v2", "activities", "remote")
	for _, dir := range []string{"v1", "v2"} {
		t.Run(dir, func(t *testing.T) {
			plan, err := sdkgen.Scan(filepath.Join(temp, dir))
			if err != nil {
				t.Fatalf("无生成文件的 Scan 失败: %v", err)
			}
			writePlanBridges(t, plan)
			first, err := sdkgen.Generate(plan)
			if err != nil {
				t.Fatalf("首次生成失败: %v", err)
			}
			second, err := sdkgen.Generate(plan)
			if err != nil {
				t.Fatalf("二次生成失败: %v", err)
			}
			for _, name := range []string{"workflow.json", "activities.json"} {
				if string(artifactBytes(t, first, name)) != string(artifactBytes(t, second, name)) {
					t.Fatalf("二次生成的 %s 不稳定", name)
				}
			}
			// 删掉生成文件后重建：版本必须一致。
			bridgePaths := make([]string, 0, len(plan.Bridges))
			for _, bridge := range plan.Bridges {
				bridgePaths = append(bridgePaths, bridge.Path)
				if err := os.Remove(bridge.Path); err != nil {
					t.Fatal(err)
				}
			}
			replan, err := sdkgen.Scan(filepath.Join(temp, dir))
			if err != nil {
				t.Fatalf("删除生成文件后 Scan 失败: %v", err)
			}
			writePlanBridges(t, replan)
			rebuilt, err := sdkgen.Generate(replan)
			if err != nil {
				t.Fatalf("删除生成文件后重建失败: %v", err)
			}
			if string(artifactBytes(t, rebuilt, "workflow.json")) != string(artifactBytes(t, first, "workflow.json")) {
				t.Fatal("重建后 workflow.json 漂移")
			}
			_ = bridgePaths
		})
	}

	// remote 契约真的进了 v2 的 WLang 程序：ExecuteActivity 的 marker 是生成的零值变量。
	v2Program := string(mustArtifactJSON(t, generate(t, filepath.Join(temp, "v2")).Workflow.Program))
	if !strings.Contains(v2Program, `"symbol":"remote.SettleRemoteActivity"`) {
		t.Fatalf("v2 WLang 里没有 remote marker 符号:\n%s", v2Program)
	}
	if !strings.Contains(v2Program, `"ExecuteActivity"`) || !strings.Contains(v2Program, `"Get"`) {
		t.Fatalf("v2 WLang 里没有 ExecuteActivity/Get:\n%s", v2Program)
	}
	if strings.Contains(v2Program, `"CallSettle"`) {
		t.Fatalf("v2 WLang 里仍有未重写的 CallSettle:\n%s", v2Program)
	}
	remoteBridge, err := os.ReadFile(filepath.Join(temp, "remote", "zz_remote_contract_gen.go"))
	if err != nil {
		t.Fatalf("remote 桥文件缺失: %v", err)
	}
	for _, want := range []string{
		"func SettleRemoteActivity(_ context.Context, _ SettleInput) (SettleOutput, error)",
		"func CallSettle(ctx workflow.Context, in SettleInput, opts ...workflow.ActivityOptions) (SettleOutput, error)",
		"workflow.ExecuteActivity(ctx, SettleRemoteActivity, in, opts...)",
	} {
		if !strings.Contains(string(remoteBridge), want) {
			t.Fatalf("remote 桥缺少 %q:\n%s", want, remoteBridge)
		}
	}
}

// copyExampleTree 把示例目录复制到 testdata 下的临时目录，并把 import 前缀改到副本，
// 保证「没有任何生成文件」的副本能独立编译与生成。
func copyExampleTree(t *testing.T, names ...string) string {
	t.Helper()
	modulePath := "github.com/donutnomad/tenuto/docs/examples/versioned-workflow"
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove("testdata") })
	tempRoot, err := os.MkdirTemp("testdata", "tmp-example-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tempRoot) })
	absolute, err := filepath.Abs(tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	copiedPrefix := modulePath + "/testdata/" + filepath.ToSlash(filepath.Base(tempRoot))
	for _, name := range names {
		source := filepath.Join(name)
		target := filepath.Join(absolute, name)
		if err := os.MkdirAll(target, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := copyGoFiles(source, target, modulePath, copiedPrefix); err != nil {
			t.Fatal(err)
		}
	}
	return absolute
}

// copyGoFiles 只复制手写 Go 文件（跳过 zz_* 生成文件与 _test.go）。
func copyGoFiles(source, target, modulePath, copiedPrefix string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		from := filepath.Join(source, name)
		to := filepath.Join(target, name)
		if entry.IsDir() {
			if err := os.MkdirAll(to, 0o755); err != nil {
				return err
			}
			if err := copyGoFiles(from, to, modulePath, copiedPrefix); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "zz_") {
			continue
		}
		data, err := os.ReadFile(from)
		if err != nil {
			return err
		}
		rewritten := strings.ReplaceAll(string(data), modulePath, copiedPrefix)
		if err := os.WriteFile(to, []byte(rewritten), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// writePlanBridges 把桥文件落盘（CLI 在 Generate 之前做的事）。
func writePlanBridges(t *testing.T, plan sdkgen.Plan) {
	t.Helper()
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatalf("写桥文件 %s: %v", bridge.Path, err)
		}
	}
}

// mustArtifactJSON 把 artifact 序列化成便于断言的字符串。
func mustArtifactJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestRegisterActivitiesMatchesManifest 锁住生成 helper 的验收点：
// activities.RegisterActivities 注册全部本地实现，上报的契约与 activities.json 逐字段一致
// （同一 wire 指纹算法），handler 可直接调用，remote 契约不被它注册。
func TestRegisterActivitiesMatchesManifest(t *testing.T) {
	registry := wfsdk.NewRegistry()
	if err := activities.RegisterActivities(registry); err != nil {
		t.Fatalf("RegisterActivities: %v", err)
	}
	manifest, err := artifact.LoadDir("v2")
	if err != nil {
		t.Fatalf("加载 activities.json: %v", err)
	}
	declared := map[string]artifact.Activity{}
	for _, activity := range manifest.Activities {
		declared[activity.Type] = activity
	}
	reported := registry.Contracts()
	if len(reported) != 4 {
		t.Fatalf("应注册 4 个本地 activity，实际 %d: %+v", len(reported), reported)
	}
	for _, contract := range reported {
		want, ok := declared[contract.Type]
		if !ok {
			t.Fatalf("上报的 %s 不在 activities.json 里（%v）", contract.Type, declared)
		}
		if contract.Version != want.Version || contract.Input != want.Input || contract.Output != want.Output {
			t.Fatalf("%s 契约与 activities.json 不一致：上报 %+v，声明 %+v", contract.Type, contract, want)
		}
	}
	// 注册的是同一份实现：handler 能直接调用。
	handler, err := registry.Get("lifecycle.reserve")
	if err != nil {
		t.Fatalf("lifecycle.reserve 未注册: %v", err)
	}
	out, err := handler(context.Background(), map[string]any{"order_id": "order-1"})
	if err != nil {
		t.Fatalf("调用 lifecycle.reserve: %v", err)
	}
	if out["reservation_id"] != "reservation-order-1" {
		t.Fatalf("lifecycle.reserve 输出异常: %+v", out)
	}
	// remote 契约 payments.settle 的实现由拥有它的 worker 提供，生成 helper 不注册。
	if _, err := registry.Get("payments.settle"); err == nil {
		t.Fatal("RegisterActivities 不能注册 remote 契约 payments.settle")
	}
}

// TestFreshGeneratedActivityHelperCompiles 锁住「首次生成即可编译」：
// 从没有 zz_* 生成文件的副本出发，生成 activities/remote 的桥文件后直接构建这两个包。
func TestFreshGeneratedActivityHelperCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("需要 go build")
	}
	temp := copyExampleTree(t, "v1", "v2", "activities", "remote")
	for _, dir := range []string{"v1", "v2"} {
		plan, err := sdkgen.Scan(filepath.Join(temp, dir))
		if err != nil {
			t.Fatalf("Scan %s: %v", dir, err)
		}
		writePlanBridges(t, plan)
	}
	for _, dir := range []string{"activities", "remote"} {
		content, err := os.ReadFile(filepath.Join(temp, dir, "zz_"+dir+"_contract_gen.go"))
		if err != nil {
			t.Fatalf("缺少 %s 的桥文件: %v", dir, err)
		}
		hasHelper := strings.Contains(string(content), "func RegisterActivities(")
		if want := dir == "activities"; hasHelper != want {
			t.Fatalf("%s 桥文件的 RegisterActivities 存在性 = %v，期望 %v", dir, hasHelper, want)
		}
	}
	command := exec.Command("go", "build", "./activities", "./remote")
	command.Dir = temp
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("首次生成的 activity 包无法构建: %v\n%s", err, output)
	}
}
