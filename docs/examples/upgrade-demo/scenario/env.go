// Package scenario 是 upgrade-demo 的场景脚本：通过 Server 的 HTTP API 启动 Run、发送
// Signal / Update / Query、切换应用的发布版本，并对每个场景的结果做断言。
//
// 同一份脚本有两个入口：cmd/driver 驱动真实的独立进程，scenario_test.go 在一个测试进程里
// 用内存后端的 Server 和真实 HTTP 跑同样的步骤。两者的区别只在 Fleet 的实现。
package scenario

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// Fleet 启动与停止应用进程。app 是 "app-a" 或 "app-b"，release 是 "v1"、"v2"、"v3"。
// Start 在 Worker 就绪后返回；Stop 在进程退出后返回。
type Fleet interface {
	Start(ctx context.Context, app, release string) error
	Stop(app, release string) error
}

// Env 是一次场景运行的环境。
type Env struct {
	URL       string
	Namespace string
	Fleet     Fleet
	Out       io.Writer
	// Timeout 是等待单个条件成立的上限。
	Timeout time.Duration
	// Checkpoint 非 nil 时在几个适合到 Console 里查看的位置被调用（例如 Run 刚暂停时）。
	Checkpoint func(what string)

	// sdk 启动 Run、发送 Signal / Update / Query、改策略、读 History；admin 读 Run 详情、Worker 与
	// Program 版本，这些是 Console 使用的运维端点，sdk/client 不提供。
	sdk   client.Client
	admin *adminAPI
	runs  []trackedRun
}

type trackedRun struct {
	workflowID string
	runID      string
	started    string // 启动时的 Program
}

// failure 让场景代码写成一条直线：任何一步失败都经 panic 回到 Run。
type failure struct{ err error }

func (e *Env) must(err error) {
	if err != nil {
		panic(failure{err})
	}
}

func (e *Env) failf(format string, args ...any) {
	panic(failure{fmt.Errorf(format, args...)})
}

func (e *Env) printf(format string, args ...any) {
	fmt.Fprintf(e.Out, format, args...)
}

func (e *Env) section(title string) {
	e.printf("\n━━ %s ━━\n", title)
}

func (e *Env) step(format string, args ...any) {
	e.printf("→ "+format+"\n", args...)
}

// expect 断言一个观察值，并把它写进输出。
func (e *Env) expect(what string, got, want any) {
	if !reflect.DeepEqual(got, want) {
		e.printf("  FAIL %s = %v，期望 %v\n", what, got, want)
		e.failf("%s = %v, want %v", what, got, want)
	}
	e.printf("  ok   %s = %v\n", what, got)
}

func (e *Env) expectContains(what, got string, wants ...string) {
	for _, want := range wants {
		if !strings.Contains(got, want) {
			e.printf("  FAIL %s 不包含 %q：%s\n", what, want, got)
			e.failf("%s does not contain %q: %s", what, want, got)
		}
	}
	e.printf("  ok   %s 包含 %s\n", what, strings.Join(wants, " 与 "))
}

func (e *Env) checkpoint(what string) {
	if e.Checkpoint != nil {
		e.Checkpoint(what)
	}
}

func (e *Env) ctx() context.Context { return context.Background() }

// ---------- 进程 ----------

// start 启动一个应用进程。Fleet.Start 在 Worker 就绪后返回：路由已经上报，定义已经登记。
func (e *Env) start(app, release string) {
	e.step("启动 %s %s", app, release)
	e.must(e.Fleet.Start(e.ctx(), app, release))
	e.expect(app+"-"+release+" 在线", e.online(app+"-"+release), true)
}

// stop 停止一个应用进程。Worker.Stop 向 Server 注销路由，进程退出时这个 build 已经退出选路。
func (e *Env) stop(app, release string) {
	e.step("停止 %s %s", app, release)
	e.must(e.Fleet.Stop(app, release))
	e.expect(app+"-"+release+" 在线（Worker.Stop 已注销路由）", e.online(app+"-"+release), false)
}

// online 报告 build 是否在活跃 Worker 列表里。
func (e *Env) online(build string) bool {
	for _, w := range e.workers() {
		if w.BuildID == build && len(w.ActivityTypes) > 0 {
			return true
		}
	}
	return false
}

func (e *Env) workers() []runtimeapi.WorkerListItem {
	var result runtimeapi.WorkerListResult
	e.must(e.admin.post(e.ctx(), runtimeapi.WorkflowRuntimeListWorkersPath,
		runtimeapi.WorkflowRuntimeListWorkersRequest{Filter: runtimeapi.WorkerListFilter{Namespace: e.Namespace}}, &result))
	return result.Items
}

func (e *Env) printWorkers() {
	builds := map[string][]string{}
	for _, w := range e.workers() {
		if len(w.ActivityTypes) > 0 {
			builds[w.BuildID] = w.ActivityTypes
		}
	}
	names := make([]string, 0, len(builds))
	for name := range builds {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		e.printf("  在线 build %-9s %s\n", name, strings.Join(builds[name], " "))
	}
}

// ---------- Run ----------

// waitForRun 轮询 cond；超时的时候把 runID 的状态与 History 写进输出，方便定位。
func (e *Env) waitForRun(runID, what string, cond func() bool) {
	deadline := time.Now().Add(e.Timeout)
	for !cond() {
		if time.Now().After(deadline) {
			if runID != "" {
				e.dump(runID)
			}
			e.failf("timed out after %s waiting for %s", e.Timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (e *Env) dump(runID string) {
	run := e.run(runID)
	e.printf("  ---- run %s (%s) status=%s policy=%s config_error=%q upgrade_error=%q\n", run.WorkflowID, runID, run.Status, run.Policy, run.ConfigError, run.UpgradeError)
	for _, event := range e.events(runID) {
		line := fmt.Sprintf("  %3d %s", event.ID, event.EventType())
		switch {
		case event.ActivityScheduled != nil:
			line += " " + event.ActivityScheduled.ActivityType + " queue=" + event.ActivityScheduled.TaskQueue
		case event.ActivityStarted != nil:
			line += " " + event.ActivityStarted.ActivityType + " worker=" + event.ActivityStarted.WorkerIdentity + " build=" + event.ActivityStarted.WorkerBuildID
		case event.WorkflowTaskFailed != nil:
			line += " " + event.WorkflowTaskFailed.Cause + ": " + event.WorkflowTaskFailed.Reason
		case event.ActivityFailed != nil:
			line += fmt.Sprintf(" %+v", *event.ActivityFailed)
		}
		e.printf("%s\n", line)
	}
}

func (e *Env) run(runID string) *runtimeapi.Run {
	var resp runtimeapi.RunWithBlocked
	e.must(e.admin.post(e.ctx(), runtimeapi.WorkflowRuntimeGetRunPath,
		runtimeapi.WorkflowRuntimeRunRequest{Namespace: e.Namespace, RunID: runID}, &resp))
	return resp.Run
}

// waitRun 等 Run 满足条件；Run 进入 failed / cancelled / terminated 时立即报错。
func (e *Env) waitRun(runID, what string, cond func(*runtimeapi.Run) bool) *runtimeapi.Run {
	var run *runtimeapi.Run
	e.waitForRun(runID, fmt.Sprintf("run %s: %s", runID, what), func() bool {
		run = e.run(runID)
		if run.Status != history.StatusRunning && run.Status != history.StatusCompleted {
			e.failf("run %s ended %s while waiting for %s", runID, run.Status, what)
		}
		return cond(run)
	})
	return run
}

func (e *Env) waitCompleted(runID string) *runtimeapi.Run {
	return e.waitRun(runID, "completed", func(run *runtimeapi.Run) bool { return run.Status == history.StatusCompleted })
}

// startRun 用 Start 启动一个 Run：workflowType 是 @Workflow 的 type，决定运行哪个定义；
// workflowID 是这次执行的业务 ID，同一个类型的多个 Run 靠它区分。
func (e *Env) startRun(workflowType, workflowID string, input map[string]any) string {
	run, err := e.sdk.ExecuteWorkflow(e.ctx(), client.StartWorkflowOptions{ID: workflowID}, workflowType, input)
	e.must(err)
	e.runs = append(e.runs, trackedRun{workflowID: workflowID, runID: run.GetRunID(), started: e.program(run.GetRunID())})
	return run.GetRunID()
}

// update 按 Workflow ID 发送一个 Update，等 Handler 返回并把结果解码到 result。
func (e *Env) update(workflowID, name string, result any) {
	ctx, cancel := context.WithTimeout(e.ctx(), e.Timeout)
	defer cancel()
	handle, err := e.sdk.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		UpdateName:   name,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	e.must(err)
	e.must(handle.Get(ctx, result))
}

// waitStage 用 Query "status" 等 Workflow 走到指定阶段。
func (e *Env) waitStage(runID, stage string) map[string]any {
	var status map[string]any
	e.waitForRun(runID, fmt.Sprintf("run %s: stage %s", runID, stage), func() bool {
		result, err := e.sdk.QueryWorkflow(e.ctx(), "", runID, "status")
		// 首个 Workflow Task 完成之前 Handler 还没注册，Query 被拒绝，稍后重试。
		var rejected *client.QueryFailedError
		if errors.As(err, &rejected) {
			return false
		}
		e.must(err)
		status = nil
		e.must(result.Get(&status))
		return status["stage"] == stage
	})
	return status
}

func (e *Env) signal(runID, name string) {
	e.must(e.sdk.SignalWorkflow(e.ctx(), "", runID, name, map[string]any{"by": "driver"}))
}

func (e *Env) setPolicy(runID, policy string) {
	e.must(e.sdk.UpdateWorkflowExecutionOptions(e.ctx(), client.UpdateWorkflowExecutionOptions{RunID: runID, Policy: workflow.Policy(policy)}))
}

// program 返回 Run 当前 Program 的可读名字：作者写在 @Workflow(version=...) 里的标签加内容 hash 前缀。
func (e *Env) program(runID string) string {
	return e.programAt(runID, e.run(runID).WorkflowConfigVersion)
}

func (e *Env) programAt(runID, contentVersion string) string {
	var cfg program.WorkflowConfig
	e.must(e.admin.post(e.ctx(), runtimeapi.WorkflowRuntimeGetConfigPath,
		runtimeapi.WorkflowRuntimeRunRequest{Namespace: e.Namespace, RunID: runID, ProgramVersion: contentVersion}, &cfg))
	return cfg.Type + " " + cfg.Version + " (" + short(contentVersion) + ")"
}

func short(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}

// latest 返回某个 Workflow 当前 latest Program 的作者版本标签。
func (e *Env) latest(workflowType string) string {
	var configs runtimeapi.WorkflowConfigListResult
	e.must(e.admin.post(e.ctx(), runtimeapi.WorkflowRuntimeListConfigsPath,
		runtimeapi.WorkflowRuntimeListConfigsRequest{Namespace: e.Namespace}, &configs))
	for _, item := range configs.Items {
		if item.WorkflowID == workflowType {
			return item.WorkflowVersion
		}
	}
	e.failf("workflow %s is not registered", workflowType)
	return ""
}

func (e *Env) events(runID string) []history.Event {
	var events []history.Event
	iter := e.sdk.GetWorkflowHistory(e.ctx(), "", runID)
	for iter.HasNext() {
		event, err := iter.Next()
		e.must(err)
		events = append(events, *event)
	}
	return events
}

// routed 返回领取并执行某个 Activity Type 的 Worker build（最后一个 ActivityStarted 事件里的 WorkerBuildID）。
// Activity 投到队列，任何订阅这个队列的 Worker 都能领取，契约不兼容的 Worker 领取时被拒绝，所以这里是契约兼容的 build。
// 这个 Type 没有开始执行过时返回空串。
func (e *Env) routed(runID, activityType string) string {
	build := ""
	for _, event := range e.events(runID) {
		if event.ActivityStarted != nil && event.ActivityStarted.ActivityType == activityType {
			build = event.ActivityStarted.WorkerBuildID
		}
	}
	return build
}

// scheduledTypes 按调度顺序返回 Run 调度过的 Activity Type。
func (e *Env) scheduledTypes(runID string) []string {
	var types []string
	for _, event := range e.events(runID) {
		if event.ActivityScheduled != nil {
			types = append(types, event.ActivityScheduled.ActivityType)
		}
	}
	return types
}

// workflowTaskWorkers 返回执行过这个 Run 的 Workflow Task 的 Worker Identity（去重、排序）。
func (e *Env) workflowTaskWorkers(runID string) []string {
	seen := map[string]bool{}
	for _, event := range e.events(runID) {
		if event.WorkflowTaskStarted != nil {
			seen[event.WorkflowTaskStarted.WorkerIdentity] = true
		}
	}
	workers := make([]string, 0, len(seen))
	for identity := range seen {
		workers = append(workers, identity)
	}
	sort.Strings(workers)
	return workers
}

// expectRoute 断言某个 Activity 由哪个 build 领取，并且 Activity 的输出也来自这个 build。
func (e *Env) expectRoute(runID, activityType, wantBuild, servedBy string) {
	e.expect(activityType+" 领取它的 build", e.routed(runID, activityType), wantBuild)
	e.expect(activityType+" 实际执行的 build", servedBy, wantBuild)
}

// expectServed 用于契约在新旧 build 上相同的 Activity：两个 build 都可以领取，断言执行它的 build 就是领取它的 build。
func (e *Env) expectServed(runID, activityType, servedBy string) {
	e.expect(activityType+" 实际执行的 build 与领取它的 build 一致", servedBy, e.routed(runID, activityType))
	e.printf("       %s 由 %s 执行（契约在新旧 build 上相同，两个 build 都可以承接）\n", activityType, servedBy)
}

func str(m map[string]any, key string) string {
	value, _ := m[key].(string)
	return value
}

func num(m map[string]any, key string) int64 {
	switch value := m[key].(type) {
	case float64:
		return int64(value)
	case int64:
		return value
	case int:
		return int64(value)
	}
	return 0
}

// adminAPI 调用 Console 使用的运维端点：请求与应答是 protocol/runtimeapi 的类型。
type adminAPI struct {
	url string
}

func (a *adminAPI) post(ctx context.Context, path string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(a.url, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		text, _ := io.ReadAll(io.LimitReader(httpResp.Body, 4096))
		return fmt.Errorf("%s: %s: %s", path, httpResp.Status, strings.TrimSpace(string(text)))
	}
	if resp == nil {
		return nil
	}
	return json.NewDecoder(httpResp.Body).Decode(resp)
}
