// Command driver 通过 runtime HTTP API 和 ready-dir 文件协议验证多进程示例。
// app2 必须设置 MP_READY_DIR 为相同目录。
//
// 场景（每个 phase 独立建 Run）：
//
// pin、v1、v2 正常完成；incompat-v1/v2 在 settle gate 等 v3 发布后释放，
// 验证不兼容重试和 pinned 恢复；v3 正常完成；pending 验证取消。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	url := flag.String("url", "http://localhost:7400", "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", "multiprocess-demo", "namespace")
	workflowType := flag.String("workflow-type", "order-flow", "Workflow Type（workflow 定义 ID）")
	orderID := flag.String("order", "demo-1", "订单号前缀（入参 order_id 为 <前缀>-<phase>）")
	timeout := flag.Duration("timeout", 20*time.Second, "单个 Run 等待终态的最长时间")
	readyDir := flag.String("ready-dir", os.Getenv("MP_READY_DIR"), "一键脚本的进程状态目录")
	flag.Parse()
	if *readyDir == "" {
		fmt.Fprintln(os.Stderr, "driver: -ready-dir (或 MP_READY_DIR) 必须指向一键脚本的状态目录")
		os.Exit(2)
	}

	d := &driver{client: &api{
		base: *url, namespace: *namespace, workflowType: *workflowType, idPrefix: *workflowType,
		runs: map[string]string{}, http: &http.Client{Timeout: 10 * time.Second},
	}, readyDir: *readyDir}
	ctx := context.Background()
	if err := d.all(ctx, *orderID, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "driver: %v\n", err)
		os.Exit(1)
	}
}

type api struct {
	base         string
	namespace    string
	workflowType string
	// idPrefix 加订单号组成每个 Run 的 Workflow ID；runs 记录 Run ID → Workflow ID。
	idPrefix string
	runs     map[string]string
	http     *http.Client
}

type runView struct {
	ID                    string         `json:"id"`
	Status                string         `json:"status"`
	WorkflowConfigVersion string         `json:"workflow_config_version"`
	Policy                string         `json:"policy"`
	UpgradeError          string         `json:"upgrade_error"`
	ConfigError           string         `json:"config_error"`
	Output                map[string]any `json:"output"`
}

type driver struct {
	client       *api
	readyDir     string
	incompatible []func(context.Context, time.Duration) error
}

func (d *driver) all(ctx context.Context, order string, timeout time.Duration) error {
	fmt.Printf("driver → %s namespace=%s workflow=%s\n", d.client.base, d.client.namespace, d.client.workflowType)
	versions, err := d.versions()
	if err != nil {
		return err
	}
	fmt.Printf("已登记的内容版本：%s\n", versions)
	start := time.Now()
	phases := []struct {
		name string
		fn   func(context.Context, string, time.Duration) error
	}{
		{name: "pin", fn: d.phasePin},
		{name: "v1", fn: d.phaseVersion("v1")},
		{name: "incompat-v1", fn: d.phaseIncompat("v1")},
		{name: "v2", fn: d.phaseVersion("v2")},
		{name: "incompat-v2", fn: d.phaseIncompat("v2")},
		{name: "upgrade", fn: d.phaseUpgrade},
		{name: "v3", fn: d.phaseVersion("v3")},
		{name: "pending", fn: d.phasePending},
	}
	for _, phase := range phases {
		if err := phase.fn(ctx, order+"-"+phase.name, timeout); err != nil {
			return fmt.Errorf("phase %s: %w", phase.name, err)
		}
	}
	if err := d.summary(ctx); err != nil {
		return err
	}
	fmt.Printf("total %s\nRESULT PASS\n", time.Since(start).Round(time.Millisecond))
	return nil
}

// phaseVersion 等待 app-1 以指定版本在线后启动 Run，并等待它完成。
func (d *driver) phaseVersion(version string) func(context.Context, string, time.Duration) error {
	return func(ctx context.Context, order string, timeout time.Duration) error {
		if err := d.waitApp1(version, 15*time.Second); err != nil {
			return err
		}
		runID, err := d.client.start(ctx, order)
		if err != nil {
			return err
		}
		view, err := d.client.waitTerminal(ctx, runID, timeout)
		if err != nil {
			return err
		}
		d.report("phase "+version, view)
		wantNote := map[string]string{"v1": "v1", "v2": "strict", "v3": "strict-v3"}[version]
		if view.Status != "completed" || view.Output["note"] != wantNote {
			return fmt.Errorf("run %s ended %s note=%v, want %s", runID, view.Status, view.Output["note"], wantNote)
		}
		return nil
	}
}

// phasePin 验证显式设置 pinned 后 Run 正常完成；跨版本固定由 incompat 恢复场景验证。
func (d *driver) phasePin(ctx context.Context, order string, timeout time.Duration) error {
	if err := d.waitApp1("v1", 15*time.Second); err != nil {
		return err
	}
	runID, err := d.client.start(ctx, order)
	if err != nil {
		return err
	}
	if err := d.client.setPolicy(ctx, runID, "pinned"); err != nil {
		return err
	}
	view, err := d.client.waitTerminal(ctx, runID, timeout)
	if err != nil {
		return err
	}
	d.report("phase pin", view)
	if view.Status != "completed" || view.Policy != "pinned" {
		return fmt.Errorf("pinned run %s ended %s policy=%s", runID, view.Status, view.Policy)
	}
	return nil
}

// phasePending 用 v3 的内容版本（v3 才是 latest）启动一个 Run，随后从外部取消：
// 它在 v3 发布之后仍以 cancelled 终结，不会被新内容改写，也不会被 auto_upgrade 重新拉起。
func (d *driver) phasePending(ctx context.Context, order string, timeout time.Duration) error {
	if err := d.waitApp1("v3", 15*time.Second); err != nil {
		return err
	}
	runID, err := d.client.start(ctx, order)
	if err != nil {
		return err
	}
	if err := d.client.cancel(ctx, runID, "driver: leave unfinished"); err != nil {
		return err
	}
	view, err := d.client.waitTerminal(ctx, runID, timeout)
	if err != nil {
		return err
	}
	d.report("phase pending (v3 content, cancelled)", view)
	if view.Status != "cancelled" {
		return fmt.Errorf("pending run %s ended %s, want cancelled", runID, view.Status)
	}
	return nil
}

// phaseIncompat 用 ledger.settle gate 固定旧 history，在 v3 发布后完成 activity 触发重放。
func (d *driver) phaseIncompat(version string) func(context.Context, string, time.Duration) error {
	client := *d.client
	client.idPrefix += "-incompat-" + version
	return func(ctx context.Context, order string, timeout time.Duration) error {
		if err := d.waitApp1(version, 15*time.Second); err != nil {
			return err
		}
		gate := filepath.Join(d.readyDir, filepath.Base(order))
		for _, suffix := range []string{".hold", ".entered", ".release"} {
			if err := os.Remove(gate + suffix); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if err := os.WriteFile(gate+".hold", nil, 0o644); err != nil {
			return err
		}
		runID, err := client.start(ctx, order)
		if err != nil {
			return err
		}
		deadline := time.Now().Add(15 * time.Second)
		for {
			if _, err := os.Stat(gate + ".entered"); err == nil {
				break
			} else if !os.IsNotExist(err) {
				return err
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("run %s never entered ledger.settle gate (app2 requires MP_READY_DIR)", runID)
			}
			time.Sleep(100 * time.Millisecond)
		}
		before, err := client.getRun(ctx, runID)
		if err != nil {
			return err
		}
		if before.Status != "running" || before.Policy != "auto_upgrade" {
			return fmt.Errorf("gated run: status=%s policy=%s", before.Status, before.Policy)
		}
		baseline, _, _, err := client.failures(ctx, runID)
		if err != nil {
			return err
		}
		d.incompatible = append(d.incompatible, func(ctx context.Context, timeout time.Duration) error {
			defer func() {
				for _, suffix := range []string{".hold", ".entered", ".release"} {
					_ = os.Remove(gate + suffix)
				}
			}()
			if err := os.WriteFile(gate+".release", nil, 0o644); err != nil {
				return err
			}
			if err := (&driver{client: &client}).waitNewFailureAfter(ctx, runID, baseline, 15*time.Second); err != nil {
				return err
			}
			view, counts, quote, err := (&driver{client: &client}).observeIncompatible(ctx, runID, 3*time.Second)
			if err != nil {
				return err
			}
			d.report("phase incompat "+version, view)
			fmt.Printf("   WorkflowTaskFailed: %d; %s\n", counts, quote)
			if view.Status != "running" || view.UpgradeError == "" || counts < baseline+2 || !strings.Contains(quote, "non_deterministic_error") || view.WorkflowConfigVersion != before.WorkflowConfigVersion {
				return fmt.Errorf("run %s did not preserve old content and retry incompatible v3: status=%s failures=%d error=%q cause=%q", runID, view.Status, counts-baseline, view.UpgradeError, quote)
			}
			// pinned 固定当前已提交内容；失败的候选 v3 从未成为 current。
			if err := client.setPolicy(ctx, runID, "pinned"); err != nil {
				return err
			}
			recovered, err := client.waitTerminal(ctx, runID, timeout)
			if err != nil {
				return err
			}
			d.report("phase recover "+version, recovered)
			if recovered.Status != "completed" || recovered.Policy != "pinned" || recovered.WorkflowConfigVersion != before.WorkflowConfigVersion || recovered.UpgradeError != "" {
				return fmt.Errorf("run %s did not recover pinned current content", runID)
			}
			return nil
		})
		return nil
	}
}

func (d *driver) phaseUpgrade(ctx context.Context, _ string, timeout time.Duration) error {
	if err := d.waitApp1("v3", 15*time.Second); err != nil {
		return err
	}
	for _, verify := range d.incompatible {
		if err := verify(ctx, timeout); err != nil {
			return err
		}
	}
	return nil
}

// waitNewFailure 等到上一次基线之后又出现一次 WorkflowTaskFailed（即 v3 选择后的那一轮）。
func (d *driver) waitNewFailureAfter(ctx context.Context, runID string, baseline int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		counts, quote, _, err := d.client.failures(ctx, runID)
		if err != nil {
			return err
		}
		if counts > baseline {
			fmt.Printf("→ 新的 WorkflowTaskFailed（第 %d 次）：%s\n", counts, quote)
			return nil
		}
		if time.Now().After(deadline) {
			view, viewErr := d.client.getRun(ctx, runID)
			if viewErr == nil {
				d.report("upgrade observation timeout", view)
			}
			return fmt.Errorf("no new WorkflowTaskFailed for run %s within %s", runID, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// observeIncompatible 在 window 内持续读取 Run 与 history：Run 保持 running，失败事件持续增长。
func (d *driver) observeIncompatible(ctx context.Context, runID string, window time.Duration) (runView, int, string, error) {
	deadline := time.Now().Add(window)
	var view runView
	var counts int
	var quote string
	for {
		next, err := d.client.getRun(ctx, runID)
		if err != nil {
			return view, 0, "", err
		}
		view = next
		if view.Status != "running" {
			return view, 0, "", fmt.Errorf("run %s ended %s while observing incompatible retry", runID, view.Status)
		}
		counts, quote, _, err = d.client.failures(ctx, runID)
		if err != nil {
			return view, 0, "", err
		}
		if time.Now().After(deadline) {
			return view, counts, quote, nil
		}
		time.Sleep(time.Second)
	}
}

// failures 统计 WorkflowTaskFailed 事件，返回次数、最新一次的 cause/reason 与当前 open 的 WFT 数。
func (c *api) failures(ctx context.Context, runID string) (int, string, int, error) {
	events, err := c.history(ctx, runID)
	if err != nil {
		return 0, "", 0, err
	}
	var counts, open int
	var quote string
	for _, raw := range events {
		var event struct {
			ID                    int64           `json:"id"`
			WorkflowTaskFailed    *eventWFTFailed `json:"workflowTaskFailed"`
			WorkflowTaskScheduled *struct{}       `json:"workflowTaskScheduled"`
			WorkflowTaskCompleted *struct{}       `json:"workflowTaskCompleted"`
			WorkflowTaskTimedOut  *struct{}       `json:"workflowTaskTimedOut"`
		}
		if json.Unmarshal(raw, &event) != nil {
			continue
		}
		switch {
		case event.WorkflowTaskFailed != nil:
			if event.WorkflowTaskFailed.Cause != "non_deterministic_error" {
				continue
			}
			counts++
			open--
			quote = fmt.Sprintf("%s: %s", event.WorkflowTaskFailed.Cause, event.WorkflowTaskFailed.Reason)
		case event.WorkflowTaskScheduled != nil:
			open++
		case event.WorkflowTaskCompleted != nil, event.WorkflowTaskTimedOut != nil:
			open--
		}
	}
	return counts, quote, open, nil
}

type eventWFTFailed struct {
	Cause  string `json:"cause"`
	Reason string `json:"reason"`
}

// versions 列出 order-flow 已登记的内容版本，用于说明 v1/v2/v3 三代共存。
func (d *driver) versions() (string, error) {
	cfg, err := d.client.getWorkflowConfig(context.Background())
	if err != nil {
		return "", err
	}
	out, _ := json.Marshal(cfg)
	return string(out), nil
}

func (c *api) getWorkflowConfig(ctx context.Context) ([]string, error) {
	var resp struct {
		Items []struct {
			WorkflowID      string `json:"workflow_id"`
			LatestVersion   string `json:"latest_version"`
			WorkflowVersion string `json:"workflow_version"`
			VersionCount    int    `json:"version_count"`
			Active          bool   `json:"active"`
		} `json:"items"`
	}
	if err := c.post(ctx, "/runtime/workflow/list-configs", map[string]string{"namespace": c.namespace}, &resp); err != nil {
		return nil, err
	}
	var out []string
	for _, item := range resp.Items {
		if item.WorkflowID != c.workflowType {
			continue
		}
		out = append(out, fmt.Sprintf("latest=%s version=%s count=%d active=%t",
			short(item.LatestVersion), item.WorkflowVersion, item.VersionCount, item.Active))
	}
	return out, nil
}

func (d *driver) summary(ctx context.Context) error {
	var result struct {
		Items []struct {
			RunID                 string `json:"run_id"`
			WorkflowType          string `json:"workflow_type"`
			Status                string `json:"status"`
			WorkflowConfigVersion string `json:"workflow_config_version"`
		} `json:"items"`
	}
	if err := d.client.post(ctx, "/runtime/workflow/list", map[string]any{
		"filter": map[string]any{"namespace": d.client.namespace, "include_terminal": true, "limit": 100},
	}, &result); err != nil {
		return err
	}
	fmt.Printf("summary: runs of %s in namespace %s\n", d.client.workflowType, d.client.namespace)
	for _, item := range result.Items {
		if item.WorkflowType != d.client.workflowType {
			continue
		}
		fmt.Printf("   %-44s %-10s %s\n", item.RunID, item.Status, short(item.WorkflowConfigVersion))
	}
	return nil
}

func (d *driver) report(label string, view runView) {
	fmt.Printf("%s: run=%s status=%s policy=%s content=%s\n", label, view.ID, view.Status, view.Policy, short(view.WorkflowConfigVersion))
	if view.UpgradeError != "" {
		fmt.Printf("   upgrade_error: %s\n", view.UpgradeError)
	}
	if view.ConfigError != "" {
		fmt.Printf("   config_error: %s\n", view.ConfigError)
	}
	if len(view.Output) > 0 {
		out, _ := json.Marshal(view.Output)
		fmt.Printf("   output: %s\n", out)
	}
}

// waitApp1 等到一键脚本把 app-1 拉起到 version 并报告就绪。脚本在 app-1 的 Activity 路由
// 与 workflow 定义都登记完成后写 <ready>/app1-version，并把 <ready>/app1-up 置为 exists。
func (d *driver) waitApp1(version string, timeout time.Duration) error {
	// 脚本在 app-1 退出后按 app1-next 重启；先写请求，再等 app1-up 报告目标版本。
	if err := os.WriteFile(d.readyDir+"/app1-next", []byte(version), 0o644); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(d.readyDir + "/app1-up"); err == nil {
			if data, err := os.ReadFile(d.readyDir + "/app1-version"); err == nil && strings.TrimSpace(string(data)) == version {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("app-1 %s did not become ready within %s", version, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *api) start(ctx context.Context, order string) (string, error) {
	var resp struct {
		RunID      string `json:"run_id"`
		WorkflowID string `json:"workflow_id"`
	}
	body := map[string]any{
		"namespace":     c.namespace,
		"workflow_type": c.workflowType,
		"workflow_id":   c.idPrefix + "-" + order,
		"input":         map[string]any{"order_id": order, "amount": 100},
	}
	if err := c.post(ctx, "/runtime/workflow/start", body, &resp); err != nil {
		return "", err
	}
	if resp.RunID == "" {
		return "", fmt.Errorf("start returned no run_id")
	}
	c.runs[resp.RunID] = resp.WorkflowID
	return resp.RunID, nil
}

func (c *api) getRun(ctx context.Context, runID string) (runView, error) {
	var resp struct {
		Run runView `json:"run"`
	}
	err := c.post(ctx, "/runtime/workflow/get-run", map[string]string{
		"namespace": c.namespace, "workflow_id": c.runs[runID], "run_id": runID,
	}, &resp)
	return resp.Run, err
}

func (c *api) history(ctx context.Context, runID string) ([]json.RawMessage, error) {
	var events []json.RawMessage
	err := c.post(ctx, "/runtime/workflow/history", map[string]string{
		"namespace": c.namespace, "workflow_id": c.runs[runID], "run_id": runID,
	}, &events)
	return events, err
}

func (c *api) setPolicy(ctx context.Context, runID, policy string) error {
	return c.post(ctx, "/runtime/workflow/set-policy", map[string]string{
		"namespace": c.namespace, "run_id": runID, "policy": policy,
	}, nil)
}

// cancel 请求取消 Run；已终结的 Run 返回 service 的 4xx，这里按已取消处理。
func (c *api) cancel(ctx context.Context, runID, reason string) error {
	err := c.post(ctx, "/runtime/workflow/cancel", map[string]string{
		"namespace": c.namespace, "workflow_id": c.runs[runID], "run_id": runID, "reason": reason,
	}, nil)
	if err != nil && strings.Contains(err.Error(), "400") {
		return nil
	}
	return err
}

func (c *api) waitTerminal(ctx context.Context, runID string, timeout time.Duration) (runView, error) {
	deadline := time.Now().Add(timeout)
	var view runView
	for {
		current, err := c.getRun(ctx, runID)
		if err != nil {
			return view, err
		}
		view = current
		if view.Status != "running" {
			if view.Status != "completed" && view.Status != "cancelled" {
				events, err := c.history(ctx, runID)
				if err != nil {
					return view, err
				}
				for _, raw := range events {
					var event map[string]json.RawMessage
					if err := json.Unmarshal(raw, &event); err != nil {
						return view, err
					}
					for _, key := range []string{"workflowFailed", "activityFailed", "workflowTaskFailed"} {
						if failure, ok := event[key]; ok {
							fmt.Printf("   run=%s %s: %s\n", runID, key, failure)
						}
					}
				}
			}
			return view, nil
		}
		if time.Now().After(deadline) {
			return view, fmt.Errorf("run %s still running after %s (upgrade_error=%q config_error=%q)",
				runID, timeout, view.UpgradeError, view.ConfigError)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *api) post(ctx context.Context, path string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST %s: %s %s", path, resp.Status, strings.TrimSpace(string(payload)))
	}
	if out == nil || len(payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("POST %s: decode: %w", path, err)
	}
	return nil
}

func short(version string) string {
	if len(version) > 12 {
		return version[:12]
	}
	return version
}
