package artifact

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
)

// writeArtifacts 在临时目录写一套最小产物。
func writeArtifacts(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	workflow := &program.WorkflowConfig{Type: "demo", Name: "Demo", Policy: "auto_upgrade", Version: "v2", Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{"demo.Charge": {
			Symbol: "demo.Charge", Type: "demo.charge", TaskQueue: "demo", Version: "v1", Input: "demo.In", Output: "demo.Out",
		}}}
	writeJSON(t, dir, WorkflowFile, workflow)
	return dir
}

func writeJSON(t *testing.T, dir, name string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestLoadDirReadsAndHashesArtifacts 覆盖完整加载与内容哈希。
func TestLoadDirReadsAndHashesArtifacts(t *testing.T) {
	dir := writeArtifacts(t)
	loaded, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if loaded.Workflow.Type != "demo" || loaded.Workflow.Version != "v2" {
		t.Fatalf("workflow 内容异常: %+v", loaded.Workflow)
	}
	if ref := loaded.Workflow.Activities["demo.Charge"]; ref.Type != "demo.charge" || ref.TaskQueue != "demo" {
		t.Fatalf("activities 内容异常: %+v", loaded.Workflow.Activities)
	}
	// 内容版本与服务端登记 workflow.json 时算出的版本一致。
	want, err := program.WorkflowConfigVersion(loaded.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ContentVersion == "" || loaded.ContentVersion != want {
		t.Fatalf("ContentVersion = %q, want %q", loaded.ContentVersion, want)
	}
	// workflow.json 换行风格不影响版本（版本按规范化 JSON 计算）。
	raw, err := os.ReadFile(filepath.Join(dir, WorkflowFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, WorkflowFile), []byte(strings.ReplaceAll(string(raw), "\n", "\r\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	crlf, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if crlf.ContentVersion != loaded.ContentVersion {
		t.Fatalf("CRLF 改变了内容版本: %s vs %s", crlf.ContentVersion, loaded.ContentVersion)
	}
}

// TestLoadDirRequiresWorkflowFile 锁住 workflow.json 必需。
func TestLoadDirRequiresWorkflowFile(t *testing.T) {
	dir := writeArtifacts(t)
	if err := os.Remove(filepath.Join(dir, WorkflowFile)); err != nil {
		t.Fatal(err)
	}
	_, err := LoadDir(dir)
	if err == nil || !strings.Contains(err.Error(), WorkflowFile) {
		t.Fatalf("缺少 workflow.json 应报错: %v", err)
	}
}

// TestClientPostsArtifactsAndWorkflow 覆盖两个入口的路径与请求体。
func TestClientPostsArtifactsAndWorkflow(t *testing.T) {
	dir := writeArtifacts(t)
	loaded, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	type captured struct {
		path          string
		authorization string
		body          map[string]any
	}
	var requests []captured
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Errorf("请求体不是 JSON: %v", err)
		}
		requests = append(requests, captured{request.URL.Path, request.Header.Get("Authorization"), decoded})
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	client := Client{BaseURL: server.URL + "/", Namespace: "demo-ns", AuthToken: "artifact-token-0123456789"}

	for _, path := range []string{RegisterPath, PublishPath} {
		if _, err := client.Submit(context.Background(), path, loaded); err != nil {
			t.Fatalf("Submit %s: %v", path, err)
		}
	}
	if len(requests) != 2 {
		t.Fatalf("期望两次请求，实际 %d", len(requests))
	}
	if requests[0].path != RegisterPath || requests[1].path != PublishPath {
		t.Fatalf("路径异常: %s / %s", requests[0].path, requests[1].path)
	}
	// 两个入口都只发服务端接收的 namespace 与 workflow。
	for _, request := range requests {
		if request.body["namespace"] != "demo-ns" || request.body["workflow"] == nil || len(request.body) != 2 {
			t.Fatalf("%s 只应发送 namespace+workflow: %+v", request.path, request.body)
		}
		if request.authorization != "Bearer artifact-token-0123456789" {
			t.Fatalf("%s Authorization = %q", request.path, request.authorization)
		}
	}
}

// TestClientReportsNon2xxBody 锁住非 2xx 时把响应体带回错误。
func TestClientReportsNon2xxBody(t *testing.T) {
	dir := writeArtifacts(t)
	loaded, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":"active version conflict"}`))
	}))
	defer server.Close()
	client := Client{BaseURL: server.URL, Namespace: "demo-ns"}
	if _, err := client.Submit(context.Background(), RegisterPath, loaded); err == nil {
		t.Fatal("非 2xx 应失败")
	} else if !strings.Contains(err.Error(), "active version conflict") || !strings.Contains(err.Error(), "409") {
		t.Fatalf("错误应包含状态码与响应体: %v", err)
	}
	if _, err := (Client{Namespace: "demo-ns"}).Submit(context.Background(), RegisterPath, loaded); err == nil {
		t.Fatal("缺少 URL 应失败")
	}
}
