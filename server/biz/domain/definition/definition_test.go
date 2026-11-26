package definition_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 定义域规则测试
// ============================================
// 测试策略:
// - 版本号：固定样例的期望值写死，锁住内容 hash 的算法与输入
// - 校验与版本解析：仓储用 infra/memory
// ============================================

// 固定样例的版本号。它们变化意味着已存内容的版本引用全部失效。
const goldenConfigVersion = "ab2c1078dd2f5a5de9b492b71b18140d"

func TestWorkflowConfigVersion(t *testing.T) {
	// Happy Path: 版本是配置规范化 JSON 的 sha256 前 16 字节；内容相同则版本相同
	version, err := program.WorkflowConfigVersion(&program.WorkflowConfig{Type: "payments", Name: "A"})
	if err != nil {
		t.Fatal(err)
	}
	if version != goldenConfigVersion {
		t.Fatalf("version = %q, want %q", version, goldenConfigVersion)
	}
	same, err := program.WorkflowConfigVersion(&program.WorkflowConfig{Type: "payments", Name: "A"})
	if err != nil || same != version {
		t.Fatalf("same content hashed to %q err=%v, want %q", same, err, version)
	}
	other, err := program.WorkflowConfigVersion(&program.WorkflowConfig{Type: "payments", Name: "B"})
	if err != nil || other == version {
		t.Fatalf("different content hashed to %q err=%v", other, err)
	}
}

func TestWorkflowConfigVersionSurvivesJSONRoundTrip(t *testing.T) {
	// Edge Case: Program 里的结构体首次序列化按字段顺序，读回后是按键排序的 map；两种形态版本相同
	type fragment struct {
		Schema  string         `json:"schema"`
		Lang    string         `json:"lang"`
		Imports map[string]any `json:"imports"`
		Count   int64          `json:"count"`
	}
	original := &program.WorkflowConfig{Type: "payments", Program: program.WLangProgram{
		Program: []any{fragment{Schema: "v2", Lang: "wflang", Imports: map[string]any{"b": "x", "a": "y"}, Count: 9007199254740993}},
	}}
	want, err := program.WorkflowConfigVersion(original)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var readBack program.WorkflowConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&readBack); err != nil {
		t.Fatal(err)
	}
	got, err := program.WorkflowConfigVersion(&readBack)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("version after round trip = %q, want %q", got, want)
	}
}

func TestCloneWorkflowConfig(t *testing.T) {
	// Happy Path: 克隆与原对象互不影响
	original := &program.WorkflowConfig{Type: "payments", Activities: map[string]program.ActivityRef{"charge": {Symbol: "charge", Type: "charge"}}}
	clone, err := definition.CloneWorkflowConfig(original)
	if err != nil {
		t.Fatal(err)
	}
	clone.Activities["charge"] = program.ActivityRef{Symbol: "charge", Type: "refund"}
	if original.Activities["charge"].Type != "charge" {
		t.Fatal("mutating the clone changed the original")
	}
}

func TestValidateWorkflowConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		workflow *program.WorkflowConfig
		wantErr  string
	}{
		// Happy Path
		{name: "minimal", workflow: &program.WorkflowConfig{Type: "payments", Program: programtest.Noop()}},
		{name: "run timeout equals execution timeout", workflow: &program.WorkflowConfig{Type: "payments", Program: programtest.Noop(), RunTimeout: "1h", ExecutionTimeout: "1h"}},
		{name: "only run timeout", workflow: &program.WorkflowConfig{Type: "payments", Program: programtest.Noop(), RunTimeout: "1h"}},
		// Edge Case: 无法解析的时长留给后续解析报错
		{name: "unparseable timeout", workflow: &program.WorkflowConfig{Type: "payments", Program: programtest.Noop(), RunTimeout: "soon", ExecutionTimeout: "1h"}},
		// Error Case
		{name: "nil", workflow: nil, wantErr: "workflow config is nil"},
		{name: "empty type", workflow: &program.WorkflowConfig{}, wantErr: "workflow type is empty"},
		{name: "missing program", workflow: &program.WorkflowConfig{Type: "payments"}, wantErr: "wlang program is empty"},
		{name: "run timeout exceeds execution timeout", workflow: &program.WorkflowConfig{Type: "payments", Program: programtest.Noop(), RunTimeout: "2h", ExecutionTimeout: "1h"},
			wantErr: "run_timeout (2h) must not exceed execution_timeout (1h)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := definition.ValidateWorkflowConfig("default", tc.workflow)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestActiveWorkflowRef(t *testing.T) {
	repo := memory.NewDefinitionRepository()
	saved, err := repo.SaveWorkflowConfigInNamespace("tenant", &program.WorkflowConfig{Type: "payments"})
	if err != nil {
		t.Fatal(err)
	}
	// Happy Path: namespace 两侧空白被规范化
	if ref, err := definition.ActiveWorkflowRef(repo, " tenant ", "payments"); err != nil || ref != saved {
		t.Fatalf("active = %+v err=%v, want %+v", ref, err, saved)
	}
	// Error Case: 其他 namespace 与只登记为 artifact 的 workflow 都没有 active
	if _, err := definition.ActiveWorkflowRef(repo, "other", "payments"); !errors.Is(err, definition.ErrWorkflowConfigNotFound) || !strings.HasPrefix(err.Error(), "workflow other/payments has no registered config version") {
		t.Fatalf("error = %v", err)
	}
	if _, err := repo.SaveWorkflowConfigArtifactInNamespace("tenant", &program.WorkflowConfig{Type: "artifact-only"}); err != nil {
		t.Fatal(err)
	}
	if _, err := definition.ActiveWorkflowRef(repo, "tenant", "artifact-only"); err == nil {
		t.Fatal("artifact-only workflow reported an active version")
	}
}

func TestResolveWorkflowConfigRef(t *testing.T) {
	repo := memory.NewDefinitionRepository()
	first, err := repo.SaveWorkflowConfigInNamespace("tenant", &program.WorkflowConfig{Type: "payments", Name: "A"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.SaveWorkflowConfigInNamespace("tenant", &program.WorkflowConfig{Type: "payments", Name: "B"})
	if err != nil {
		t.Fatal(err)
	}

	// Happy Path: 空版本取 active；显式版本取已保存的那一版
	if ref, err := definition.ResolveWorkflowConfigRef(repo, "tenant", "payments", ""); err != nil || ref != second {
		t.Fatalf("default version = %+v err=%v, want %+v", ref, err, second)
	}
	if ref, err := definition.ResolveWorkflowConfigRef(repo, "tenant", "payments", first.Version); err != nil || ref != first {
		t.Fatalf("explicit version = %+v err=%v, want %+v", ref, err, first)
	}
	// Edge Case: 空 namespace 解析到 default
	if _, err := repo.SaveWorkflowConfigInNamespace("", &program.WorkflowConfig{Type: "payments", Name: "D"}); err != nil {
		t.Fatal(err)
	}
	if ref, err := definition.ResolveWorkflowConfigRef(repo, "", "payments", ""); err != nil || ref.Namespace != protocol.DefaultNamespace {
		t.Fatalf("default namespace ref = %+v err=%v", ref, err)
	}
	// Error Case
	if _, err := definition.ResolveWorkflowConfigRef(repo, "tenant", "", ""); err == nil || err.Error() != "workflow type is empty" {
		t.Fatalf("empty id error = %v", err)
	}
	if _, err := definition.ResolveWorkflowConfigRef(repo, "tenant", "payments", "missing"); !errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("missing version error = %v, want ErrWorkflowConfigNotFound", err)
	}
	if _, err := definition.ResolveWorkflowConfigRef(repo, "tenant", "unknown", ""); err == nil || !strings.Contains(err.Error(), "has no registered config version") {
		t.Fatalf("unknown workflow error = %v", err)
	}
}
