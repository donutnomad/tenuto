package execution_test

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ChooseProgramVersion 是 AutoUpgrade 在 Workflow Task 边界的选版规则：只有运行中的 auto_upgrade Run 尝试采用
// latest；候选内容不可读或与目标 Worker 冲突时沿用当前版本并给出暂停消息；冲突无法判定时本次沿用当前版本、不暂停。
func TestChooseProgramVersion(t *testing.T) {
	autoUpgrade := func() *execution.Run {
		return &execution.Run{Status: history.StatusRunning, Policy: execution.RunPolicyAutoUpgrade, WorkflowConfigVersion: "v1"}
	}
	for _, test := range []struct {
		name      string
		run       *execution.Run
		candidate execution.UpgradeCandidate
		want      execution.ProgramVersionChoice
	}{
		{
			name:      "Happy Path: a running auto_upgrade run adopts a different latest version",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{Version: "v2"},
			want:      execution.ProgramVersionChoice{Version: "v2"},
		},
		{
			name:      "Edge Case: a pinned run keeps its version",
			run:       &execution.Run{Status: history.StatusRunning, Policy: execution.RunPolicyPinned, WorkflowConfigVersion: "v1"},
			candidate: execution.UpgradeCandidate{Version: "v2"},
			want:      execution.ProgramVersionChoice{Version: "v1"},
		},
		{
			name:      "Edge Case: a closed auto_upgrade run keeps its version",
			run:       &execution.Run{Status: history.StatusCompleted, Policy: execution.RunPolicyAutoUpgrade, WorkflowConfigVersion: "v1"},
			candidate: execution.UpgradeCandidate{Version: "v2"},
			want:      execution.ProgramVersionChoice{Version: "v1"},
		},
		{
			name:      "Edge Case: no latest version keeps the current version",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{},
			want:      execution.ProgramVersionChoice{Version: "v1"},
		},
		{
			name:      "Edge Case: latest equal to the current version is not a candidate, even with recorded errors",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{Version: "v1", ContentErr: errors.New("missing")},
			want:      execution.ProgramVersionChoice{Version: "v1"},
		},
		{
			name:      "Error Case: unreadable candidate content pauses the run on the current version",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{Version: "v2", ContentErr: errors.New("missing")},
			want:      execution.ProgramVersionChoice{Version: "v1", ConfigError: "candidate workflow content v2 is unavailable: missing"},
		},
		{
			name:      "Error Case: a target worker conflict pauses the run on the current version",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{Version: "v2", TargetConflict: errors.New("target cannot run inventory.reserve")},
			want:      execution.ProgramVersionChoice{Version: "v1", ConfigError: "target cannot run inventory.reserve"},
		},
		{
			name:      "Error Case: a target conflict that cannot be checked keeps the current version without pausing",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{Version: "v2", TargetCheckErr: errors.New("list active workers: connection refused")},
			want:      execution.ProgramVersionChoice{Version: "v1"},
		},
		{
			name: "Error Case: a detected target conflict pauses even when another check failed",
			run:  autoUpgrade(),
			candidate: execution.UpgradeCandidate{
				Version: "v2", TargetConflict: errors.New("conflict"), TargetCheckErr: errors.New("unavailable"),
			},
			want: execution.ProgramVersionChoice{Version: "v1", ConfigError: "conflict"},
		},
		{
			name:      "Error Case: history payloads the candidate cannot read pause the run on the current version",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{Version: "v2", PayloadConflict: errors.New("auto upgrade to v2 refused")},
			want:      execution.ProgramVersionChoice{Version: "v1", ConfigError: "auto upgrade to v2 refused"},
		},
		{
			name:      "Error Case: a payload check that cannot complete keeps the current version without pausing",
			run:       autoUpgrade(),
			candidate: execution.UpgradeCandidate{Version: "v2", PayloadCheckErr: errors.New("read history: connection refused")},
			want:      execution.ProgramVersionChoice{Version: "v1"},
		},
		{
			name: "Error Case: unreadable content is reported before a target conflict",
			run:  autoUpgrade(),
			candidate: execution.UpgradeCandidate{
				Version: "v2", ContentErr: errors.New("missing"), TargetConflict: errors.New("conflict"),
			},
			want: execution.ProgramVersionChoice{Version: "v1", ConfigError: "candidate workflow content v2 is unavailable: missing"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.run.ChooseProgramVersion(test.candidate); got != test.want {
				t.Fatalf("ChooseProgramVersion() = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestIsUpgradeCandidate(t *testing.T) {
	run := &execution.Run{Status: history.StatusRunning, Policy: execution.RunPolicyAutoUpgrade, WorkflowConfigVersion: "v1"}
	for _, test := range []struct {
		name    string
		run     *execution.Run
		version string
		want    bool
	}{
		{name: "Happy Path: a different version", run: run, version: "v2", want: true},
		{name: "Edge Case: the current version", run: run, version: "v1", want: false},
		{name: "Edge Case: no version", run: run, version: "", want: false},
		{name: "Edge Case: a pinned run", run: &execution.Run{Status: history.StatusRunning, Policy: execution.RunPolicyPinned, WorkflowConfigVersion: "v1"}, version: "v2", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.run.IsUpgradeCandidate(test.version); got != test.want {
				t.Fatalf("IsUpgradeCandidate(%q) = %v, want %v", test.version, got, test.want)
			}
		})
	}
}
