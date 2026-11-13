package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteWorkerWorkflowTaskSkipsTerminalRun(t *testing.T) {
	backend := &workflowTaskBackendStub{status: history.StatusCompleted}
	result := ExecuteWorkerWorkflowTask(context.Background(), context.Background(), protocol.Task{RunID: "run-1"}, backend,
		WorkerWorkflowTaskOptions{WorkflowTaskOptions: WorkflowTaskOptions{
			Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
				t.Fatal("terminal run was executed")
				return nil, nil
			},
		}})
	if result.Outcome != "skipped" || result.ReportErr != nil || backend.configReads != 0 {
		t.Fatalf("result=%+v config reads=%d", result, backend.configReads)
	}
}

func TestExecuteWorkerWorkflowTaskSelectsIncrementalPath(t *testing.T) {
	backend := &workflowTaskBackendStub{config: programtest.Workflow("wf")}
	called := false
	task := protocol.Task{RunID: "run-1", ExecutorABIVersion: "abi-v1"}
	result := ExecuteWorkerWorkflowTask(context.Background(), context.Background(), task, backend,
		WorkerWorkflowTaskOptions{
			WorkflowTaskOptions: WorkflowTaskOptions{
				Capabilities: program.CurrentWLangCapabilities(),
				Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
					t.Fatal("full-history path was selected")
					return nil, nil
				},
			},
			IncrementalABI: "abi-v1",
			RunIncremental: func(context.Context, context.Context, protocol.Task) WorkflowTaskResult {
				called = true
				return WorkflowTaskResult{}
			},
		})
	if !called || result.ReportErr != nil || backend.configReads != 0 {
		t.Fatalf("called=%t result=%+v config reads=%d", called, result, backend.configReads)
	}
}

func TestExecuteWorkerWorkflowTaskReportsStatusReadTimeout(t *testing.T) {
	taskCtx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	backend := &workflowTaskBackendStub{}
	result := ExecuteWorkerWorkflowTask(taskCtx, taskCtx, protocol.Task{RunID: "run-1"}, backend, WorkerWorkflowTaskOptions{})
	if result.Outcome != "timeout" || result.ReportErr != nil || backend.timeoutCtx == nil || backend.timeoutCtx.Err() != nil {
		t.Fatalf("result=%+v timeout context=%v", result, backend.timeoutCtx)
	}
}

// ============================================
// 热路径：task 携带 ArtifactFingerprint 时 definition 只在 cache miss 读取
// ============================================
// 测试策略:
// - Happy Path: cold（cache miss）读一次 definition 并 admit；warm（cache hit）不再读取
// - Error Case: cache miss 后 definition 读取失败 → 返回错误，不执行
// ============================================

func incrementalWorkerOptions(t *testing.T, backend *workflowTaskBackendStub, session *incrementalTestSession, fullHistoryRuns *int) WorkerWorkflowTaskOptions {
	t.Helper()
	incremental := incrementalTestOptions(t, session)
	incremental.Capabilities = program.CurrentWLangCapabilities()
	opts := WorkerWorkflowTaskOptions{
		WorkflowTaskOptions: WorkflowTaskOptions{
			Capabilities: program.CurrentWLangCapabilities(),
			Execute: func(context.Context, *program.WorkflowConfig, []history.Event) ([]history.Command, error) {
				*fullHistoryRuns++
				return nil, nil
			},
		},
		IncrementalABI: "abi-v1",
	}
	opts.RunIncremental = func(taskCtx, runtimeCtx context.Context, task protocol.Task) WorkflowTaskResult {
		key, err := ExecutionCacheKeyForTask(task, "abi-v1")
		require.NoError(t, err)
		incremental.Key = key
		return RunIncrementalWorkflowTask(taskCtx, runtimeCtx, task, backend, incremental, opts.WorkflowTaskOptions)
	}
	return opts
}

func TestExecuteWorkerWorkflowTaskReadsDefinitionOnlyOnCacheMiss(t *testing.T) {
	task := protocol.Task{
		RunID: "run-1", WorkflowType: "wf", ExecutorABIVersion: "abi-v1", ArtifactFingerprint: "sha256:artifact",
	}

	t.Run("Happy Path: warm task reuses the cached session without reading the definition", func(t *testing.T) {
		backend := &workflowTaskBackendStub{config: programtest.Workflow("wf")}
		session := &incrementalTestSession{}
		fullHistoryRuns := 0
		opts := incrementalWorkerOptions(t, backend, session, &fullHistoryRuns)

		cold := ExecuteWorkerWorkflowTask(context.Background(), context.Background(), task, backend, opts)
		require.NoError(t, cold.ReportErr)
		assert.Equal(t, 1, backend.configReads)

		warm := ExecuteWorkerWorkflowTask(context.Background(), context.Background(), task, backend, opts)
		require.NoError(t, warm.ReportErr)
		assert.Equal(t, 1, backend.configReads)
		assert.Equal(t, 2, session.activations)
		assert.Zero(t, fullHistoryRuns)
	})

	t.Run("Error Case: definition read failure is reported without executing", func(t *testing.T) {
		backend := &workflowTaskBackendStub{config: programtest.Workflow("wf")}
		session := &incrementalTestSession{}
		fullHistoryRuns := 0
		opts := incrementalWorkerOptions(t, backend, session, &fullHistoryRuns)
		loadErr := errors.New("definition store unavailable")
		backend.configErr = loadErr

		result := ExecuteWorkerWorkflowTask(context.Background(), context.Background(), task, backend, opts)
		assert.Equal(t, "error", result.Outcome)
		assert.ErrorIs(t, result.ReportErr, loadErr)
		assert.Zero(t, fullHistoryRuns)
		assert.Zero(t, session.activations)
	})
}
