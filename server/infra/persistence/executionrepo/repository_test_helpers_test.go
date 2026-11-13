package executionrepo

import (
	"os"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

// newTestStore 返回独立 MySQL 库上的执行仓储；没有开启 MySQL 后端测试时跳过 t。
func newTestStore(t *testing.T) *Repository {
	t.Helper()
	repository, err := New(mysqltest.NewDB(t), Config{})
	if err != nil {
		t.Fatal(err)
	}
	return repository
}

func createTestRun(t *testing.T, repository *Repository, runID string) {
	t.Helper()
	if err := repository.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(rootBranch(t, &execution.Run{
			ID: runID, WorkflowID: "test-wf", Status: history.StatusRunning, CreatedAt: time.Now(),
		}))
	}); err != nil {
		t.Fatal(err)
	}
}

// rootBranch 给测试 Run 补上 root branch token：tree 与 branch 都取 Run ID，
// 与 execution.PrepareStartedRun 为新 Run 生成的 token 一致。
func rootBranch(t testing.TB, run *execution.Run) *execution.Run {
	t.Helper()
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{TreeID: run.ID, BranchID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	run.BranchToken = token
	return run
}
