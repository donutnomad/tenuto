package persistence

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/gsql"
	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

func TestLatestWorkflowConfigRefErrorContract(t *testing.T) {
	store := newTestStore(t)
	_, err := store.Definitions.LatestWorkflowConfigRefInNamespace("payments", "missing")
	if !errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("missing config error = %v", err)
	}

	databaseFailure := errors.New("config database unavailable")
	previousCallbacks := slices.Clone(gsql.QueryCallbacks)
	gsql.QueryCallbacks = append(gsql.QueryCallbacks, func(db *gorm.DB) {
		db.AddError(databaseFailure)
	})
	t.Cleanup(func() { gsql.QueryCallbacks = previousCallbacks })
	_, err = store.Definitions.LatestWorkflowConfigRefInNamespace("", "workflow")
	if !errors.Is(err, databaseFailure) {
		t.Fatalf("database error = %v", err)
	}
	if errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("database error classified as missing config: %v", err)
	}
}

func TestListAllReturnsDatabaseError(t *testing.T) {
	db := newUnconnectedDB(t, &gorm.Config{TranslateError: true})
	// persistence.New 会在库上做一次探测，这里直接建仓储，句柄保持未连接。
	runs, err := executionrepo.New(db, executionrepo.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := runs.CountRuns(""); err == nil {
		t.Fatal("CountRuns error = nil after the database connection was closed")
	}
}

func TestRunPersistsWorkflowConfigVersion(t *testing.T) {
	store := newTestStore(t)

	run := &execution.Run{
		ID:                    "run-version-refs",
		WorkflowID:            "order-flow",
		WorkflowConfigVersion: "prog_456",
		Status:                history.StatusRunning,
		CreatedAt:             time.Now(),
	}
	if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(rootBranch(t, run)) }); err != nil {
		t.Fatal(err)
	}

	got, err := store.Executions.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkflowConfigVersion != run.WorkflowConfigVersion {
		t.Fatalf("version refs lost after persistence: %+v", got)
	}
}

func TestWorkflowConfigPersistsAcrossStoreInstances(t *testing.T) {
	db := mysqltest.NewDB(t)
	store, err := New(db, Config{})
	if err != nil {
		t.Fatal(err)
	}

	workflow := programtest.Workflow("order-flow")
	workflow.Name = "订单流程"
	workflow.Activities = map[string]programpkg.ActivityRef{"activities.ReserveInventory": {Symbol: "activities.ReserveInventory", Type: "activities.ReserveInventory"}}
	ref, err := store.Definitions.SaveWorkflowConfigInNamespace("", workflow)
	if err != nil {
		t.Fatal(err)
	}

	// 新的 Store 实例只经数据库读回配置，不共享前一个实例的任何内存状态。
	reopened, err := New(db, Config{})
	if err != nil {
		t.Fatal(err)
	}

	got, err := reopened.Definitions.GetWorkflowConfig(ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != workflow.Type || len(got.Program.Program) == 0 || len(got.Activities) != 1 {
		t.Fatalf("unexpected persisted config: %+v", got)
	}
	latest, err := reopened.Definitions.LatestWorkflowConfigRefInNamespace("", workflow.Type)
	if err != nil {
		t.Fatal(err)
	}
	if latest != ref {
		t.Fatalf("latest ref = %+v, want %+v", latest, ref)
	}
}
