package persistence

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// 多个副本同时启动带搜索属性的 Run（UUIDv7 的 Run ID 单调递增，新行都落在 tenuto_run_search_attributes 的同一个间隙里）、
// 并改写各自的属性：任何一条语句都不出现死锁（1213，仓储的重放会把它藏起来，这里在语句层面观察），也不被串行化到超时。
func TestConcurrentRunStartsWithSearchAttributesDoNotDeadlock(t *testing.T) {
	db := mysqltest.NewDB(t)
	var deadlocks atomic.Int64
	observe := func(tx *gorm.DB) {
		if infradb.IsDeadlock(tx.Error) {
			deadlocks.Add(1)
		}
	}
	callbacks := db.Callback()
	for name, register := range map[string]func(string, func(*gorm.DB)) error{
		"create": callbacks.Create().After("gorm:create").Register,
		"delete": callbacks.Delete().After("gorm:delete").Register,
		"update": callbacks.Update().After("gorm:update").Register,
		"query":  callbacks.Query().After("gorm:query").Register,
		"raw":    callbacks.Raw().After("gorm:raw").Register,
		"row":    callbacks.Row().After("gorm:row").Register,
	} {
		if err := register("test:observe-deadlock-"+name, observe); err != nil {
			t.Fatal(err)
		}
	}
	store, err := New(db, Config{})
	if err != nil {
		t.Fatal(err)
	}

	const workers, runsPerWorker = 8, 15
	started := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, workers*runsPerWorker)
	for w := range workers {
		wg.Go(func() {
			for i := range runsPerWorker {
				id := uuid.Must(uuid.NewV7()).String()
				run := rootBranch(t, &execution.Run{
					ID: id, Namespace: "tenant-a", WorkflowID: fmt.Sprintf("wf-%d-%d", w, i), WorkflowConfigVersion: "v1",
					Status: history.StatusRunning, CreatedAt: time.Now(),
					SearchAttributes: map[string]string{"tenant": "a", "order": id},
				})
				if err := store.Executions.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(run) }); err != nil {
					errs <- fmt.Errorf("create %s: %w", id, err)
					continue
				}
				started := history.NewWorkflowStartedEvent(nil, id, "", run.WorkflowID, "flow", 0)
				started.WorkflowStarted.SearchAttributes = run.SearchAttributes
				if _, err := store.Executions.Append(id, id, id, started); err != nil {
					errs <- fmt.Errorf("start %s: %w", id, err)
					continue
				}
				// 改写属性：删一个、加一个、留一个。
				upsert := history.NewSearchAttributesUpsertedEvent(map[string]any{"order": "", "stage": "paid"})
				if _, err := store.Executions.Append(id, id, id, upsert); err != nil {
					errs <- fmt.Errorf("upsert %s: %w", id, err)
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	elapsed := time.Since(started)
	t.Logf("%d runs started and updated by %d workers in %s", workers*runsPerWorker, workers, elapsed.Round(time.Millisecond))
	if n := deadlocks.Load(); n > 0 {
		t.Fatalf("%d statements failed with deadlock (1213)", n)
	}
	if elapsed > 30*time.Second {
		t.Fatalf("concurrent starts took %s: they were serialized on a lock", elapsed)
	}

	result, err := execution.ListRuns(store.Executions, execution.RunListFilter{
		Namespace: "tenant-a", SearchAttributes: map[string]string{"stage": "paid", "order": ""}, Limit: 1000, IncludeTerminal: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != workers*runsPerWorker {
		t.Fatalf("runs matching the rewritten attributes = %d, want %d", result.Total, workers*runsPerWorker)
	}
}
