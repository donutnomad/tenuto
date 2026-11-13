package persistence

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/batch/batchtest"
)

// ============================================
// 批量操作 gorm 持久化测试
// ============================================
// 测试策略:
// - 仓储契约（round-trip、进度与游标、按 patch 更新、version 乐观锁、列表排序与分页）：与内存实现共用 batchtest
// - MySQL: 并发保存同一版本只有一个成功
// ============================================

func TestBatchRepositoryContract(t *testing.T) {
	batchtest.RunRepositoryContract(t, func(t *testing.T) batch.Repository { return newTestStore(t).Batches })
}

func TestMySQLBatchConcurrentSavesOfOneVersion(t *testing.T) {
	// MySQL: 多个写入方基于同一版本同时保存，恰好一个成功，其余得到 ErrConcurrentUpdate
	store := requireMySQL(t)
	created, err := batch.New("op-race", batch.NewParams{Type: batch.TypeEnums.Cancel}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Batches.CreateOperation(created); err != nil {
		t.Fatal(err)
	}
	const writers = 8
	errs := make([]error, writers)
	var wg sync.WaitGroup
	// 所有写入方先各自读到同一个版本，再一起保存。
	ops := make([]*batch.Operation, writers)
	for i := range writers {
		op, err := store.Batches.GetOperation("default", "op-race")
		if err != nil {
			t.Fatal(err)
		}
		ops[i] = op
	}
	for i, op := range ops {
		wg.Go(func() {
			if i%2 == 0 {
				errs[i] = op.Cancel(time.Now())
			} else {
				errs[i] = op.Begin(time.Now())
			}
			if errs[i] == nil {
				errs[i] = store.Batches.UpdateOperation(op)
			}
		})
	}
	wg.Wait()
	saved := 0
	for i, err := range errs {
		switch {
		case err == nil:
			saved++
		case !errors.Is(err, batch.ErrConcurrentUpdate):
			t.Errorf("writer %d: %v", i, err)
		}
	}
	if saved != 1 {
		t.Fatalf("%d writers saved the same version, want exactly 1", saved)
	}
	got, err := store.Batches.GetOperation("default", "op-race")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != created.Version+1 {
		t.Fatalf("version = %d, want %d", got.Version, created.Version+1)
	}
}
