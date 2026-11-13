package memory_test

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/batch/batchtest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestBatchRepositoryContract(t *testing.T) {
	batchtest.RunRepositoryContract(t, func(*testing.T) batch.Repository { return memory.NewBatchRepository() })
}
