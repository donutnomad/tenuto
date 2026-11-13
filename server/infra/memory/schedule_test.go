package memory_test

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule/scheduletest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestScheduleRepositoryContract(t *testing.T) {
	scheduletest.RunRepositoryContract(t, func(*testing.T) schedule.Repository { return memory.NewScheduleRepository() })
}
