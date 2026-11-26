package memory_test

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestRunListOrderContract(t *testing.T) {
	executiontest.RunListOrderContract(t, func(*testing.T) execution.ExecutionManager { return memory.NewExecutionManager() })
}

func TestRunListVisibilityContract(t *testing.T) {
	executiontest.RunListVisibilityContract(t, func(*testing.T) execution.ExecutionManager { return memory.NewExecutionManager() })
}

func TestRecoveryRunIDsContract(t *testing.T) {
	executiontest.RecoveryRunIDsContract(t, func(*testing.T) execution.ExecutionManager { return memory.NewExecutionManager() })
}

func TestRunScanWithoutTotalContract(t *testing.T) {
	executiontest.RunScanWithoutTotalContract(t, func(*testing.T) execution.ExecutionManager { return memory.NewExecutionManager() })
}
