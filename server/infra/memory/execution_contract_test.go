package memory_test

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func newContractManager(*testing.T) execution.ExecutionManager { return memory.NewExecutionManager() }

func TestMemoryEventRoundTripContract(t *testing.T) {
	executiontest.EventRoundTripContract(t, newContractManager)
}

func TestMemoryEventNumberTypesContract(t *testing.T) {
	executiontest.EventNumberTypesContract(t, newContractManager)
}

func TestMemoryEventsByDedupeKeysContract(t *testing.T) {
	executiontest.EventsByDedupeKeysContract(t, newContractManager)
}

func TestMemoryStatePatchContract(t *testing.T) {
	executiontest.StatePatchContract(t, newContractManager)
}

func TestMemoryExecutionStateContract(t *testing.T) {
	executiontest.ExecutionStateContract(t, newContractManager)
}

func TestMemoryWorkflowTaskStartContract(t *testing.T) {
	executiontest.WorkflowTaskStartContract(t, newContractManager)
}

func TestMemoryTransactionContract(t *testing.T) {
	executiontest.TransactionContract(t, newContractManager)
}

func TestMemoryHistoryBranchContract(t *testing.T) {
	executiontest.HistoryBranchContract(t, newContractManager)
}

func TestMemoryRunReadUnderLockContract(t *testing.T) {
	executiontest.RunReadUnderLockContract(t, newContractManager)
}

func TestMemoryEventTimestampContract(t *testing.T) {
	executiontest.EventTimestampContract(t, func(_ *testing.T, clk clock.Clock) execution.ExecutionManager {
		return memory.NewExecutionManager().WithClock(clk)
	})
}
