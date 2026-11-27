package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

func newContractManager(t *testing.T) execution.ExecutionManager { return newTestStore(t).Executions }

func TestGormEventRoundTripContract(t *testing.T) {
	executiontest.EventRoundTripContract(t, newContractManager)
}

func TestGormEventNumberTypesContract(t *testing.T) {
	executiontest.EventNumberTypesContract(t, newContractManager)
}

func TestGormEventsByDedupeKeysContract(t *testing.T) {
	executiontest.EventsByDedupeKeysContract(t, newContractManager)
}

func TestGormStatePatchContract(t *testing.T) {
	executiontest.StatePatchContract(t, newContractManager)
}

func TestGormExecutionStateContract(t *testing.T) {
	executiontest.ExecutionStateContract(t, newContractManager)
}

func TestGormWorkflowTaskStartContract(t *testing.T) {
	executiontest.WorkflowTaskStartContract(t, newContractManager)
}

func TestGormTransactionContract(t *testing.T) {
	executiontest.TransactionContract(t, newContractManager)
}

func TestGormHistoryBranchContract(t *testing.T) {
	executiontest.HistoryBranchContract(t, newContractManager)
}

func TestGormRunReadUnderLockContract(t *testing.T) {
	executiontest.RunReadUnderLockContract(t, newContractManager)
}

func TestGormEventTimestampContract(t *testing.T) {
	executiontest.EventTimestampContract(t, func(t *testing.T, clk clock.Clock) execution.ExecutionManager {
		return newMySQLStore(t, Config{Clock: clk}).Executions
	})
}
