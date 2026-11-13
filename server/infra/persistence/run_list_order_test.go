package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
)

func TestRunListOrderContract(t *testing.T) {
	executiontest.RunListOrderContract(t, func(t *testing.T) execution.ExecutionManager { return newTestStore(t).Executions })
}

func TestRunListVisibilityContract(t *testing.T) {
	executiontest.RunListVisibilityContract(t, func(t *testing.T) execution.ExecutionManager { return newTestStore(t).Executions })
}
