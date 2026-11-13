package execution

import (
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// closeRun 在 Run 结束的事务里写入终态（completed 时含输出），并关闭它仍未触发的 timer
// （TimerStatusEnums.RunClosed，不写事件，与 Temporal 关闭 Run 时丢弃挂起的 timer 一致）。
func closeRun(tx ExecutionTx, run *Run, status history.Status, output map[string]any) error {
	if err := run.MarkTerminal(status, output); err != nil {
		return err
	}
	if err := tx.UpdateRun(run); err != nil {
		return err
	}
	return tx.CloseScheduledTimers(run.ID)
}

// MarkTerminal sets the Run's terminal status, and its output for a completed
// Run. The caller persists it in the transaction that appends the terminal
// History event, or for continue-as-new and retry, in the transaction that
// creates the next Run.
func (r *Run) MarkTerminal(status history.Status, output map[string]any) error {
	if !history.IsTerminalStatus(status) {
		return fmt.Errorf("terminal run status required: %s", status)
	}
	r.setStatus(status)
	if status == history.StatusCompleted {
		r.setOutput(output)
	}
	return nil
}

// ReconcileRunStatus writes the status of the Run's latest lifecycle event,
// and the output of a WorkflowCompleted event, onto the Run. Recovery uses it
// for a Run whose terminal event committed before its status: continue-as-new
// and retried failures close the Run only when the next Run is created.
func ReconcileRunStatus(tx ExecutionTx, run *Run, event history.Event, status history.Status) error {
	run.setStatus(status)
	if status == history.StatusCompleted && event.WorkflowCompleted != nil {
		run.setOutput(event.WorkflowCompleted.Output)
	}
	if err := tx.UpdateRun(run); err != nil {
		return err
	}
	if history.IsTerminalStatus(status) {
		return tx.CloseScheduledTimers(run.ID)
	}
	return nil
}

// Terminate commits the terminal History event and status in one transaction.
// inserted=false means History already holds a terminal event, which may be of
// another type (completed, failed, cancelled, continued-as-new). The status
// projection then stays untouched: recovery derives it from the event that
// actually closed the Run (ReconcileRunStatus).
func Terminate(tx ExecutionTx, run *Run, treeID, branchID, reason string) (bool, error) {
	_, inserted, err := tx.AppendIdempotent(run.ID, treeID, branchID, history.NewWorkflowTerminatedEvent(reason))
	if err != nil || !inserted {
		return false, err
	}
	return true, closeRun(tx, run, history.StatusTerminated, nil)
}

// CheckTerminationStatus preserves the direct API's terminal-state policy
// after History reconciliation.
func CheckTerminationStatus(runID string, status history.Status) error {
	if history.IsTerminalStatus(status) {
		return protocol.InvalidRequestf("workflow %s is %s", runID, status)
	}
	return nil
}

func EnsureTerminable(current *Run) error {
	return CheckTerminationStatus(current.ID, current.Status)
}

func TerminationReason(reason string) string {
	if reason == "" {
		return "terminated"
	}
	return reason
}

// AppendWorkflowFailed writes the terminal WorkflowFailed event inside the
// caller's transaction and marks the Run failed in the same transaction. A
// failure that is retried (NewExecutionRunID set) keeps the Run running until
// CommitRetryRun creates the retry Run and closes this one: recovery finds the
// Run by its running status and starts the retry. inserted=false means History
// already holds a terminal event and the Run stays untouched.
func AppendWorkflowFailed(tx ExecutionTx, run *Run, treeID, branchID string, failed history.Event) (inserted bool, err error) {
	if failed.WorkflowFailed == nil {
		return false, fmt.Errorf("workflow failed event required for run %s", run.ID)
	}
	_, inserted, err = tx.AppendIdempotent(run.ID, treeID, branchID, failed)
	if err != nil || !inserted || failed.WorkflowFailed.NewExecutionRunID != "" {
		return inserted, err
	}
	return true, closeRun(tx, run, history.StatusFailed, nil)
}
