package execution

import (
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/google/uuid"
)

type NewRunOptions struct {
	ID                    string
	Namespace             string
	WorkflowID            string
	WorkflowType          string
	WorkflowConfigVersion string
	FirstRunID            string
	ContinuedRunID        string
	// Parent* 标识启动本 Run 的父工作流节点；顶层 Run 留空。
	ParentRunID            string
	ParentChildID          string
	ParentScheduledEventID int64
	Input                  map[string]any
	SearchAttributes       map[string]string
	DeadlineAt             *time.Time
	Attempt                int
	Initiator              string
	BranchToken            []byte
	CreatedAt              time.Time
}

// WorkflowStartOptions identifies the pinned program used by a new Run.
type WorkflowStartOptions struct {
	Namespace      string
	WorkflowID     string
	WorkflowType   string
	ProgramVersion string
	Input          map[string]any
	Initiator      string
	Now            time.Time
}

func NewWorkflowVersionRun(opts WorkflowStartOptions) (*Run, error) {
	if opts.WorkflowType == "" || opts.ProgramVersion == "" {
		return nil, protocol.InvalidRequestf("workflow config ref is incomplete")
	}
	if opts.WorkflowID == "" {
		return nil, protocol.InvalidRequestf("workflow id is empty")
	}
	if err := errors.Join(
		protocol.CheckLength("namespace", opts.Namespace, protocol.MaxNamespaceLength),
		protocol.CheckLength("workflow id", opts.WorkflowID, protocol.MaxWorkflowIDLength),
		protocol.CheckLength("workflow type", opts.WorkflowType, protocol.MaxWorkflowTypeLength),
	); err != nil {
		return nil, err
	}
	return NewRun(NewRunOptions{
		Namespace:             opts.Namespace,
		WorkflowID:            opts.WorkflowID,
		WorkflowType:          opts.WorkflowType,
		WorkflowConfigVersion: opts.ProgramVersion,
		Input:                 opts.Input,
		Attempt:               1,
		Initiator:             opts.Initiator,
		CreatedAt:             opts.Now,
	}), nil
}

func NewID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

func ExecutionTimeoutMs(value string) int64 {
	return ConfiguredTimeout(value).Milliseconds()
}

func NewRun(opts NewRunOptions) *Run {
	id := opts.ID
	if id == "" {
		id = NewID()
	}
	branchToken := opts.BranchToken
	if len(branchToken) == 0 {
		branchToken, _ = SerializeBranchToken(&HistoryBranch{TreeID: id, BranchID: id})
	}
	firstRunID := opts.FirstRunID
	if firstRunID == "" {
		firstRunID = id
	}
	attempt := opts.Attempt
	if attempt < 1 {
		attempt = 1
	}
	return &Run{
		ID:                     id,
		Namespace:              protocol.NormalizeNamespace(opts.Namespace),
		WorkflowID:             opts.WorkflowID,
		WorkflowType:           opts.WorkflowType,
		WorkflowConfigVersion:  opts.WorkflowConfigVersion,
		FirstRunID:             firstRunID,
		ContinuedRunID:         opts.ContinuedRunID,
		ParentRunID:            opts.ParentRunID,
		ParentChildID:          opts.ParentChildID,
		ParentScheduledEventID: opts.ParentScheduledEventID,
		Status:                 history.StatusRunning,
		Input:                  opts.Input,
		SearchAttributes:       opts.SearchAttributes,
		DeadlineAt:             opts.DeadlineAt,
		BranchToken:            branchToken,
		CreatedAt:              opts.CreatedAt,
		Attempt:                attempt,
		Initiator:              opts.Initiator,
	}
}

// PrepareStartedRun returns the History identity and first event of a Run built
// by NewRun, before the database transaction starts. The caller resolves the
// pinned definition.
func PrepareStartedRun(run *Run, workflowName string, executionTimeoutMs int64) (treeID, branchID string, event history.Event, err error) {
	if run.WorkflowType == "" || run.WorkflowConfigVersion == "" {
		return "", "", history.Event{}, protocol.InvalidRequestf("workflow config ref is incomplete")
	}
	branch, err := DeserializeBranchToken(run.BranchToken)
	if err != nil {
		return "", "", history.Event{}, err
	}
	event = history.NewWorkflowStartedEvent(run.Input, run.FirstRunID, run.ContinuedRunID, run.WorkflowID, workflowName, executionTimeoutMs)
	event.WorkflowStarted.Attempt = run.Attempt
	event.WorkflowStarted.Initiator = run.Initiator
	event.WorkflowStarted.SearchAttributes = cloneStringMap(run.SearchAttributes)
	return branch.TreeID, branch.BranchID, event, nil
}

// CommitStartedRun keeps the Run, current pointer and Started event atomic.
// It also composes with transactions that append other events, such as UpdateAccepted.
func CommitStartedRun(tx ExecutionTx, run *Run, expectedCurrent, treeID, branchID string, started history.Event) error {
	if err := tx.CreateRun(run); err != nil {
		return err
	}
	ok, err := tx.SetCurrentRunCAS(run.Namespace, run.WorkflowID, expectedCurrent, run.ID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrCurrentRunConflict
	}
	_, err = tx.Append(run.ID, treeID, branchID, started)
	return err
}
