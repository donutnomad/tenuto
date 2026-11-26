package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

type workflowCompletionCaptureContextKey struct{}

type workflowCompletionCapture struct {
	activation history.WorkflowTaskCompletionRequest
	ack        history.WorkflowTaskCompletionAck
	captured   bool
}

func (s *Server) GetWorkflowActivation(ctx context.Context, request history.WorkflowActivationRequest) (history.WorkflowActivation, error) {
	if err := ctx.Err(); err != nil {
		return history.WorkflowActivation{}, err
	}
	if request.Task.RunID == "" || request.ActivationID == "" {
		return history.WorkflowActivation{}, protocol.InvalidRequestf("workflow activation request is incomplete")
	}
	generation := request.ExecutionGeneration
	state, err := s.mgr.GetWorkflowExecutionState(request.Task.RunID)
	if err != nil {
		return history.WorkflowActivation{}, err
	}
	if state.ExecutionGeneration != generation {
		return history.WorkflowActivation{}, &history.ColdRestoreRequiredError{Reason: "execution generation changed"}
	}
	if request.AfterEventID > state.LastEventID {
		return history.WorkflowActivation{}, &history.ColdRestoreRequiredError{Reason: "worker cursor is ahead of server history"}
	}
	events, err := s.mgr.GetEventsRange(request.Task.RunID, request.AfterEventID, 0)
	if err != nil {
		return history.WorkflowActivation{}, err
	}
	if len(events) == 0 {
		return history.WorkflowActivation{}, &history.ColdRestoreRequiredError{Reason: "activation delta is empty"}
	}
	if err := history.ValidateEventSequence(request.AfterEventID, events); err != nil {
		return history.WorkflowActivation{}, &history.ColdRestoreRequiredError{Reason: err.Error()}
	}
	return history.WorkflowActivation{
		ActivationID:        request.ActivationID,
		ExecutionGeneration: generation,
		Events:              events,
		ThroughEventID:      events[len(events)-1].ID,
	}, nil
}

func (s *Server) CompleteWorkflowActivation(ctx context.Context, request history.WorkflowTaskCompletionRequest) (history.WorkflowTaskCompletionAck, error) {
	if request.Task.RunID == "" || request.Task.ScheduledEventID == 0 {
		return history.WorkflowTaskCompletionAck{}, protocol.InvalidRequestf("workflow completion task identity is incomplete")
	}
	if request.Activation.ActivationID == "" || request.Activation.ActivationID != history.WorkflowActivationID(request.Task) {
		return history.WorkflowTaskCompletionAck{Accepted: false}, protocol.InvalidRequestf("workflow completion activation identity is invalid")
	}
	digest, err := history.DigestCommands(request.Commands)
	if err != nil {
		return history.WorkflowTaskCompletionAck{}, err
	}
	if digest != request.Activation.CommandDigest {
		return history.WorkflowTaskCompletionAck{Accepted: false}, protocol.InvalidRequestf("workflow completion command digest is invalid")
	}
	if request.Activation.ExecutionGeneration != request.Task.ExecutionGeneration {
		return history.WorkflowTaskCompletionAck{Accepted: false}, protocol.InvalidRequestf("workflow completion execution generation is invalid")
	}

	capture := &workflowCompletionCapture{activation: request}
	ctx = context.WithValue(ctx, workflowCompletionCaptureContextKey{}, capture)
	if err := s.HandleWorkflowTaskCompleted(ctx, request.Task.RunID, request.Task.ScheduledEventID, request.Commands, request.Task.WorkerIdentity); err != nil {
		return history.WorkflowTaskCompletionAck{}, err
	}
	if !capture.captured {
		return history.WorkflowTaskCompletionAck{
			Accepted:            false,
			ActivationID:        request.Activation.ActivationID,
			CommandDigest:       request.Activation.CommandDigest,
			ExecutionGeneration: request.Activation.ExecutionGeneration,
			StickyGeneration:    request.Task.StickyGeneration,
		}, nil
	}
	return capture.ack, nil
}

// activatedThroughEventID 返回增量激活路径上 Worker 已看到的最后一个事件 ID；全量路径返回 0。
func activatedThroughEventID(ctx context.Context) int64 {
	capture, ok := ctx.Value(workflowCompletionCaptureContextKey{}).(*workflowCompletionCapture)
	if !ok || capture == nil {
		return 0
	}
	return capture.activation.Activation.ActivatedThroughEventID
}

func (s *Server) captureWorkflowTaskCompletion(
	ctx context.Context,
	result execution.WorkflowTaskCompletionResult,
	closeEvent history.Event,
	commandEvents []history.Event,
	commands []history.Command,
) error {
	capture, ok := ctx.Value(workflowCompletionCaptureContextKey{}).(*workflowCompletionCapture)
	if !ok || capture == nil || !result.ClosedInserted {
		return nil
	}
	if result.ClosedEventID == 0 {
		return errors.New("workflow completion store omitted authoritative close event ID")
	}
	closeEvent.ID = result.ClosedEventID
	committed := []history.Event{closeEvent}
	for index, eventResult := range result.CommandEvents {
		if index >= len(commandEvents) {
			return fmt.Errorf("workflow completion command result %d has no source event", index)
		}
		if !eventResult.Inserted {
			continue
		}
		event := commandEvents[index]
		event.ID = eventResult.EventID
		committed = append(committed, event)
	}
	if err := history.ValidateEventSequence(capture.activation.Activation.ActivatedThroughEventID, committed); err != nil {
		return fmt.Errorf("workflow completion store returned non-contiguous authoritative tail: %w", err)
	}
	terminal := false
	for _, command := range commands {
		switch command.Type {
		case history.CmdCompleteWorkflow, history.CmdFailWorkflow, history.CmdCancelWorkflow, history.CmdContinueAsNew:
			terminal = true
		}
	}
	capture.ack = history.WorkflowTaskCompletionAck{
		Accepted:                true,
		CommittedThroughEventID: committed[len(committed)-1].ID,
		CommittedEvents:         committed,
		ActivationID:            capture.activation.Activation.ActivationID,
		CommandDigest:           capture.activation.Activation.CommandDigest,
		ExecutionGeneration:     capture.activation.Activation.ExecutionGeneration,
		StickyGeneration:        capture.activation.Task.StickyGeneration,
		Terminal:                terminal,
	}
	if !terminal &&
		capture.activation.Task.ExecutorABIVersion == history.ExecutorABIVersionV1 &&
		capture.activation.Task.WorkerIdentity != "" {
		generation, assigned, err := s.mgr.AssignStickyExecution(
			capture.activation.Task.RunID,
			capture.activation.Activation.ExecutionGeneration,
			capture.activation.Task.StickyGeneration,
			capture.activation.Task.WorkerIdentity,
			capture.activation.Task.WorkerBuildID,
		)
		// sticky 只是路由优化：分配失败或被 fence 拒绝时回执不带新的 sticky 会话，
		// 下一个 Workflow Task 走普通队列。
		switch {
		case err != nil:
			s.logger.Warn("assign sticky execution failed; workflow completion is acknowledged without sticky",
				slog.String("run_id", capture.activation.Task.RunID),
				slog.Any("err", err),
			)
		case assigned:
			capture.ack.StickyGeneration = generation
		}
	}
	capture.captured = true
	return nil
}
