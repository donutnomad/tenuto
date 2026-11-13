package runtimehttp

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

type activityRuntimeNumberSpy struct {
	output map[string]any
}

func (s *activityRuntimeNumberSpy) BeginActivityTask(context.Context, protocol.Task) (protocol.ActivityTaskBeginResult, error) {
	return protocol.ActivityTaskBeginResult{Execute: true}, nil
}

func (s *activityRuntimeNumberSpy) RecordActivityHeartbeat(context.Context, protocol.Task, map[string]any) (protocol.ActivityHeartbeatResult, error) {
	return protocol.ActivityHeartbeatResult{}, nil
}

func (s *activityRuntimeNumberSpy) CheckActivityTaskCancellation(context.Context, protocol.Task) (protocol.ActivityCancellationResult, error) {
	return protocol.ActivityCancellationResult{}, nil
}

func (s *activityRuntimeNumberSpy) CompleteActivityTask(_ context.Context, _ protocol.Task, output map[string]any, _ *protocol.Failure) error {
	s.output = output
	return nil
}

func (s *activityRuntimeNumberSpy) FailActivityTask(context.Context, protocol.Task, protocol.Failure) error {
	return nil
}

func TestActivityRuntimeHTTPPreservesUint64Output(t *testing.T) {
	spy := &activityRuntimeNumberSpy{}
	server := httptest.NewServer(NewActivityRuntimeHTTPHandler(activityRuntimeForTest{API: spy}, testTaskTokens(), slog.New(slog.DiscardHandler)))
	defer server.Close()
	task := protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-uint64", ScheduledEventID: 7}
	token, err := testTaskTokens().Encode(task)
	if err != nil {
		t.Fatal(err)
	}
	task.TaskToken = token
	if err := wirePost(context.Background(), server.URL, runtimeapi.ActivityRuntimeCompletePath, runtimeapi.ActivityRuntimeCompleteRequest{
		TaskToken: task.TaskToken, Task: task, Output: map[string]any{"maximum": ^uint64(0)},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if spy.output["maximum"] != ^uint64(0) {
		t.Fatalf("HTTP activity output = %#v", spy.output)
	}
}

func (*activityRuntimeNumberSpy) ReleaseActivityTask(context.Context, protocol.Task) error {
	return nil
}
