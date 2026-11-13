package runtimehttp

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

func TestRuntimeLeaseUsesSignedTaskIdentity(t *testing.T) {
	codec, err := dispatch.NewTaskTokenCodec([]byte("lease-bound-secret-0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-lease", ScheduledEventID: 9, TaskQueue: "payments"}
	token, err := codec.EncodeLease(task, "queue-lease-a")
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyRuntimeTaskLease(codec, protocol.TaskLease{
		Token: "queue-lease-a",
		Task:  protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "tenant-b", RunID: "tampered", ScheduledEventID: 100, TaskQueue: "other", TaskToken: token},
	}, protocol.TaskTypeActivity)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Task.Namespace != task.Namespace || verified.Task.RunID != task.RunID || verified.Task.TaskQueue != task.TaskQueue {
		t.Fatalf("verified lease route = %+v", verified.Task)
	}
	if _, err := verifyRuntimeTaskLease(codec, protocol.TaskLease{Token: "different", Task: protocol.Task{TaskToken: token}}, protocol.TaskTypeActivity); err == nil {
		t.Fatal("accepted a token from another queue lease")
	}
}

func TestRuntimeRequestsRequireTaskToken(t *testing.T) {
	if _, err := activityRuntimeRequestTask(testTaskTokens(), "", protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-a", ScheduledEventID: 1}); err == nil {
		t.Fatal("accepted an activity request without a task token")
	}
	if _, _, err := workflowTaskRequestRef(testTaskTokens(), ""); err == nil {
		t.Fatal("accepted a workflow task request without a task token")
	}
}
