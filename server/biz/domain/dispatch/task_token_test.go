package dispatch

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func testCodec(t *testing.T) *TaskTokenCodec {
	t.Helper()
	codec, err := NewTaskTokenCodec([]byte("task-token-test-secret-0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func TestTaskTokenRoundTrip(t *testing.T) {
	token, err := testCodec(t).Encode(protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            "run-token-roundtrip",
		ActivityID:       "capture",
		ScheduledEventID: 42,
		Attempt:          3,
		ActivityType:     "custodian.Capture",
		TaskQueue:        "custodian",
	})
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := testCodec(t).Decode(token)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != protocol.TaskTypeActivity || decoded.RunID != "run-token-roundtrip" || decoded.ScheduledEventID != 42 || decoded.Attempt != 3 {
		t.Fatalf("unexpected decoded token: %+v", decoded)
	}
}

func TestTaskTokenCodecRequiresStrongSecretAndSeparatesTrustDomains(t *testing.T) {
	if _, err := NewTaskTokenCodec([]byte("short")); err == nil {
		t.Fatal("short task token secret was accepted")
	}
	codecA, err := NewTaskTokenCodec([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	if err != nil {
		t.Fatal(err)
	}
	codecB, err := NewTaskTokenCodec([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := codecA.Encode(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-codec", ScheduledEventID: 9})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codecA.Decode(token); err != nil {
		t.Fatal(err)
	}
	if _, err := codecB.Decode(token); err == nil {
		t.Fatal("task token was accepted by a different trust domain")
	}
}

func TestTaskTokenCodecBindsQueueLease(t *testing.T) {
	codec, err := NewTaskTokenCodec([]byte("lease-bound-secret-0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	task := protocol.Task{Type: protocol.TaskTypeActivity, Namespace: "tenant-a", RunID: "run-lease", ScheduledEventID: 9, TaskQueue: "payments"}
	token, err := codec.EncodeLease(task, "queue-lease-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.DecodeLease(token, "queue-lease-a"); err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(string(token), taskTokenPrefix))
	if err != nil {
		t.Fatal(err)
	}
	var payload taskTokenPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	taskSignature, err := codec.signTask(payload.Task)
	if err != nil {
		t.Fatal(err)
	}
	if payload.Signature != taskSignature {
		t.Fatal("lease-bound token changed the task signature")
	}
	if _, err := codec.DecodeLease(token, "queue-lease-b"); err == nil {
		t.Fatal("lease-bound task token accepted a different queue lease")
	}
	plainToken, err := codec.Encode(task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.DecodeLease(plainToken, "queue-lease-a"); err == nil {
		t.Fatal("plain task token was accepted for queue acknowledgement")
	}
}

func TestTaskTokenRejectsTamperedPayload(t *testing.T) {
	token, err := testCodec(t).Encode(protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            "run-token-original",
		ScheduledEventID: 9,
		Attempt:          1,
	})
	if err != nil {
		t.Fatal(err)
	}
	tampered := tamperTaskTokenRunID(t, token, "run-token-tampered")

	if _, err := testCodec(t).Decode(tampered); err == nil {
		t.Fatal("expected tampered task token to fail verification")
	}
}

func TestTaskTokenRoundTripsActivationFences(t *testing.T) {
	task := protocol.Task{
		Type:                protocol.TaskTypeWorkflow,
		Namespace:           "demo",
		RunID:               "run-v2",
		ScheduledEventID:    42,
		WorkerIdentity:      "worker-a",
		WorkerBuildID:       "build-a",
		ExecutorABIVersion:  "workflow-executor/v1",
		ExecutionGeneration: 7,
		StickyGeneration:    11,
		ActivationID:        "activation-a",
	}
	token, err := testCodec(t).Encode(task)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(token), taskTokenPrefix) {
		t.Fatalf("token = %q", token)
	}
	decoded, err := testCodec(t).Decode(token)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ExecutionGeneration != 7 || decoded.StickyGeneration != 11 || decoded.ActivationID != "activation-a" || decoded.WorkerIdentity != "worker-a" {
		t.Fatalf("decoded fences = %+v", decoded)
	}
}

func TestTaskTokenPreservesActivityInputIntegers(t *testing.T) {
	task := protocol.Task{
		Type: protocol.TaskTypeActivity, RunID: "run-input", ScheduledEventID: 9,
		Input: map[string]any{"maximum": ^uint64(0), "count": int64(9007199254740993)},
	}
	token, err := testCodec(t).Encode(task)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := testCodec(t).Decode(token)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Input["maximum"] != ^uint64(0) || decoded.Input["count"] != int64(9007199254740993) {
		t.Fatalf("task input = %#v", decoded.Input)
	}
}

func TestTaskTokenRejectsUnknownPrefix(t *testing.T) {
	token, err := testCodec(t).Encode(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-prefix", ScheduledEventID: 9})
	if err != nil {
		t.Fatal(err)
	}
	other := protocol.TaskToken("wftask_v1_" + strings.TrimPrefix(string(token), taskTokenPrefix))
	if _, err := testCodec(t).Decode(other); err == nil {
		t.Fatal("token with another prefix was accepted")
	}
}

func tamperTaskTokenRunID(t *testing.T, token protocol.TaskToken, runID string) protocol.TaskToken {
	t.Helper()

	raw := strings.TrimPrefix(string(token), taskTokenPrefix)
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		t.Fatal(err)
	}
	var payload taskTokenPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	payload.Task.RunID = runID
	data, err = json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.TaskToken(taskTokenPrefix + base64.RawURLEncoding.EncodeToString(data))
}

// Happy Path: Task.Stamp 必须随 task token 编码 + HMAC 签名 + 解码完整往返，
// 这是 stamp 穿 HTTP 的唯一通道（请求体 Task 在 activityRuntimeRequestTask 里被丢弃，
// 只有 token 解码的 Task 是权威）。
func TestTaskTokenRoundTripPreservesStamp(t *testing.T) {
	task := protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            "run-1",
		ScheduledEventID: 10,
		Attempt:          2,
		Stamp:            7,
	}
	token, err := testCodec(t).Encode(task)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := testCodec(t).Decode(token)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Stamp != 7 {
		t.Fatalf("decoded stamp = %d, want 7", decoded.Stamp)
	}
}

// Edge Case: normalizeTaskForToken 不得丢弃 Stamp（它只清 TaskToken / 规范化 Attempt+Namespace）。
func TestNormalizeTaskForTokenKeepsStamp(t *testing.T) {
	got := normalizeTaskForToken(protocol.Task{Type: protocol.TaskTypeActivity, RunID: "r", ScheduledEventID: 1, Stamp: 9})
	if got.Stamp != 9 {
		t.Fatalf("normalize dropped stamp: got %d", got.Stamp)
	}
}
