package execution

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

func matrixFailure(message, errorType string) protocol.Failure {
	return protocol.Failure{Message: message, Type: errorType}
}

// type 策略是完全相等匹配（大小写敏感），message 改写不影响 type 命中。
func TestNonRetryableFailureTypeMatrix(t *testing.T) {
	types := []string{"PaymentDeclined"}
	for _, test := range []struct {
		name    string
		failure protocol.Failure
		want    bool
	}{
		{name: "type exact", failure: matrixFailure("card declined", "PaymentDeclined"), want: true},
		{name: "type hit after message rewrite", failure: matrixFailure("something else entirely", "PaymentDeclined"), want: true},
		{name: "different type", failure: matrixFailure("card declined", "PaymentError"), want: false},
		{name: "message contains type name", failure: matrixFailure("PaymentDeclined: card declined", "PaymentError"), want: false},
		{name: "type case differs", failure: matrixFailure("card declined", "paymentdeclined"), want: false},
		{name: "empty type", failure: matrixFailure("PaymentDeclined", ""), want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := NonRetryableFailure(test.failure, types, nil); got != test.want {
				t.Fatalf("NonRetryableFailure(%+v) = %v, want %v", test.failure, got, test.want)
			}
		})
	}
}

// 两条列表互相独立：message 策略保持子串语义，只配 types 时 message 不参与。
func TestNonRetryableFailureListIndependence(t *testing.T) {
	substring := NonRetryableFailure(matrixFailure("the card expired while charging", ""), nil, []string{"card expired"})
	if !substring {
		t.Fatal("message pattern must match as substring")
	}
	if NonRetryableFailure(matrixFailure("the card expired while charging", ""), nil, []string{"PaymentDeclined"}) {
		t.Fatal("message pattern matched without being present")
	}

	typesOnly := NonRetryableFailure(matrixFailure("PaymentDeclined", "GatewayError"), []string{"PaymentDeclined"}, nil)
	if typesOnly {
		t.Fatal("message must not be consulted when only types are configured")
	}
	if !NonRetryableFailure(matrixFailure("anything", "GatewayError"), []string{"GatewayError"}, nil) {
		t.Fatal("type list must match configured type")
	}
	if NonRetryableFailure(matrixFailure("anything", "GatewayError"), nil, []string{"GatewayError"}) {
		t.Fatal("type must not be matched by message patterns")
	}
}

// 显式 flag 优先于两条列表：列表皆空也判定为不可重试。
func TestNonRetryableFailureFlagBeatsLists(t *testing.T) {
	failure := protocol.Failure{Message: "m", Type: "T", NonRetryable: true}
	if !NonRetryableFailure(failure, nil, nil) {
		t.Fatal("nonRetryable flag with empty lists must stop retries")
	}
	if !NonRetryableFailure(failure, []string{"Other"}, []string{"unrelated"}) {
		t.Fatal("nonRetryable flag must win over non-matching lists")
	}
	if NonRetryableFailure(protocol.Failure{Message: "m"}, nil, nil) {
		t.Fatal("plain failure with empty lists must stay retryable")
	}
}

func TestDecideWorkflowRetryFailureMatrix(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

	nonRetryable := &program.RetryPolicy{
		MaxAttempts:            5,
		NonRetryableErrorTypes: []string{"PaymentDeclined"},
	}
	retry, delay, state := DecideWorkflowRetry(&Run{Attempt: 1}, nonRetryable, matrixFailure("card declined", "PaymentDeclined"), now)
	if retry || delay != 0 || state != "NonRetryableFailure" {
		t.Fatalf("type hit: retry=%v delay=%v state=%q", retry, delay, state)
	}

	unbounded := &program.RetryPolicy{}
	retry, delay, state = DecideWorkflowRetry(&Run{Attempt: 4}, unbounded, matrixFailure("transient", "GatewayError"), now)
	if !retry || delay <= 0 || state != "InProgress" {
		t.Fatalf("max attempts 0: retry=%v delay=%v state=%q", retry, delay, state)
	}

	retry, delay, state = DecideWorkflowRetry(&Run{Attempt: 1}, nil, matrixFailure("transient", ""), now)
	if retry || delay != 0 || state != "RetryPolicyNotSet" {
		t.Fatalf("nil policy: retry=%v delay=%v state=%q", retry, delay, state)
	}
}

func TestActivityRetryStateMatrix(t *testing.T) {
	noPolicy := &ActivityState{Attempt: 100, Status: ActivityStatusEnums.Started}
	if got := ActivityRetryState(noPolicy, matrixFailure("m", "T")); got != "" {
		t.Fatalf("nil policy state = %q, want retry under the default unlimited policy", got)
	}

	flagged := &ActivityState{
		Attempt:     1,
		Status:      ActivityStatusEnums.Started,
		RetryPolicy: &history.RetryPolicy{MaxAttempts: 5, NonRetryableErrors: []string{"permanent"}},
	}
	if got := ActivityRetryState(flagged, protocol.Failure{Message: "m", Type: "T", NonRetryable: true}); got != "NonRetryableFailure" {
		t.Fatalf("flag state = %q, want NonRetryableFailure", got)
	}
	if got := ActivityRetryState(flagged, matrixFailure("permanent backend outage", "")); got != "NonRetryableFailure" {
		t.Fatalf("message pattern state = %q, want NonRetryableFailure", got)
	}

	capped := &ActivityState{
		Attempt:     3,
		Status:      ActivityStatusEnums.Started,
		RetryPolicy: &history.RetryPolicy{MaxAttempts: 3},
	}
	if got := ActivityRetryState(capped, matrixFailure("transient", "GatewayError")); got != "MaximumAttemptsReached" {
		t.Fatalf("capped state = %q, want MaximumAttemptsReached", got)
	}

	retryable := &ActivityState{
		Attempt:     1,
		Status:      ActivityStatusEnums.Started,
		RetryPolicy: &history.RetryPolicy{MaxAttempts: 3},
	}
	if got := ActivityRetryState(retryable, matrixFailure("transient", "GatewayError")); got != "" {
		t.Fatalf("retryable state = %q, want empty", got)
	}
}

// failureRecordingTx 只实现 FailActivity 用到的四个方法，其余通过嵌入的 nil 接口暴露。
type failureRecordingTx struct {
	ExecutionTx
	state  *ActivityState
	events []history.Event
}

func (tx *failureRecordingTx) GetWorkflowExecutionStateForUpdate(runID string) (*WorkflowExecutionState, error) {
	return &WorkflowExecutionState{RunID: runID}, nil
}

func (tx *failureRecordingTx) GetActivityStateForUpdate(runID string, scheduledEventID int64) (*ActivityState, bool, error) {
	if tx.state == nil || tx.state.RunID != runID || tx.state.ScheduledEventID != scheduledEventID {
		return nil, false, nil
	}
	return tx.state, true, nil
}

func (tx *failureRecordingTx) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	event.ID = int64(len(tx.events) + 1)
	tx.events = append(tx.events, event)
	return event.ID, true, nil
}

func (tx *failureRecordingTx) UpdateActivityState(state *ActivityState) error {
	tx.state = state
	return nil
}

func matrixPayload() protocol.Payload {
	return protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`"card-1"`)}
}

// 结构化失败原样落进中间 attempt 事件与终态事件，字段不丢。
func TestFailActivityKeepsStructuredFailurePayload(t *testing.T) {
	tx := &failureRecordingTx{state: &ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: ActivityStatusEnums.Started, StartedEventID: 5,
		RetryPolicy: &history.RetryPolicy{MaxAttempts: 5, NonRetryableErrorTypes: []string{"PaymentDeclined"}},
	}}
	completion := ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now()}
	retryable := protocol.Failure{Message: "gateway timeout", Type: "GatewayTimeout", Details: protocol.Payloads{matrixPayload()}}

	result, err := FailActivity(tx, completion, retryable)
	if err != nil {
		t.Fatalf("FailActivity retry: %v", err)
	}
	if !result.Applied || result.Terminal || result.RetryState == nil || result.RetryState.Attempt != 2 {
		t.Fatalf("retry result = %+v", result)
	}

	terminal := protocol.Failure{
		Message: "card declined", Type: "PaymentDeclined", NonRetryable: true,
		Details: protocol.Payloads{matrixPayload()},
	}
	completion.Attempt, completion.Stamp = 2, 2
	result, err = FailActivity(tx, completion, terminal)
	if err != nil {
		t.Fatalf("FailActivity terminal: %v", err)
	}
	if !result.Applied || !result.Terminal || result.RetryState != nil {
		t.Fatalf("terminal result = %+v", result)
	}

	if len(tx.events) != 2 {
		t.Fatalf("events = %d, want 2: %+v", len(tx.events), tx.events)
	}
	attemptFailed := tx.events[0].ActivityAttemptFailed
	if attemptFailed == nil {
		t.Fatalf("first event is not ActivityAttemptFailed: %+v", tx.events[0])
	}
	if attemptFailed.Attempt != 1 || attemptFailed.Error != "gateway timeout" {
		t.Fatalf("attempt failed scalars: attempt=%d error=%q", attemptFailed.Attempt, attemptFailed.Error)
	}
	if attemptFailed.Failure == nil {
		t.Fatal("attempt failed event lost its Failure payload")
	}
	if attemptFailed.Failure.Message != "gateway timeout" || attemptFailed.Failure.Type != "GatewayTimeout" || attemptFailed.Failure.NonRetryable {
		t.Fatalf("attempt failed failure = %+v", attemptFailed.Failure)
	}
	if len(attemptFailed.Failure.Details) != 1 ||
		attemptFailed.Failure.Details[0].Encoding != protocol.PayloadEncodingJSONPlainV1 ||
		string(attemptFailed.Failure.Details[0].Data) != `"card-1"` {
		t.Fatalf("attempt failed details = %+v", attemptFailed.Failure.Details)
	}

	failed := tx.events[1].ActivityFailed
	if failed == nil {
		t.Fatalf("second event is not ActivityFailed: %+v", tx.events[1])
	}
	if failed.Attempt != 2 || failed.Error != "card declined" || failed.RetryState != "NonRetryableFailure" {
		t.Fatalf("terminal scalars: attempt=%d error=%q retryState=%q", failed.Attempt, failed.Error, failed.RetryState)
	}
	if failed.Failure == nil {
		t.Fatal("terminal event lost its Failure payload")
	}
	if failed.Failure.Message != "card declined" || failed.Failure.Type != "PaymentDeclined" || !failed.Failure.NonRetryable {
		t.Fatalf("terminal failure = %+v", failed.Failure)
	}
	if len(failed.Failure.Details) != 1 || string(failed.Failure.Details[0].Data) != `"card-1"` {
		t.Fatalf("terminal details = %+v", failed.Failure.Details)
	}
}

// message-only 失败：Type 保持空，Error 文本与 Failure.Message 一致。
func TestFailActivityKeepsMessageOnlyFailure(t *testing.T) {
	tx := &failureRecordingTx{state: &ActivityState{
		RunID: "run-1", ScheduledEventID: 7, ActivityID: "charge", ActivityType: "charge",
		Attempt: 1, Stamp: 1, Status: ActivityStatusEnums.Started, StartedEventID: 5,
		RetryPolicy: &history.RetryPolicy{MaxAttempts: 3},
	}}
	completion := ActivityTransition{RunID: "run-1", TreeID: "run-1", BranchID: "run-1", ScheduledEventID: 7, Attempt: 1, Stamp: 1, Now: time.Now()}
	failure := protocol.Failure{Message: "dial tcp: i/o timeout"}

	result, err := FailActivity(tx, completion, failure)
	if err != nil {
		t.Fatalf("FailActivity: %v", err)
	}
	if !result.Applied || result.Terminal || result.RetryState == nil {
		t.Fatalf("result = %+v", result)
	}
	if len(tx.events) != 1 || tx.events[0].ActivityAttemptFailed == nil {
		t.Fatalf("events = %+v", tx.events)
	}
	attemptFailed := tx.events[0].ActivityAttemptFailed
	if attemptFailed.Error != "dial tcp: i/o timeout" {
		t.Fatalf("error text = %q", attemptFailed.Error)
	}
	if attemptFailed.Failure == nil {
		t.Fatal("message-only failure was dropped")
	}
	if attemptFailed.Failure.Type != "" || attemptFailed.Failure.NonRetryable || len(attemptFailed.Failure.Details) != 0 {
		t.Fatalf("message-only failure shape = %+v", attemptFailed.Failure)
	}
	if attemptFailed.Failure.Message != "dial tcp: i/o timeout" {
		t.Fatalf("failure message = %q", attemptFailed.Failure.Message)
	}
}
