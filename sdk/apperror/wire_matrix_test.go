package apperror_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// 空 message 仍是失败：Failure 非零，Type 保留，还原后 Message/Error 都为空。
func TestZeroMessageTypedFailureSurvivesWire(t *testing.T) {
	failure, err := apperror.Encode(apperror.New("", "PaymentDeclined"), apperror.JSONEncoder)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if reflect.DeepEqual(failure, protocol.Failure{}) {
		t.Fatalf("empty message produced zero failure: %+v", failure)
	}
	if failure.Type != "PaymentDeclined" {
		t.Fatalf("type = %q, want PaymentDeclined", failure.Type)
	}
	if failure.Message != "" || failure.NonRetryable || failure.Cause != nil || len(failure.Details) != 0 {
		t.Fatalf("unexpected failure shape: %+v", failure)
	}

	decoded := apperror.FromFailure(failure, apperror.JSONDecoder)
	if decoded.Type() != "PaymentDeclined" || decoded.Message() != "" || decoded.Error() != "" {
		t.Fatalf("decoded = type %q, message %q, error %q", decoded.Type(), decoded.Message(), decoded.Error())
	}
	if decoded.NonRetryable() || decoded.HasDetails() || decoded.Unwrap() != nil {
		t.Fatalf("decoded flags: nonRetryable=%v hasDetails=%v cause=%v", decoded.NonRetryable(), decoded.HasDetails(), decoded.Unwrap())
	}
}

// 显式不可重试标记往返保持，nil cause 不产生 Cause 节点。
func TestNonRetryableFlagRoundTrip(t *testing.T) {
	failure, err := apperror.Encode(apperror.NewNonRetryable("m", "T", nil), apperror.JSONEncoder)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !failure.NonRetryable {
		t.Fatalf("nonRetryable not encoded: %+v", failure)
	}
	if failure.Cause != nil {
		t.Fatalf("nil cause produced node: %+v", failure.Cause)
	}
	if failure.Type != "T" || failure.Message != "m" {
		t.Fatalf("scalars: %+v", failure)
	}

	decoded := apperror.FromFailure(failure, apperror.JSONDecoder)
	if !decoded.NonRetryable() || decoded.Unwrap() != nil {
		t.Fatalf("decoded nonRetryable=%v cause=%v", decoded.NonRetryable(), decoded.Unwrap())
	}
}

// cause 链逐层独立：先 Unwrap 再 As 内层；直接 As(decoded) 会命中外层同类型。
func TestCauseChainKeepsLayerFlags(t *testing.T) {
	inner := apperror.NewNonRetryable("inner boom", "InnerType", nil)
	outer := apperror.NewWithCause("outer context", "OuterType", inner)

	failure, err := apperror.Encode(outer, apperror.JSONEncoder)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if failure.NonRetryable {
		t.Fatal("inner nonRetryable flag leaked to outer layer")
	}
	if failure.Type != "OuterType" || failure.Message != "outer context" {
		t.Fatalf("outer scalars: %+v", failure)
	}
	if failure.Cause == nil || failure.Cause.Type != "InnerType" || failure.Cause.Message != "inner boom" || !failure.Cause.NonRetryable {
		t.Fatalf("cause: %+v", failure.Cause)
	}

	decoded := apperror.FromFailure(failure, apperror.JSONDecoder)
	if decoded.Type() != "OuterType" || decoded.NonRetryable() {
		t.Fatalf("decoded outer: type %q nonRetryable %v", decoded.Type(), decoded.NonRetryable())
	}
	cause := errors.Unwrap(decoded)
	if cause == nil {
		t.Fatal("outer layer does not expose its cause")
	}
	var innerErr *apperror.Error
	if !errors.As(cause, &innerErr) {
		t.Fatalf("inner cause is not *apperror.Error: %T", cause)
	}
	if innerErr.Type() != "InnerType" || !innerErr.NonRetryable() || innerErr.Message() != "inner boom" {
		t.Fatalf("inner = type %q nonRetryable %v message %q", innerErr.Type(), innerErr.NonRetryable(), innerErr.Message())
	}
}

// 普通 error 无 type；fmt.Errorf 包装的顶层 type 是 wrapError，内层 type 留在 cause。
func TestPlainAndWrappedFailureTypes(t *testing.T) {
	plain, err := apperror.Encode(errors.New("boom"), apperror.JSONEncoder)
	if err != nil {
		t.Fatalf("Encode plain: %v", err)
	}
	if plain.Type != "" {
		t.Fatalf("plain error type = %q, want empty", plain.Type)
	}
	if plain.Message != "boom" || plain.NonRetryable || plain.Cause != nil || len(plain.Details) != 0 {
		t.Fatalf("plain error shape: %+v", plain)
	}
	if got := apperror.FromFailure(plain, apperror.JSONDecoder); got.Type() != "" || got.Message() != "boom" {
		t.Fatalf("decoded plain: type %q message %q", got.Type(), got.Message())
	}

	wrapped, err := apperror.Encode(fmt.Errorf("ctx: %w", apperror.New("inner boom", "InnerType")), apperror.JSONEncoder)
	if err != nil {
		t.Fatalf("Encode wrapped: %v", err)
	}
	if wrapped.Type != "wrapError" {
		t.Fatalf("wrapped type = %q, want wrapError", wrapped.Type)
	}
	if wrapped.NonRetryable {
		t.Fatal("wrapped error must stay retryable at top level")
	}
	if wrapped.Message != "ctx: inner boom" {
		t.Fatalf("wrapped message = %q", wrapped.Message)
	}
	if wrapped.Cause == nil || wrapped.Cause.Type != "InnerType" || wrapped.Cause.Message != "inner boom" {
		t.Fatalf("wrapped cause: %+v", wrapped.Cause)
	}

	decoded := apperror.FromFailure(wrapped, apperror.JSONDecoder)
	if decoded.Type() != "wrapError" || decoded.NonRetryable() {
		t.Fatalf("decoded wrapped: type %q nonRetryable %v", decoded.Type(), decoded.NonRetryable())
	}
	var innerErr *apperror.Error
	if cause := errors.Unwrap(decoded); cause == nil || !errors.As(cause, &innerErr) || innerErr.Type() != "InnerType" {
		t.Fatalf("decoded wrapped cause: %v", errors.Unwrap(decoded))
	}
}

// 自定义 encoding 由调用方 decoder 解码：本包保留 payload 原样，不拒绝、不改写。
func TestCustomEncodingDelegatedToDecoder(t *testing.T) {
	const customEncoding = "application/x-test-v1"
	failure := protocol.Failure{
		Message: "card rejected",
		Type:    "PaymentDeclined",
		Details: protocol.Payloads{{Encoding: customEncoding, Data: []byte(`"card-9"`)}},
	}

	var seen []string
	decoder := func(payload protocol.Payload, target any) error {
		seen = append(seen, payload.Encoding)
		if payload.Encoding != customEncoding {
			return fmt.Errorf("decoder got encoding %q", payload.Encoding)
		}
		return json.Unmarshal(payload.Data, target)
	}

	decoded := apperror.FromFailure(failure, decoder)
	raw := decoded.RawDetails()
	if len(raw) != 1 || raw[0].Encoding != customEncoding || string(raw[0].Data) != `"card-9"` {
		t.Fatalf("raw details not preserved: %+v", raw)
	}
	var card string
	if err := decoded.Details(&card); err != nil {
		t.Fatalf("Details: %v", err)
	}
	if card != "card-9" {
		t.Fatalf("decoded detail = %q", card)
	}
	if len(seen) != 1 || seen[0] != customEncoding {
		t.Fatalf("decoder calls = %+v", seen)
	}

	// 自定义 encoding 无法用默认 json decoder 解，正是 converter 层存在的理由。
	if err := apperror.JSONDecoder(raw[0], new(string)); err == nil {
		t.Fatal("JSONDecoder accepted a non-json encoding")
	}
}

// details 边界：无载荷与目标数越界各自返回哨兵错误。
func TestDetailTargetBounds(t *testing.T) {
	empty := apperror.FromFailure(protocol.Failure{Message: "m", Type: "T"}, apperror.JSONDecoder)
	if err := empty.Details(new(string)); !errors.Is(err, apperror.ErrNoDetails) {
		t.Fatalf("no details error = %v, want ErrNoDetails", err)
	}

	one, err := apperror.Encode(apperror.New("m", "T", "card-1"), apperror.JSONEncoder)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded := apperror.FromFailure(one, apperror.JSONDecoder)
	if err := decoded.Details(new(string), new(string)); !errors.Is(err, apperror.ErrTooManyArguments) {
		t.Fatalf("too many targets error = %v, want ErrTooManyArguments", err)
	}
	var card string
	if err := decoded.Details(&card); err != nil {
		t.Fatalf("single target: %v", err)
	}
	if card != "card-1" {
		t.Fatalf("detail = %q", card)
	}
}
