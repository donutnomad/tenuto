package apperror_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

type customError struct{ code int }

func (e *customError) Error() string { return fmt.Sprintf("custom %d", e.code) }

func encode(t *testing.T, err error) protocol.Failure {
	t.Helper()
	failure, encodeErr := apperror.Encode(err, apperror.JSONEncoder)
	if encodeErr != nil {
		t.Fatalf("Encode: %v", encodeErr)
	}
	return failure
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := apperror.New("card has expired", "CardExpired", "card-1", 42)
	failure := encode(t, original)

	if failure.Message != "card has expired" || failure.Type != "CardExpired" {
		t.Fatalf("scalar fields: %+v", failure)
	}
	if failure.NonRetryable || failure.Cause != nil {
		t.Fatalf("unexpected flags: %+v", failure)
	}
	if len(failure.Details) != 2 {
		t.Fatalf("details: %+v", failure.Details)
	}

	decoded := apperror.FromFailure(failure, apperror.JSONDecoder)
	if decoded.Type() != "CardExpired" || decoded.Message() != "card has expired" || decoded.NonRetryable() {
		t.Fatalf("decoded scalars: %+v", decoded)
	}
	var card string
	var count int
	if err := decoded.Details(&card, &count); err != nil {
		t.Fatalf("Details: %v", err)
	}
	if card != "card-1" || count != 42 {
		t.Fatalf("decoded details: %q %d", card, count)
	}
}

// cause 链：外层先 Unwrap，再 As 内层；直接 As(err) 会命中外层同类型。
func TestEncodeDecodeCauseChain(t *testing.T) {
	inner := apperror.NewNonRetryable("inner boom", "InnerType", nil)
	original := apperror.NewWithCause("context lost", "OuterType", inner, "detail")

	failure := encode(t, original)
	if failure.Cause == nil || failure.Cause.Type != "InnerType" || !failure.Cause.NonRetryable {
		t.Fatalf("cause: %+v", failure.Cause)
	}

	decoded := apperror.FromFailure(failure, apperror.JSONDecoder)
	unwrapped := errors.Unwrap(decoded)
	if unwrapped == nil {
		t.Fatal("cause not exposed through Unwrap")
	}
	var innerErr *apperror.Error
	if !errors.As(unwrapped, &innerErr) {
		t.Fatalf("errors.As inner: %T", unwrapped)
	}
	if innerErr.Type() != "InnerType" || !innerErr.NonRetryable() {
		t.Fatalf("inner app error: %+v", innerErr)
	}
}

func TestEncodePlainError(t *testing.T) {
	failure := encode(t, errors.New("plain failure"))
	if failure.Type != "" {
		t.Fatalf("plain error type = %q, want empty", failure.Type)
	}
	if failure.NonRetryable || failure.Cause != nil || len(failure.Details) != 0 {
		t.Fatalf("plain error shape: %+v", failure)
	}
}

// 顶层只看最外层错误：包装后 type 变成 wrapError，内层 flag 不提升到顶层。
func TestEncodeWrappedError(t *testing.T) {
	inner := apperror.NewNonRetryable("inner boom", "InnerType", nil)
	failure := encode(t, fmt.Errorf("wrap context: %w", inner))

	if failure.Type != "wrapError" {
		t.Fatalf("top type = %q, want wrapError", failure.Type)
	}
	if failure.NonRetryable {
		t.Fatal("inner non-retryable flag must not be promoted to top level")
	}
	if failure.Cause == nil || failure.Cause.Type != "InnerType" || !failure.Cause.NonRetryable {
		t.Fatalf("cause: %+v", failure.Cause)
	}
}

func TestEncodeGoTypeName(t *testing.T) {
	failure := encode(t, &customError{code: 7})
	if failure.Type != "customError" {
		t.Fatalf("type = %q, want customError", failure.Type)
	}
	if plain := encode(t, errors.New("x")); plain.Type != "" {
		t.Fatalf("errors.errorString type = %q, want empty", plain.Type)
	}
}

// 自定义 encoding 由调用方 decoder 解码，本包不改写也不拒绝。
func TestCustomEncodingPreserved(t *testing.T) {
	payload := protocol.Payload{Encoding: "application/x-custom-v2", Data: []byte(`"card-9"`)}
	failure := protocol.Failure{Message: "m", Type: "T", Details: protocol.Payloads{payload}}

	decoder := func(p protocol.Payload, target any) error {
		if p.Encoding != "application/x-custom-v2" {
			return fmt.Errorf("unexpected encoding %q", p.Encoding)
		}
		return json.Unmarshal(p.Data, target)
	}
	decoded := apperror.FromFailure(failure, decoder)
	if got := decoded.RawDetails(); len(got) != 1 || got[0].Encoding != "application/x-custom-v2" {
		t.Fatalf("raw details not preserved: %+v", got)
	}
	var card string
	if err := decoded.Details(&card); err != nil {
		t.Fatalf("Details: %v", err)
	}
	if card != "card-9" {
		t.Fatalf("detail = %q", card)
	}
}

func TestDetailsBounds(t *testing.T) {
	empty := apperror.FromFailure(protocol.Failure{Message: "m"}, apperror.JSONDecoder)
	if err := empty.Details(new(string)); !errors.Is(err, apperror.ErrNoDetails) {
		t.Fatalf("empty details error = %v", err)
	}
	one := apperror.New("m", "T", "only")
	decoded := apperror.FromFailure(encode(t, one), apperror.JSONDecoder)
	if err := decoded.Details(new(string), new(string)); !errors.Is(err, apperror.ErrTooManyArguments) {
		t.Fatalf("too many targets error = %v", err)
	}
}

// 已编码载荷直接复用，不再次调用 encoder。
func TestNewEncodedReusesPayloads(t *testing.T) {
	payload, err := protocol.NewJSONPayload("card-1")
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	encodingErr := errors.New("encoder must not be called")
	failingEncoder := func(any) (protocol.Payload, error) { return protocol.Payload{}, encodingErr }

	original := apperror.NewEncoded("m", "T", protocol.Payloads{payload}, apperror.Options{NonRetryable: true})
	failure, err := apperror.Encode(original, failingEncoder)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(failure.Details) != 1 || string(failure.Details[0].Data) != `"card-1"` || !failure.NonRetryable {
		t.Fatalf("payloads not reused: %+v", failure)
	}
}

func TestEncodeNilError(t *testing.T) {
	if _, err := apperror.Encode(nil, apperror.JSONEncoder); err == nil {
		t.Fatal("expected error for nil error")
	}
}

func TestLocalDetailValuesReadable(t *testing.T) {
	appErr, ok := apperror.New("m", "T", "card-1", 3).(*apperror.Error)
	if !ok {
		t.Fatal("constructor must return *apperror.Error")
	}
	var card string
	var count int
	if err := appErr.Details(&card, &count); err != nil {
		t.Fatalf("Details: %v", err)
	}
	if card != "card-1" || count != 3 {
		t.Fatalf("values: %q %d", card, count)
	}
}

// 超时失败的超时种类在 FromFailure / ToFailure 之间保留，Message 与之分开。
func TestTimeoutTypeRoundTrip(t *testing.T) {
	failure := protocol.Failure{Message: "activity StartToClose timeout", TimeoutType: protocol.TimeoutTypeStartToClose}
	err := apperror.FromFailure(failure, apperror.JSONDecoder)
	if err.TimeoutType() != protocol.TimeoutTypeStartToClose || err.Message() != failure.Message {
		t.Fatalf("decoded timeout = %q message %q", err.TimeoutType(), err.Message())
	}
	encoded, encodeErr := apperror.Encode(err, apperror.JSONEncoder)
	if encodeErr != nil || encoded.TimeoutType != protocol.TimeoutTypeStartToClose {
		t.Fatalf("encoded = %+v, err = %v", encoded, encodeErr)
	}
	if plain := apperror.FromFailure(protocol.Failure{Message: "boom"}, apperror.JSONDecoder); plain.TimeoutType() != "" {
		t.Fatalf("plain failure timeout = %q", plain.TimeoutType())
	}
}
