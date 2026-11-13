package protocol_test

import (
	"encoding/json"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func mustPayload(t *testing.T, value any) protocol.Payload {
	t.Helper()
	payload, err := protocol.NewJSONPayload(value)
	if err != nil {
		t.Fatalf("NewJSONPayload(%v): %v", value, err)
	}
	return payload
}

// 结构化字段（含 cause）走同一次 JSON 往返后逐字段保持。
func TestFailureJSONRoundTrip(t *testing.T) {
	original := protocol.Failure{
		Message:      "card has expired",
		Type:         "CardExpired",
		NonRetryable: true,
		Details:      protocol.Payloads{mustPayload(t, "card-1"), mustPayload(t, 42)},
		Cause: &protocol.Failure{
			Message: "inner boom",
			Type:    "InnerType",
		},
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded protocol.Failure
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Message != original.Message || decoded.Type != original.Type || !decoded.NonRetryable {
		t.Fatalf("scalar fields lost: %+v", decoded)
	}
	if len(decoded.Details) != 2 || decoded.Cause == nil || decoded.Cause.Type != "InnerType" {
		t.Fatalf("structured fields lost: %+v", decoded)
	}
	var card string
	if err := decoded.Details[0].Decode(&card); err != nil || card != "card-1" {
		t.Fatalf("detail decode: %q %v", card, err)
	}
}

func TestFailureNormalizeCanonicalizesDetailsAndCause(t *testing.T) {
	failure := protocol.Failure{
		Message: "outer",
		Details: protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{ "b" : 1, "a" : 2 }`)}},
		Cause: &protocol.Failure{
			Message: "inner",
			Details: protocol.Payloads{{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`[ 1 , 2 ]`)}},
		},
	}
	normalized, err := failure.Normalize()
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got := string(normalized.Details[0].Data); got != `{"a":2,"b":1}` {
		t.Fatalf("outer detail not canonical: %s", got)
	}
	if got := string(normalized.Cause.Details[0].Data); got != `[1,2]` {
		t.Fatalf("cause detail not canonical: %s", got)
	}
}

func TestFailureValidateRejectsUnsupportedEncoding(t *testing.T) {
	failure := protocol.Failure{
		Details: protocol.Payloads{{Encoding: "protobuf/v1", Data: []byte{0x01}}},
	}
	if err := failure.Validate(); err == nil {
		t.Fatal("expected unsupported encoding error")
	}
}

func TestUpdateOutcomeValidateChecksCause(t *testing.T) {
	outcome := protocol.UpdateOutcome{Failure: &protocol.Failure{
		Message: "outer",
		Cause:   &protocol.Failure{Details: protocol.Payloads{{Encoding: "protobuf/v1", Data: []byte{0x01}}}},
	}}
	if err := outcome.Validate(); err == nil {
		t.Fatal("expected nested cause validation error")
	}
}

func TestNormalizeUpdateOutcomeKeepsFailureShape(t *testing.T) {
	outcome := protocol.UpdateOutcome{Failure: &protocol.Failure{
		Message:      "card has expired",
		Type:         "CardExpired",
		NonRetryable: true,
		Details:      protocol.Payloads{mustPayload(t, "card-1")},
		Cause:        &protocol.Failure{Message: "inner", Type: "InnerType"},
	}}
	normalized, err := protocol.NormalizeUpdateOutcome(outcome)
	if err != nil {
		t.Fatalf("NormalizeUpdateOutcome: %v", err)
	}
	if normalized.Failure == nil || !normalized.Failure.NonRetryable || normalized.Failure.Type != "CardExpired" {
		t.Fatalf("failure shape lost: %+v", normalized.Failure)
	}
	if normalized.Failure.Cause == nil || normalized.Failure.Cause.Type != "InnerType" {
		t.Fatalf("cause lost: %+v", normalized.Failure)
	}
	if len(normalized.Failure.Details) != 1 {
		t.Fatalf("details lost: %+v", normalized.Failure.Details)
	}
}
