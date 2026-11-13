package engine

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func TestJSONPayloadRoundTripsWithoutLosingNumberTokens(t *testing.T) {
	input := json.RawMessage(`{"signed":9223372036854775807,"unsigned":18446744073709551615,"items":[null,true,"ok"]}`)
	payload, err := protocol.NewJSONPayload(input)
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	if payload.Encoding != protocol.PayloadEncodingJSONPlainV1 {
		t.Fatalf("encoding = %q", payload.Encoding)
	}
	if got, want := string(payload.Data), `{"items":[null,true,"ok"],"signed":9223372036854775807,"unsigned":18446744073709551615}`; got != want {
		t.Fatalf("canonical data = %s, want %s", got, want)
	}

	var output struct {
		Signed   int64  `json:"signed"`
		Unsigned uint64 `json:"unsigned"`
		Items    []any  `json:"items"`
	}
	if err := payload.Decode(&output); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if output.Signed != math.MaxInt64 || output.Unsigned != math.MaxUint64 || len(output.Items) != 3 {
		t.Fatalf("decoded payload = %+v", output)
	}
}

func TestJSONPayloadPreservesSupportedShapes(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "null", value: nil, want: "null"},
		{name: "scalar", value: "created", want: `"created"`},
		{name: "array", value: []any{1, true, nil}, want: `[1,true,null]`},
		{name: "object", value: map[string]any{"z": 1, "a": "x"}, want: `{"a":"x","z":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := protocol.NewJSONPayload(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(payload.Data); got != tt.want {
				t.Fatalf("payload = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestUpdateOutcomeRequiresExactlyOneBranch(t *testing.T) {
	payload, err := protocol.NewJSONPayload(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := (protocol.UpdateOutcome{}).Validate(); err == nil {
		t.Fatal("empty outcome must fail validation")
	}
	if err := (protocol.UpdateOutcome{Success: &payload, Failure: &protocol.Failure{Message: "boom"}}).Validate(); err == nil {
		t.Fatal("two-branch outcome must fail validation")
	}
	if err := (protocol.UpdateOutcome{Success: &payload}).Validate(); err != nil {
		t.Fatalf("success outcome: %v", err)
	}
	if err := (protocol.UpdateOutcome{Failure: &protocol.Failure{Message: "boom"}}).Validate(); err != nil {
		t.Fatalf("failure outcome: %v", err)
	}
}
