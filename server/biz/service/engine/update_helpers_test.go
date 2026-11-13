package engine

import (
	"encoding/json"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

// successOutput 解码 outcome 的成功载荷（测试只关心投影值）。
func successOutput(outcome *protocol.UpdateOutcome) map[string]any {
	if outcome == nil || outcome.Success == nil {
		return nil
	}
	var output map[string]any
	if err := json.Unmarshal(outcome.Success.Data, &output); err != nil {
		return nil
	}
	return output
}

// mustPayload 编码测试用成功载荷。
func mustPayload(t *testing.T, value any) *protocol.Payload {
	t.Helper()
	payload, err := protocol.NewJSONPayload(value)
	if err != nil {
		t.Fatalf("NewJSONPayload: %v", err)
	}
	return &payload
}

// updateArgs 把测试值逐个编码成 Update 的位置参数。
func updateArgs(values ...any) protocol.Payloads {
	args := make(protocol.Payloads, 0, len(values))
	for _, value := range values {
		payload, err := protocol.NewJSONPayload(value)
		if err != nil {
			panic(err)
		}
		args = append(args, payload)
	}
	return args
}

// firstUpdateArg 把首个位置参数解码成 map，供测试 validator 检查字段。
func firstUpdateArg(args protocol.Payloads) map[string]any {
	if len(args) == 0 {
		return nil
	}
	var value map[string]any
	if err := json.Unmarshal(args[0].Data, &value); err != nil {
		return nil
	}
	return value
}
