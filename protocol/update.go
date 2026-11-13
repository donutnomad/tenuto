package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const PayloadEncodingJSONPlainV1 = "json/plain-v1"

// ErrUpdateRejected 是 Update 被无状态 input validator 拒绝的哨兵。它是服务端与 SDK 共享的
// 同一个错误值：进程内调用与经 HTTP 传输的 client 都能 errors.Is 命中，拒绝原因回到 error 文本。
var ErrUpdateRejected = errors.New("workflow update rejected")

const maxUpdatePayloadBytes = 4 * 1024 * 1024
const maxUpdatePayloadCount = 1024

type Payload struct {
	Encoding string `json:"encoding"`
	Data     []byte `json:"data"`
}

type Payloads []Payload

// UpdateOutcome 是 Update 完成的唯一结果表示：Failure 非 nil 是失败，Success 非 nil 是成功，
// 两者都不设置表示这个 outcome 无效。失败载荷与 Activity/Workflow 失败共用同一个 protocol.Failure。
type UpdateOutcome struct {
	Success *Payload `json:"success,omitempty"`
	Failure *Failure `json:"failure,omitempty"`
}

// UpdateFailureTypeWorkflowClosed 是 Run 在 Update 完成之前关闭时 Update 的失败类型，与 Temporal 的
// AcceptedUpdateCompletedWorkflow 同名。
const UpdateFailureTypeWorkflowClosed = "AcceptedUpdateCompletedWorkflow"

// WorkflowClosedUpdateOutcome 是已接受、未完成的 Update 在 Run 关闭后的结果：不可重试的失败。
func WorkflowClosedUpdateOutcome() *UpdateOutcome {
	return &UpdateOutcome{Failure: &Failure{
		Message:      "workflow update failed because the workflow closed before the update completed",
		Type:         UpdateFailureTypeWorkflowClosed,
		NonRetryable: true,
	}}
}

type UpdateWaitStage string

var UpdateWaitStageEnums = struct {
	Accepted  UpdateWaitStage
	Completed UpdateWaitStage
}{Accepted: "Accepted", Completed: "Completed"}

// NormalizeUpdateWaitStage 校验调用方要等待的阶段：空值表示 Accepted，其他取值只能是 Accepted 或 Completed。
func NormalizeUpdateWaitStage(stage UpdateWaitStage) (UpdateWaitStage, error) {
	switch stage {
	case "":
		return UpdateWaitStageEnums.Accepted, nil
	case UpdateWaitStageEnums.Accepted, UpdateWaitStageEnums.Completed:
		return stage, nil
	}
	return "", InvalidRequestf("unknown update wait stage %q: use %q or %q", stage, UpdateWaitStageEnums.Accepted, UpdateWaitStageEnums.Completed)
}

type UpdateReachedStage string

var UpdateReachedStageEnums = struct {
	Accepted  UpdateReachedStage
	Completed UpdateReachedStage
}{Accepted: "Accepted", Completed: "Completed"}

// WorkflowUpdateResult 是 Update 的对外结果。Outcome 是结果的唯一载体：
// Failure 非 nil 即失败，Success 非 nil 即成功。
type WorkflowUpdateResult struct {
	UpdateID string `json:"update_id"`
	// RunID 是接受这次 Update 的 Run；按 Workflow ID 发出的 Update 之后都按它定位。
	RunID           string             `json:"run_id,omitempty"`
	AcceptedEventID int64              `json:"accepted_event_id"`
	Stage           UpdateReachedStage `json:"stage,omitempty"`
	Outcome         *UpdateOutcome     `json:"outcome,omitempty"`
}

func NewJSONPayload(value any) (Payload, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return Payload{}, fmt.Errorf("encode JSON payload: %w", err)
	}
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return Payload{}, fmt.Errorf("canonicalize JSON payload: %w", err)
	}
	if len(canonical) > maxUpdatePayloadBytes {
		return Payload{}, InvalidRequestf("encode JSON payload: payload exceeds %d bytes", maxUpdatePayloadBytes)
	}
	return Payload{Encoding: PayloadEncodingJSONPlainV1, Data: canonical}, nil
}

func (p Payload) Decode(target any) error {
	if p.Encoding != PayloadEncodingJSONPlainV1 {
		return InvalidRequestf("unsupported payload encoding %q", p.Encoding)
	}
	if len(p.Data) == 0 {
		return InvalidRequestf("payload data is empty")
	}
	if err := json.Unmarshal(p.Data, target); err != nil {
		return InvalidRequestf("decode JSON payload: %w", err)
	}
	return nil
}

func (p Payload) Validate() error {
	_, err := CanonicalPayload(p)
	return err
}

func (o UpdateOutcome) Validate() error {
	if (o.Success == nil) == (o.Failure == nil) {
		return InvalidRequestf("update outcome must contain exactly one of success or failure")
	}
	if o.Success != nil {
		if _, err := CanonicalPayload(*o.Success); err != nil {
			return InvalidRequestf("invalid success payload: %w", err)
		}
	}
	if o.Failure != nil {
		if err := o.Failure.Validate(); err != nil {
			return InvalidRequestf("invalid failure: %w", err)
		}
	}
	return nil
}

func NormalizePayloads(payloads Payloads) (Payloads, error) {
	if payloads == nil {
		return nil, nil
	}
	if len(payloads) > maxUpdatePayloadCount {
		return nil, InvalidRequestf("payload count exceeds %d", maxUpdatePayloadCount)
	}
	normalized := make(Payloads, len(payloads))
	totalBytes := 0
	for i, payload := range payloads {
		canonical, err := CanonicalPayload(payload)
		if err != nil {
			return nil, InvalidRequestf("payload %d: %w", i, err)
		}
		normalized[i] = canonical
		totalBytes += len(canonical.Data)
		if totalBytes > maxUpdatePayloadBytes {
			return nil, InvalidRequestf("payload data exceeds %d bytes", maxUpdatePayloadBytes)
		}
	}
	return normalized, nil
}

func NormalizeUpdateOutcome(outcome UpdateOutcome) (UpdateOutcome, error) {
	if err := outcome.Validate(); err != nil {
		return UpdateOutcome{}, err
	}
	if outcome.Success != nil {
		payload, err := CanonicalPayload(*outcome.Success)
		if err != nil {
			return UpdateOutcome{}, err
		}
		return UpdateOutcome{Success: &payload}, nil
	}
	failure, err := outcome.Failure.Normalize()
	if err != nil {
		return UpdateOutcome{}, err
	}
	return UpdateOutcome{Failure: &failure}, nil
}

func CanonicalPayload(p Payload) (Payload, error) {
	if p.Encoding != PayloadEncodingJSONPlainV1 {
		return Payload{}, InvalidRequestf("unsupported payload encoding %q", p.Encoding)
	}
	if len(p.Data) > maxUpdatePayloadBytes {
		return Payload{}, InvalidRequestf("payload exceeds %d bytes", maxUpdatePayloadBytes)
	}
	canonical, err := canonicalJSON(p.Data)
	if err != nil {
		return Payload{}, err
	}
	return Payload{Encoding: p.Encoding, Data: canonical}, nil
}

func canonicalJSON(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, InvalidRequestf("multiple JSON values")
		}
		return nil, err
	}
	return json.Marshal(value)
}
