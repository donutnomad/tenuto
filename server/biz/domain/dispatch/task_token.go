package dispatch

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
	"github.com/donutnomad/tenuto/protocol"
)

// taskTokenPrefix marks a runtime task token.
const taskTokenPrefix = "wftask_"

// MinimumTaskTokenSecretLength is the startup requirement for production token codecs.
const MinimumTaskTokenSecretLength = 32

// TaskTokenCodec signs and verifies runtime task tokens with one immutable secret.
// The application builds one codec from its configuration and injects it into
// the engine and the HTTP boundary.
type TaskTokenCodec struct {
	secret []byte
}

func NewTaskTokenCodec(secret []byte) (*TaskTokenCodec, error) {
	if len(secret) < MinimumTaskTokenSecretLength {
		return nil, fmt.Errorf("task token secret must contain at least %d bytes", MinimumTaskTokenSecretLength)
	}
	return &TaskTokenCodec{secret: append([]byte(nil), secret...)}, nil
}

// NewRandomTaskTokenCodec 用一次性随机密钥构造 codec，只适用于单进程：其他进程无法校验它签发的 token。
func NewRandomTaskTokenCodec() *TaskTokenCodec {
	// crypto/rand.Read 不返回错误：随机源不可用时它直接终止进程。
	secret := make([]byte, MinimumTaskTokenSecretLength)
	_, _ = rand.Read(secret)
	return &TaskTokenCodec{secret: secret}
}

type taskTokenPayload struct {
	Task           protocol.Task `json:"task"`
	LeaseTokenHash string        `json:"lease_token_hash,omitempty"`
	LeaseSignature string        `json:"lease_signature,omitempty"`
	Signature      string        `json:"signature"`
}

func (c *TaskTokenCodec) Encode(task protocol.Task) (protocol.TaskToken, error) {
	return c.encode(task, "")
}

func (c *TaskTokenCodec) EncodeLease(task protocol.Task, leaseToken string) (protocol.TaskToken, error) {
	return c.encode(task, taskLeaseTokenHash(leaseToken))
}

func (c *TaskTokenCodec) encode(task protocol.Task, leaseTokenHash string) (protocol.TaskToken, error) {
	if task.Type == "" {
		return "", fmt.Errorf("task token type is empty")
	}
	if task.RunID == "" {
		return "", fmt.Errorf("task token run id is empty")
	}
	if task.ScheduledEventID == 0 {
		return "", fmt.Errorf("task token scheduled event id is empty")
	}
	task = normalizeTaskForToken(task)
	signature, err := c.signTask(task)
	if err != nil {
		return "", err
	}
	var leaseSignature string
	if leaseTokenHash != "" {
		leaseSignature, err = c.signLease(task, leaseTokenHash)
		if err != nil {
			return "", err
		}
	}
	data, err := json.Marshal(taskTokenPayload{
		Task:           task,
		LeaseTokenHash: leaseTokenHash,
		LeaseSignature: leaseSignature,
		Signature:      signature,
	})
	if err != nil {
		return "", err
	}
	return protocol.TaskToken(taskTokenPrefix + base64.RawURLEncoding.EncodeToString(data)), nil
}

func (c *TaskTokenCodec) Decode(token protocol.TaskToken) (protocol.Task, error) {
	payload, err := c.decode(token)
	if err != nil {
		return protocol.Task{}, err
	}
	return payload.Task, nil
}

func (c *TaskTokenCodec) DecodeLease(token protocol.TaskToken, leaseToken string) (protocol.Task, error) {
	payload, err := c.decode(token)
	if err != nil {
		return protocol.Task{}, err
	}
	if payload.LeaseTokenHash == "" {
		return protocol.Task{}, protocol.InvalidRequestf("task token is not bound to a queue lease")
	}
	if payload.LeaseSignature == "" {
		return protocol.Task{}, protocol.InvalidRequestf("task token queue lease signature is empty")
	}
	actual := taskLeaseTokenHash(leaseToken)
	if !hmac.Equal([]byte(payload.LeaseTokenHash), []byte(actual)) {
		return protocol.Task{}, protocol.InvalidRequestf("task token queue lease is invalid")
	}
	expected, err := c.signLease(payload.Task, payload.LeaseTokenHash)
	if err != nil {
		return protocol.Task{}, err
	}
	if !hmac.Equal([]byte(payload.LeaseSignature), []byte(expected)) {
		return protocol.Task{}, protocol.InvalidRequestf("task token queue lease signature is invalid")
	}
	return payload.Task, nil
}

func (c *TaskTokenCodec) decode(token protocol.TaskToken) (taskTokenPayload, error) {
	encoded, ok := strings.CutPrefix(string(token), taskTokenPrefix)
	if !ok {
		return taskTokenPayload{}, protocol.InvalidRequestf("task token prefix is invalid")
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return taskTokenPayload{}, protocol.InvalidRequestf("decode task token: %w", err)
	}
	var payload taskTokenPayload
	if err := jsonvalue.Unmarshal(data, &payload); err != nil {
		return taskTokenPayload{}, protocol.InvalidRequestf("unmarshal task token: %w", err)
	}
	if payload.Task.RunID == "" || payload.Task.ScheduledEventID == 0 {
		return taskTokenPayload{}, protocol.InvalidRequestf("task token payload is incomplete")
	}
	if payload.Signature == "" {
		return taskTokenPayload{}, protocol.InvalidRequestf("task token signature is empty")
	}
	expected, err := c.signTask(payload.Task)
	if err != nil {
		return taskTokenPayload{}, err
	}
	if !hmac.Equal([]byte(payload.Signature), []byte(expected)) {
		return taskTokenPayload{}, protocol.InvalidRequestf("task token signature is invalid")
	}
	return payload, nil
}

// Attach signs task and returns it carrying the token.
func (c *TaskTokenCodec) Attach(task protocol.Task) (protocol.Task, error) {
	token, err := c.Encode(task)
	if err != nil {
		return protocol.Task{}, err
	}
	task.TaskToken = token
	return task, nil
}

// normalizeTaskForToken 留下 token 签名的寻址与 fence 字段：载荷（Input、LastHeartbeatDetails）与 TraceContext
// 不进 token，token 的大小与载荷无关。Worker 从投递的任务读载荷，Server 从 mutable state 读。
func normalizeTaskForToken(task protocol.Task) protocol.Task {
	task.TaskToken = ""
	task.Input = nil
	task.LastHeartbeatDetails = nil
	task.TraceContext = nil
	task.Namespace = protocol.NormalizeNamespace(task.Namespace)
	if task.Type == protocol.TaskTypeActivity {
		task.Attempt = max(task.Attempt, 1)
	}
	return task
}

func (c *TaskTokenCodec) signTask(task protocol.Task) (string, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("marshal task token body: %w", err)
	}
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (c *TaskTokenCodec) signLease(task protocol.Task, leaseTokenHash string) (string, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return "", fmt.Errorf("marshal task token lease body: %w", err)
	}
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write([]byte("queue-lease:v1\x00"))
	_, _ = mac.Write(data)
	_, _ = mac.Write([]byte("\x00"))
	_, _ = mac.Write([]byte(leaseTokenHash))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func taskLeaseTokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}
