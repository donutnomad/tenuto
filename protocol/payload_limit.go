package protocol

import (
	"encoding/json"
	"fmt"
)

// DefaultPayloadSizeLimit 是单个载荷（Workflow 输入与结果、Signal 输入、Update 参数、Activity 输入、输出与心跳
// 详情、子 Workflow 与 Continue-As-New 的输入、Marker 数据）JSON 编码后的缺省字节上限，与 Temporal limit.blobSize.error 的缺省值相同。
const DefaultPayloadSizeLimit = 2 * 1024 * 1024

// MaxPayloadSizeLimit 是可配置的载荷上限的最大值：一个载荷加上 task token 与其余字段要放得进 4MB 的
// runtime 请求体。
const MaxPayloadSizeLimit = 3 * 1024 * 1024

// FailureTypePayloadSizeLimitExceeded 是载荷超过上限时 Activity 不可重试失败的类型。
const FailureTypePayloadSizeLimitExceeded = "PayloadSizeLimitExceeded"

// FailureTypeActivityResultRejected 是 Server 以不可重试的 4xx 拒绝 Activity 结果后，Worker 改报的失败类型。
const FailureTypeActivityResultRejected = "ActivityResultRejected"

// PayloadSize 返回 payload 的 JSON 编码字节数；编码失败时返回 0，由后续的编码步骤报告错误。
func PayloadSize(payload any) int {
	if payload == nil {
		return 0
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return 0
	}
	return len(data)
}

// CheckPayloadSize 在 payload 超过 limit 字节时返回错误；limit <= 0 表示不检查。what 写进错误信息，例如
// "activity result"。
func CheckPayloadSize(what string, payload any, limit int) error {
	if limit <= 0 {
		return nil
	}
	return CheckPayloadBytes(what, PayloadSize(payload), limit)
}

// CheckPayloadBytes 在 size 超过 limit 字节时返回错误；limit <= 0 表示不检查。
func CheckPayloadBytes(what string, size, limit int) error {
	if limit > 0 && size > limit {
		return fmt.Errorf("%s is %d bytes, exceeds the payload size limit of %d bytes", what, size, limit)
	}
	return nil
}

// PayloadSizeLimitFailure 是载荷超限的 Activity 失败：不可重试，同样的输入重跑只会再次超限。
func PayloadSizeLimitFailure(err error) Failure {
	return Failure{Message: err.Error(), Type: FailureTypePayloadSizeLimitExceeded, NonRetryable: true}
}
