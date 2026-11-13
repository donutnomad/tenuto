package protocol

import (
	"time"
)

// RetryPolicy 是 WorkflowConfig JSON 里的重试策略：时长写成 Go duration 字符串（如 "1s"）。
// 未设置的字段取 Server 的缺省值。
type RetryPolicy struct {
	MaxAttempts        int      `json:"max_attempts"`                   // 0 = 无限重试
	InitialInterval    string   `json:"initial_interval,omitempty"`     // 缺省 1s
	BackoffCoefficient float64  `json:"backoff_coefficient,omitempty"`  // 缺省 2.0
	MaxInterval        string   `json:"max_interval,omitempty"`         // 缺省 100 × InitialInterval
	NonRetryableErrors []string `json:"non_retryable_errors,omitempty"` // message 子串匹配，命中则跳过重试
	// NonRetryableErrorTypes 按错误 Type 完全相等匹配（大小写敏感）。
	NonRetryableErrorTypes []string `json:"non_retryable_error_types,omitempty"`
}

// ParsedRetryPolicy 是 Activity 调度命令携带的重试策略。零值字段表示未设置，由 Server 补缺省值。
type ParsedRetryPolicy struct {
	MaxAttempts        int
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaxInterval        time.Duration
	NonRetryableErrors []string // message 子串匹配，命中则跳过重试
	// NonRetryableErrorTypes 是错误类型完全相等匹配（大小写敏感）。与 NonRetryableErrors
	// 是两条独立策略，任一命中即停止重试；不做「先 type 后 message」的自动降级。
	NonRetryableErrorTypes []string
}

// ParseRetryPolicy 把 RetryPolicy 的字符串时长解析成 ParsedRetryPolicy；未设置的字段保持零值。
func ParseRetryPolicy(rp *RetryPolicy) (*ParsedRetryPolicy, error) {
	p := &ParsedRetryPolicy{
		MaxAttempts:            rp.MaxAttempts,
		BackoffCoefficient:     rp.BackoffCoefficient,
		NonRetryableErrors:     rp.NonRetryableErrors,
		NonRetryableErrorTypes: rp.NonRetryableErrorTypes,
	}
	var err error
	if rp.InitialInterval != "" {
		p.InitialInterval, err = time.ParseDuration(rp.InitialInterval)
		if err != nil {
			return nil, InvalidRequestf("parse initial_interval: %w", err)
		}
	}
	if rp.MaxInterval != "" {
		p.MaxInterval, err = time.ParseDuration(rp.MaxInterval)
		if err != nil {
			return nil, InvalidRequestf("parse max_interval: %w", err)
		}
	}
	return p, nil
}
