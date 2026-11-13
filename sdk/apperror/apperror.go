package apperror

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/donutnomad/tenuto/protocol"
)

// Encoder 把单个 detail 值编码成 wire payload。宿主各自提供：原生 Activity 用
// JSONEncoder，解释器用 registry 感知的编码器（typed record 需要走注册表）。
type Encoder func(any) (protocol.Payload, error)

// Decoder 把单个 wire payload 解码到目标值。客户端传入自己的 DataConverter，
// 因此自定义 encoding 仍由既有 converter 层解码，本包不假设 json/plain。
type Decoder func(protocol.Payload, any) error

var (
	// ErrNoDetails reports that an application error carries no detail payloads.
	ErrNoDetails = errors.New("no application error details")
	// ErrDetailTarget reports that a detail decode target is missing or not a non-nil pointer.
	ErrDetailTarget = errors.New("application error detail target must be a non-nil pointer")
	// ErrTooManyArguments reports more decode targets than available detail payloads.
	ErrTooManyArguments = errors.New("too many application error detail targets")
)

// JSONEncoder 是 protocol 的默认 payload 编码（json/plain-v1）。
func JSONEncoder(value any) (protocol.Payload, error) { return protocol.NewJSONPayload(value) }

// JSONDecoder 用 payload 自带的 encoding 解码，仅适用于 json/plain-v1。
func JSONDecoder(payload protocol.Payload, target any) error { return payload.Decode(target) }

// Options 是构造应用错误时的可选属性。
type Options struct {
	NonRetryable bool
	Cause        error
	Details      []any
}

// Error 是 Workflow、Activity、Update 与客户端共用的原生 Go 应用错误。
//
// details 有两态：本地构造时存原始值（encoding 推迟到跨边界），解码既有 Failure 时
// 存原始 payload + 调用方 converter，避免二次编码与 encoding 语义漂移。
type Error struct {
	message      string
	typeName     string
	timeoutType  protocol.TimeoutType
	details      []any
	payloads     protocol.Payloads
	decoder      Decoder
	nonRetryable bool
	cause        error
}

// New 构造可重试的应用错误。
func New(message, errorType string, details ...any) error {
	return NewWithOptions(message, errorType, Options{Details: details})
}

// NewWithCause 构造带 cause 的可重试应用错误。
func NewWithCause(message, errorType string, cause error, details ...any) error {
	return NewWithOptions(message, errorType, Options{Cause: cause, Details: details})
}

// NewNonRetryable 构造不可重试的应用错误。
func NewNonRetryable(message, errorType string, cause error, details ...any) error {
	return NewWithOptions(message, errorType, Options{NonRetryable: true, Cause: cause, Details: details})
}

// NewWithOptions 是唯一构造入口。
func NewWithOptions(message, errorType string, options Options) error {
	return &Error{
		message:      message,
		typeName:     errorType,
		details:      append([]any(nil), options.Details...),
		nonRetryable: options.NonRetryable,
		cause:        options.Cause,
	}
}

// NewEncoded 用已编码载荷构造应用错误。解释器宿主把 workflow 侧的 detail 值编码后
// 走这条入口，避免同一份 details 被编码两次。
func NewEncoded(message, errorType string, payloads protocol.Payloads, options Options) error {
	return &Error{
		message:      message,
		typeName:     errorType,
		payloads:     append(protocol.Payloads(nil), payloads...),
		nonRetryable: options.NonRetryable,
		cause:        options.Cause,
	}
}

// Error 返回错误文案。
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

// Unwrap 暴露 cause，支持 errors.Is/errors.As 沿链检查。
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// Type 返回错误的业务类型；空串表示 untyped（普通 Go error 或旧数据）。
func (e *Error) Type() string {
	if e == nil {
		return ""
	}
	return e.typeName
}

// TimeoutType 返回超时种类；空串表示这不是超时错误。Activity 超时后重试耗尽、或 ScheduleToClose、
// ScheduleToStart 到期时，Workflow 拿到的错误带超时种类，Message 是给人读的说明。
func (e *Error) TimeoutType() protocol.TimeoutType {
	if e == nil {
		return ""
	}
	return e.timeoutType
}

// Message 返回错误文案。
func (e *Error) Message() string { return e.Error() }

// NonRetryable 报告该错误是否显式声明为不可重试。
func (e *Error) NonRetryable() bool { return e != nil && e.nonRetryable }

// HasDetails 报告是否带 detail 载荷。
func (e *Error) HasDetails() bool {
	if e == nil {
		return false
	}
	return len(e.payloads) > 0 || len(e.details) > 0
}

// RawDetails 返回原始 payload；自定义 encoding 由调用方 converter 解码。
func (e *Error) RawDetails() protocol.Payloads {
	if e == nil {
		return nil
	}
	return append(protocol.Payloads(nil), e.payloads...)
}

// Details 把 detail 载荷逐个解码到 target。解码器优先级：FromFailure 注入的
// converter → 本地构造值的 JSON 往返。没有 detail 时返回 ErrNoDetails。
func (e *Error) Details(targets ...any) error {
	if e == nil || !e.HasDetails() {
		return ErrNoDetails
	}
	count := len(e.payloads)
	if count == 0 {
		count = len(e.details)
	}
	if len(targets) > count {
		return fmt.Errorf("%w: have %d details, got %d targets", ErrTooManyArguments, count, len(targets))
	}
	for i, target := range targets {
		if err := e.decodeDetail(i, target); err != nil {
			return fmt.Errorf("decode application error detail %d: %w", i, err)
		}
	}
	return nil
}

func (e *Error) decodeDetail(index int, target any) error {
	if index < len(e.payloads) {
		if e.decoder != nil {
			return e.decoder(e.payloads[index], target)
		}
		return JSONDecoder(e.payloads[index], target)
	}
	raw, err := json.Marshal(e.details[index])
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

// ToFailure 编码成共享 Failure。已带 payload 的错误原样复用载荷，不二次编码。
func (e *Error) ToFailure(encode Encoder) (protocol.Failure, error) {
	if encode == nil {
		encode = JSONEncoder
	}
	failure := protocol.Failure{
		Message:      e.message,
		Type:         e.typeName,
		TimeoutType:  e.timeoutType,
		NonRetryable: e.nonRetryable,
	}
	if len(e.payloads) > 0 {
		failure.Details = append(protocol.Payloads(nil), e.payloads...)
	} else if len(e.details) > 0 {
		payloads := make(protocol.Payloads, len(e.details))
		for i, detail := range e.details {
			payload, err := encode(detail)
			if err != nil {
				return protocol.Failure{}, fmt.Errorf("encode application error detail %d: %w", i, err)
			}
			payloads[i] = payload
		}
		failure.Details = payloads
	}
	if e.cause != nil {
		cause, err := Encode(e.cause, encode)
		if err != nil {
			return protocol.Failure{}, err
		}
		failure.Cause = &cause
	}
	return failure, nil
}

// FromFailure 把 wire Failure 还原成 Go 错误。payload 保持原样，details 的最终解码
// 交给传入的 converter，因此自定义 encoding 不会被本包改写或拒绝。
func FromFailure(failure protocol.Failure, decoder Decoder) *Error {
	e := &Error{
		message:      failure.Message,
		typeName:     failure.Type,
		timeoutType:  failure.TimeoutType,
		payloads:     append(protocol.Payloads(nil), failure.Details...),
		decoder:      decoder,
		nonRetryable: failure.NonRetryable,
	}
	if failure.Cause != nil {
		e.cause = FromFailure(*failure.Cause, decoder)
	}
	return e
}

// Encode 把任意 Go error 映射成共享 Failure。只读顶层错误，与 Temporal 的
// 顶层 error → ApplicationFailureInfo 语义一致：
//
//	*apperror.Error            → 其字段（cause 递归）
//	errors.New / 普通 error     → message + 反射类型名（errors.errorString 记为空 type）
//	fmt.Errorf("%w") 包装       → Type="wrapError"，内层错误进 cause，不提升内层 flag/type
func Encode(err error, encode Encoder) (protocol.Failure, error) {
	if err == nil {
		return protocol.Failure{}, errors.New("cannot encode nil error")
	}
	if appErr, ok := err.(*Error); ok {
		return appErr.ToFailure(encode)
	}
	failure := protocol.Failure{Message: err.Error(), Type: goErrorType(err)}
	if inner := errors.Unwrap(err); inner != nil {
		cause, causeErr := Encode(inner, encode)
		if causeErr != nil {
			return protocol.Failure{}, causeErr
		}
		failure.Cause = &cause
	}
	return failure, nil
}

// goErrorType 返回错误的 Go 类型名，作为 Failure.Type：errors.errorString 记为空串，
// 指针去掉星号，匿名类型退回完整类型串。
func goErrorType(err error) string {
	typ := reflect.TypeOf(err)
	if typ == nil {
		return ""
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.PkgPath() == "errors" && typ.Name() == "errorString" {
		return ""
	}
	if name := typ.Name(); name != "" {
		return name
	}
	return typ.String()
}

// CanceledType 是取消错误的 Type。取消错误与应用错误共用 Error 与 Failure 的形态，
// 只以这个保留的 Type 区分，对标 Temporal 的 CanceledError。
const CanceledType = protocol.CanceledFailureType

// NewCanceled 构造取消错误：不可重试，details 随取消一起保存。
func NewCanceled(details ...any) error {
	return NewWithOptions("canceled", CanceledType, Options{NonRetryable: true, Details: details})
}

// IsCanceled 报告 err 或它的 cause 链上是否有取消错误。
func IsCanceled(err error) bool {
	return CanceledIn(err) != nil
}

// CanceledIn 返回 err 或它的 cause 链上的第一个取消错误；没有时返回 nil。
func CanceledIn(err error) *Error {
	for err != nil {
		var appErr *Error
		if !errors.As(err, &appErr) {
			return nil
		}
		if appErr.Type() == CanceledType {
			return appErr
		}
		err = appErr.Unwrap()
	}
	return nil
}
