package protocol

import (
	"errors"
	"fmt"
)

// ErrInvalidRequest 标记由请求本身引起的失败：参数不合法，或请求与目标当前的状态不相容。
// 调用方改正请求后才值得重试。HTTP 层把它映射为 400，并把错误文本原样返回；
// 没有这个标记、也不是已知哨兵的错误属于服务端故障。
var ErrInvalidRequest = errors.New("invalid request")

type invalidRequestError struct{ err error }

func (e *invalidRequestError) Error() string { return e.err.Error() }

func (e *invalidRequestError) Unwrap() error { return e.err }

func (e *invalidRequestError) Is(target error) bool { return target == ErrInvalidRequest }

// InvalidRequestf 的用法与 fmt.Errorf 相同（包括 %w），返回的错误文本不变，并满足 errors.Is(err, ErrInvalidRequest)。
func InvalidRequestf(format string, args ...any) error {
	return &invalidRequestError{err: fmt.Errorf(format, args...)}
}
