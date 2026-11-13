package engine

import (
	"errors"
	"github.com/donutnomad/tenuto/protocol"
)

// UpdateRejectedError 是 update validator 拒绝的具名错误：
//   - Error() 与 fmt.Errorf("%w: %w", ErrUpdateRejected, cause) 文本逐字符一致；
//   - 哨兵只有 protocol.ErrUpdateRejected 一个值（engine/sdk 都是它的别名），进程内与经 HTTP
//     的 client 用同一个值判断；
//   - Cause() 暴露 validator 的原始错误，传输层据此按 apperror.Encode 编成共享 Failure。
type UpdateRejectedError struct {
	cause error
}

// newUpdateRejectedError 包装 validator 的拒绝原因。
func newUpdateRejectedError(cause error) error { return &UpdateRejectedError{cause: cause} }

func (e *UpdateRejectedError) Error() string {
	return protocol.ErrUpdateRejected.Error() + ": " + e.cause.Error()
}

// Unwrap 保留 validator 原因，errors.Is/As 可继续命中调用方自己的哨兵与 typed 应用错误。
func (e *UpdateRejectedError) Unwrap() error { return e.cause }

// Is 让 errors.Is 命中唯一的拒绝哨兵。
func (e *UpdateRejectedError) Is(target error) bool { return target == protocol.ErrUpdateRejected }

// Cause 返回 validator 的原始错误（可能是 typed 应用错误）。
func (e *UpdateRejectedError) Cause() error { return e.cause }

// UpdateRejectionCause 从错误链里取出 validator 的拒绝原因，供传输层按既有 apperror.Encode
// 编成共享 Failure 跨 wire 传递。返回 false 表示该错误不是 validator 拒绝。
func UpdateRejectionCause(err error) (error, bool) {
	var rejected *UpdateRejectedError
	if errors.As(err, &rejected) {
		return rejected.Cause(), true
	}
	if errors.Is(err, protocol.ErrUpdateRejected) {
		return err, true
	}
	return nil, false
}
