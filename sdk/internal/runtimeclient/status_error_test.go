package runtimeclient

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

// 413 既是不可重试的请求错误，也是载荷超限：Workflow Task 的提交据此记为 payload_size_limit_exceeded 失败。
func TestStatusErrorClassifiesRequestEntityTooLarge(t *testing.T) {
	tooLarge := fmt.Errorf("complete: %w", &StatusError{Code: http.StatusRequestEntityTooLarge})
	if !errors.Is(tooLarge, protocol.ErrPayloadTooLarge) || !errors.Is(tooLarge, protocol.ErrInvalidRequest) {
		t.Fatalf("413 = %v, want both ErrPayloadTooLarge and ErrInvalidRequest", tooLarge)
	}
	badRequest := &StatusError{Code: http.StatusBadRequest}
	if errors.Is(badRequest, protocol.ErrPayloadTooLarge) || !errors.Is(badRequest, protocol.ErrInvalidRequest) {
		t.Fatalf("400 = %v, want only ErrInvalidRequest", badRequest)
	}
}
