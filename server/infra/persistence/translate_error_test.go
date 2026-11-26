package persistence

import (
	"strings"
	"testing"

	"gorm.io/gorm"
)

// Error Case: 没有开启 TranslateError 的句柄被拒绝；仓储依赖 gorm.ErrDuplicatedKey 识别唯一键冲突。
func TestNewRejectsHandleWithoutTranslateError(t *testing.T) {
	handle := newUnconnectedDB(t, &gorm.Config{})
	if _, err := New(handle, Config{}); err == nil || !strings.Contains(err.Error(), "TranslateError") {
		t.Fatalf("New on a handle without TranslateError: err=%v", err)
	}
	translated := newUnconnectedDB(t, &gorm.Config{TranslateError: true})
	if _, err := New(translated, Config{}); err != nil && strings.Contains(err.Error(), "TranslateError") {
		t.Fatalf("New on a translated handle: %v", err)
	}
}
