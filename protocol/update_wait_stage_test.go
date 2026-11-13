package protocol

import (
	"errors"
	"testing"
)

func TestNormalizeUpdateWaitStage(t *testing.T) {
	for in, want := range map[UpdateWaitStage]UpdateWaitStage{"": "Accepted", "Accepted": "Accepted", "Completed": "Completed"} {
		if got, err := NormalizeUpdateWaitStage(in); err != nil || got != want {
			t.Fatalf("NormalizeUpdateWaitStage(%q) = %q, %v", in, got, err)
		}
	}
	// Error Case：大小写不符或拼错的阶段被拒绝（HTTP 400），不再当作 Completed 长轮询。
	for _, in := range []UpdateWaitStage{"accepted", "Complete", "done"} {
		if _, err := NormalizeUpdateWaitStage(in); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("NormalizeUpdateWaitStage(%q) err = %v, want invalid request", in, err)
		}
	}
}
