package coroutine

import (
	"testing"
)

// TestScheduler_DeterministicOrder 验证协程按 Spawn 顺序确定性执行
func TestScheduler_DeterministicOrder(t *testing.T) {
	s := NewScheduler()

	var order []string

	s.Spawn("a", func(yield func()) {
		order = append(order, "a-1")
		yield()
		order = append(order, "a-2")
	})

	s.Spawn("b", func(yield func()) {
		order = append(order, "b-1")
		yield()
		order = append(order, "b-2")
	})

	s.RunBatch()
	s.Reclaim()

	// 协作式调度：a-1 → a yield → b-1 → b yield → a-2 → b-2
	expected := []string{"a-1", "b-1", "a-2", "b-2"}
	if len(order) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, order)
	}
	for i := range expected {
		if order[i] != expected[i] {
			t.Fatalf("at index %d: expected %q, got %q\nfull: %v", i, expected[i], order[i], order)
		}
	}
}

// TestScheduler_SingleCoroutine 单个协程正常执行完毕
func TestScheduler_SingleCoroutine(t *testing.T) {
	s := NewScheduler()

	var result string
	s.Spawn("only", func(yield func()) {
		result = "done"
	})

	s.RunBatch()
	s.Reclaim()

	if result != "done" {
		t.Fatalf("expected %q, got %q", "done", result)
	}
}

// TestScheduler_MultipleYields 多次 yield 的确定性：每次醒来取得进展（Unblocked），调度器继续轮转。
func TestScheduler_MultipleYields(t *testing.T) {
	s := NewScheduler()

	var order []int

	s.Spawn("a", func(yield func()) {
		order = append(order, 1)
		yield()
		s.Unblocked()
		order = append(order, 3)
		yield()
		s.Unblocked()
		order = append(order, 5)
	})

	s.Spawn("b", func(yield func()) {
		order = append(order, 2)
		yield()
		s.Unblocked()
		order = append(order, 4)
		yield()
		s.Unblocked()
		order = append(order, 6)
	})

	s.RunBatch()
	s.Reclaim()

	expected := []int{1, 2, 3, 4, 5, 6}
	if len(order) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, order)
	}
	for i := range expected {
		if order[i] != expected[i] {
			t.Fatalf("at index %d: expected %d, got %d", i, expected[i], order[i])
		}
	}
}

// TestScheduler_ReplayDeterminism 多次运行结果完全一致（确定性验证）
func TestScheduler_ReplayDeterminism(t *testing.T) {
	run := func() []string {
		s := NewScheduler()
		var order []string

		s.Spawn("x", func(yield func()) {
			order = append(order, "x1")
			yield()
			order = append(order, "x2")
		})
		s.Spawn("y", func(yield func()) {
			order = append(order, "y1")
			yield()
			order = append(order, "y2")
		})
		s.Spawn("z", func(yield func()) {
			order = append(order, "z1")
			yield()
			order = append(order, "z2")
		})

		s.RunBatch()
		s.Reclaim()
		return order
	}

	first := run()
	for i := 0; i < 100; i++ {
		got := run()
		if len(got) != len(first) {
			t.Fatalf("run %d: length mismatch: %v vs %v", i, first, got)
		}
		for j := range first {
			if got[j] != first[j] {
				t.Fatalf("run %d: at %d: expected %q, got %q", i, j, first[j], got[j])
			}
		}
	}
}
