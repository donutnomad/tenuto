package interpreter

import (
	"sync"
	"testing"

	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================
// parsedProgram：解析后的 WLang AST 按内容缓存并跨 Run 共享
// ============================================
// 测试策略:
// - Happy Path: 相同 program 返回同一个 AST，不同 program 各自解析
// - Integration: 多个 executor 并发执行同一个共享 AST（含 defer 与闭包），-race 下无数据竞争
// ============================================

func TestParsedProgram(t *testing.T) {
	deferNode := compileWLangSource(t, []byte(wlangFunctionDeferDirectSrc), "parsed-program-defer", "W", "w.go")
	otherNode := compileWLangSource(t, []byte(executionSessionSequentialActivities), "parsed-program-other", "W", "w.go")

	t.Run("Happy Path: identical programs share one parsed AST", func(t *testing.T) {
		first, err := parsedProgram(deferNode.Program)
		require.NoError(t, err)
		second, err := parsedProgram(deferNode.Program)
		require.NoError(t, err)
		other, err := parsedProgram(otherNode.Program)
		require.NoError(t, err)

		assert.Same(t, first, second)
		assert.NotSame(t, first, other)
	})

	t.Run("Integration: concurrent runs execute the shared AST", func(t *testing.T) {
		// Reserve 已完成：body 越过第一个 Activity，登记 defer，再调度 Pay。
		history := []historypkg.Event{
			executionSessionEvent(1, historypkg.NewWorkflowStartedEvent(map[string]any{"order": "one"}, "", "", "", "", 0)),
			executionSessionEvent(2, historypkg.NewActivityScheduledEvent(historypkg.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.Reserve"})),
			executionSessionEvent(3, historypkg.NewActivityCompletedEvent(historypkg.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{}})),
		}
		const runs = 16
		results := make([][]historypkg.Command, runs)
		errs := make([]error, runs)
		var wg sync.WaitGroup
		for i := range runs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i], errs[i] = NewWorkflowExecutor(deferNode, history).Execute()
			}()
		}
		wg.Wait()

		for i := range runs {
			require.NoError(t, errs[i])
			require.Len(t, results[i], 1)
			assert.Equal(t, historypkg.CmdScheduleActivity, results[i][0].Type)
			assert.Equal(t, "activities.Pay", results[i][0].ActivityType)
		}
	})
}
