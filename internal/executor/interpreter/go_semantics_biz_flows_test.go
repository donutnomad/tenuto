package interpreter

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/internal/bizprobe"
	"github.com/donutnomad/tenuto/protocol/history"
)

func bizJSON(t *testing.T, v any) string { return canonicalJSON(t, v) }

func TestBizFlowSaga(t *testing.T) {
	for _, name := range []string{"BizSaga", "BizSagaSingle"} {
		w := newBizWorld(t, name, `{"ID": "o1", "Amount": 100}`)
		w.wft()
		w.complete("reserve", 0, `{"ID": "r-1"}`)
		w.wft()
		w.fail("charge", 0, "card declined", "Declined", true)
		w.wft()
		if len(w.acts) == 1 {
			w.complete("release", 0, `{}`)
			w.wft()
		}
		out := w.outcome()
		t.Logf("%s cold outcome = %s", name, out)
		var want string
		if name == "BizSaga" {
			want = bizJSON(t, bizprobe.BizFlowOut{Text: "compensated: " + bizText(out, "compensated: "), List: []string{"release r-1"}})
		} else {
			want = bizJSON(t, bizprobe.BizFlowOut{Text: "undone: " + bizText(out, "undone: ")})
		}
		w.check(name, want)
	}
}

// bizText 从结果 JSON 的 Text 字段取出 prefix 之后的部分（错误文本由 SDK 决定，不属于 Go 语义比较的范围）。
func bizText(outcome, prefix string) string {
	var o bizprobe.BizFlowOut
	if json.Unmarshal([]byte(outcome), &o) != nil {
		return "<no output>"
	}
	return strings.TrimPrefix(o.Text, prefix)
}

func TestBizFlowFanOut(t *testing.T) {
	w := newBizWorld(t, "BizFanOut", `{"Regions": ["eu", "us", "ap"]}`)
	w.wft()
	w.complete("count", 0, `{"Counts": {"a": 1, "b": 2}, "Amount": 5}`)
	w.fail("count", 0, "boom", "Boom", false)
	w.complete("count", 0, `{"Counts": {"a": 10}, "Amount": 7}`)
	w.wft()
	out := w.outcome()
	var o bizprobe.BizFlowOut
	_ = json.Unmarshal([]byte(out), &o)
	errText := "<missing>"
	if len(o.List) == 1 {
		errText = o.List[0]
	}
	w.check("BizFanOut", bizJSON(t, bizprobe.BizFlowOut{Counts: map[string]int{"a": 11, "b": 2}, Amount: 12, N: 3, List: []string{errText}}))
	if !strings.HasPrefix(errText, "us: ") {
		t.Errorf("BizFanOut 错误归属：%s", errText)
	}

	w = newBizWorld(t, "BizFanOutPlusEq", `{"Regions": ["eu", "us"]}`)
	w.wft()
	w.complete("count", 0, `{"Counts": {"a": 1, "b": 2}}`)
	w.complete("count", 0, `{"Counts": {"a": 10}}`)
	w.wft()
	w.check("BizFanOutPlusEq", bizJSON(t, bizprobe.BizFlowOut{Counts: map[string]int{"a": 11, "b": 2}}))
}

func TestBizFlowQuoteRace(t *testing.T) {
	w := newBizWorld(t, "BizQuoteRace", `{"ID": "q"}`)
	t.Logf("wft1: %s", bizCommandSummary(w.wft()))
	w.complete("quoteB", 0, `{"Amount": 90}`)
	t.Logf("wft2: %s", bizCommandSummary(w.wft()))
	w.complete("quoteA", 0, `{"Amount": 80}`)
	t.Logf("wft3: %s", bizCommandSummary(w.wft()))
	w.check("BizQuoteRace/both", bizJSON(t, bizprobe.BizFlowOut{List: []string{"B:90", "A:80"}, Amount: 80, Text: "false"}))

	w = newBizWorld(t, "BizQuoteRace", `{"ID": "q"}`)
	w.wft()
	w.fireTimers()
	t.Logf("timeout wft: %s", bizCommandSummary(w.wft()))
	w.check("BizQuoteRace/timeout", bizJSON(t, bizprobe.BizFlowOut{Amount: -1, Text: "true"}))

	w = newBizWorld(t, "BizQuoteRace", `{"ID": "q"}`)
	w.wft()
	w.complete("quoteA", 0, `{"Amount": 70}`)
	w.complete("quoteB", 0, `{"Amount": 60}`)
	w.wft()
	w.check("BizQuoteRace/same-batch", bizJSON(t, bizprobe.BizFlowOut{List: []string{"A:70", "B:60"}, Amount: 60, Text: "false"}))
}

func TestBizFlowApprovals(t *testing.T) {
	w := newBizWorld(t, "BizApprovals", `{"Need": 2}`)
	w.wft()
	w.signal("approve", `{"User": " Alice ", "OK": true}`)
	w.signal("approve", `{"User": "alice", "OK": true}`)
	w.wft()
	w.signal("approve", `{"User": "Bob", "OK": true}`)
	w.wft()
	w.check("BizApprovals/ok", bizJSON(t, bizprobe.BizFlowOut{Text: "approved", List: []string{"alice", "dup:alice", "bob"}, N: 2}))

	w = newBizWorld(t, "BizApprovals", `{"Need": 2}`)
	w.signal("approve", `{"User": "Carol", "OK": false, "Reason": "budget"}`)
	w.wft()
	w.check("BizApprovals/reject", bizJSON(t, bizprobe.BizFlowOut{Text: "rejected by carol: budget", List: []string{"carol"}}))

	w = newBizWorld(t, "BizApprovalDeadline", `{"Need": 2}`)
	w.wft()
	w.signal("approve", `{"User": "u1", "OK": true}`)
	w.wft()
	w.fireTimers()
	w.wft()
	w.check("BizApprovalDeadline", bizJSON(t, bizprobe.BizFlowOut{N: 1, List: []string{"u1"}, Text: "true"}))
}

func TestBizFlowPoll(t *testing.T) {
	w := newBizWorld(t, "BizPoll", `{"ID": "job"}`)
	w.wft()
	w.complete("poll", 0, `{"Status": "running"}`)
	w.wft()
	w.fireTimers()
	w.wft()
	w.fail("poll", 0, "flaky", "Transient", false)
	w.wft()
	w.fireTimers()
	w.wft()
	w.complete("poll", 0, `{"Done": true, "Status": "done"}`)
	w.wft()
	out := w.outcome()
	var o bizprobe.BizFlowOut
	_ = json.Unmarshal([]byte(out), &o)
	errText := "<missing>"
	if len(o.List) == 3 {
		errText = o.List[1]
	}
	w.check("BizPoll", bizJSON(t, bizprobe.BizFlowOut{N: 3, Text: "done", List: []string{"running", errText, "done"}}))
	// 第二个 Timer 的时长：attempt=2 → 4s。
	var delays []string
	for _, e := range w.events {
		if e.TimerScheduled != nil {
			delays = append(delays, strings.TrimSpace(strings.Repeat(" ", 0))+jsonInt(e.TimerScheduled.DelayMs))
		}
	}
	if strings.Join(delays, ",") != "1000,4000" {
		t.Errorf("BizPoll timer delays = %v, Go = [1000 4000]", delays)
	}
}

func jsonInt(v int64) string { b, _ := json.Marshal(v); return string(b) }

func TestBizFlowContinueAsNew(t *testing.T) {
	pages := []string{`{"ID": "c1", "Items": ["x", "y"]}`, `{"ID": "c2", "Items": ["y"]}`, `{"ID": "", "Items": ["z"]}`}
	// 原生 Go 侧：同样的状态变化。
	st := bizprobe.BizCanState{}
	var wantCAN []string
	for i, p := range pages {
		var page bizprobe.BizRes
		_ = json.Unmarshal([]byte(p), &page)
		if st.Seen == nil {
			st.Seen = map[string]int{}
		}
		for _, it := range page.Items {
			st.Seen[it]++
			st.Log = append(st.Log, it)
		}
		st.Iter++
		st.Cursor = page.ID
		if i < 2 {
			b, _ := json.Marshal(st)
			wantCAN = append(wantCAN, string(b))
		}
	}
	input := `{}`
	for i, p := range pages {
		w := newBizWorld(t, "BizCan", input)
		w.wft()
		if w.err != nil || w.done {
			t.Fatalf("BizCan run %d: %s", i+1, w.outcome())
		}
		w.complete("fetch-page", 0, p)
		w.wft()
		if i < 2 {
			want := "continue-as-new  " + canonicalJSON(t, json.RawMessage(wantCAN[i]))
			w.check("BizCan/run"+jsonInt(int64(i+1)), want)
			for _, c := range w.last {
				if c.Type == history.CmdContinueAsNew {
					b, _ := json.Marshal(c.Input)
					input = string(b)
				}
			}
			continue
		}
		w.check("BizCan/final", bizJSON(t, bizprobe.BizFlowOut{N: 3, Counts: st.Seen, List: st.Log}))
	}
}

func TestBizFlowChild(t *testing.T) {
	w := newBizWorld(t, "BizChildAgg", `{"Regions": ["eu", "us"]}`)
	w.wft()
	ids := []string{}
	for id := range w.child {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) != 2 {
		t.Fatalf("children: %v", ids)
	}
	w.completeChild(ids[0], `{"Status": "done", "Tags": ["t1"]}`)
	w.completeChild(ids[1], `{"Status": "running", "Tags": null}`)
	w.wft()
	w.check("BizChildAgg", bizJSON(t, bizprobe.BizFlowOut{N: 1, List: []string{"t1"}}))

	w = newBizWorld(t, "BizChildTagsAppend", `{}`)
	w.wft()
	for id := range w.child {
		w.completeChild(id, `{"Tags": ["a"]}`)
	}
	w.wft()
	w.check("BizChildTagsAppend", bizJSON(t, bizprobe.BizFlowOut{List: []string{"a", "parent"}, N: 2}))
}

func TestBizFlowCart(t *testing.T) {
	w := newBizWorld(t, "BizCartFlow", `{}`)
	w.wft()
	w.update("add", "u1", `{"SKU": "a", "Qty": 2}`)
	w.update("add", "u2", `{"SKU": "b", "Qty": 1}`)
	w.update("add", "u3", `{"SKU": "a", "Qty": 1}`)
	w.update("add", "u4", `{"SKU": "z", "Qty": 0}`)
	cmds := w.wft()
	var outcomes []string
	for _, c := range findUpdateCompletes(cmds) {
		if c.UpdateOutcome.Success != nil {
			outcomes = append(outcomes, c.UpdateID+"="+string(c.UpdateOutcome.Success.Data))
		} else {
			outcomes = append(outcomes, c.UpdateID+"!"+c.UpdateOutcome.Failure.Message)
		}
	}
	wantOutcomes := []string{
		"u1=" + bizJSON(t, bizprobe.BizFlowOut{N: 1, Amount: 2}), "u2=" + bizJSON(t, bizprobe.BizFlowOut{N: 2, Amount: 3}),
		"u3=" + bizJSON(t, bizprobe.BizFlowOut{N: 2, Amount: 4}), "u4!bad qty 0 for z",
	}
	if strings.Join(outcomes, " ") != strings.Join(wantOutcomes, " ") {
		t.Errorf("update outcomes:\n  Go     = %v\n  解释器 = %v", wantOutcomes, outcomes)
	}
	if got, want := w.query("cart", map[string]any{}), bizJSON(t, map[string]any{"items": map[string]int{"a": 3, "b": 1}, "history": []string{"a", "b", "a"}, "total": 4}); got != want {
		t.Errorf("query cart:\n  Go     = %s\n  解释器 = %s", want, got)
	}
	w.update("checkout", "u5", "")
	ck := w.wft()
	t.Logf("checkout wft: %s", bizCommandSummary(ck))
	for _, c := range findUpdateCompletes(ck) {
		t.Logf("checkout outcome: success=%v failure=%+v", c.UpdateOutcome.Success != nil, c.UpdateOutcome.Failure)
	}
	if !w.done {
		t.Errorf("BizCartFlow: checkout 的 Workflow Task 只完成了 Update，Await 条件已满足但主协程没有返回；再执行一个 Workflow Task：")
		w.done = false
		t.Logf("extra wft: %s", bizCommandSummary(w.wft()))
	}
	w.check("BizCartFlow", bizJSON(t, bizprobe.BizFlowOut{N: 4, Counts: map[string]int{"a": 3, "b": 1}, List: []string{"a", "b", "a"}}))

	w = newBizWorld(t, "BizCartPlusEq", `{}`)
	w.wft()
	w.update("add", "u1", `{"SKU": "a", "Qty": 2}`)
	w.update("add", "u2", `{"SKU": "b", "Qty": 1}`)
	w.wft()
	w.check("BizCartPlusEq", bizJSON(t, bizprobe.BizFlowOut{N: 3, Counts: map[string]int{"a": 2, "b": 1}}))
}

func TestBizFlowCancel(t *testing.T) {
	w := newBizWorld(t, "BizCancelCleanup", `{"ID": "x"}`)
	w.wft()
	w.cancelRequest()
	t.Logf("after cancel request: %s", bizCommandSummary(w.wft()))
	if _, ok := w.actType[w.firstOpen("cleanup")]; !ok {
		w.cancelled("long", 0)
		t.Logf("after ActivityCancelled: %s", bizCommandSummary(w.wft()))
	}
	w.complete("cleanup", 0, `{"ID": "c9"}`)
	t.Logf("after cleanup: %s", bizCommandSummary(w.wft()))
	w.check("BizCancelCleanup", "cancelled")
}

func (w *bizWorld) firstOpen(typ string) string {
	for id := range w.acts {
		if w.actType[id] == typ {
			return id
		}
	}
	return ""
}

func TestBizFlowUpdateParamAssign(t *testing.T) {
	w := newBizWorld(t, "BizIsoUpdateParamAssign", `{}`)
	w.wft()
	w.update("add", "u1", `{"SKU": "a", "Qty": 0}`)
	w.update("add", "u2", `{"SKU": "b", "Qty": 1}`)
	cmds := w.wft()
	for _, c := range findUpdateCompletes(cmds) {
		t.Logf("%s success=%v failure=%+v", c.UpdateID, c.UpdateOutcome.Success != nil, c.UpdateOutcome.Failure)
	}
	w.check("BizIsoUpdateParamAssign", bizJSON(t, bizprobe.BizFlowOut{N: 2}))
}

func TestBizFlowNamedActivity(t *testing.T) {
	w := newBizWorld(t, "BizNamedActivity", `{"Status": "done"}`)
	cmds := w.wft()
	wantInput := bizJSON(t, bizprobe.BizNamedIn{Status: bizprobe.BizStDone, Prio: bizprobe.BizHigh, ByStatus: map[bizprobe.BizSt]int{bizprobe.BizStDone: 1}})
	if len(cmds) != 1 || bizJSON(t, cmds[0].Input) != wantInput {
		t.Errorf("activity input:\n  Go     = %s\n  解释器 = %s", wantInput, bizJSON(t, cmds))
	}
	w.complete("classify", 0, `{"Status": "done", "Prio": 2, "ByStatus": {"done": 5}, "Level": 0}`)
	w.wft()
	w.check("BizNamedActivity", bizJSON(t, bizprobe.BizNamedOut{Text: "urgent-final", Prio: 3, ByStatus: map[bizprobe.BizSt]int{"done": 5}, Level: 255}))
}
