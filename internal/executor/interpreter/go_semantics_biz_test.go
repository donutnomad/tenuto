package interpreter

import (
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/bizprobe"
)

const bizOrder = `{"Order": {"ID": "o-1", "Customer": "c", "Coupons": ["PCT10", "OFF250"],
  "Lines": [{"SKU": "b", "Qty": 2, "Cents": 1999, "Tags": ["food"]},
            {"SKU": "a", "Qty": 1, "Cents": 50000, "Tags": ["tech", "x"], "Meta": {"k": "v"}},
            {"SKU": "c", "Qty": 3, "Cents": 333, "Tags": ["food"]},
            {"SKU": "a", "Qty": 2, "Cents": 10},
            {"SKU": "d", "Qty": 2, "Cents": 1999, "Tags": ["food"]}],
  "Notes": {"audit": ["created"]}, "Ptr": {"SKU": "p", "Qty": 1}},
  "Raw": "a=1;b = 2;bad;c=x;a=5;", "Nums": [3, 7, -1, 4, 12, 6], "Kv": {"a": 3, "b": 1, "c": 9},
  "U": 0, "I8": 100, "F": 2.5, "When": "2026-11-25T08:00:00Z", "Dur": 1500000000}`

// TestGoSemanticsBusinessDifferential 比较 bizprobe 里纯计算的业务 Workflow：原生 Go 调用一次，解释器冷重放与
// sticky 各执行一次，三者结果相同。BizTime 的 workflow.Now 取 Started 事件的时间，期望值单独计算。
func TestGoSemanticsBusinessDifferential(t *testing.T) {
	for _, tc := range []struct {
		fn    any
		name  string
		input string
	}{
		{bizprobe.BizOrderTotals, "BizOrderTotals", bizOrder},
		{bizprobe.BizOrderTotals, "BizOrderTotals", `{"Order": {"ID": "o", "Coupons": ["PCTx"]}}`},
		{bizprobe.BizGroupAndSort, "BizGroupAndSort", bizOrder},
		{bizprobe.BizClosures, "BizClosures", bizOrder},
		{bizprobe.BizMultiReturn, "BizMultiReturn", bizOrder},
		{bizprobe.BizErrorWrap, "BizErrorWrap", bizOrder},
		{bizprobe.BizErrorWrap, "BizErrorWrap", `{"Order": {"ID": "o", "Lines": [{"SKU": "z", "Qty": 0}]}}`},
		{bizprobe.BizErrorWrap, "BizErrorWrap", `{"Order": {"ID": "o", "Lines": [{"SKU": "z", "Qty": 1, "Cents": -1}]}}`},
		{bizprobe.BizStrings, "BizStrings", bizOrder},
		{bizprobe.BizNumbers, "BizNumbers", bizOrder},
		{bizprobe.BizNestedMutation, "BizNestedMutation", bizOrder},
		{bizprobe.BizNestedMutation, "BizNestedMutation", `{}`},
		{bizprobe.BizStructSliceShare, "BizStructSliceShare", `{}`},
		{bizprobe.BizPointerField, "BizPointerField", bizOrder},
		{bizprobe.BizPointerField, "BizPointerField", `{}`},
		{bizprobe.BizMethods, "BizMethods", bizOrder},
		{bizprobe.BizControlFlow, "BizControlFlow", bizOrder},
		{bizprobe.BizTime, "BizTime", bizOrder},
		{bizprobe.BizIsoMapPlusEqInt64, "BizIsoMapPlusEqInt64", `{}`},
		{bizprobe.BizIsoMapPlusEqInt, "BizIsoMapPlusEqInt", `{}`},
		{bizprobe.BizIsoMapPlusEqCall, "BizIsoMapPlusEqCall", bizOrder},
		{bizprobe.BizIsoMapPlusEqFloat, "BizIsoMapPlusEqFloat", `{}`},
		{bizprobe.BizIsoMapPlusEqString, "BizIsoMapPlusEqString", `{}`},
		{bizprobe.BizIsoMapPlusEqInputMap, "BizIsoMapPlusEqInputMap", bizOrder},
		{bizprobe.BizIsoRecursiveClosure, "BizIsoRecursiveClosure", `{}`},
		{bizprobe.BizIsoMapPlusEqPlain, "BizIsoMapPlusEqPlain", `{}`},
		{bizprobe.BizIsoMapPlusEqOutField, "BizIsoMapPlusEqOutField", `{}`},
		{bizprobe.BizIsoNamedSliceAppend, "BizIsoNamedSliceAppend", `{}`},
		{bizprobe.BizIsoNamedSliceLiteralAppend, "BizIsoNamedSliceLiteralAppend", `{}`},
		{bizprobe.BizIsoNamedSliceHelper, "BizIsoNamedSliceHelper", `{}`},
		{bizprobe.BizIsoNamedSliceInputAppend, "BizIsoNamedSliceInputAppend", `{"Tags": ["a"]}`},
		{bizprobe.BizIsoNamedSliceInputAppend, "BizIsoNamedSliceInputAppend", `{}`},
		{bizprobe.BizIsoInputFieldAssign, "BizIsoInputFieldAssign", `{"X": 1}`},
		{bizprobe.BizIsoInputIncr, "BizIsoInputIncr", `{"X": 1}`},
		{bizprobe.BizIsoInputReassign, "BizIsoInputReassign", `{}`},
		{bizprobe.BizIsoHelperParamAssign, "BizIsoHelperParamAssign", `{"X": 1}`},
		{bizprobe.BizIsoInputCopyAssign, "BizIsoInputCopyAssign", `{"X": 1}`},
		{bizprobe.BizNamedMethods, "BizNamedMethods", `{"Status":"done","Statuses":["done","running","x"],"ByStatus":{"done":2,"x":1},"Amount":100,"Rate":1.25,"Level":250,"Flag":true,"Prio":1,"Code":"done","Ptr":{"SKU":"p"},"Ptrs":{"a":{"SKU":"a","Qty":1},"b":{"SKU":"a","Qty":1}},"Lines":[{"SKU":"x","Qty":2},{"SKU":"y","Qty":1},{"SKU":"z","Qty":3}],"Nums":[1,2,3]}`},
		{bizprobe.BizNamedMethods, "BizNamedMethods", `{"Prio": 2}`},
		{bizprobe.BizNilRewrites, "BizNilRewrites", `{"Status":"done","Statuses":["done","running","x"],"ByStatus":{"done":2,"x":1},"Amount":100,"Rate":1.25,"Level":250,"Flag":true,"Prio":1,"Code":"done","Ptr":{"SKU":"p"},"Ptrs":{"a":{"SKU":"a","Qty":1},"b":{"SKU":"a","Qty":1}},"Lines":[{"SKU":"x","Qty":2},{"SKU":"y","Qty":1},{"SKU":"z","Qty":3}],"Nums":[1,2,3]}`},
		{bizprobe.BizNilRewrites, "BizNilRewrites", `{}`},
		{bizprobe.BizPointers, "BizPointers", `{"Status":"done","Statuses":["done","running","x"],"ByStatus":{"done":2,"x":1},"Amount":100,"Rate":1.25,"Level":250,"Flag":true,"Prio":1,"Code":"done","Ptr":{"SKU":"p"},"Ptrs":{"a":{"SKU":"a","Qty":1},"b":{"SKU":"a","Qty":1}},"Lines":[{"SKU":"x","Qty":2},{"SKU":"y","Qty":1},{"SKU":"z","Qty":3}],"Nums":[1,2,3]}`},
		{bizprobe.BizPointers, "BizPointers", `{}`},
		{bizprobe.BizMakeCap, "BizMakeCap", `{"Status":"done","Statuses":["done","running","x"],"ByStatus":{"done":2,"x":1},"Amount":100,"Rate":1.25,"Level":250,"Flag":true,"Prio":1,"Code":"done","Ptr":{"SKU":"p"},"Ptrs":{"a":{"SKU":"a","Qty":1},"b":{"SKU":"a","Qty":1}},"Lines":[{"SKU":"x","Qty":2},{"SKU":"y","Qty":1},{"SKU":"z","Qty":3}],"Nums":[1,2,3]}`},
		{bizprobe.BizMakeCap, "BizMakeCap", `{}`},
		{bizprobe.BizNamedMapInput, "BizNamedMapInput", `{"Stock": {"a": 3, "b": 1}}`},
		{bizprobe.BizNamedSliceRangeInput, "BizNamedSliceRangeInput", `{"Tags": ["a", "b"]}`},
		{bizprobe.BizIsoFuncVar, "BizIsoFuncVar", bizOrder},
		{bizprobe.BizIsoFloatDivZero, "BizIsoFloatDivZero", `{"F": 2}`},
	} {
		want := runNativeDiff(t, tc.fn, tc.input)
		if tc.name == "BizErrorWrap" && want == "panic" {
			// 原生侧调用 SDK 桩会 panic；按 Go 的 fmt.Errorf 与 Temporal ApplicationError.Error()（只返回 message）手算。
			if strings.Contains(tc.input, "-1") {
				want = canonicalJSON(t, bizprobe.BizOut{Text: "app:order o line 0: negative price"})
			} else {
				want = "error: order o line 0: line z: qty 0 must be positive"
			}
		}
		if tc.name == "BizTime" {
			want = canonicalJSON(t, bizprobe.BizTimeCalc(time.Date(2026, 11, 27, 8, 0, 0, 0, time.UTC)))
		}
		w := newBizWorld(t, tc.name, tc.input)
		w.wft()
		w.check(tc.name, want)
	}
}
