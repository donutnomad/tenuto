package bizprobe

import (
	"fmt"
	"strings"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// 具名类型统一、nil 改写、SamePointer、make 容量相关的差分 Workflow。

type BizCents int64

type BizRate float64

type BizLevel uint8

type BizFlag bool

type BizPrio int

const (
	BizLow BizPrio = iota
	BizMid
	BizHigh
)

type BizCode BizSt

func (s BizSt) Final() bool { return s == BizStDone }

func (s BizSt) Label() string { return strings.ToUpper(string(s)) }

func (c BizCents) Add(o BizCents) BizCents { return c + o }

func (p BizPrio) Next() BizPrio {
	if p >= BizHigh {
		return BizHigh
	}
	return p + 1
}

type BizNamedIn struct {
	Status   BizSt
	Statuses []BizSt
	ByStatus map[BizSt]int
	Amount   BizCents
	Rate     BizRate
	Level    BizLevel
	Flag     BizFlag
	Prio     BizPrio
	Code     BizCode
	Ptr      *BizLine
	Ptrs     map[string]*BizLine
	Lines    []BizLine
	Nums     []int
}

type BizNamedOut struct {
	Text     string
	List     []string
	ByStatus map[BizSt]int
	Amount   BizCents
	Level    BizLevel
	Prio     BizPrio
	N        int
	F        float64
}

func BizNamedMethods(ctx workflow.Context, in BizNamedIn) (BizNamedOut, error) {
	out := []string{}
	if in.Status.Final() {
		out = append(out, "final")
	}
	out = append(out, in.Status.Label())
	local := BizStRunning
	out = append(out, fmt.Sprint(local.Final()), local.Label())
	for _, s := range in.Statuses {
		switch s {
		case "done":
			out = append(out, "d")
		case BizStRunning:
			out = append(out, "r")
		default:
			out = append(out, "?"+string(s))
		}
		if s.Final() {
			out = append(out, "F")
		}
	}
	total := in.Amount.Add(BizCents(250)).Add(in.Amount * 2)
	p := in.Prio.Next()
	q := BizMid
	q++
	lvl := in.Level + 10
	rate := float64(in.Rate) * 2
	if in.Flag {
		out = append(out, "flag")
	}
	if BizSt(in.Code) == BizStDone {
		out = append(out, "code-done")
	}
	counts := map[BizSt]int{}
	for k, v := range in.ByStatus {
		counts[k] = counts[k] + v
	}
	counts[in.Status] = counts[in.Status] + 1
	return BizNamedOut{Text: strings.Join(out, ","), Amount: total, Prio: p, Level: lvl, F: rate, N: int(q), ByStatus: counts}, nil
}

func bizPick(ok bool) ([]string, error) {
	if !ok {
		return nil, fmt.Errorf("none")
	}
	return []string{"a"}, nil
}

func bizCountAll(xs []string, m map[string]int) int {
	n := len(xs) + len(m)
	for range xs {
		n++
	}
	return n
}

func BizNilRewrites(ctx workflow.Context, in BizNamedIn) (BizNamedOut, error) {
	out := []string{}
	xs, err := bizPick(len(in.Nums) > 0)
	out = append(out, fmt.Sprint(xs == nil, err != nil, len(xs)))
	xs = append(xs, "z")
	res := BizRes{Items: nil, Counts: nil}
	if res.Items == nil && res.Counts == nil {
		out = append(out, "nil-fields")
	}
	items := res.Items
	items = append(items, "i")
	res.Items = items
	res.Items = nil
	for range res.Items {
		out = append(out, "never")
	}
	n := bizCountAll(nil, nil) + bizCountAll(xs, map[string]int{"k": 1})
	var m map[string]int = nil
	v, ok := m["x"]
	out = append(out, fmt.Sprint(n, v, ok, len(m), m == nil))
	lines := in.Lines
	if lines == nil {
		out = append(out, "nil-lines")
	}
	lines = nil
	lines = append(lines, BizLine{SKU: "n"})
	return BizNamedOut{Text: strings.Join(out, "|"), List: xs, N: len(lines)}, nil
}

func BizPointers(ctx workflow.Context, in BizNamedIn) (BizNamedOut, error) {
	out := []string{}
	p := in.Ptr
	q := p
	var z *BizLine
	out = append(out, fmt.Sprint(p == q, p == z, p != nil, z == nil))
	a := in.Ptrs["a"]
	b := in.Ptrs["b"]
	a2 := in.Ptrs["a"]
	out = append(out, fmt.Sprint(a == b, a == a2, in.Ptrs["missing"] == nil))
	if a != nil {
		a.Qty = 42
	}
	again := in.Ptrs["a"]
	n := 0
	if again != nil {
		n = again.Qty
	}
	seen := map[string]bool{}
	for k, ptr := range in.Ptrs {
		if ptr == a {
			seen[k] = true
		}
	}
	// map 的遍历顺序在 Go 里是随机的：先排序键，结果才与解释器可比。
	var names []string
	for k := range seen {
		names = append(names, k)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	keys := ""
	for _, k := range names {
		keys += k
	}
	out = append(out, keys)
	return BizNamedOut{Text: strings.Join(out, "|"), N: n}, nil
}

func BizMakeCap(ctx workflow.Context, in BizNamedIn) (BizNamedOut, error) {
	lines := make([]BizLine, 0, len(in.Lines))
	for _, l := range in.Lines {
		if l.Qty > 1 {
			lines = append(lines, l)
		}
	}
	nums := make([]int, len(in.Nums), len(in.Nums)*2+1)
	for i, v := range in.Nums {
		nums[i] = v * v
	}
	nums = append(nums, -1)
	names := make([]string, 0, 4)
	for _, l := range lines {
		names = append(names, l.SKU)
	}
	sum := 0
	for _, v := range nums {
		sum += v
	}
	return BizNamedOut{List: names, N: sum*100 + len(lines)}, nil
}

// 具名类型字段经 Activity 输入编码、结果解码后再比较。
func BizNamedActivity(ctx workflow.Context, in BizNamedIn) (BizNamedOut, error) {
	var res BizNamedIn
	if err := workflow.ExecuteActivity(ctx, "classify", BizNamedIn{Status: in.Status, Prio: BizHigh, ByStatus: map[BizSt]int{BizStDone: 1}}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &res); err != nil {
		return BizNamedOut{}, err
	}
	text := ""
	switch {
	case res.Prio > BizMid && res.Status.Final():
		text = "urgent-final"
	case res.Prio > BizMid:
		text = "urgent"
	default:
		text = "normal"
	}
	return BizNamedOut{Text: text, Prio: res.Prio + 1, ByStatus: res.ByStatus, Level: res.Level - 1}, nil
}

type BizStockIn struct {
	Stock BizStock
	Tags  BizTags
}

func (s BizStock) Total() int {
	n := 0
	for _, v := range s {
		n += v
	}
	return n
}

func (ts BizTags) Has(x string) bool {
	for _, t := range ts {
		if t == x {
			return true
		}
	}
	return false
}

func BizNamedMapInput(ctx workflow.Context, in BizStockIn) (BizNamedOut, error) {
	s := in.Stock
	ok := s.Reserve("a", 2)
	total := s.Total()
	local := BizStock{"x": 1}
	local["y"] = 2
	return BizNamedOut{N: total*100 + local.Total(), Text: fmt.Sprint(ok, s["a"], len(s))}, nil
}

func BizNamedSliceRangeInput(ctx workflow.Context, in BizStockIn) (BizNamedOut, error) {
	n := 0
	for _, t := range in.Tags {
		if t != "" {
			n++
		}
	}
	return BizNamedOut{N: n, Text: fmt.Sprint(in.Tags.Has("b"), len(in.Tags))}, nil
}
