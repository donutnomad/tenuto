package interpreter

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// Go 语义差分探针（go_semantics_probe_test.go）：同一份源码原生 Go 调用一次、生成 WLang 程序由解释器执行一次，
// 结果必须相同；SemDifference 开头的探针是 go-subset 文档列出的差异，测试断言解释器的结果与文档一致。

type SemOut struct {
	Sum   int
	Count int
	Text  string
	Flag  bool
	List  []int
	Value any
}

type SemItem struct {
	Name string
	Qty  int
	Tags []string
}

type SemOrder struct {
	ID    string
	Main  SemItem
	Items []SemItem
	Attrs map[string]string
	Ptr   *SemItem
}

type SemStatus string

type SemInput struct {
	Order  SemOrder
	Items  []SemItem
	ByName map[string]SemItem
	Ptrs   map[string]*SemItem
	Plain  map[string]*SemPlain
	V      any
	ID     int64
	Status SemStatus
}

// ---- 结构体值语义 ----

func SemStructCopyFromSlice(ctx workflow.Context, in SemInput) (SemOut, error) {
	items := []SemItem{{Name: "a", Qty: 1}}
	v := items[0]
	v.Qty = 5
	w := items[0]
	return SemOut{Sum: w.Qty, Count: v.Qty}, nil
}

func SemRangeCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	items := []SemItem{{Name: "a", Qty: 1}, {Name: "b", Qty: 2}}
	for _, it := range items {
		it.Qty = 9
	}
	sum := 0
	for _, it := range items {
		sum += it.Qty
	}
	return SemOut{Sum: sum}, nil
}

func SemMapStructCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string]SemItem{"a": {Name: "a", Qty: 1}}
	v := m["a"]
	v.Qty = 7
	w := m["a"]
	return SemOut{Sum: w.Qty, Count: v.Qty}, nil
}

func SemMapStructCopyCommaOK(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string]SemItem{"a": {Name: "a", Qty: 1}}
	if v, ok := m["a"]; ok {
		v.Qty = 7
	}
	w := m["a"]
	return SemOut{Sum: w.Qty}, nil
}

func SemStructAssignCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := SemItem{Qty: 1}
	b := a
	b.Qty = 2
	return SemOut{Sum: a.Qty*10 + b.Qty}, nil
}

func semBump(it SemItem) int {
	it.Qty = 100
	return it.Qty
}

func SemFuncParamCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := SemItem{Qty: 1}
	got := semBump(a)
	return SemOut{Sum: a.Qty, Count: got}, nil
}

func SemNestedStructCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	o := SemOrder{Main: SemItem{Qty: 1}, Items: []SemItem{{Qty: 1}}}
	o2 := o
	o2.Main.Qty = 5
	o2.Items[0] = SemItem{Qty: 6}
	first := o.Items[0]
	return SemOut{Sum: o.Main.Qty, Count: first.Qty}, nil
}

func SemStoreThenModify(ctx workflow.Context, in SemInput) (SemOut, error) {
	it := SemItem{Qty: 1}
	m := map[string]SemItem{}
	m["a"] = it
	items := []SemItem{}
	items = append(items, it)
	o := SemOrder{Main: it}
	it.Qty = 2
	a := m["a"]
	b := items[0]
	return SemOut{Sum: a.Qty + b.Qty*10 + o.Main.Qty*100}, nil
}

func SemInputStructCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	o := in.Order
	o.Main.Qty = 50
	m := in.Order.Main
	m.Qty = 60
	first := in.Items[0]
	first.Qty = 70
	again := in.Items[0]
	return SemOut{Sum: in.Order.Main.Qty, Count: again.Qty}, nil
}

func SemInputMapStructCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	v := in.ByName["a"]
	v.Qty = 99
	w := in.ByName["a"]
	return SemOut{Sum: w.Qty}, nil
}

func SemStructFieldSliceShared(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := SemItem{Tags: []string{"x"}}
	b := a
	b.Tags[0] = "y"
	return SemOut{Text: a.Tags[0]}, nil
}

func SemStructReturnedCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	o := SemOrder{Main: SemItem{Qty: 1}}
	main := o.Main
	main.Qty = 3
	return SemOut{Sum: o.Main.Qty}, nil
}

func SemClosureStructCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	base := SemItem{Qty: 1}
	mk := func(q int) SemItem {
		c := base
		c.Qty = q
		return c
	}
	x := mk(5)
	y := mk(6)
	return SemOut{Sum: base.Qty, Count: x.Qty*10 + y.Qty}, nil
}

// ---- slice 别名与 append ----

func SemSliceAlias(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := []int{1, 2, 3}
	b := a[:2]
	b[0] = 9
	c := a[1:]
	c[1] = 8
	return SemOut{List: a, Sum: len(c)}, nil
}

func SemMakeCapAlias(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := make([]int, 2, 10)
	b := a
	b = append(b, 5)
	a = append(a, 7)
	return SemOut{Sum: b[2], Count: a[2]}, nil
}

func SemAppendBeyondCapCopies(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := []int{1, 2}
	b := a
	b = append(b, 3)
	b[0] = 9
	return SemOut{Sum: a[0], Count: len(a)*10 + len(b)}, nil
}

func SemSubsliceAppendOverwrites(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := []int{1, 2, 3, 4}
	b := a[:2]
	b = append(b, 99)
	return SemOut{List: a, Sum: cap(b)}, nil
}

func SemCopyBuiltin(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := []int{1, 2, 3}
	b := make([]int, 2)
	n := copy(b, a)
	b[0] = 7
	return SemOut{List: a, Sum: n + b[0]}, nil
}

func SemDeleteDuringRange(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string]int{"a": 1, "b": 2, "c": 3}
	for k, v := range m {
		if v%2 == 1 {
			delete(m, k)
		}
	}
	return SemOut{Count: len(m), Sum: m["b"]}, nil
}

func SemRemoveFromSlice(ctx workflow.Context, in SemInput) (SemOut, error) {
	xs := []int{1, 2, 3, 4, 5}
	out := xs[:0]
	for _, x := range xs {
		if x%2 == 0 {
			out = append(out, x)
		}
	}
	return SemOut{List: xs, Count: len(out)}, nil
}

func SemRangeOverModifiedSlice(ctx workflow.Context, in SemInput) (SemOut, error) {
	xs := []int{1, 2, 3}
	sum := 0
	for i, x := range xs {
		if i == 0 {
			xs[2] = 30
			xs = append(xs, 100)
		}
		sum += x
	}
	return SemOut{Sum: sum, Count: len(xs)}, nil
}

// ---- map 增删改查 ----

func SemMapCRUD(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string]int{}
	m["a"]++
	m["b"] += 2
	m["c"] = m["c"] + 3
	delete(m, "a")
	_, okA := m["a"]
	v, okB := m["b"]
	// Go 的 map 遍历顺序随机：只记录遍历到的 key 集合。
	keys := ""
	for k := range m {
		if k == "b" || k == "c" {
			keys += "+"
		} else {
			keys += k
		}
	}
	return SemOut{Count: len(m), Sum: v, Flag: !okA && okB, Text: keys}, nil
}

func SemNestedMap(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string]map[string]int{}
	inner := m["x"]
	n := len(inner) + inner["y"]
	if m["x"] == nil {
		m["x"] = map[string]int{}
	}
	m["x"]["y"] = 3
	m["x"]["y"]++
	again := m["x"]
	return SemOut{Sum: n, Count: again["y"]}, nil
}

func SemMapOfSlices(ctx workflow.Context, in SemInput) (SemOut, error) {
	groups := map[string][]string{}
	for _, name := range []string{"a:ab", "a:ac", "b:bd"} {
		parts := strings.Split(name, ":")
		key := parts[0]
		list := groups[key]
		list = append(list, name)
		groups[key] = list
	}
	a := groups["a"]
	return SemOut{Count: len(groups), Sum: len(a), Text: strings.Join(a, ",")}, nil
}

func SemMapOfStructUpdate(ctx workflow.Context, in SemInput) (SemOut, error) {
	stock := map[string]SemItem{}
	names := ""
	for _, it := range in.Items {
		cur, seen := stock[it.Name]
		if !seen {
			names += it.Name
		}
		cur.Name = it.Name
		cur.Qty += it.Qty
		stock[it.Name] = cur
	}
	total := 0
	for _, v := range stock {
		total += v.Qty
	}
	return SemOut{Sum: total, Text: names, Count: len(stock)}, nil
}

func SemMapPointerValues(ctx workflow.Context, in SemInput) (SemOut, error) {
	p := in.Ptrs["a"]
	q := in.Ptrs["missing"]
	text := "nil"
	if q != nil {
		text = "non-nil"
	}
	sum := 0
	if p != nil {
		p.Qty += 10
		sum = p.Qty
	}
	again := in.Ptrs["a"]
	if again != nil {
		sum = sum*1000 + again.Qty
	}
	return SemOut{Sum: sum, Text: text}, nil
}

func SemPointerEquality(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := in.Ptrs["a"]
	b := in.Ptrs["b"]
	c := in.Ptrs["a"]
	count := 0
	if a == c {
		count = 1
	}
	return SemOut{Flag: a == b, Count: count}, nil
}

func SemMapNamedValue(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string]SemStatus{"a": "ok"}
	s := m["x"]
	t := m["a"]
	return SemOut{Flag: s == "" && t == "ok", Text: string(t)}, nil
}

// ---- any 比较与断言 ----

func SemDifferenceAnyTyped(ctx workflow.Context, in SemInput) (SemOut, error) {
	var a any = in.ID
	var b any = 3
	var c any = int64(3)
	var d any = in.Status
	count := 0
	if a == 3 {
		count += 1
	}
	if b == 3.0 {
		count += 10
	}
	if c == 3 {
		count += 100
	}
	if d == "ok" {
		count += 1000
	}
	if b == c {
		count += 10000
	}
	return SemOut{Count: count}, nil
}

func SemAnyFieldFromInput(ctx workflow.Context, in SemInput) (SemOut, error) {
	count := 0
	if in.V == nil {
		count += 1
	}
	if in.V == "x" {
		count += 10
	}
	if in.V == 1.0 {
		count += 100
	}
	if in.V == true {
		count += 1000
	}
	switch in.V {
	case "x":
		count += 10000
	case 1.0:
		count += 20000
	case nil:
		count += 30000
	}
	return SemOut{Count: count}, nil
}

func SemTypeSwitchPayload(ctx workflow.Context, input map[string]any) (SemOut, error) {
	text := ""
	for _, key := range []string{"s", "n", "b", "l", "m", "z", "missing"} {
		switch v := input[key].(type) {
		case string:
			text += "string:" + v + " "
		case float64:
			text += "float:" + strconv.FormatFloat(v, 'f', -1, 64) + " "
		case int:
			text += "int "
		case int64:
			text += "int64 "
		case bool:
			text += "bool "
		case []any:
			text += "list:" + strconv.Itoa(len(v)) + " "
		case map[string]any:
			text += "map:" + strconv.Itoa(len(v)) + " "
		case nil:
			text += "nil "
		default:
			text += "other "
		}
	}
	return SemOut{Text: text}, nil
}

func SemPayloadArithmetic(ctx workflow.Context, input map[string]any) (SemOut, error) {
	items, _ := input["items"].([]any)
	total := 0.0
	names := []string{}
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		price, _ := item["price"].(float64)
		qty, _ := item["qty"].(float64)
		total += price * qty
		if name, ok := item["name"].(string); ok {
			names = append(names, name)
		}
	}
	return SemOut{Sum: int(total * 100), Text: strings.Join(names, ","), Count: len(items)}, nil
}

func SemAnyMapMutation(ctx workflow.Context, input map[string]any) (SemOut, error) {
	state := map[string]any{"count": 0, "tags": []any{}}
	for i := 0; i < 3; i++ {
		n, _ := state["count"].(int)
		state["count"] = n + 1
	}
	tags, _ := state["tags"].([]any)
	tags = append(tags, "a")
	state["tags"] = tags
	count := 0
	if state["count"] == 3 {
		count = 1
	}
	return SemOut{Count: count, Sum: len(tags), Value: state}, nil
}

func SemAnyNestedCompare(ctx workflow.Context, input map[string]any) (SemOut, error) {
	meta, _ := input["meta"].(map[string]any)
	count := 0
	if meta["status"] == "paid" {
		count += 1
	}
	if meta["retries"] == 2.0 {
		count += 10
	}
	if meta["retries"] == 2 {
		count += 100
	}
	if input["meta"] != nil {
		count += 1000
	}
	return SemOut{Count: count}, nil
}

// ---- switch 带 init 与循环 ----

func semKind(i int) string {
	return []string{"a", "b", "c"}[i%3]
}

func SemSwitchInitInLoop(ctx workflow.Context, in SemInput) (SemOut, error) {
	count := 0
	text := ""
	for i := 0; i < 6; i++ {
		switch k := semKind(i); k {
		case "a":
			continue
		case "b":
			text += "b"
			break
		default:
			text += "c"
		}
		count++
	}
	return SemOut{Count: count, Text: text}, nil
}

func SemSwitchInitAnyInLoop(ctx workflow.Context, input map[string]any) (SemOut, error) {
	list, _ := input["list"].([]any)
	count := 0
	text := ""
outer:
	for _, raw := range list {
		switch v := raw; v {
		case "stop":
			break outer
		case "skip":
			continue
		case 1.0:
			text += "one "
		default:
			text += "other "
		}
		count++
	}
	return SemOut{Count: count, Text: text}, nil
}

func SemSwitchInitReturn(ctx workflow.Context, input map[string]any) (SemOut, error) {
	v := "outer"
	switch v := input["v"]; v {
	case "a":
		return SemOut{Text: "A"}, nil
	}
	return SemOut{Text: v}, nil
}

func SemLabeledContinue(ctx workflow.Context, in SemInput) (SemOut, error) {
	count := 0
rows:
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			if j == 1 {
				continue rows
			}
			count += 10*i + j
		}
	}
	return SemOut{Count: count}, nil
}

func SemClosureLoopCapture(ctx workflow.Context, in SemInput) (SemOut, error) {
	fns := []func() int{}
	for _, x := range []int{1, 2, 3} {
		fns = append(fns, func() int { return x })
	}
	for i := 0; i < 3; i++ {
		fns = append(fns, func() int { return i * 10 })
	}
	sum := 0
	for _, f := range fns {
		sum = sum*10 + f()
	}
	return SemOut{Sum: sum}, nil
}

func SemDeferInLoop(ctx workflow.Context, in SemInput) (out SemOut, err error) {
	for i := 0; i < 3; i++ {
		defer func() { out.Text += strconv.Itoa(i) }()
	}
	return SemOut{Text: "r"}, nil
}

func SemErrorsSprintf(ctx workflow.Context, in SemInput) (SemOut, error) {
	n, err := strconv.Atoi("x")
	text := ""
	if err != nil {
		text = fmt.Sprintf("bad:%d", n)
	}
	return SemOut{Text: text}, nil
}

// ---- append 最小复现 ----

func SemAppendNilLocal(ctx workflow.Context, in SemInput) (SemOut, error) {
	var list []string
	list = append(list, "x")
	return SemOut{Count: len(list)}, nil
}

func SemAppendMissingMapKey(ctx workflow.Context, in SemInput) (SemOut, error) {
	groups := map[string][]string{}
	list := groups["k"]
	list = append(list, "x")
	groups["k"] = list
	return SemOut{Count: len(groups["k"])}, nil
}

func SemAppendPresentMapKey(ctx workflow.Context, in SemInput) (SemOut, error) {
	groups := map[string][]string{"k": {"a"}}
	list := groups["k"]
	list = append(list, "x")
	groups["k"] = list
	return SemOut{Count: len(groups["k"])}, nil
}

func SemAppendDirectMapKey(ctx workflow.Context, in SemInput) (SemOut, error) {
	groups := map[string][]string{}
	groups["k"] = append(groups["k"], "x")
	return SemOut{Count: len(groups["k"])}, nil
}

func SemAppendNilStructField(ctx workflow.Context, in SemInput) (SemOut, error) {
	it := SemItem{}
	it.Tags = append(it.Tags, "x")
	return SemOut{Count: len(it.Tags)}, nil
}

func SemAppendInputNilField(ctx workflow.Context, in SemInput) (SemOut, error) {
	tags := in.Order.Main.Tags
	tags = append(tags, "x")
	return SemOut{Count: len(tags)}, nil
}

func SemAppendAfterCapCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := make([]int, 0, 10)
	b := a
	b = append(b, 5)
	return SemOut{Count: len(b)}, nil
}

func SemAppendCapSelf(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := make([]int, 0, 10)
	a = append(a, 5)
	return SemOut{Count: len(a)}, nil
}

func SemAppendSizedCopy(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := make([]int, 2)
	b := a
	b = append(b, 5)
	return SemOut{Count: len(b)}, nil
}

func SemAppendCapPassToFunc(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := make([]string, 0, 4)
	a = semAdd(a, "x")
	return SemOut{Count: len(a)}, nil
}

func semAdd(xs []string, v string) []string {
	xs = append(xs, v)
	return xs
}

func SemDifferenceAnyNumbers(ctx workflow.Context, input map[string]any) (SemOut, error) {
	var b any = 3
	count := 0
	if b == 3.0 {
		count += 1
	}
	var f any = 3.0
	if f == 3 {
		count += 10
	}
	var c any = int64(3)
	if c == 3 {
		count += 100
	}
	if input["n"] == b {
		count += 1000
	}
	return SemOut{Count: count}, nil
}

func SemAppendRuntimeZeroLenCap(ctx workflow.Context, in SemInput) (SemOut, error) {
	n := len(in.Items)
	a := make([]int, n, n+4)
	a = append(a, 1)
	return SemOut{Count: len(a)}, nil
}

func SemAppendCapStruct(ctx workflow.Context, in SemInput) (SemOut, error) {
	out := make([]SemItem, 0, len(in.Items))
	for _, it := range in.Items {
		out = append(out, it)
	}
	return SemOut{Count: len(out)}, nil
}

func SemAppendCapStrings(ctx workflow.Context, in SemInput) (SemOut, error) {
	names := make([]string, 0, 8)
	names = append(names, "a", "b")
	return SemOut{Text: strings.Join(names, ",")}, nil
}

func SemAppendCapSpread(ctx workflow.Context, in SemInput) (SemOut, error) {
	all := make([]string, 0, 8)
	all = append(all, in.Order.Main.Tags...)
	return SemOut{Count: len(all)}, nil
}

func SemAppendZeroStructField(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string]SemItem{}
	it := m["missing"]
	tags := it.Tags
	tags = append(tags, "x")
	var z SemItem
	t2 := z.Tags
	t2 = append(t2, "y")
	lit := SemItem{Name: "a"}
	t3 := lit.Tags
	t3 = append(t3, "z")
	return SemOut{Count: len(tags) + len(t2) + len(t3)}, nil
}

func semNilList() []string {
	return nil
}

func SemAppendAfterNilAssign(ctx workflow.Context, in SemInput) (SemOut, error) {
	xs := []string{"a"}
	xs = nil
	xs = append(xs, "b")
	ys := semNilList()
	ys = append(ys, "c")
	return SemOut{Count: len(xs) + len(ys)}, nil
}

func SemAppendMissingInputMapKey(ctx workflow.Context, in SemInput) (SemOut, error) {
	idx := map[string][]SemItem{}
	for _, it := range in.Items {
		list := idx[it.Name]
		list = append(list, it)
		idx[it.Name] = list
	}
	return SemOut{Count: len(idx)}, nil
}

func SemAppendMissingAnyMapKey(ctx workflow.Context, input map[string]any) (SemOut, error) {
	m := map[string][]any{}
	l := m["x"]
	l = append(l, 1)
	return SemOut{Count: len(l)}, nil
}

func SemRangeLenMissingSlice(ctx workflow.Context, in SemInput) (SemOut, error) {
	m := map[string][]int{}
	l := m["x"]
	n := 0
	for range l {
		n++
	}
	return SemOut{Count: len(l) + n, Flag: l == nil}, nil
}

func SemCapConstLen(ctx workflow.Context, in SemInput) (SemOut, error) {
	n := 2
	a := make([]int, n, n+3)
	a = append(a, 5)
	return SemOut{Count: len(a)}, nil
}

func SemCapConstLenInput(ctx workflow.Context, input map[string]any) (SemOut, error) {
	n := 2
	a := make([]int, n, n+3)
	a = append(a, 5)
	return SemOut{Count: len(a)}, nil
}

// ---- 外部 JSON 里的 null ----

func SemNullFields(ctx workflow.Context, in SemInput) (SemOut, error) {
	text := in.Order.ID + "|" + in.Order.Main.Name
	if in.Status == "" {
		text += "|empty"
	}
	count := in.Order.Main.Qty + len(in.Order.Items) + len(in.Order.Main.Tags)
	flag := in.Order.ID == "" && in.Order.Main.Name == "" && in.Order.Ptr == nil
	return SemOut{Text: text, Count: count, Flag: flag, Sum: int(in.ID)}, nil
}

const (
	SemStatusPending SemStatus = "pending"
	SemStatusPaid    SemStatus = "paid"
)

func SemNamedStringField(ctx workflow.Context, in SemInput) (SemOut, error) {
	count := 0
	if in.Status == "" {
		count += 1
	}
	if in.Status == SemStatusPending {
		count += 10
	}
	if in.Status != SemStatusPaid {
		count += 100
	}
	var s SemStatus
	if s == "" {
		count += 1000
	}
	o := SemInput{}
	if o.Status == "" {
		count += 10000
	}
	switch in.Status {
	case SemStatusPending:
		count += 100000
	case "":
		count += 200000
	default:
		count += 300000
	}
	return SemOut{Count: count}, nil
}

type SemPlain struct {
	Name string
	Qty  int
	OK   bool
}

func SemZeroBasicFields(ctx workflow.Context, in SemPlain) (SemOut, error) {
	count := 0
	if in.Name == "" {
		count += 1
	}
	if in.Qty == 0 {
		count += 10
	}
	if !in.OK {
		count += 100
	}
	return SemOut{Count: count, Text: in.Name + "!"}, nil
}

type SemPrio int

type SemTask struct {
	Status SemStatus
	Prio   SemPrio
}

func SemNamedVariants(ctx workflow.Context, in SemTask) (SemOut, error) {
	count := 0
	if in.Status == SemStatus("paid") {
		count += 1
	}
	s := in.Status
	if s == SemStatusPaid {
		count += 10
	}
	other := SemTask{Status: SemStatusPaid}
	if in.Status == other.Status {
		count += 100
	}
	if in.Prio == 2 {
		count += 1000
	}
	local := SemTask{Status: SemStatusPaid, Prio: 2}
	if local.Status == SemStatusPaid {
		count += 100000
	}
	if local.Prio == 2 {
		count += 1000000
	}
	return SemOut{Count: count}, nil
}

func SemNamedIntOrder(ctx workflow.Context, in SemTask) (SemOut, error) {
	count := 0
	if in.Prio > 1 {
		count += 1
	}
	return SemOut{Count: count}, nil
}

func semDecide(d map[string]any) string {
	switch d["action"] {
	case "approve":
		return "A"
	case "reject":
		return "R"
	}
	if d["urgent"] == true {
		return "U"
	}
	return "-"
}

func semLookup(m map[string]SemItem, k string) int {
	v := m[k]
	return v.Qty + 1
}

func SemHelperRewrites(ctx workflow.Context, input map[string]any) (SemOut, error) {
	m := map[string]SemItem{}
	return SemOut{Text: semDecide(input), Count: semLookup(m, "x")}, nil
}

func SemDurationMap(ctx workflow.Context, input map[string]any) (SemOut, error) {
	waits := map[string]time.Duration{"a": time.Second}
	d := waits["missing"]
	e := waits["a"]
	return SemOut{Sum: int(d), Count: int(e / time.Millisecond)}, nil
}

func SemAnyNilPointer(ctx workflow.Context, input map[string]any) (SemOut, error) {
	count := 0
	if input["x"] == nil {
		count += 1
	}
	var p *SemItem
	var v any = p
	if v == nil {
		count += 10
	}
	var e error
	if e == nil {
		count += 100
	}
	return SemOut{Count: count}, nil
}

func semClassify(d map[string]any) string {
	switch k := d["kind"]; k {
	case "a":
		return "A"
	default:
		return "B"
	}
}

func SemSwitchInitTerminating(ctx workflow.Context, input map[string]any) (SemOut, error) {
	return SemOut{Text: semClassify(input)}, nil
}

func SemSwitchInitTerminatingMain(ctx workflow.Context, input map[string]any) (SemOut, error) {
	switch k := input["kind"]; k {
	case "a":
		return SemOut{Text: "A"}, nil
	default:
		return SemOut{Text: "B"}, nil
	}
}

// 典型业务代码：Activity 返回的订单状态是具名字符串类型。
func SemActivityStatusBranch(ctx workflow.Context, input map[string]any) (SemOut, error) {
	var r SemTask
	if err := workflow.ExecuteActivity(ctx, "load-task", input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &r); err != nil {
		return SemOut{}, err
	}
	return SemOut{Text: semRoute(r.Status, r.Prio)}, nil
}

// ---- 具名基础类型：载荷各来源与常量、字面量比较 ----

type SemLine struct {
	SKU    string
	Status SemStatus
	Prio   SemPrio
	Qty    int
}

type SemOrderInput struct {
	Lines    []SemLine
	Statuses []SemStatus
	Limits   map[SemStatus]SemPrio
	Prio     SemPrio
}

// semRoute 是典型的业务分支：按订单状态与优先级决定处理方式。
func semRoute(status SemStatus, prio SemPrio) string {
	switch status {
	case SemStatusPaid:
		if prio > 1 {
			return "ship-fast"
		}
		return "ship"
	case SemStatusPending:
		return "wait"
	case "":
		return "empty"
	}
	return "refund"
}

func SemNamedOrderLines(ctx workflow.Context, in SemOrderInput) (SemOut, error) {
	text := ""
	count := 0
	for _, line := range in.Lines {
		text += semRoute(line.Status, line.Prio) + ","
		if line.Status != SemStatusPaid {
			count++
		}
		if line.Prio >= in.Prio && line.Prio+1 > 2 {
			count += 10
		}
	}
	for _, s := range in.Statuses {
		if s == SemStatusPending {
			count += 100
		}
	}
	if in.Limits[SemStatusPaid] == 3 {
		count += 1000
	}
	if limit, ok := in.Limits["missing"]; !ok && limit == 0 {
		count += 10000
	}
	total := in.Prio * 2
	return SemOut{Text: text, Count: count, Sum: int(total)}, nil
}

func SemNamedConversions(ctx workflow.Context, in SemOrderInput) (SemOut, error) {
	text := ""
	for _, line := range in.Lines {
		text += string(line.Status) + ":" + strings.ToUpper(string(line.Status)) + ","
		if line.Status == SemStatus("paid") {
			text += "!"
		}
	}
	byStatus := map[SemStatus][]string{}
	for _, line := range in.Lines {
		list := byStatus[line.Status]
		list = append(list, line.SKU)
		byStatus[line.Status] = list
	}
	return SemOut{Text: text, Count: len(byStatus[SemStatusPaid])*10 + len(byStatus["none"])}, nil
}

// 典型业务代码：子 Workflow 返回具名类型字段。
func SemChildStatusBranch(ctx workflow.Context, input map[string]any) (SemOut, error) {
	var r SemTask
	if err := workflow.ExecuteChildWorkflow(ctx, "child-task", input).Get(ctx, &r); err != nil {
		return SemOut{}, err
	}
	return SemOut{Text: semRoute(r.Status, r.Prio)}, nil
}

// Signal 解进结构体。
func SemSignalStatusBranch(ctx workflow.Context, input map[string]any) (SemOut, error) {
	var r SemTask
	if err := workflow.WaitSignal(ctx, "task").Get(ctx, &r); err != nil {
		return SemOut{}, err
	}
	return SemOut{Text: semRoute(r.Status, r.Prio)}, nil
}

// Update 参数解进结构体。
func SemUpdateStatusBranch(ctx workflow.Context, input map[string]any) (SemOut, error) {
	if err := workflow.SetUpdateHandler(ctx, "route", func(ctx workflow.Context, task SemTask) (SemOut, error) {
		return SemOut{Text: semRoute(task.Status, task.Prio), Count: int(task.Prio) + 1}, nil
	}); err != nil {
		return SemOut{}, err
	}
	return SemOut{}, workflow.Await(ctx, func() bool { return false })
}

// ---- 预分配容量的 slice ----

type SemBatch struct {
	Names []string
	IDs   []int
	Items []SemItem
}

func SemPreallocAppend(ctx workflow.Context, in SemInput) (SemOut, error) {
	names := make([]string, 0, len(in.Items))
	items := make([]SemItem, 0, len(in.Items))
	qty := make([]int, 0, 4)
	for _, it := range in.Items {
		names = append(names, it.Name)
		items = append(items, it)
		qty = append(qty, it.Qty)
	}
	sum := 0
	for _, q := range qty {
		sum += q
	}
	text := strings.Join(names, ",")
	for i, it := range items {
		text += fmt.Sprintf("|%d:%s", i, it.Name)
	}
	batch := SemBatch{Names: names, IDs: qty, Items: items}
	sized := make([]string, 1, 3)
	sized = append(sized, in.Order.ID)
	sized = append(sized, names...)
	return SemOut{Text: text, Sum: sum, Count: len(batch.Names) + len(batch.Items)*10 + len(sized)*100, List: qty}, nil
}

// ---- nil slice 与 map 零值 ----

func semFilter(items []SemItem, min int) []SemItem {
	if len(items) == 0 {
		return nil
	}
	var out []SemItem
	for _, it := range items {
		if it.Qty >= min {
			out = append(out, it)
		}
	}
	return out
}

func semTags(it SemItem) ([]string, map[string]int) {
	return nil, nil
}

func semCount(xs []string, m map[string]int) int {
	xs = append(xs, "x")
	return len(xs) + len(m)
}

func SemNilSlices(ctx workflow.Context, in SemInput) (SemOut, error) {
	big := semFilter(in.Items, 2)
	big = append(big, SemItem{Name: "extra"})
	none := semFilter(nil, 0)
	for range none {
		return SemOut{Text: "unexpected"}, nil
	}
	none = append(none, SemItem{Name: "n"})
	tags, attrs := semTags(SemItem{})
	tags = append(tags, "t")
	item := SemItem{Name: "a", Tags: nil}
	itemTags := item.Tags
	itemTags = append(itemTags, "b")
	item.Tags = itemTags
	var later []int = nil
	later = append(later, 1)
	m := map[string]int{"a": 1}
	m = nil
	n := len(m) + m["a"] + len(attrs)
	return SemOut{Count: len(big)*1000 + len(none)*100 + len(tags)*10 + len(item.Tags) + n + semCount(nil, nil)*10000, Sum: len(later)}, nil
}

func SemMissingKeyCollections(ctx workflow.Context, in SemInput) (SemOut, error) {
	byName := map[string][]string{}
	for _, it := range in.Items {
		names := byName[it.Name]
		names = append(names, it.Tags...)
		byName[it.Name] = names
	}
	nested := map[string]map[string]int{"a": {"x": 1}}
	inner := nested["missing"]
	n := len(inner) + inner["x"] + nested["missing"]["x"] + nested["a"]["x"]*10
	for range inner {
		n += 1000
	}
	if inner == nil {
		n += 100
	}
	b := map[string]int{}
	nested["b"] = b
	b["y"] = 2
	n += nested["b"]["y"] * 10000
	words := byName["missing"]
	for range words {
		n += 1000
	}
	return SemOut{Count: n, Sum: len(byName), Text: strings.Join(byName["a"], ",")}, nil
}

// ---- 指针按地址比较 ----

func SemPointerIdentity(ctx workflow.Context, in SemInput) (SemOut, error) {
	p := in.Ptrs["a"]
	q := p
	r := in.Ptrs["b"]
	count := 0
	if p == q {
		count += 1
	}
	if p != r {
		count += 10
	}
	holder := SemOrder{Ptr: p}
	if holder.Ptr == q && in.Order.Ptr != p {
		count += 100
	}
	var none *SemItem
	if none == nil && p != nil && none != p {
		count += 1000
	}
	list := []*SemItem{p, r}
	for _, it := range list {
		if it == q {
			count += 10000
		}
	}
	if in.Ptrs["missing"] == none {
		count += 100000
	}
	return SemOut{Count: count}, nil
}

// ---- 文档列出的差异（期望值是解释器的结果，与 go-subset 的「与 Go 的差异」一致） ----

// 切片超出长度、未超出 make 给出的容量：与 Go 相同合法，新元素是零值，写入经底层数组对 a 的后续切片可见。
func SemMakeSliceBeyondLen(ctx workflow.Context, in SemInput) (SemOut, error) {
	a := make([]int, 0, 10)
	b := a[:3]
	b[2] = 4
	c := a[1:4]
	return SemOut{Count: len(b) + cap(a) + cap(c)*100, Sum: c[1], List: c}, nil
}

// 具名基础类型放进 any 后与底层类型的值相等。
func SemDifferenceAnyNamed(ctx workflow.Context, in SemInput) (SemOut, error) {
	var d any = in.Status
	count := 0
	if d == "ok" {
		count += 1
	}
	if _, ok := d.(string); ok {
		count += 10
	}
	return SemOut{Count: count}, nil
}

type SemTags []string

type SemCounts map[string]int

// 具名 slice、map 放进 any 后对底层类型的断言成立（Go 不成立），断言回具名类型同样成立。
func SemDifferenceAnyNamedContainers(ctx workflow.Context, in SemInput) (SemOut, error) {
	var x any = SemTags{"a"}
	var m any = SemCounts{"a": 1}
	count := 0
	if _, ok := x.([]string); ok {
		count += 1
	}
	if tags, ok := x.(SemTags); ok {
		count += 10 * len(tags)
	}
	if _, ok := m.(map[string]int); ok {
		count += 100
	}
	if counts, ok := m.(SemCounts); ok {
		count += 1000 * len(counts)
	}
	return SemOut{Count: count}, nil
}

type SemRef struct {
	P *SemPlain
}

// 指针放进 any 后比较、含指针字段的结构体比较：Go 按地址，解释器按指向的内容。
func SemDifferencePointerContent(ctx workflow.Context, in SemInput) (SemOut, error) {
	p, r := in.Plain["a"], in.Plain["b"]
	var a any = p
	var b any = r
	count := 0
	if a == b {
		count += 1
	}
	if (SemRef{P: p}) == (SemRef{P: r}) {
		count += 10
	}
	return SemOut{Count: count}, nil
}

// 指针放进 any 后比较，指向的结构体含 slice 字段：Go 按地址比较，解释器按内容比较时遇到 slice 而 panic。
func SemDifferencePointerInAnyPanics(ctx workflow.Context, in SemInput) (SemOut, error) {
	var a any = in.Ptrs["a"]
	var b any = in.Ptrs["b"]
	return SemOut{Flag: a == b}, nil
}

type SemScalarPointers struct {
	A, B *int
	S, T *SemStatus
	L, M *[]int
	N, O *map[string]int
	P    map[string]*int
}

// 指向基础类型、具名基础类型、slice、map 的指针与 map 里取出的这类指针：Go 按地址比较，解释器按指向的内容比较。
func SemDifferenceScalarPointers(ctx workflow.Context, in SemScalarPointers) (SemOut, error) {
	c := 0
	if in.A == in.B {
		c += 1
	}
	a := in.A
	if a == in.A {
		c += 10
	}
	if in.S == in.T {
		c += 100
	}
	if in.L == in.M {
		c += 1000
	}
	if in.P["x"] == in.P["y"] {
		c += 10000
	}
	if in.N == in.O {
		c += 100000
	}
	return SemOut{Count: c}, nil
}

// 保存在 any 变量里的类型化 slice、map 保留动态类型：x.([]any)、x.(map[string]any) 与对应的 case 不成立，
// 断言回自己的类型成立；装着 nil slice、nil map 的 any 不等于 nil。
func SemAnyTypedContainers(ctx workflow.Context, in SemInput) (SemOut, error) {
	var x any = []SemItem{{Name: "a"}}
	var m any = map[string]SemItem{"k": {Qty: 2}}
	var ns []SemItem
	var nilSlice any = ns
	count := 0
	if _, ok := x.([]any); ok {
		count += 1
	}
	if items, ok := x.([]SemItem); ok {
		count += 10 * len(items)
	}
	if _, ok := m.(map[string]any); ok {
		count += 100
	}
	switch m.(type) {
	case map[string]any:
		count += 1000
	case map[string]SemItem:
		count += 10000
	}
	if nilSlice == nil {
		count += 100000
	}
	var raw any = []any{1, "a"}
	if list, ok := raw.([]any); ok {
		count += 1000000 * len(list)
	}
	return SemOut{Count: count}, nil
}

// 类型化的 slice 存进 map[string]any 或 []any 之后只剩载体：读出来的值断言成 []any 成立（Go 不成立）。
func SemDifferenceAnyContainerNested(ctx workflow.Context, in SemInput) (SemOut, error) {
	box := map[string]any{"items": []SemItem{{Name: "a"}}}
	list := []any{map[string]SemItem{"k": {}}}
	count := 0
	if _, ok := box["items"].([]any); ok {
		count += 1
	}
	if _, ok := list[0].(map[string]any); ok {
		count += 10
	}
	return SemOut{Count: count}, nil
}

// make 的剩余容量里是元素类型的零值：结构体元素经超出长度的切片读到零值结构体；长度与容量可以是任意整数类型。
func SemMakeSpareCapacityZero(ctx workflow.Context, in SemInput) (SemOut, error) {
	var n int32 = 1
	var c uint8 = 4
	items := make([]SemItem, n, c)
	more := items[:3]
	last := more[2]
	last.Qty = len(last.Tags) + 7
	more[1] = last
	grown := []SemItem{{Qty: 1}}
	grown = append(grown, SemItem{Qty: 2})
	grown = append(grown, SemItem{Qty: 3})
	all := grown[:cap(grown)]
	tail := all[len(all)-1]
	second, third := more[1], more[2]
	return SemOut{Count: len(more)*10 + cap(items), Sum: second.Qty + tail.Qty, Text: tail.Name + third.Name}, nil
}

// 下标、切片边界与 range n 可以是任意整数类型（int32、uint8 等），与 Go 相同。
func SemSmallIntIndexes(ctx workflow.Context, in SemInput) (SemOut, error) {
	xs := []int{10, 20, 30, 40}
	var i int32 = 1
	var j uint8 = 3
	var k int16 = 2
	xs[k] = xs[i] + xs[j]
	ys := xs[i:j]
	var n int32 = 3
	var total int32
	for r := range n {
		total += r
	}
	var m uint8 = 2
	count := 0
	for range m {
		count++
	}
	return SemOut{List: ys, Sum: xs[k], Count: len(ys)*100 + count*10, Text: fmt.Sprint(total)}, nil
}

// 负数下标与 Go 一样 panic（Workflow Task 以 workflow_panic 失败）。
func SemSmallIntIndexNegative(ctx workflow.Context, in SemInput) (SemOut, error) {
	xs := []int{1, 2}
	var i int8 = -1
	return SemOut{Sum: xs[i]}, nil
}
