// Package bizprobe 是 interpreter 差分测试用的业务 Workflow：测试按包目录生成 WLang 程序（与 tenuto-workflow-gen
// 相同），同一份源码原生 Go 调用一次、解释器经冷重放与 sticky 各执行一次，结果按 Go 语义比较。
package bizprobe

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// 按真实业务写法写的差分 Workflow（go_semantics_biz_test.go 与原生 Go 比较）。

type BizLine struct {
	SKU   string
	Qty   int
	Price float64
	Cents int64
	Tags  []string
	Meta  map[string]string
}

type BizOrder struct {
	ID       string
	Customer string
	Lines    []BizLine
	Coupons  []string
	Notes    map[string][]string
	Priority int
	Ptr      *BizLine
}

type BizIn struct {
	Order BizOrder
	Raw   string
	Nums  []int
	Kv    map[string]int
	U     uint32
	I8    int8
	F     float64
	Dur   time.Duration
}

type BizOut struct {
	Text   string
	N      int
	N64    int64
	F      float64
	List   []string
	Lines  []BizLine
	Groups map[string][]BizLine
	Totals map[string]int64
	Flag   bool
}

// ---- 订单金额：整数分、浮点折扣、四舍五入、格式化 ----

func bizLineTotal(l BizLine) int64 { return int64(l.Qty) * l.Cents }

func (l BizLine) Total() int64 { return int64(l.Qty) * l.Cents }

func (o BizOrder) Subtotal() int64 {
	var sum int64
	for _, l := range o.Lines {
		sum += l.Total()
	}
	return sum
}

func BizOrderTotals(ctx workflow.Context, in BizIn) (BizOut, error) {
	sub := in.Order.Subtotal()
	var discount int64
	for _, c := range in.Order.Coupons {
		switch {
		case strings.HasPrefix(c, "PCT"):
			pct, err := strconv.Atoi(strings.TrimPrefix(c, "PCT"))
			if err != nil {
				return BizOut{}, fmt.Errorf("bad coupon %s: %w", c, err)
			}
			discount += sub * int64(pct) / 100
		case strings.HasPrefix(c, "OFF"):
			v, err := strconv.ParseInt(strings.TrimPrefix(c, "OFF"), 10, 64)
			if err != nil {
				return BizOut{}, fmt.Errorf("bad coupon %s: %w", c, err)
			}
			discount += v
		}
	}
	total := sub - discount
	if total < 0 {
		total = 0
	}
	tax := math.Round(in.F*0.0825*100) / 100
	text := fmt.Sprintf("%s sub=%d disc=%d total=%d.%02d tax=%.2f", in.Order.ID, sub, discount, total/100, total%100, tax)
	return BizOut{Text: text, N64: total, F: tax}, nil
}

// ---- 分组 + 手写排序（sort 不在白名单内时的常见写法） ----

func bizSortLines(ls []BizLine, less func(a, b BizLine) bool) {
	for i := 1; i < len(ls); i++ {
		for j := i; j > 0 && less(ls[j], ls[j-1]); j-- {
			ls[j], ls[j-1] = ls[j-1], ls[j]
		}
	}
}

func BizGroupAndSort(ctx workflow.Context, in BizIn) (BizOut, error) {
	groups := map[string][]BizLine{}
	totals := map[string]int64{}
	for _, l := range in.Order.Lines {
		key := "untagged"
		if len(l.Tags) > 0 {
			key = l.Tags[0]
		}
		list := groups[key]
		list = append(list, l)
		groups[key] = list
		totals[key] = totals[key] + bizLineTotal(l)
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	// Go 的 map 遍历顺序随机：手写插入排序后再用，结果与遍历顺序无关。
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	for _, k := range keys {
		g := groups[k]
		bizSortLines(g, func(a, b BizLine) bool {
			if a.Qty != b.Qty {
				return a.Qty > b.Qty
			}
			return a.SKU < b.SKU
		})
		groups[k] = g
	}
	all := []BizLine{}
	all = append(all, in.Order.Lines...)
	bizSortLines(all, func(a, b BizLine) bool { return a.Total() > b.Total() })
	names := []string{}
	for _, l := range all {
		names = append(names, l.SKU)
	}
	return BizOut{Groups: groups, Totals: totals, List: names, Text: strings.Join(keys, ",")}, nil
}

// ---- 闭包：计数器、捕获并修改外部变量、闭包返回多值 ----

func BizClosures(ctx workflow.Context, in BizIn) (BizOut, error) {
	count := 0
	seen := map[string]bool{}
	var order []string
	visit := func(sku string) (bool, int) {
		if seen[sku] {
			return false, count
		}
		seen[sku] = true
		count++
		order = append(order, sku)
		return true, count
	}
	dups := 0
	for _, l := range in.Order.Lines {
		if ok, _ := visit(l.SKU); !ok {
			dups++
		}
	}
	makeAcc := func(base int) func(int) int {
		total := base
		return func(d int) int {
			total += d
			return total
		}
	}
	acc := makeAcc(100)
	acc(1)
	acc(2)
	last := acc(3)
	var fib func(n int) int
	fib = func(n int) int {
		if n < 2 {
			return n
		}
		return fib(n-1) + fib(n-2)
	}
	return BizOut{N: count*1000 + dups*100 + last, N64: int64(fib(15)), List: order}, nil
}

// ---- 多返回值、命名返回值与 defer 改写错误 ----

func bizParse(raw string) (key string, val int, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("parse %s: %w", raw, err)
		}
	}()
	parts := strings.Split(raw, "=")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("missing '='")
	}
	key = strings.TrimSpace(parts[0])
	val, err = strconv.Atoi(strings.TrimSpace(parts[1]))
	return
}

func BizMultiReturn(ctx workflow.Context, in BizIn) (BizOut, error) {
	kv := map[string]int{}
	var errs []string
	for _, field := range strings.Split(in.Raw, ";") {
		if field == "" {
			continue
		}
		k, v, err := bizParse(field)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		kv[k] = kv[k] + v
	}
	// map 的遍历顺序在 Go 里是随机的：先排序键，结果才与解释器可比。
	var names []string
	for k := range kv {
		names = append(names, k)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	keys := ""
	for _, k := range names {
		keys += k + "=" + strconv.Itoa(kv[k]) + " "
	}
	return BizOut{Text: keys, List: errs, N: len(kv)}, nil
}

// ---- error 包装链与最终失败 ----

func bizValidate(l BizLine) error {
	if l.Qty <= 0 {
		return fmt.Errorf("line %s: qty %d must be positive", l.SKU, l.Qty)
	}
	if l.Cents < 0 {
		return workflow.NewApplicationError("negative price", "InvalidPrice")
	}
	return nil
}

func BizErrorWrap(ctx workflow.Context, in BizIn) (BizOut, error) {
	for i, l := range in.Order.Lines {
		if err := bizValidate(l); err != nil {
			wrapped := fmt.Errorf("order %s line %d: %w", in.Order.ID, i, err)
			if workflow.IsApplicationError(err, "InvalidPrice") {
				return BizOut{Text: "app:" + wrapped.Error()}, nil
			}
			return BizOut{}, wrapped
		}
	}
	var err error
	if err == nil {
		return BizOut{Text: "ok"}, nil
	}
	return BizOut{}, err
}

// ---- 字符串与格式化 ----

func BizStrings(ctx workflow.Context, in BizIn) (BizOut, error) {
	out := []string{}
	out = append(out, fmt.Sprintf("[%-8s|%8s|%5.1f|%x|%08b|%+d|%v|%t]", "ab", "cd", 3.14159, 255, 5, 7, 1.5, true))
	out = append(out, fmt.Sprintf("%v|%v|%v|%g|%e", 1e21, 100000000.0, 0.000001, 1e-7, 123456.789))
	out = append(out, fmt.Sprintf("%d|%5d|%-5d|%05d|%x|%X|%o", -42, 42, 42, -42, -255, 3054, 8))
	out = append(out, fmt.Sprintf("%s|%10.3s|%v", "héllo", "abcdef", "x"))
	out = append(out, fmt.Sprint("a", 1, 2, "b", "c", 3.5, true))
	out = append(out, fmt.Sprintln("a", 1, true))
	out = append(out, strconv.FormatFloat(1.0/3, 'f', -1, 64), strconv.FormatFloat(2.5, 'e', 3, 64), strconv.FormatFloat(1e21, 'g', -1, 64))
	out = append(out, strconv.FormatInt(-255, 16), strconv.FormatInt(1<<40, 36))
	out = append(out, strings.Repeat("ab", 3), strings.Replace("aaaa", "a", "b", 2), strings.TrimLeft("xxyhixy", "xy"), strings.Trim("  hi  ", " "))
	out = append(out, strconv.Itoa(len("héllo")), strconv.Itoa(strings.Index("héllo", "l")), strconv.Itoa(strings.Count("cheese", "e")), strconv.Itoa(strings.Count("five", "")))
	fields := strings.Fields("  a b\t c\n ")
	out = append(out, strings.Join(fields, "/"), strconv.Itoa(len(strings.Split("", ","))), strconv.Itoa(len(strings.Split("a,b,", ","))))
	b, err := strconv.ParseBool("TRUE")
	out = append(out, strconv.FormatBool(b), fmt.Sprint(err == nil))
	f, err := strconv.ParseFloat("1e3", 64)
	out = append(out, fmt.Sprintf("%v %v", f, err))
	n, err := strconv.ParseInt("-0x1f", 0, 64)
	out = append(out, fmt.Sprintf("%d %v", n, err))
	_, err = strconv.Atoi("99999999999999999999")
	out = append(out, err.Error())
	_, err = strconv.ParseInt("300", 10, 8)
	out = append(out, err.Error())
	out = append(out, strings.ToUpper(in.Raw))
	out = append(out, strconv.FormatBool(strings.EqualFold("Go", "GO") == true && strings.Contains(in.Raw, "") == true && fmt.Sprint(1) == "1" && strings.LastIndex("go gopher", "go") == 3 && strings.HasSuffix("x.json", ".json") && strings.TrimSuffix("x.json", ".json") == "x" && strings.TrimRight("abccc", "c") == "ab" && strings.TrimSpace("\t a \n") == "a" && strings.ToLower("ABC") == "abc" && strings.ReplaceAll("a-b-c", "-", "") == "abc" && strings.HasPrefix("abc", "ab") && strings.TrimPrefix("abc", "ab") == "c" && fmt.Sprintf("%v", "") == "" && strings.Join(nil, ",") == ""))
	return BizOut{List: out}, nil
}

// ---- 整数、浮点与 Duration 运算 ----

func BizNumbers(ctx workflow.Context, in BizIn) (BizOut, error) {
	out := []string{}
	a, b := -7, 2
	out = append(out, fmt.Sprintf("%d %d %d %d", a/b, a%b, -a/b, a%-b))
	var big int64 = math.MaxInt64
	big++
	out = append(out, strconv.FormatInt(big, 10))
	var u uint32 = in.U
	u -= 1
	out = append(out, fmt.Sprint(u))
	var i8 int8 = in.I8
	i8 += 100
	out = append(out, fmt.Sprint(i8))
	d := in.Dur
	d2 := d * 3
	d3 := d2 / time.Millisecond
	d4 := time.Duration(len(in.Nums)) * time.Minute
	d5 := d4 + 90*time.Second
	out = append(out, fmt.Sprintf("%d %d %d %d", int64(d2), int64(d3), int64(d5), int64(d5%time.Minute)))
	timeout := 2 * time.Hour
	if d5 < timeout && d5 > time.Minute {
		out = append(out, "between")
	}
	secs := int64(d5 / time.Second)
	out = append(out, fmt.Sprintf("%dm%02ds", secs/60, secs%60))
	f := in.F
	out = append(out, fmt.Sprintf("%v %v %v %v", f/1.0 > 1e308, math.IsNaN(f-f), math.Floor(-2.5), math.Round(-2.5)))
	out = append(out, fmt.Sprintf("%v %v %v", math.Trunc(2.7), math.Mod(7, 3), math.Sqrt(2)))
	mx := math.Inf(-1)
	mn := math.Inf(1)
	for range in.Nums {
		mx = math.Max(mx, in.F*2)
		mn = math.Min(mn, in.F-1)
	}
	out = append(out, fmt.Sprintf("%v %v", mx, mn))
	avg := 0.0
	if len(in.Nums) > 0 {
		sum := 0
		for _, n := range in.Nums {
			sum += n
		}
		avg = in.F / 3
		_ = sum
	}
	out = append(out, strconv.FormatFloat(avg, 'f', 3, 64))
	return BizOut{List: out}, nil
}

// ---- 嵌套结构体/slice/map 修改 ----

func BizNestedMutation(ctx workflow.Context, in BizIn) (BizOut, error) {
	o := in.Order
	for i := range o.Lines {
		l := o.Lines[i]
		l.Qty *= 2
		if l.Meta == nil {
			l.Meta = map[string]string{}
		}
		meta := l.Meta
		meta["doubled"] = "yes"
		l.Meta = meta
		tags := l.Tags
		tags = append(tags, "x2")
		l.Tags = tags
		lines := o.Lines
		lines[i] = l
	}
	if o.Notes == nil {
		o.Notes = map[string][]string{}
	}
	notes := o.Notes["audit"]
	notes = append(notes, "doubled")
	notes = append(notes, "again")
	nm := o.Notes
	nm["audit"] = notes
	// 原输入的 Lines 与 o.Lines 共享底层数组，Go 里 in.Order.Lines 同样被修改。
	sharedQty := 0
	for _, l := range in.Order.Lines {
		sharedQty += l.Qty
	}
	copyOrder := o
	copyOrder.ID = "copy"
	fresh := []BizLine{}
	fresh = append(fresh, o.Lines...)
	copyOrder.Lines = fresh
	if len(copyOrder.Lines) > 0 {
		first := copyOrder.Lines[0]
		first.SKU = "changed"
		cl := copyOrder.Lines
		cl[0] = first
	}
	firstSKU := ""
	if len(o.Lines) > 0 {
		fl := o.Lines[0]
		firstSKU = fl.SKU
	}
	return BizOut{Lines: o.Lines, N: sharedQty, Text: o.ID + "|" + copyOrder.ID + "|" + firstSKU, List: o.Notes["audit"]}, nil
}

// ---- 结构体里的 slice 在结构体复制后共享元素 ----

func BizStructSliceShare(ctx workflow.Context, in BizIn) (BizOut, error) {
	a := BizLine{SKU: "a", Tags: []string{"x", "y"}, Meta: map[string]string{"k": "v"}}
	b := a
	bt := b.Tags
	bt[0] = "changed"
	bm := b.Meta
	bm["k"] = "changed"
	b.SKU = "b"
	m := map[string]BizLine{"a": a}
	c := m["a"]
	ct := c.Tags
	ct[1] = "viaMap"
	return BizOut{Text: a.SKU + a.Tags[0] + a.Tags[1] + a.Meta["k"]}, nil
}

// ---- 指针字段 ----

func BizPointerField(ctx workflow.Context, in BizIn) (BizOut, error) {
	o := in.Order
	n := 0
	if o.Ptr != nil {
		o.Ptr.Qty += 10
		n = in.Order.Ptr.Qty
	}
	p := o.Ptr
	if p != nil {
		p.SKU = "viaP"
	}
	text := "nil"
	if in.Order.Ptr != nil {
		text = in.Order.Ptr.SKU
	}
	return BizOut{N: n, Text: text}, nil
}

// ---- 方法：具名 slice 类型与 map 类型上的方法 ----

type BizLines []BizLine

func (ls BizLines) Sum() int64 {
	var s int64
	for _, l := range ls {
		s += l.Total()
	}
	return s
}

func (ls BizLines) Filter(pred func(BizLine) bool) BizLines {
	out := BizLines{}
	for _, l := range ls {
		if pred(l) {
			out = append(out, l)
		}
	}
	return out
}

type BizStock map[string]int

func (s BizStock) Reserve(sku string, n int) bool {
	if s[sku] < n {
		return false
	}
	s[sku] -= n
	return true
}

func BizMethods(ctx workflow.Context, in BizIn) (BizOut, error) {
	ls := BizLines{}
	ls = append(ls, in.Order.Lines...)
	big := ls.Filter(func(l BizLine) bool { return l.Qty >= 2 })
	stock := BizStock{}
	for k, v := range in.Kv {
		stock[k] = v
	}
	reserved := []string{}
	for _, l := range in.Order.Lines {
		if stock.Reserve(l.SKU, l.Qty) {
			reserved = append(reserved, l.SKU)
		}
	}
	return BizOut{N64: ls.Sum()*1000 + big.Sum(), N: len(big), List: reserved, Totals: map[string]int64{"left": int64(stock["a"])}}, nil
}

// ---- 结构体比较、switch 多值、带标签 break ----

type BizKey struct {
	Region string
	Tier   int
}

func BizControlFlow(ctx workflow.Context, in BizIn) (BizOut, error) {
	k1 := BizKey{"eu", 1}
	k2 := BizKey{Region: "eu", Tier: 1}
	seen := map[string]int{}
	seen[k1.Region+strconv.Itoa(k1.Tier)] = seen[k1.Region+strconv.Itoa(k1.Tier)] + 1
	seen[k2.Region+strconv.Itoa(k2.Tier)] = seen[k2.Region+strconv.Itoa(k2.Tier)] + 1
	out := []string{}
	if k1 == k2 {
		out = append(out, "eq")
	}
	out = append(out, strconv.Itoa(seen["eu1"]))
	for _, n := range in.Nums {
		switch n {
		case 1, 2, 3:
			out = append(out, "small")
		case 4:
			out = append(out, "four")
		default:
			if n < 0 {
				out = append(out, "neg")
				break
			}
			out = append(out, "big")
		}
	}
	found := -1
outer:
	for i := 0; i < len(in.Nums); i++ {
		for j := i + 1; j < len(in.Nums); j++ {
			if in.Nums[i]+in.Nums[j] == 10 {
				found = i*10 + j
				break outer
			}
		}
	}
	x := 1
	if x := 2; x > 1 {
		out = append(out, strconv.Itoa(x))
	}
	out = append(out, strconv.Itoa(x))
	return BizOut{List: out, N: found}, nil
}

// ---- 时间计算 ----

func BizTime(ctx workflow.Context, in BizIn) (BizOut, error) {
	return BizTimeCalc(workflow.Now(ctx)), nil
}

func BizTimeCalc(now time.Time) BizOut {
	deadline := now.Add(36 * time.Hour)
	out := []string{now.Format(time.RFC3339), deadline.Format("2006-01-02 15:04"), now.Add(-24 * time.Hour).Format(time.DateOnly)}
	left := deadline.Sub(now)
	out = append(out, fmt.Sprintf("%d", int64(left/time.Hour)))
	out = append(out, strconv.FormatInt(now.Unix(), 10), strconv.FormatInt(now.UnixMilli(), 10))
	if deadline.After(now) && !deadline.Equal(now) {
		out = append(out, "after")
	}
	return BizOut{List: out}
}

type BizIsoOut struct {
	A map[string]int64
	B map[string]int
	C map[string]float64
	D map[string]string
	N int
}

func BizIsoMapPlusEqInt64(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	m := map[string]int64{}
	m["x"] += 5
	return BizIsoOut{A: m}, nil
}

func BizIsoMapPlusEqInt(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	m := map[string]int{}
	m["x"] += 5
	m["y"]++
	return BizIsoOut{B: m}, nil
}

func BizIsoMapPlusEqCall(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	m := map[string]int64{}
	for _, l := range in.Order.Lines {
		m[l.SKU] += bizLineTotal(l)
	}
	return BizIsoOut{A: m}, nil
}

func BizIsoMapPlusEqFloat(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	m := map[string]float64{}
	m["x"] += 1.5
	return BizIsoOut{C: m}, nil
}

func BizIsoMapPlusEqString(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	m := map[string]string{}
	m["x"] += "a"
	return BizIsoOut{D: m}, nil
}

func BizIsoMapPlusEqInputMap(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	m := in.Kv
	m["zz"] += 2
	m["a"] += 2
	n := 0
	for _, v := range m {
		n += v
	}
	return BizIsoOut{N: n}, nil
}

func BizIsoRecursiveClosure(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	var fib func(n int) int
	fib = func(n int) int {
		if n < 2 {
			return n
		}
		return fib(n-1) + fib(n-2)
	}
	return BizIsoOut{N: fib(10)}, nil
}

func BizIsoFuncVar(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	var pick func(a, b int) int
	if in.F > 1 {
		pick = func(a, b int) int { return a }
	} else {
		pick = func(a, b int) int { return b }
	}
	return BizIsoOut{N: pick(1, 2)}, nil
}

func BizIsoFloatDivZero(ctx workflow.Context, in BizIn) (BizIsoOut, error) {
	total := 0.0
	for range in.Nums {
		total += 1
	}
	ratio := in.F / total
	n := 0
	if ratio > 1e300 {
		n = 1
	}
	return BizIsoOut{N: n}, nil
}

type BizPlainIn struct{ X int }

type BizPlainOut struct{ N int }

func BizIsoMapPlusEqPlain(ctx workflow.Context, in BizPlainIn) (BizPlainOut, error) {
	m := map[string]int{}
	m["x"] += 5
	m["y"]++
	return BizPlainOut{N: m["x"]*10 + m["y"]}, nil
}

type BizCountOut struct {
	N      int
	Counts map[string]int
}

func BizIsoMapPlusEqOutField(ctx workflow.Context, in BizPlainIn) (BizCountOut, error) {
	m := map[string]int{}
	m["x"] += 5
	return BizCountOut{N: m["x"]}, nil
}

type BizTags []string

type BizTagsOut struct {
	Tags BizTags
	N    int
}

func BizIsoNamedSliceAppend(ctx workflow.Context, in BizPlainIn) (BizTagsOut, error) {
	var t BizTags
	t = append(t, "a")
	t = append(t, "b")
	return BizTagsOut{Tags: t, N: len(t)}, nil
}

func BizIsoNamedSliceLiteralAppend(ctx workflow.Context, in BizPlainIn) (BizTagsOut, error) {
	t := BizTags{}
	t = append(t, "a")
	return BizTagsOut{Tags: t, N: len(t)}, nil
}

func bizCollectTags(xs []string) BizTags {
	var out BizTags
	for _, x := range xs {
		out = append(out, x)
	}
	return out
}

func BizIsoNamedSliceHelper(ctx workflow.Context, in BizPlainIn) (BizTagsOut, error) {
	t := bizCollectTags([]string{"x", "y"})
	return BizTagsOut{Tags: t, N: len(t)}, nil
}

type BizTagsIn struct{ Tags BizTags }

func BizIsoNamedSliceInputAppend(ctx workflow.Context, in BizTagsIn) (BizTagsOut, error) {
	t := in.Tags
	t = append(t, "z")
	n := 0
	for range t {
		n++
	}
	return BizTagsOut{Tags: t, N: n}, nil
}

func BizIsoInputFieldAssign(ctx workflow.Context, in BizPlainIn) (BizPlainOut, error) {
	in.X = in.X + 1
	return BizPlainOut{N: in.X}, nil
}

func BizIsoInputIncr(ctx workflow.Context, in BizPlainIn) (BizPlainOut, error) {
	in.X++
	return BizPlainOut{N: in.X}, nil
}

func BizIsoInputReassign(ctx workflow.Context, in BizPlainIn) (BizPlainOut, error) {
	if in.X == 0 {
		in = BizPlainIn{X: 7}
	}
	return BizPlainOut{N: in.X}, nil
}

func bizBump(p BizPlainIn) int {
	p.X += 10
	return p.X
}

func BizIsoHelperParamAssign(ctx workflow.Context, in BizPlainIn) (BizPlainOut, error) {
	return BizPlainOut{N: bizBump(in)}, nil
}

func BizIsoInputCopyAssign(ctx workflow.Context, in BizPlainIn) (BizPlainOut, error) {
	st := in
	st.X++
	return BizPlainOut{N: st.X*100 + in.X}, nil
}
