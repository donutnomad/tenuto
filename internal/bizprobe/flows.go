package bizprobe

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// 带 Activity、Signal、Selector、Update、Query、子 Workflow 与 ContinueAsNew 的业务 Workflow（go_semantics_biz_flows_test.go 驱动）。

type BizSt string

const (
	BizStDone    BizSt = "done"
	BizStRunning BizSt = "running"
)

type BizTags2 []string

type BizRes struct {
	ID     string
	Amount int64
	Items  []string
	Counts map[string]int
	Done   bool
	Status BizSt
	Tags   BizTags2
}

type BizFlowIn struct {
	ID      string
	Regions []string
	Need    int
	Amount  int64
}

type BizFlowOut struct {
	Text   string
	N      int
	Amount int64
	List   []string
	Counts map[string]int
}

// ---- Saga：成功的步骤登记补偿闭包，失败时倒序补偿 ----

func BizSaga(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	var compensations []func(ctx workflow.Context) error
	var log []string
	var res BizRes
	if err := workflow.ExecuteActivity(ctx, "reserve", in, opts).Get(ctx, &res); err != nil {
		return BizFlowOut{}, err
	}
	reservation := res.ID
	compensations = append(compensations, func(ctx workflow.Context) error {
		log = append(log, "release "+reservation)
		return workflow.ExecuteActivity(ctx, "release", reservation, opts).Get(ctx, nil)
	})
	var charge BizRes
	if err := workflow.ExecuteActivity(ctx, "charge", in, opts).Get(ctx, &charge); err != nil {
		for i := len(compensations) - 1; i >= 0; i-- {
			comp := compensations[i]
			if cerr := comp(ctx); cerr != nil {
				log = append(log, "comp failed: "+cerr.Error())
			}
		}
		return BizFlowOut{Text: "compensated: " + err.Error(), List: log}, nil
	}
	return BizFlowOut{Text: "ok", Amount: charge.Amount}, nil
}

// Saga 的另一种常见写法：只用一个补偿闭包变量，不放进 slice。
func BizSagaSingle(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	var res BizRes
	if err := workflow.ExecuteActivity(ctx, "reserve", in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &res); err != nil {
		return BizFlowOut{}, err
	}
	undo := func(ctx workflow.Context) error {
		return workflow.ExecuteActivity(ctx, "release", res.ID, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, nil)
	}
	var charge BizRes
	if err := workflow.ExecuteActivity(ctx, "charge", in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &charge); err != nil {
		if uerr := undo(ctx); uerr != nil {
			return BizFlowOut{}, fmt.Errorf("undo failed: %w (original: %v)", uerr, err)
		}
		return BizFlowOut{Text: "undone: " + err.Error()}, nil
	}
	return BizFlowOut{Text: "ok", Amount: charge.Amount}, nil
}

// ---- 并行扇出：futures slice，逐个 Get，聚合计数 ----

func BizFanOut(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	futures := []workflow.ActivityFuture{}
	for _, r := range in.Regions {
		futures = append(futures, workflow.ExecuteActivity(ctx, "count", r, opts))
	}
	totals := map[string]int{}
	var errs []string
	var amount int64
	for i, f := range futures {
		var res BizRes
		if err := f.Get(ctx, &res); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", in.Regions[i], err))
			continue
		}
		for k, v := range res.Counts {
			totals[k] = totals[k] + v
		}
		amount += res.Amount
	}
	return BizFlowOut{Counts: totals, List: errs, Amount: amount, N: len(futures)}, nil
}

// 扇出聚合的最常见写法：totals[k] += v。
func BizFanOutPlusEq(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	futures := []workflow.ActivityFuture{}
	for _, r := range in.Regions {
		futures = append(futures, workflow.ExecuteActivity(ctx, "count", r, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}))
	}
	totals := map[string]int{}
	for _, f := range futures {
		var res BizRes
		if err := f.Get(ctx, &res); err != nil {
			return BizFlowOut{}, err
		}
		for k, v := range res.Counts {
			totals[k] += v
		}
	}
	return BizFlowOut{Counts: totals}, nil
}

// ---- Selector：两个报价先到先得，带超时，结束后取消剩余 ----

func BizQuoteRace(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	cctx, cancel := workflow.WithCancel(ctx)
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	fa := workflow.ExecuteActivity(cctx, "quoteA", in, opts)
	fb := workflow.ExecuteActivity(cctx, "quoteB", in, opts)
	timeout := workflow.NewChannel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		if err := workflow.Sleep(cctx, 30*time.Second); err == nil {
			timeout.Send(ctx, "timeout")
		}
	})
	var quotes []string
	best := int64(-1)
	timedOut := false
	received := 0
	sel := workflow.NewSelector(ctx)
	take := func(name string, f workflow.ActivityFuture) {
		var q BizRes
		if err := f.Get(ctx, &q); err != nil {
			quotes = append(quotes, name+":err:"+err.Error())
		} else {
			quotes = append(quotes, name+":"+strconv.FormatInt(q.Amount, 10))
			if best < 0 || q.Amount < best {
				best = q.Amount
			}
		}
		received++
	}
	sel.AddFuture(ctx, fa, func(f workflow.ActivityFuture) { take("A", f) })
	sel.AddFuture(ctx, fb, func(f workflow.ActivityFuture) { take("B", f) })
	sel.AddReceive(ctx, timeout, func(ch workflow.Channel, more bool) {
		var s string
		ch.Receive(ctx, &s)
		timedOut = true
	})
	for i := 0; received < 2 && !timedOut; i++ {
		sel.Select(ctx)
	}
	cancel()
	return BizFlowOut{List: quotes, Amount: best, Text: fmt.Sprint(timedOut)}, nil
}

// ---- 审批：Signal 队列，去重，达到人数或被拒绝 ----

type BizApproval struct {
	User   string
	OK     bool
	Reason string
}

func BizApprovals(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	approvals := map[string]bool{}
	var order []string
	for i := 0; len(approvals) < in.Need; i++ {
		var sig BizApproval
		if err := workflow.WaitSignal(ctx, "approve").Get(ctx, &sig); err != nil {
			return BizFlowOut{}, err
		}
		user := strings.ToLower(strings.TrimSpace(sig.User))
		if _, dup := approvals[user]; dup {
			order = append(order, "dup:"+user)
			continue
		}
		approvals[user] = sig.OK
		order = append(order, user)
		if !sig.OK {
			return BizFlowOut{Text: "rejected by " + user + ": " + sig.Reason, List: order}, nil
		}
	}
	return BizFlowOut{Text: "approved", List: order, N: len(approvals)}, nil
}

// 审批带截止时间：Selector 同时等 Signal 与超时。
func BizApprovalDeadline(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	deadline := workflow.Now(ctx).Add(time.Hour)
	got := 0
	var users []string
	expired := false
	timer := workflow.NewChannel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		_ = workflow.Sleep(ctx, deadline.Sub(workflow.Now(ctx)))
		timer.Send(ctx, true)
	})
	for i := 0; got < in.Need && !expired; i++ {
		sel := workflow.NewSelector(ctx)
		sel.AddSignalFuture(ctx, workflow.WaitSignal(ctx, "approve"), func(f workflow.SignalFuture) {
			var a BizApproval
			_ = f.Get(ctx, &a)
			users = append(users, a.User)
			got++
		})
		sel.AddReceive(ctx, timer, func(ch workflow.Channel, more bool) {
			var v bool
			ch.Receive(ctx, &v)
			expired = true
		})
		sel.Select(ctx)
	}
	return BizFlowOut{N: got, List: users, Text: fmt.Sprint(expired)}, nil
}

// ---- 轮询重试：指数退避 ----

func BizPoll(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	var history []string
	for attempt := 1; attempt <= 5; attempt++ {
		var st BizRes
		err := workflow.ExecuteActivity(ctx, "poll", in.ID, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &st)
		switch {
		case err != nil && workflow.IsApplicationError(err, "Fatal"):
			return BizFlowOut{Text: "fatal", List: history}, err
		case err != nil:
			history = append(history, "err:"+err.Error())
		case st.Done && st.Status == BizStDone:
			history = append(history, string(st.Status))
			return BizFlowOut{N: attempt, Text: string(st.Status), List: history}, nil
		default:
			history = append(history, string(st.Status))
		}
		backoff := time.Duration(attempt*attempt) * time.Second
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
		if err := workflow.Sleep(ctx, backoff); err != nil {
			return BizFlowOut{}, err
		}
	}
	return BizFlowOut{Text: "gave up", List: history}, fmt.Errorf("poll %s: gave up after %d attempts", in.ID, 5)
}

// ---- ContinueAsNew：分页处理，把状态传给下一代 ----

type BizCanState struct {
	Iter   int
	Seen   map[string]int
	Log    []string
	Cursor string
	Tags   BizTags2
}

func BizCan(ctx workflow.Context, input BizCanState) (BizFlowOut, error) {
	st := input
	if st.Seen == nil {
		st.Seen = map[string]int{}
	}
	var page BizRes
	if err := workflow.ExecuteActivity(ctx, "fetch-page", st.Cursor, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &page); err != nil {
		return BizFlowOut{}, err
	}
	for _, it := range page.Items {
		seen := st.Seen
		seen[it] = seen[it] + 1
		log := st.Log
		log = append(log, it)
		st.Log = log
	}
	st.Iter++
	st.Cursor = page.ID
	if st.Iter < 3 && page.ID != "" {
		return BizFlowOut{}, workflow.ContinueAsNew(ctx, "", st)
	}
	return BizFlowOut{N: st.Iter, Counts: st.Seen, List: st.Log, Text: st.Cursor}, nil
}

// ---- 子 Workflow：结果里有具名 slice 字段与具名字符串字段 ----

func BizChildAgg(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	var futures []workflow.ChildWorkflowFuture
	for _, r := range in.Regions {
		futures = append(futures, workflow.ExecuteChildWorkflow(ctx, "region-report", BizFlowIn{ID: r}))
	}
	var tags []string
	n := 0
	for _, f := range futures {
		var res BizRes
		if err := f.Get(ctx, &res); err != nil {
			return BizFlowOut{}, err
		}
		if res.Status == BizStDone {
			n++
		}
		for _, t := range res.Tags {
			tags = append(tags, t)
		}
	}
	return BizFlowOut{N: n, List: tags}, nil
}

func BizChildTagsAppend(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	var res BizRes
	if err := workflow.ExecuteChildWorkflow(ctx, "region-report", in).Get(ctx, &res); err != nil {
		return BizFlowOut{}, err
	}
	tags := res.Tags
	tags = append(tags, "parent")
	return BizFlowOut{List: tags, N: len(tags)}, nil
}

// ---- Update 修改状态，Query 读取 ----

type BizAddItem struct {
	SKU string
	Qty int
}

type BizCart struct {
	Items      map[string]int
	History    []string
	Total      int
	CheckedOut bool
}

func BizCartFlow(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	cart := BizCart{Items: map[string]int{}}
	if err := workflow.SetUpdateHandler(ctx, "add", func(ctx workflow.Context, it BizAddItem) (BizFlowOut, error) {
		if it.Qty <= 0 {
			return BizFlowOut{}, fmt.Errorf("bad qty %d for %s", it.Qty, it.SKU)
		}
		items := cart.Items
		items[it.SKU] = items[it.SKU] + it.Qty
		hist := cart.History
		hist = append(hist, it.SKU)
		cart.History = hist
		cart.Total += it.Qty
		return BizFlowOut{N: len(cart.Items), Amount: int64(cart.Total)}, nil
	}); err != nil {
		return BizFlowOut{}, err
	}
	if err := workflow.SetUpdateHandler(ctx, "checkout", func(ctx workflow.Context) error {
		if cart.Total == 0 {
			return fmt.Errorf("empty cart")
		}
		cart.CheckedOut = true
		return nil
	}); err != nil {
		return BizFlowOut{}, err
	}
	if err := workflow.SetQueryHandler(ctx, "cart", func(ctx workflow.Context, input map[string]any) map[string]any {
		return map[string]any{"items": cart.Items, "history": cart.History, "total": cart.Total}
	}); err != nil {
		return BizFlowOut{}, err
	}
	if err := workflow.Await(ctx, func() bool { return cart.CheckedOut }); err != nil {
		return BizFlowOut{}, err
	}
	return BizFlowOut{N: cart.Total, Counts: cart.Items, List: cart.History}, nil
}

// Update handler 里最常见的写法：cart.Items[sku] += qty。
func BizCartPlusEq(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	cart := BizCart{Items: map[string]int{}}
	if err := workflow.SetUpdateHandler(ctx, "add", func(ctx workflow.Context, it BizAddItem) (BizFlowOut, error) {
		items := cart.Items
		items[it.SKU] += it.Qty
		cart.Total += it.Qty
		return BizFlowOut{N: len(cart.Items)}, nil
	}); err != nil {
		return BizFlowOut{}, err
	}
	if err := workflow.Await(ctx, func() bool { return cart.Total >= 3 }); err != nil {
		return BizFlowOut{}, err
	}
	return BizFlowOut{N: cart.Total, Counts: cart.Items}, nil
}

// ---- 取消：等待 Activity 时收到取消，用断开的 context 做清理 ----

func BizCancelCleanup(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	var res BizRes
	err := workflow.ExecuteActivity(ctx, "long", in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &res)
	if err != nil && workflow.IsCanceledError(err) {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		var c BizRes
		if cerr := workflow.ExecuteActivity(dctx, "cleanup", in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(dctx, &c); cerr != nil {
			return BizFlowOut{}, cerr
		}
		return BizFlowOut{Text: "cleaned " + c.ID}, err
	}
	if err != nil {
		return BizFlowOut{}, err
	}
	return BizFlowOut{Text: "done " + res.ID}, nil
}

func BizIsoUpdateParamAssign(ctx workflow.Context, in BizFlowIn) (BizFlowOut, error) {
	total := 0
	if err := workflow.SetUpdateHandler(ctx, "add", func(ctx workflow.Context, it BizAddItem) (BizFlowOut, error) {
		if it.Qty == 0 {
			it.Qty = 1
		}
		total += it.Qty
		return BizFlowOut{N: total}, nil
	}); err != nil {
		return BizFlowOut{}, err
	}
	if err := workflow.Await(ctx, func() bool { return total >= 2 }); err != nil {
		return BizFlowOut{}, err
	}
	return BizFlowOut{N: total}, nil
}
