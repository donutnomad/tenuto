package bizprobe

import (
	"fmt"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type ChanIn struct{ ID string }

type ChanOut struct {
	Text string
	Flag bool
}

// 直接 Receive 到 string。
func ChanString(ctx workflow.Context, in ChanIn) (ChanOut, error) {
	ch := workflow.NewChannel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		_ = workflow.Sleep(ctx, time.Second)
		ch.Send(ctx, "hello")
	})
	var s string
	ch.Receive(ctx, &s)
	return ChanOut{Text: s}, nil
}

// Selector 回调里 Receive 到 string。
func ChanSelectString(ctx workflow.Context, in ChanIn) (ChanOut, error) {
	ch := workflow.NewChannel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		_ = workflow.Sleep(ctx, time.Second)
		ch.Send(ctx, "timeout")
	})
	got := ""
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(ctx, ch, func(c workflow.Channel, more bool) {
		var s string
		c.Receive(ctx, &s)
		got = s
	})
	sel.Select(ctx)
	return ChanOut{Text: got}, nil
}

// Selector 回调里 Receive 到 bool。
func ChanSelectBool(ctx workflow.Context, in ChanIn) (ChanOut, error) {
	ch := workflow.NewChannel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		_ = workflow.Sleep(ctx, time.Second)
		ch.Send(ctx, true)
	})
	got := false
	sel := workflow.NewSelector(ctx)
	sel.AddReceive(ctx, ch, func(c workflow.Channel, more bool) {
		var v bool
		c.Receive(ctx, &v)
		got = v
	})
	sel.Select(ctx)
	return ChanOut{Flag: got}, nil
}

// 发送结构体与 int，分别接收到同类型的目标。
func ChanStructAndInt(ctx workflow.Context, in ChanIn) (ChanOut, error) {
	ch := workflow.NewBufferedChannel(ctx, 2)
	ch.Send(ctx, ChanOut{Text: in.ID, Flag: true})
	ch.Send(ctx, 7)
	var o ChanOut
	ch.Receive(ctx, &o)
	var n int
	ch.Receive(ctx, &n)
	o.Text = fmt.Sprint(o.Text, "-", n+1)
	return o, nil
}
