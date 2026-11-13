package coroutine

// Selector 是 workflow.Selector 的数据结构与无 yield 的试探算法。谁 park、谁被唤醒，以及 WLang 回调
// （receive / send / future / default）的执行都由 interpreter 的 host 层驱动。
//
// coroutine 层不认识 WLang 闭包，所以 case 不存回调函数，只存 case 的种类、channel、发送值与声明序下标；
// TrySelect 只决定选中哪个 case（readyIndex）并预存接收到的值，host 层按 readyIndex 执行对应的 WLang 回调。
// 多个 case 同时 ready 时只选一个的护栏在本层的 callback 里。
type Selector struct {
	name  string
	cases []*selectCase // 按声明顺序累积的 case
	// hasDefault 记录是否有 default 分支；default 回调由 host 层持有并执行。
	hasDefault bool
	// readyIndex 是竞争护栏与选中结果：-1 表示尚无 case ready，>=0 是第一个 ready 的 case 下标。
	// 试探（本层 TrySelect）与检查-yield 循环（host 层）分在两层，所以它是字段而不是局部变量：TrySelect 未命中时
	// 登记 callback 后 park，channel 之后被 send/close 时 callback 设 readyIndex，host 醒来经 ReadyIndex() 重查。
	readyIndex int
	// readyMore 是命中 receive case 时回调应收到的 more：false 表示命中来自 channel 关闭，
	// 当场命中与 park 后被唤醒两条路径都记录，host 原样交给回调。
	readyMore bool
}

// caseKind 区分 case 种类：本层不存回调函数，用显式 kind 区分 receive、send 与 future。
type caseKind int

const (
	caseReceive caseKind = iota
	caseSend
	caseFuture
)

// selectCase 是一个 case：回调用 index 标识，host 层据此 InvokeClosure。
type selectCase struct {
	kind      caseKind
	channel   *Channel // receive 与 send case 的 channel
	sendValue any      // send case 要发送的值；receive case 忽略
	// index 是该 case 的声明序下标，恒等于 cases 切片下标；host 层拿 readyIndex 执行第 index 个 WLang 回调。
	index int
	// future 是 future case 的目标；其余 kind 为 nil。
	future *Future
	// consumed 表示 case 已被消费：future 一旦 ready 恒 ready，closed channel 每次试探都 ready，
	// 不排除已消费的 case，同一个 selector 重复 Select 会永远命中声明序第一个。host 执行回调时经 MarkConsumed 置位，
	// TrySelect 跳过已消费的 case。
	consumed bool
}

// NewSelector 创建一个空 Selector，readyIndex 初始为 -1（尚无 case ready）。
func NewSelector(name string) *Selector {
	return &Selector{name: name, readyIndex: -1}
}

// ReadyIndex 返回已 ready 的 case 下标（-1 表示没有）。host 的检查-yield 循环每次 park 醒来调用它重查：
// TrySelect 未命中时登记的 callback 会在 channel 之后被 send/close 时设置它。
func (s *Selector) ReadyIndex() int {
	return s.readyIndex
}

// ReadyMore 返回命中的 receive case 的 more（channel 关闭且没有剩余值时为 false）。
func (s *Selector) ReadyMore() bool {
	return s.readyMore
}

// AddReceive 追加一个 receive case，返回 s 以便链式调用。
func (s *Selector) AddReceive(c *Channel) *Selector {
	s.cases = append(s.cases, &selectCase{kind: caseReceive, channel: c, index: len(s.cases)})
	return s
}

// AddSend 追加一个发送 v 的 send case。
func (s *Selector) AddSend(c *Channel, v any) *Selector {
	s.cases = append(s.cases, &selectCase{kind: caseSend, channel: c, sendValue: v, index: len(s.cases)})
	return s
}

// AddFuture 追加一个 future case。future 的具体类型（Activity、timer、子 Workflow 等）由 host 层的 AddXxxFuture 校验。
func (s *Selector) AddFuture(f *Future) *Selector {
	s.cases = append(s.cases, &selectCase{kind: caseFuture, future: f, index: len(s.cases)})
	return s
}

// AddDefault 标记本 Selector 带 default 分支；default 回调由 host 层持有。
func (s *Selector) AddDefault() {
	s.hasDefault = true
}

// Reset 重置竞争护栏（readyIndex=-1），host 在每次 Select 入口调用。上一次 Select 返回时 host 已经运行
// cleanups 移除全部残留 callback，所以重置不会丢失已登记回调的信号。
func (s *Selector) Reset() {
	s.readyIndex = -1
}

// MarkConsumed 把第 index 个 case 标记为已消费。host 在执行回调前调用：future case 每次都调用，
// receive case 在 more==false（channel 关闭）命中时调用；send case 不调用。
func (s *Selector) MarkConsumed(index int) {
	if index >= 0 && index < len(s.cases) {
		s.cases[index].consumed = true
	}
}

// HasDefault 报告是否有 default 分支，host 在 TrySelect 返回 -1 时据此决定走 default 还是 park。
func (s *Selector) HasDefault() bool {
	return s.hasDefault
}

// TrySelect 按声明顺序试探一次各 case，不 yield；检查-yield 循环与 default 的执行在 host 层。
//
// 返回：
//   - readyIndex：第一个当场 ready 的 case 的声明序下标；没有 case 当场 ready 时为 -1。host 据此执行回调
//     （readyIndex>=0）、走 default（-1 且 HasDefault）或 park。
//   - cleanups：每个未命中、已把 callback 登记进 channel 阻塞队列的 case 各一个清理函数。host 在 Select
//     最终返回时（park 醒来命中，或走 default）全部运行，移除残留 callback。
//
// 竞争护栏：每个 callback 首先检查 s.readyIndex >= 0，已被占用时返回 false，值不被消费、留在它自己的 channel，
// 所以只有第一个 ready 的 case 被选中。
//
// receive case 命中时把值预存进 c.recValue，host 稍后执行回调、回调里的 c.Receive() 优先取这个值。
func (s *Selector) TrySelect() (readyIndex int, cleanups []func()) {
	// readyIndex 从 NewSelector / Reset 设的 -1 开始；这里不重置，以免抹掉 park 之后 callback 设的值。
	for _, pair := range s.cases {
		if pair.consumed {
			continue
		}
		switch pair.kind {
		case caseReceive:
			c := pair.channel
			idx := pair.index
			// 竞争护栏要求后触发的 callback 能返回 false 拒绝消费值，而 NewReceiveCallback 包装的 fn 恒为 true，
			// 所以这里直接构造 receiveCallback。
			callback := &receiveCallback{
				fn: func(v any, more bool) bool {
					if s.readyIndex >= 0 {
						return false
					}
					// 本层不执行 WLang 回调，只预存值并记下选中的 case，由 host 层执行回调。
					// channel 关闭唤醒时 more=false，不预存值，回调的 Receive 报告关闭。
					if more {
						c.recValue = &v
					}
					s.readyIndex = idx
					s.readyMore = more
					return true
				},
			}
			v, ok, more := c.receiveAsyncImpl(callback)
			if ok || !more { // 当场命中：有值，或 channel 已关闭
				// 占位，此前登记的 callback 之后触发时看到已被占用。
				s.readyIndex = pair.index
				s.readyMore = more
				// 有值时预存，供 host 回调里的 Receive 取值；channel 关闭时不预存，host 按 more 处理关闭语义。
				if more {
					c.recValue = &v
				}
				return s.readyIndex, cleanups
			}
			// callback 已进 blockedReceives，登记清理函数。
			cleanups = append(cleanups, func() {
				c.removeReceiveCallback(callback)
			})

		case caseSend:
			c := pair.channel
			idx := pair.index
			callback := &sendCallback{
				value: pair.sendValue,
				fn: func() bool {
					if s.readyIndex >= 0 {
						return false
					}
					// send case 的值由 blockedSend 移交，本层只记选中的 case，host 层执行回调。
					s.readyIndex = idx
					return true
				},
			}
			ok := c.sendAsyncImpl(pair.sendValue, callback)
			if ok { // 当场命中：值被 receiver 取走或进入缓冲
				s.readyIndex = pair.index
				return s.readyIndex, cleanups
			}
			// callback 已进 blockedSends，登记清理函数。
			cleanups = append(cleanups, func() {
				c.removeSendCallback(callback)
			})

		case caseFuture:
			fut := pair.future
			idx := pair.index
			// future 的 channel 只 Close 不 Send，callback 由 Set 之后的 Close 触发（fn(nil, false)）。
			// consumed 由 host 执行回调时经 MarkConsumed 置位。
			callback := &receiveCallback{
				fn: func(v any, more bool) bool {
					if s.readyIndex >= 0 {
						return false
					}
					s.readyIndex = idx
					return true
				},
			}
			_, ok, _ := fut.GetAsync(callback) // 未 ready 时 callback 已登记
			if ok {
				s.readyIndex = pair.index
				return s.readyIndex, cleanups
			}
			// callback 已进 future channel 的 blockedReceives，登记清理函数。
			cleanups = append(cleanups, func() {
				fut.RemoveReceiveCallback(callback)
			})
		}
	}
	// 没有 case 当场 ready：readyIndex 仍为 -1，host 走 default 或 park。之后某个 channel 被 send/close 时，
	// 已登记的 callback 设置 readyIndex（竞争护栏保证只选一个），host 醒来经 ReadyIndex() 重查。
	return s.readyIndex, cleanups
}
