package coroutine

// Channel 是 workflow.Channel 的数据结构与无 yield 的交付原语，语义与 Go channel 相同（含缓冲与关闭）。
// 谁 park、谁被唤醒，以及 Send/Receive 的检查-yield 循环与 WLang 值转换都由 interpreter 的 host 层驱动。
type Channel struct {
	name            string             // 可读名
	size            int                // 缓冲容量，0 为非缓冲
	buffer          []any              // 已缓冲的消息
	blockedSends    []*sendCallback    // 缓冲满时等待的发送方
	blockedReceives []*receiveCallback // 没有消息可取时等待的接收方
	closed          bool               // channel 是否已关闭
	// recValue 是接收前预取的值：Selector 命中 receive case 时预存，回调里的 Receive 优先取它。
	recValue *any
}

// sendCallback 是登记在 blockedSends 里的发送方。fn 返回 false 表示回调没有接受交付，值仍待交付。
type sendCallback struct {
	value any
	fn    func() bool
}

// receiveCallback 是登记在 blockedReceives 里的接收方。fn 返回 false 表示回调没有接受该值，值仍待交付。
type receiveCallback struct {
	fn func(v any, more bool) bool
}

// NewChannel 创建非缓冲 channel。
func NewChannel() *Channel {
	return &Channel{}
}

// NewBufferedChannel 创建容量为 size 的缓冲 channel。
func NewBufferedChannel(size int) *Channel {
	return &Channel{size: size}
}

// Len 返回待取的消息数：缓冲里的、阻塞的发送方，以及预取的值。
func (c *Channel) Len() int {
	result := len(c.buffer) + len(c.blockedSends)
	if c.recValue != nil {
		result = result + 1
	}
	return result
}

// receiveAsyncImpl 尝试取一个值。ok=true 表示取到值；more=false 表示 channel 已关闭且没有剩余值。
// 顺序：预取值 → 缓冲首个值（并把一个阻塞的发送方移入缓冲）→ 已关闭 → 逐个尝试阻塞的发送方 → 登记 callback。
func (c *Channel) receiveAsyncImpl(callback *receiveCallback) (v any, ok bool, more bool) {
	if c.recValue != nil {
		r := *c.recValue
		c.recValue = nil
		return r, true, true
	}
	if len(c.buffer) > 0 {
		r := c.buffer[0]
		c.buffer[0] = nil
		c.buffer = c.buffer[1:]

		// 缓冲腾出一个位置，把第一个接受交付的阻塞发送方移入缓冲。
		for len(c.blockedSends) > 0 {
			b := c.blockedSends[0]
			c.blockedSends[0] = nil
			c.blockedSends = c.blockedSends[1:]
			if b.fn() {
				c.buffer = append(c.buffer, b.value)
				break
			}
		}

		return r, true, true
	}
	if c.closed {
		return nil, false, false
	}
	for len(c.blockedSends) > 0 {
		b := c.blockedSends[0]
		c.blockedSends[0] = nil
		c.blockedSends = c.blockedSends[1:]
		if b.fn() {
			return b.value, true, true
		}
	}
	if callback != nil {
		c.blockedReceives = append(c.blockedReceives, callback)
	}
	return nil, false, true
}

// removeReceiveCallback 从 blockedReceives 移除 callback。
func (c *Channel) removeReceiveCallback(callback *receiveCallback) {
	for i, blockedCallback := range c.blockedReceives {
		if callback == blockedCallback {
			c.blockedReceives = append(c.blockedReceives[:i], c.blockedReceives[i+1:]...)
			break
		}
	}
}

// removeSendCallback 从 blockedSends 移除 callback。
func (c *Channel) removeSendCallback(callback *sendCallback) {
	for i, blockedCallback := range c.blockedSends {
		if callback == blockedCallback {
			c.blockedSends = append(c.blockedSends[:i], c.blockedSends[i+1:]...)
			break
		}
	}
}

// sendAsyncImpl 尝试发送 v。ok=true 表示值已交付给接收方或进入缓冲。
// 顺序：已关闭时 panic → 逐个尝试阻塞的接收方 → 缓冲未满则入缓冲 → 否则把 pair 登记进 blockedSends。
func (c *Channel) sendAsyncImpl(v any, pair *sendCallback) (ok bool) {
	if c.closed {
		panic("Closed channel")
	}
	for len(c.blockedReceives) > 0 {
		blockedGet := c.blockedReceives[0].fn
		c.blockedReceives[0] = nil
		c.blockedReceives = c.blockedReceives[1:]
		// 回调返回 false 表示值未被消费
		if blockedGet(v, true) {
			return true
		}
	}
	if len(c.buffer) < c.size {
		c.buffer = append(c.buffer, v)
		return true
	}
	if pair != nil {
		c.blockedSends = append(c.blockedSends, pair)
	}
	return false
}

// SendAsync 非阻塞发送：不能当场交付时返回 false，不登记发送方。
func (c *Channel) SendAsync(v any) (ok bool) {
	return c.sendAsyncImpl(v, nil)
}

// ReceiveAsync 非阻塞接收：不能当场取到值时返回 ok=false，不登记接收方。把值写进 WLang 变量由 host 层完成。
func (c *Channel) ReceiveAsync() (v any, ok bool, more bool) {
	return c.receiveAsyncImpl(nil)
}

// Close 关闭 channel，以 fn(nil, false) 唤醒全部阻塞的接收方。之后的发送会 panic（见 sendAsyncImpl）。
func (c *Channel) Close() {
	c.closed = true
	// 回调可能修改 blockedReceives，按副本迭代。
	cp := append(c.blockedReceives[:0:0], c.blockedReceives...)
	for _, callback := range cp {
		callback.fn(nil, false)
	}
}

// SendCallback 与 ReceiveCallback 是导出的 callback 句柄：sendCallback/receiveCallback 是本包私有类型，
// interpreter 的 host 层用它们构造回调并交给 SendWithCallback / ReceiveWithCallback。本层只提供构造与登记，
// yield、park 与值转换留在 host 层。
type SendCallback struct {
	inner *sendCallback
}

// ReceiveCallback 见 SendCallback。
type ReceiveCallback struct {
	inner *receiveCallback
}

// NewSendCallback 构造一个 send callback：值被接收方取走时调用 onConsumed。host 侧的回调总是接受交付。
func NewSendCallback(value any, onConsumed func()) *SendCallback {
	return &SendCallback{inner: &sendCallback{
		value: value,
		fn: func() bool {
			onConsumed()
			return true
		},
	}}
}

// NewReceiveCallback 构造一个 receive callback：有值交付或 channel 关闭时调用 onDelivered。host 侧的回调总是接受交付。
func NewReceiveCallback(onDelivered func(v any, more bool)) *ReceiveCallback {
	return &ReceiveCallback{inner: &receiveCallback{
		fn: func(v any, more bool) bool {
			onDelivered(v, more)
			return true
		},
	}}
}

// SendWithCallback 不 yield 地发送 v：consumed=true 表示值当场交付或进入缓冲；false 表示 cb 已登记进
// blockedSends，等接收方取走时触发，等待由 host 层的检查-yield 循环完成。
func (c *Channel) SendWithCallback(v any, cb *SendCallback) (consumed bool) {
	return c.sendAsyncImpl(v, cb.inner)
}

// ReceiveWithCallback 不 yield 地接收：未取到值时 cb 已登记进 blockedReceives，等发送方或关闭时触发。
func (c *Channel) ReceiveWithCallback(cb *ReceiveCallback) (v any, ok bool, more bool) {
	return c.receiveAsyncImpl(cb.inner)
}

// Closed 报告 channel 是否已关闭，host 层的 Send 循环每轮检查它。
func (c *Channel) Closed() bool {
	return c.closed
}

// RemoveReceiveCallback 移除 cb。host 层的 receive 循环重试前移除上一轮登记的 callback，
// 避免同一个接收方在 blockedReceives 里重复堆积。
func (c *Channel) RemoveReceiveCallback(cb *ReceiveCallback) {
	c.removeReceiveCallback(cb.inner)
}

// RemoveSendCallback 移除 cb，与 RemoveReceiveCallback 对称。
func (c *Channel) RemoveSendCallback(cb *SendCallback) {
	c.removeSendCallback(cb.inner)
}

// BlockedReceiveCount 返回登记在 blockedReceives 里的 callback 数，测试用它断言 Select 结束后没有残留 callback。
func (c *Channel) BlockedReceiveCount() int {
	return len(c.blockedReceives)
}

// BlockedSendCount 返回登记在 blockedSends 里的 callback 数，与 BlockedReceiveCount 对称。
func (c *Channel) BlockedSendCount() int {
	return len(c.blockedSends)
}
