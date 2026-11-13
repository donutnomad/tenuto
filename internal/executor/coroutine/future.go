package coroutine

// Future 是一次异步结果（值、错误、是否 ready）加一个内嵌 channel。Set 关闭这个 channel 广播 ready：
// Selector 的 future case 经 GetAsync 在 channel 上登记 callback，关闭时被触发。
type Future struct {
	value   any
	err     error
	ready   bool
	channel *Channel
}

// NewFuture 创建一个未 ready 的 Future。
func NewFuture() *Future {
	return &Future{channel: NewChannel()}
}

// Set 写入结果并关闭 channel，唤醒全部经 GetAsync 登记的 callback。重复 Set 会 panic。
func (f *Future) Set(value any, err error) {
	if f.ready {
		panic("already set")
	}
	f.value = value
	f.err = err
	f.ready = true
	f.channel.Close()
}

// IsReady 报告是否已 Set。
func (f *Future) IsReady() bool {
	return f.ready
}

// GetValueAndError 返回 Set 写入的值与错误。
func (f *Future) GetValueAndError() (any, error) {
	return f.value, f.err
}

// GetAsync 是 Selector 的试探原语。future 的 channel 从不发送、只在 Set 时关闭：receiveAsyncImpl 返回
// more=false 即已 ready；more=true 即未 ready，callback 已登记，等 Set 关闭 channel 时触发。
func (f *Future) GetAsync(callback *receiveCallback) (v any, ok bool, err error) {
	_, _, more := f.channel.receiveAsyncImpl(callback)
	if more {
		return nil, false, nil
	}
	if !f.ready {
		panic("not ready future channel closed") // channel 只在 Set 里关闭
	}
	return f.value, true, f.err
}

// RemoveReceiveCallback 移除未命中的 Selector case 登记的 callback。
func (f *Future) RemoveReceiveCallback(callback *receiveCallback) {
	f.channel.removeReceiveCallback(callback)
}
