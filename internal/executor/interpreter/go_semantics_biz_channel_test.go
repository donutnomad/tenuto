package interpreter

import (
	"testing"
)

// Channel 的 Receive 与 Go 相同按目标类型接收：string、bool、int 与结构体；目标是接口类型时生成期报错。
func TestBizChannelReceiveTyped(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"ChanString", `{"Flag":false,"Text":"hello"}`},
		{"ChanSelectString", `{"Flag":false,"Text":"timeout"}`},
		{"ChanSelectBool", `{"Flag":true,"Text":""}`},
		{"ChanStructAndInt", `{"Flag":true,"Text":"c-8"}`},
	} {
		w := newBizWorld(t, tc.name, `{"ID": "c"}`)
		w.wft()
		w.fireTimers()
		w.wft()
		w.check(tc.name, tc.want)
	}
}
