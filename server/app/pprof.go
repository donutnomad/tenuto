package app

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

// pprof 采样率：只在开启 WORKFLOW_PPROF_ADDR 时设置。mutex 记 1/100 的争用事件；block 对阻塞超过 100µs 的
// 事件按时长采样，开销小，足以看出锁与 channel 等待的位置。
const (
	pprofMutexFraction = 100
	pprofBlockRate     = int(100 * time.Microsecond)
)

// startPprof 在 addr 上提供 /debug/pprof/。调试用：不鉴权、不走业务 HTTP 的中间件，只应绑定本机或内网地址。
func (a *application) startPprof(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen pprof on %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	runtime.SetMutexProfileFraction(pprofMutexFraction)
	runtime.SetBlockProfileRate(pprofBlockRate)
	a.pprofServer = &http.Server{Handler: mux, ReadHeaderTimeout: defaultReadHeaderTimeout}
	go func() {
		if err := a.pprofServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.telemetry.Logger.Error("pprof listener stopped", "err", err)
		}
	}()
	a.telemetry.Logger.Warn("pprof debug listener is enabled; do not expose it publicly", "address", listener.Addr().String())
	return nil
}
