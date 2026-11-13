// Command app-b 是订单应用的 Worker 进程。
//
//	app-b -url http://localhost:7400 -namespace upgrade-demo -version v1
//
// SDK 接入代码在 appb.Start（../../appb/worker.go）；这里只解析参数、处理退出信号。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appb"
)

func main() {
	url := flag.String("url", "http://localhost:7400", "Tenuto Server HTTP 地址")
	namespace := flag.String("namespace", "upgrade-demo", "namespace")
	version := flag.String("version", "v1", "发布版本：v1 / v2")
	flag.Parse()

	// SIGINT / SIGTERM 取消 ctx；Stop 取消 poll、等在途任务的处理协程结束，并向 Server 注销本 Worker 的路由。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w, err := appb.Start(ctx, appb.Options{URL: *url, Namespace: *namespace, Release: *version})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("app-b ready namespace=%s version=%s", *namespace, *version)
	<-ctx.Done()
	w.Stop()
}
