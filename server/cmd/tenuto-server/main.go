package main

//go:generate go -C ../../.. tool gogen -async=true ./...

//go:generate go -C ../../.. run ./compiler/cmd/tenuto-workflow-gen generate ./docs/examples/versioned-workflow/v1 ./docs/examples/versioned-workflow/v2 ./docs/examples/startup-workflow ./docs/examples/saga-workflow ./docs/examples/multiprocess/contracts ./docs/examples/multiprocess/v1 ./docs/examples/multiprocess/v2 ./docs/examples/multiprocess/v3 ./docs/examples/upgrade-demo/appa/v1 ./docs/examples/upgrade-demo/appa/v2 ./docs/examples/upgrade-demo/appa/v3 ./docs/examples/upgrade-demo/appb/v1 ./docs/examples/upgrade-demo/appb/v2 ./cmd/tenuto-cluster/workload/order ./cmd/tenuto-cluster/workload/steps

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/donutnomad/tenuto/server/app"
	"github.com/donutnomad/tenuto/telemetry"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	os.Exit(run())
}

const usage = `usage: tenuto-server [migrate]

  tenuto-server          start the server; the database schema must be up to date
  tenuto-server migrate  apply pending schema migrations to WORKFLOW_DB_DSN and TASKQ_DB_DSN, then exit
`

func run() int {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	switch {
	case len(os.Args) == 1:
		return serve()
	case len(os.Args) == 2 && os.Args[1] == "migrate":
		return migrate()
	case len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help"):
		fmt.Print(usage)
		return 0
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
}

// migrate 执行 schema 迁移后退出。部署时在启动或升级 Server 之前运行一次；多个副本同时运行也安全。
func migrate() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("migrating Tenuto database schema", "version", version, "commit", commit)
	if err := app.Migrate(ctx, slog.Default()); err != nil {
		slog.Error("migrate Tenuto database schema failed", "err", err)
		return 1
	}
	return 0
}

func serve() int {
	cfg, err := app.LoadConfig()
	if err != nil {
		slog.Error("load Tenuto Server configuration failed", "err", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg.Telemetry.ServiceVersion = version
	observability, err := telemetry.New(ctx, cfg.Telemetry)
	if err != nil {
		slog.Error("initialize OpenTelemetry failed", "err", err)
		return 1
	}
	slog.SetDefault(observability.Providers().Logger)
	defer func() {
		if err := observability.Shutdown(context.Background()); err != nil {
			slog.Error("shutdown OpenTelemetry failed", "err", err)
		}
	}()
	slog.Info("starting Tenuto Server", "version", version, "commit", commit, "build_time", buildTime)
	if err := app.Run(ctx, cfg, app.WithTelemetry(observability.Providers())); err != nil {
		slog.Error("Tenuto Server stopped with an error", "err", err)
		return 1
	}
	slog.Info("Tenuto Server stopped")
	return 0
}
