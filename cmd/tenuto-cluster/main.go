// Command tenuto-cluster 在一个 MySQL 上启动多个 tenuto-server 副本与独立的 Activity Worker 进程，
// 用于多副本故障注入（failover）、长时间故障浸泡（soak）、滚动升级演练（upgrade）与压测（bench）。
//
//	tenuto-cluster failover -dsn 'user:pass@tcp(127.0.0.1:3306)/' -db tenuto_e2e_1 [flags]
//	tenuto-cluster soak     -dsn 'user:pass@tcp(127.0.0.1:3306)/' -db tenuto_e2e_soak -duration 2h [flags]
//	tenuto-cluster upgrade  -dsn 'user:pass@tcp(127.0.0.1:3306)/' -db tenuto_e2e_up -server-bin-a A -server-bin-b B [flags]
//	tenuto-cluster bench    -dsn 'user:pass@tcp(127.0.0.1:3306)/' -db tenuto_bench_1 [flags]
//
// 工具只创建、删除 -db 指定的库（名字必须以 tenuto_e2e 或 tenuto_bench 开头，且事先不存在），
// 不重启、不改配置 MySQL 服务端：数据库与网络故障由进程内代理注入。只在 Unix 上运行（kill -9、SIGSTOP、
// ps、lsof）。用法与核对项见 docs/operations/cluster-testing.md。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "failover":
		err = runFailover(ctx, os.Args[2:])
	case "soak":
		err = runSoak(ctx, os.Args[2:])
	case "upgrade":
		err = runUpgrade(ctx, os.Args[2:])
	case "bench":
		err = runBench(ctx, os.Args[2:])
	case "worker":
		err = runWorker(ctx, os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenuto-cluster:", err)
		if errors.Is(err, errViolations) {
			os.Exit(1)
		}
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tenuto-cluster failover|soak|upgrade|bench [flags]   (tenuto-cluster <mode> -h lists the flags)")
}

// errViolations 表示核对发现了违反引擎保证的结果，进程以 1 退出；其他错误以 2 退出。
var errViolations = errors.New("verification found violations")

// defaultClusterDBMaxOpenConns 是 -db-max-open-conns 的缺省值：三副本 × 两个池 × 20 = 120 条，低于 MySQL 缺省的
// max_connections（151），给工具自己的连接与 migrate 留出余量。Server 的缺省值 50 时三副本最多 300 条，共用一个
// 缺省配置的 MySQL 会遇到 Error 1040。
const defaultClusterDBMaxOpenConns = 20

// commonConfig 是两种模式共用的集群参数。
type commonConfig struct {
	DSN               string
	DB                string
	Keep              bool
	Replicas          int
	Workers           int
	WorkerConcurrency int
	BasePort          int
	ServerBin         string
	Out               string
	Namespace         string
	ReadyTimeout      time.Duration
	ServerEnv         envList
	Proxies           bool
	Auth              bool
	// DBMaxOpenConns 是每个副本 Workflow 与 TaskQ 两个连接池各自的上限（WORKFLOW_DB_MAX_OPEN_CONNS）；0 不设置，
	// 用 Server 的缺省值。
	DBMaxOpenConns int
}

// register 登记通用参数；proxies 是 -proxies 的缺省值（故障模式开、bench 关：代理多一跳，会计入延迟）。
func (c *commonConfig) register(fs *flag.FlagSet, defaultDB string, proxies bool) {
	fs.StringVar(&c.DSN, "dsn", os.Getenv("TENUTO_CLUSTER_DSN"), "MySQL DSN without a database name, e.g. 'user:pass@tcp(127.0.0.1:3306)/' (env TENUTO_CLUSTER_DSN)")
	fs.StringVar(&c.DB, "db", defaultDB, "database to create; must start with tenuto_e2e or tenuto_bench and must not exist")
	fs.BoolVar(&c.Keep, "keep", false, "keep the database after the run (default: drop it)")
	fs.IntVar(&c.Replicas, "replicas", 3, "tenuto-server replicas")
	fs.IntVar(&c.Workers, "workers", 2, "Activity worker processes")
	fs.IntVar(&c.WorkerConcurrency, "worker-concurrency", 16, "Activity slots per worker process")
	fs.IntVar(&c.BasePort, "base-port", 17400, "load balancer port; replica i serves HTTP on base+1+i, matching gRPC on base+101+i, its matching proxy on base+201+i, its database proxy on base+301+i and pprof on base+401+i")
	fs.StringVar(&c.ServerBin, "server-bin", "", "tenuto-server binary (default: build ./server/cmd/tenuto-server into -out/bin)")
	fs.StringVar(&c.Out, "out", "", "output directory for logs, execution records and reports (default .run/tenuto-cluster/<db>)")
	fs.StringVar(&c.Namespace, "namespace", "cluster", "namespace of the workload")
	fs.DurationVar(&c.ReadyTimeout, "ready-timeout", 90*time.Second, "how long a replica or worker may take to become ready")
	fs.Var(&c.ServerEnv, "server-env", "extra KEY=VALUE for every replica (repeatable), e.g. -server-env WORKFLOW_RECONCILE_INTERVAL=10s")
	fs.BoolVar(&c.Auth, "auth", false, "enable Bearer authentication: replicas get a random WORKFLOW_HTTP_AUTH_TOKEN, workers and the tool's clients send it (every -server-bin must support it)")
	fs.IntVar(&c.DBMaxOpenConns, "db-max-open-conns", defaultClusterDBMaxOpenConns, "WORKFLOW_DB_MAX_OPEN_CONNS for every replica: the cap of each of its two pools (Workflow and TaskQ), so the cluster opens at most replicas x 2 x this many MySQL connections; 0 keeps the server default (50)")
	fs.BoolVar(&c.Proxies, "proxies", proxies, "route each replica's MySQL connections and the replicas' matching traffic through in-process proxies (required by db-*, partition and isolate faults)")
}

func (c *commonConfig) validate() error {
	if c.DSN == "" {
		return errors.New("-dsn (or TENUTO_CLUSTER_DSN) is required")
	}
	if _, err := parseBaseDSN(c.DSN); err != nil {
		return err
	}
	if err := validateDBName(c.DB); err != nil {
		return err
	}
	if c.Replicas < 1 || c.Workers < 1 || c.WorkerConcurrency < 1 {
		return errors.New("-replicas, -workers and -worker-concurrency must be at least 1")
	}
	if c.DBMaxOpenConns < 0 {
		return errors.New("-db-max-open-conns must not be negative")
	}
	if c.BasePort < 1024 || c.BasePort+401+c.Replicas > 65535 {
		return errors.New("-base-port out of range")
	}
	if c.Out == "" {
		c.Out = ".run/tenuto-cluster/" + c.DB
	}
	return nil
}

// envList 是可重复的 KEY=VALUE 参数。
type envList []string

func (e *envList) String() string { return strings.Join(*e, ",") }

func (e *envList) Set(value string) error {
	if !strings.Contains(value, "=") {
		return fmt.Errorf("%q is not KEY=VALUE", value)
	}
	*e = append(*e, value)
	return nil
}
