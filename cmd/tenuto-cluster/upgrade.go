package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

// upgradeConfig 是 upgrade 模式的参数：负载与 failover 相同，场景是滚动升级。
type upgradeConfig struct {
	orderRun
	ServerBinA  string
	ServerBinB  string
	UpgradeAt   time.Duration
	Gap         time.Duration
	StopTimeout time.Duration
}

func parseUpgradeFlags(args []string) (upgradeConfig, error) {
	var cfg upgradeConfig
	fs := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	cfg.register(fs)
	fs.StringVar(&cfg.ServerBinA, "server-bin-a", "", "tenuto-server the cluster starts on (default: -server-bin, or build ./server/cmd/tenuto-server)")
	fs.StringVar(&cfg.ServerBinB, "server-bin-b", "", "tenuto-server the replicas are upgraded to (default: the same binary as A)")
	fs.DurationVar(&cfg.UpgradeAt, "upgrade-at", 15*time.Second, "start the upgrade this long after the first workflow start")
	fs.DurationVar(&cfg.Gap, "upgrade-gap", 10*time.Second, "wait after a replica is ready on B before stopping the next one")
	fs.DurationVar(&cfg.StopTimeout, "stop-timeout", 30*time.Second, "SIGTERM grace period of a replica before SIGKILL")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	if cfg.UpgradeAt < 0 || cfg.Gap < 0 || cfg.StopTimeout <= 0 {
		return cfg, errors.New("-upgrade-at and -upgrade-gap must not be negative, -stop-timeout must be positive")
	}
	return cfg, nil
}

// runUpgrade 在 A 上启动集群并开始负载；-upgrade-at 后用 B 执行 tenuto-server migrate，再逐个副本：
// 从负载均衡器摘除、SIGTERM 优雅停止、换成 B 启动、等就绪后加回，间隔 -upgrade-gap。负载在整个过程中
// 持续启动；全部 Run 关闭后按 failover 的核对项核对。副本优雅停止超时被 SIGKILL、B 启动失败与 B 的
// migrate 失败都是违规。
func runUpgrade(ctx context.Context, args []string) error {
	cfg, err := parseUpgradeFlags(args)
	if err != nil {
		return err
	}
	if cfg.ServerBinA != "" {
		cfg.ServerBin = cfg.ServerBinA
	}
	binA, err := buildServer(ctx, cfg.commonConfig)
	if err != nil {
		return err
	}
	binB := cfg.ServerBinB
	if binB == "" {
		binB = binA
	}
	startWindow := time.Duration(float64(cfg.Workflows) / cfg.StartRate * float64(time.Second))
	if startWindow < cfg.UpgradeAt+time.Duration(cfg.Replicas)*cfg.Gap {
		fmt.Printf("WARNING: workflows are started for %s only; the upgrade starts at %s and needs at least %s more: raise -workflows to keep traffic flowing\n",
			startWindow.Round(time.Second), cfg.UpgradeAt, time.Duration(cfg.Replicas)*cfg.Gap)
	}
	return runOrders(ctx, "upgrade", cfg.orderRun, binA, func(ctx context.Context, cl *cluster, start time.Time) ([]string, []string) {
		return rollingUpgrade(ctx, cl, cfg, binA, binB, start)
	})
}

func rollingUpgrade(ctx context.Context, cl *cluster, cfg upgradeConfig, binA, binB string, start time.Time) (notes, violations []string) {
	notes = append(notes, "server A: "+describeBinary(binA), "server B: "+describeBinary(binB))
	if sleepCtx(ctx, time.Until(start.Add(cfg.UpgradeAt))) != nil {
		return notes, nil
	}
	began := time.Now()
	cl.recordFault("migrate", "server B")
	if err := migrate(ctx, binB, cl.db.dsn()); err != nil {
		return notes, append(violations, fmt.Sprintf("migrate with server B failed: %v", err))
	}
	for i, replica := range cl.replicas {
		cl.lb.setUp(i, false)
		cl.recordFault(actionUpgradeStop, replica.name)
		if !replica.stop(cfg.StopTimeout) {
			violations = append(violations, fmt.Sprintf("%s did not exit within %s of SIGTERM and was killed", replica.name, cfg.StopTimeout))
		}
		replica.setPath(binB)
		cl.recordFault(actionUpgradeStart, replica.name)
		if err := cl.startReplica(ctx, i); err != nil {
			if ctx.Err() != nil {
				return notes, violations
			}
			violations = append(violations, fmt.Sprintf("%s did not start on server B: %v", replica.name, err))
			continue
		}
		if i < len(cl.replicas)-1 && sleepCtx(ctx, cfg.Gap) != nil {
			return notes, violations
		}
	}
	notes = append(notes, fmt.Sprintf("rolling upgrade of %d replicas took %s", len(cl.replicas), time.Since(began).Round(time.Millisecond)))
	return notes, violations
}

// describeBinary 是二进制的路径与 SHA-256 前 12 位（区分 A 与 B 是否同一个构建）。
func describeBinary(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return path + " (" + err.Error() + ")"
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return path + " (" + err.Error() + ")"
	}
	return path + " sha256:" + hex.EncodeToString(hash.Sum(nil))[:12]
}
