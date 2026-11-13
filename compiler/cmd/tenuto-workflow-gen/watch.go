package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
)

// watchInterval 是内容哈希轮询间隔；保存后重新生成通常在 250ms 内触发。
const watchInterval = 250 * time.Millisecond

// watchDirsOf 取本次生成读过源码的目录。只监听这些目录里的手写 Go 文件：
// 输出 JSON 与 zz_*_contract_gen.go 永不参与哈希，避免自循环。
func watchDirsOf(result sdkgen.PackageResult) []string {
	seen := map[string]bool{}
	var dirs []string
	add := func(dir string) {
		if dir == "" || seen[dir] {
			return
		}
		seen[dir] = true
		dirs = append(dirs, dir)
	}
	for _, file := range result.WatchFiles {
		add(filepath.Dir(file))
	}
	for _, bridge := range result.Bridges {
		add(filepath.Dir(bridge.Path))
	}
	add(result.Dir)
	sort.Strings(dirs)
	return dirs
}

// hashWatchDirs 计算被监听目录的合并内容哈希。只哈希手写 Go 源码。
func hashWatchDirs(dirs []string) (string, error) {
	digest := sha256.New()
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return "", fmt.Errorf("读取 %s 失败: %w", dir, err)
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || strings.HasPrefix(name, "zz_") {
				continue
			}
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			path := filepath.Join(dir, name)
			data, err := os.ReadFile(path)
			if err != nil {
				return "", fmt.Errorf("读取 %s 失败: %w", path, err)
			}
			if sdkgen.IsGeneratedSource(data) {
				continue
			}
			fmt.Fprintf(digest, "%s\x00", path)
			digest.Write(data)
			fmt.Fprintf(digest, "\x00")
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// watchDirsWithInterval 轮询依赖源码内容哈希，变化时调用 regenerate。dirs 每轮重新取：
// 重新生成可能引入新的依赖目录。失败时打印诊断并保留上次成功产物；后续保存会继续尝试。
func watchDirsWithInterval(ctx context.Context, dirs func() []string, regenerate func() error, stderr io.Writer, interval time.Duration) error {
	hash, err := hashWatchDirs(dirs())
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "watching %d dirs (ctrl-c to stop)\n", len(dirs()))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			next, err := hashWatchDirs(dirs())
			if err != nil {
				fmt.Fprintf(stderr, "watch failed: %v\n", err)
				continue
			}
			if next == hash {
				continue
			}
			hash = next
			if err := regenerate(); err != nil {
				fmt.Fprintf(stderr, "generation failed: %v\n", err)
			}
			// 新加入的依赖目录从当前内容开始比较，不会立即再触发一次。
			if rehashed, err := hashWatchDirs(dirs()); err == nil {
				hash = rehashed
			}
		}
	}
}

// watchAllDirs 监听多个已生成包的依赖源码：任一目录变化时重新生成全部目录。
// 首轮生成由调用方完成，因此多目录 + -watch 不会只处理第一个目录。监听目录随每次成功的
// 生成结果更新：后来才 import 的本地包、首轮失败后修好的包的依赖都会被监听。
func watchAllDirs(ctx context.Context, dirs []string, results []sdkgen.PackageResult, options generateOptions, stderr io.Writer, interval time.Duration) error {
	latest := map[string]sdkgen.PackageResult{}
	for _, result := range results {
		latest[result.Dir] = result
	}
	watchList := func() []string {
		current := make([]sdkgen.PackageResult, 0, len(latest))
		for _, result := range latest {
			current = append(current, result)
		}
		return watchDirsForPackages(current, dirs)
	}
	if len(watchList()) == 0 {
		return nil
	}
	return watchDirsWithInterval(ctx, watchList, func() error {
		// 逐个目录重生成并收集错误：一个目录失败不能截断其余目录，
		// 否则它们的本次修改会被 watch 哈希吞掉、长期停留在旧产物。
		var errs []error
		for _, dir := range dirs {
			result, err := generatePackage(ctx, dir, options)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", dir, err))
				continue
			}
			latest[result.Dir] = result
		}
		return errors.Join(errs...)
	}, stderr, interval)
}

// watchDirsForPackages 合并多个生成结果的监听目录与命令行传入的包目录（去重、排序）。
// 首轮生成失败的包没有结果，仍要监听它自己的目录，修好源码后才会触发重新生成。
func watchDirsForPackages(results []sdkgen.PackageResult, roots []string) []string {
	seen := map[string]bool{}
	var dirs []string
	add := func(dir string) {
		if seen[dir] {
			return
		}
		seen[dir] = true
		dirs = append(dirs, dir)
	}
	for _, result := range results {
		for _, dir := range watchDirsOf(result) {
			add(dir)
		}
	}
	for _, root := range roots {
		if abs, err := filepath.Abs(root); err == nil {
			add(abs)
		}
	}
	sort.Strings(dirs)
	return dirs
}
