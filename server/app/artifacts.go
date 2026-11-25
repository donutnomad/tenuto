package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/donutnomad/tenuto/program"
)

// registerWorkflowArtifacts 加载启动时配置的生成产物。
//
// 语义与发布协议一致：
//   - 默认只登记内容版本（按 hash 幂等），不切换 latest；旧 Worker 重启不会回退 latest。
//   - Publish=true 时把 latest 指向加载的产物；仅在显式部署/回滚时开启。
//
// 失败直接返回错误并阻止启动：调用方拿到具体文件与原因，保留上一个可用 latest。
func (a *application) registerWorkflowArtifacts(ctx context.Context) error {
	directions := a.cfg.Artifacts.Dirs
	if len(directions) == 0 {
		return nil
	}
	namespace := a.cfg.Artifacts.Namespace
	if namespace == "" {
		namespace = "default"
	}
	registered := 0
	published := 0
	for _, dir := range directions {
		paths, err := findWorkflowArtifacts(dir)
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			return fmt.Errorf("workflow artifacts: no workflow.json under %s", dir)
		}
		for _, path := range paths {
			workflow, err := loadWorkflowArtifact(path)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if a.cfg.Artifacts.Publish {
				ref, err := a.server.Definitions().RegisterWorkflowInNamespace(namespace, workflow)
				if err != nil {
					return fmt.Errorf("publish workflow artifact %s: %w", path, err)
				}
				published++
				a.telemetry.Logger.Info("published workflow artifact",
					slog.String("file", path),
					slog.String("workflow_type", ref.WorkflowType),
					slog.String("version", ref.Version),
				)
				continue
			}
			ref, err := a.server.Definitions().RegisterWorkflowArtifactInNamespace(namespace, workflow)
			if err != nil {
				return fmt.Errorf("register workflow artifact %s: %w", path, err)
			}
			registered++
			a.telemetry.Logger.Info("registered workflow artifact",
				slog.String("file", path),
				slog.String("workflow_type", ref.WorkflowType),
				slog.String("version", ref.Version),
			)
		}
	}
	a.telemetry.Logger.Info("workflow artifacts loaded",
		slog.Int("registered", registered),
		slog.Int("published", published),
		slog.String("namespace", namespace),
	)
	return nil
}

func findWorkflowArtifacts(dir string) ([]string, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("workflow artifacts: %w", err)
	}
	if !info.IsDir() {
		if filepath.Base(dir) != "workflow.json" {
			return nil, fmt.Errorf("workflow artifacts: %s is not a directory or workflow.json", dir)
		}
		return []string{dir}, nil
	}
	var paths []string
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Name() == "workflow.json" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("workflow artifacts: %w", err)
	}
	return paths, nil
}

func loadWorkflowArtifact(path string) (*program.WorkflowConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("workflow artifact %s: %w", path, err)
	}
	var workflow program.WorkflowConfig
	if err := json.Unmarshal(data, &workflow); err != nil {
		return nil, fmt.Errorf("workflow artifact %s: %w", path, err)
	}
	if workflow.Type == "" {
		return nil, fmt.Errorf("workflow artifact %s: workflow type is empty", path)
	}
	return &workflow, nil
}
