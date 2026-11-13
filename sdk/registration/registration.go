// Package registration 携带随二进制发布的 Workflow 定义，并在 Worker 启动时登记。
//
// 生成器在 Workflow 源码包里写出 zz_<pkg>_artifacts_gen.go，把同目录的 workflow.json
// embed 进二进制；作者把生成的 WorkflowDefinition() 放进 worker.Options.Workflows（sdk/worker）。
// 运行期不需要源码目录，也不需要磁盘上的 JSON。
package registration

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
)

// ArtifactFile 是 Definition 必须提供的文件名（Runtime 接受的 WorkflowConfig bundle）。
const ArtifactFile = "workflow.json"

// Definition 是一份随二进制发布的 Workflow 产物。
//
// 生成代码把 embed.FS 放进 FS；FS 只需要在根目录提供 workflow.json。
type Definition struct {
	FS fs.FS
}

// Load 读出 workflow.json 并返回 workflow id，便于错误信息定位到具体定义。
func (d Definition) Load() (workflowID string, workflowJSON []byte, err error) {
	if d.FS == nil {
		return "", nil, fmt.Errorf("registration: workflow definition has no filesystem")
	}
	data, err := fs.ReadFile(d.FS, ArtifactFile)
	if err != nil {
		return "", nil, fmt.Errorf("registration: read embedded %s: %w", ArtifactFile, err)
	}
	var header struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return "", nil, fmt.Errorf("registration: decode embedded %s: %w", ArtifactFile, err)
	}
	if header.ID == "" {
		return "", nil, fmt.Errorf("registration: embedded %s has an empty workflow id", ArtifactFile)
	}
	return header.ID, data, nil
}

// Registrar 登记一份 workflow.json 原文。
//
// Worker 经它连接的 Server 登记定义，作者不需要实现或配置它。
type Registrar interface {
	// RegisterWorkflowJSON 登记 workflowJSON 并返回它的内容版本：新内容首次出现时成为 latest，
	// 同一内容重复登记保持 latest。
	RegisterWorkflowJSON(ctx context.Context, namespace string, workflowJSON []byte) (version string, err error)
}
