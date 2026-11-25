// Package artifact 加载 `tenuto-workflow-gen` 产出的 JSON 产物，并封装两个注册入口。
//
// 产物目录里只有 workflow.json：程序与 Activity 契约（`activities` 字段）都在其中。
// 校验失败的错误里带文件名，便于直接定位到出错的那份 JSON。
package artifact

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	programpkg "github.com/donutnomad/tenuto/program"
)

// WorkflowFile 是生成器写出的产物文件名。
const WorkflowFile = "workflow.json"

// Artifacts 是一个产物目录的强类型视图。
type Artifacts struct {
	Dir string
	// Workflow 是 runtime 接受的 WorkflowConfig bundle；Activity 契约在 Workflow.Activities。
	Workflow *programpkg.WorkflowConfig
	// ContentVersion 是 workflow.json 的内容版本，与服务端登记后报告的版本相同
	// （program.WorkflowConfigVersion）。
	ContentVersion string
}

// LoadDir 读取并校验一个产物目录。
func LoadDir(dir string) (Artifacts, error) {
	artifacts := Artifacts{Dir: dir}
	workflowData, err := readRequired(dir, WorkflowFile)
	if err != nil {
		return Artifacts{}, err
	}
	workflow := &programpkg.WorkflowConfig{}
	if err := unmarshalFile(WorkflowFile, workflowData, workflow); err != nil {
		return Artifacts{}, err
	}
	if workflow.ID == "" {
		return Artifacts{}, fmt.Errorf("%s: workflow id 为空", filepath.Join(dir, WorkflowFile))
	}
	artifacts.Workflow = workflow

	version, err := programpkg.WorkflowConfigVersion(workflow)
	if err != nil {
		return Artifacts{}, err
	}
	artifacts.ContentVersion = version
	return artifacts, nil
}

// readRequired 读取必需文件。
func readRequired(dir, name string) ([]byte, error) {
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取 %s 失败: %w", path, err)
	}
	return data, nil
}

// unmarshalFile 解析 JSON，错误里带文件名。
func unmarshalFile(name string, data []byte, target any) error {
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("%s: JSON 解析失败: %w", name, err)
	}
	return nil
}
