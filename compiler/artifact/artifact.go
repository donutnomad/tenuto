// Package artifact 加载 `tenuto-workflow-gen` 产出的 JSON 产物，并封装两个注册入口。
//
// 产物目录里 workflow.json 是必需项；wlang.json / activities.json 存在时必须能解析。校验失败的错误里带文件名，
// 便于直接定位到出错的那份 JSON。
package artifact

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	programpkg "github.com/donutnomad/tenuto/program"
)

// 产物文件名。
const (
	WorkflowFile   = "workflow.json"
	ArtifactFile   = "wlang.json"
	ActivitiesFile = "activities.json"
)

// Activity 是 activities.json 的一项：Workflow 侧静态声明的 activity 契约。
//
// Input/Output 是与 Go 包名/类型名无关的 wire 指纹（config.WLangWireFingerprint，
// worker 侧对应 activity.ContractOf），不是 WLang type descriptor ID。
type Activity struct {
	Type    string `json:"type"`
	Version string `json:"version,omitempty"`
	Input   string `json:"input,omitempty"`
	Output  string `json:"output,omitempty"`
	Queue   string `json:"queue,omitempty"`
	Symbol  string `json:"symbol"`
}

// Manifest 把 WorkflowConfig 里的 activity 引用转成 activities.json 的清单项。
func Manifest(refs []programpkg.ActivityRef) []Activity {
	activities := make([]Activity, 0, len(refs))
	for _, ref := range refs {
		activities = append(activities, Activity{
			Type:    ref.Type,
			Version: ref.Version,
			Input:   ref.Input,
			Output:  ref.Output,
			Queue:   ref.TaskQueue,
			Symbol:  ref.Symbol,
		})
	}
	return activities
}

// Artifacts 是一个产物目录的强类型视图。
type Artifacts struct {
	Dir string
	// Workflow 是 runtime 接受的 WorkflowConfig bundle，必需。
	Workflow *programpkg.WorkflowConfig
	// Artifact/Activities 是可选产物，缺失时留零值。
	Artifact   *programpkg.WLangProgram
	Activities []Activity
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

	if data, ok, err := readOptional(dir, ArtifactFile); err != nil {
		return Artifacts{}, err
	} else if ok {
		wlang := &programpkg.WLangProgram{}
		if err := unmarshalFile(ArtifactFile, data, wlang); err != nil {
			return Artifacts{}, err
		}
		if wlang.Schema != programpkg.WLangProgramSchemaV2 {
			return Artifacts{}, fmt.Errorf("%s: schema %q 不是 WLang v2", filepath.Join(dir, ArtifactFile), wlang.Schema)
		}
		artifacts.Artifact = wlang
	}
	if data, ok, err := readOptional(dir, ActivitiesFile); err != nil {
		return Artifacts{}, err
	} else if ok {
		var activities []Activity
		if err := unmarshalFile(ActivitiesFile, data, &activities); err != nil {
			return Artifacts{}, err
		}
		for index, activity := range activities {
			if strings.TrimSpace(activity.Type) == "" {
				return Artifacts{}, fmt.Errorf("%s: 第 %d 项的 type 为空", filepath.Join(dir, ActivitiesFile), index+1)
			}
			if strings.TrimSpace(activity.Symbol) == "" {
				return Artifacts{}, fmt.Errorf("%s: 第 %d 项（%s）的 symbol 为空", filepath.Join(dir, ActivitiesFile), index+1, activity.Type)
			}
		}
		artifacts.Activities = activities
	}
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

// readOptional 读取可选文件；不存在时返回 ok=false。
func readOptional(dir, name string) ([]byte, bool, error) {
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("读取 %s 失败: %w", path, err)
	}
	return data, true, nil
}

// unmarshalFile 解析 JSON，错误里带文件名。
func unmarshalFile(name string, data []byte, target any) error {
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("%s: JSON 解析失败: %w", name, err)
	}
	return nil
}
