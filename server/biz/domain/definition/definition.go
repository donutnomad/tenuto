// Package definition 是 Workflow 定义域：不可变的配置内容版本，以及 Run 引用版本的解析规则。
package definition

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	wlregistry "github.com/donutnomad/wlang/registry"
)

// WorkflowConfigRef 指向一个配置内容版本；Version 是配置 JSON 的内容 hash。
type WorkflowConfigRef struct {
	Namespace  string
	WorkflowID string
	Version    string
}

var ErrWorkflowConfigNotFound = errors.New("workflow config not found")

func CloneWorkflowConfig(wf *program.WorkflowConfig) (*program.WorkflowConfig, error) {
	data, err := json.Marshal(wf)
	if err != nil {
		return nil, fmt.Errorf("clone workflow config: %w", err)
	}
	var cloned program.WorkflowConfig
	if err := json.Unmarshal(data, &cloned); err != nil {
		return nil, fmt.Errorf("restore workflow config clone: %w", err)
	}
	return &cloned, nil
}

// ValidateWorkflowConfig 校验配置能否作为内容版本保存：ID 必填，WLang 程序产物有效，
// run_timeout 不超过 execution_timeout。
func ValidateWorkflowConfig(workflow *program.WorkflowConfig) error {
	if workflow == nil {
		return fmt.Errorf("workflow config is nil")
	}
	if workflow.ID == "" {
		return protocol.InvalidRequestf("workflow id is empty")
	}
	if err := workflow.Program.ValidateArtifact(); err != nil {
		return err
	}
	if workflow.RunTimeout != "" && workflow.ExecutionTimeout != "" {
		runTimeout, runErr := time.ParseDuration(workflow.RunTimeout)
		executionTimeout, executionErr := time.ParseDuration(workflow.ExecutionTimeout)
		if runErr == nil && executionErr == nil && runTimeout > executionTimeout {
			return protocol.InvalidRequestf("run_timeout (%s) must not exceed execution_timeout (%s)", workflow.RunTimeout, workflow.ExecutionTimeout)
		}
	}
	return nil
}

// ActiveWorkflowRef 返回 workflow 的 active 配置版本。每次都读仓储，
// 任一副本激活的版本对所有副本立即可见。没有 active 版本时返回满足
// errors.Is(err, ErrWorkflowConfigNotFound) 的错误，仓储故障按原因包装返回。
func ActiveWorkflowRef(configs ConfigRepository, namespace, workflowID string) (WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	ref, err := configs.LatestWorkflowConfigRefInNamespace(namespace, workflowID)
	if errors.Is(err, ErrWorkflowConfigNotFound) {
		return WorkflowConfigRef{}, fmt.Errorf("workflow %s/%s has no registered config version: %w", namespace, workflowID, ErrWorkflowConfigNotFound)
	}
	if err != nil {
		return WorkflowConfigRef{}, fmt.Errorf("load active config version of workflow %s/%s: %w", namespace, workflowID, err)
	}
	return ref, nil
}

// ResolveWorkflowConfigRef 解析 Run 要引用的配置版本：version 为空取 active，否则要求该版本已保存。
func ResolveWorkflowConfigRef(configs ConfigRepository, namespace, workflowID, version string) (WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	if workflowID == "" {
		return WorkflowConfigRef{}, protocol.InvalidRequestf("workflow id is empty")
	}
	if version == "" {
		return ActiveWorkflowRef(configs, namespace, workflowID)
	}
	ref := WorkflowConfigRef{Namespace: namespace, WorkflowID: workflowID, Version: version}
	if _, err := configs.GetWorkflowConfig(ref); err != nil {
		return WorkflowConfigRef{}, err
	}
	return ref, nil
}

// CheckEntryInput 校验 input 能被 ref 指向的配置的入口类型接受；入口没有类型化入参时放行。
func CheckEntryInput(configs ConfigRepository, ref WorkflowConfigRef, input map[string]any) error {
	workflow, err := configs.GetWorkflowConfig(ref)
	if err != nil {
		return err
	}
	entryID, descriptors, ok := workflow.EntryInputType()
	if !ok {
		return nil
	}
	registry := wlregistry.New()
	if err := registry.BindDescriptors(descriptors); err != nil {
		return protocol.InvalidRequestf("bind entry descriptors: %w", err)
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode entry input: %w", err)
	}
	if _, err := registry.DecodeJSONValue(entryID, raw); err != nil {
		return protocol.InvalidRequestf("input does not match entry type %q: %w", entryID, err)
	}
	return nil
}
