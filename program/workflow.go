package program

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
)

// WorkflowConfig 是登记到服务端的 Workflow：一段 WLang 程序、程序引用的 Activity，以及运行期策略。
type WorkflowConfig struct {
	TargetWorkerIdentity string `json:"target_worker_identity,omitempty"`
	Type                 string `json:"type"`
	Name                 string `json:"name"`
	// Policy 是定义声明的版本策略：AutoUpgrade（缺省）或 Pinned。
	Policy string `json:"policy,omitempty"`
	// Version 是作者声明的协议/发布标签，与内容 hash 分开维护。
	Version string `json:"version,omitempty"`
	// Program 是 Workflow 的 WLang 程序，Worker 按 Event History 重放执行它。
	Program WLangProgram `json:"program"`
	// Activities 以程序里的 Activity 符号为键，给出路由与契约。
	Activities       map[string]ActivityRef `json:"activities,omitempty"`
	ExecutionTimeout string                 `json:"execution_timeout,omitempty"` // 整个 execution（所有代）的最大存活时间
	RunTimeout       string                 `json:"run_timeout,omitempty"`       // 单代 run 的最大存活时间
	RetryPolicy      *RetryPolicy           `json:"retry_policy,omitempty"`      // workflow 级重试策略
}

// ActivityRef 是程序里一个 Activity 符号的路由与契约。
type ActivityRef struct {
	Symbol    string `json:"symbol"`
	Type      string `json:"type"`
	TaskQueue string `json:"task_queue,omitempty"`
	// Input/Output 是 wire 结构指纹（忽略 Go 包名/类型名），用于与 worker 上报对照。
	Input  string `json:"input,omitempty"`
	Output string `json:"output,omitempty"`
}

// RetryPolicy 重试策略，定义在 protocol。
type RetryPolicy = protocol.RetryPolicy

// SymbolRefs 返回带类型的 Activity 引用，Symbol 缺省时取映射键。
func (wf *WorkflowConfig) SymbolRefs() map[string]ActivityRef {
	refs := make(map[string]ActivityRef, len(wf.Activities))
	for symbol, ref := range wf.Activities {
		if ref.Type == "" {
			continue
		}
		if ref.Symbol == "" {
			ref.Symbol = symbol
		}
		refs[symbol] = ref
	}
	return refs
}

// EntryInputType 返回 Workflow 入口的 input descriptor ID 与它所需的 Types 图，供调用方在跨
// Workflow 边界（child / continue-as-new）校验载荷。第二个返回值为 false 表示入口没有类型化入参。
func (wf *WorkflowConfig) EntryInputType() (string, map[string]WLangTypeDescriptor, bool) {
	if wf.Program.Entry.Input == "" {
		return "", nil, false
	}
	return wf.Program.Entry.Input, wf.Program.Types, true
}

// WorkflowConfigVersion 返回配置的内容版本：配置规范化 JSON 的 sha256 前 16 字节。
// 服务端以它标识登记的内容，生成器打印的也是它。
//
// 规范化把每个 JSON 对象的键排成有序：生成器产出的配置在 Program 里带着结构体，首次序列化按字段
// 声明顺序，经 HTTP 或存储读回后变成 map、按键名排序。两种形态内容相同，版本也相同。
func WorkflowConfigVersion(wf *WorkflowConfig) (string, error) {
	data, err := json.Marshal(wf)
	if err != nil {
		return "", fmt.Errorf("marshal workflow config version: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return "", fmt.Errorf("canonicalize workflow config version: %w", err)
	}
	canonical, err := json.Marshal(tree)
	if err != nil {
		return "", fmt.Errorf("canonicalize workflow config version: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:16]), nil
}
