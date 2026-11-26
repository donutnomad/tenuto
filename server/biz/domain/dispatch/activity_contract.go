package dispatch

import (
	"fmt"
	"slices"
	"strings"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
)

// ActivityContractReport 是一次 Activity 契约对照的结果：Mismatched 逐个维度给出
// Workflow 声明的值与 Worker 上报的值。
type ActivityContractReport struct {
	Namespace  string
	Type       string
	TaskQueue  string
	BuildID    string
	Identity   string
	Declared   protocol.ActivityContract
	Reported   []protocol.ActivityContract
	Mismatched []protocol.ActivityContractFieldReport
}

// Compatible 报告声明的维度是否全部匹配。
func (r ActivityContractReport) Compatible() bool { return len(r.Mismatched) == 0 }

// MismatchError 生成不兼容时的错误，包含 activity type、期望/实际值与 worker identity。
func (r ActivityContractReport) MismatchError() error {
	fields := make([]string, 0, len(r.Mismatched))
	for _, report := range r.Mismatched {
		fields = append(fields, fmt.Sprintf("%s expected %q actual %q", report.Field, report.Expected, report.Actual))
	}
	return protocol.InvalidRequestf(
		"activity %q contract mismatch on worker %q (namespace %q queue %q build %q): %s",
		r.Type, r.Identity, r.Namespace, r.TaskQueue, r.BuildID, strings.Join(fields, ", "),
	)
}

// CheckActivityContract 把 Workflow 静态声明的契约与 route 上 Worker 的上报逐维度对照。
//
// 只比较 Workflow 声明过的维度；Worker 上报的值不同、或没有上报该维度，都记为 Mismatched。
// Workflow 没有声明任何维度时不对照。
func CheckActivityContract(declared protocol.ActivityContract, route ActivityRoute) ActivityContractReport {
	report := ActivityContractReport{
		Namespace: protocol.NormalizeNamespace(route.Namespace),
		Type:      route.Type,
		TaskQueue: route.TaskQueue,
		BuildID:   route.BuildID,
		Identity:  route.Identity,
		Declared:  declared,
		Reported:  route.Contracts,
	}
	if declared.HasDeclaration() {
		report.Mismatched = protocol.CompareActivityContracts(declared, route.Contracts)
	}
	return report
}

// contractCompatibleRoutes 在候选 route 中优先保留与声明契约不冲突的 worker。
//
// 同一 activity type 可能同时存在多个活跃 build 或队列（滚动发布），只按 build/weight
// 选路会在有兼容 worker 时选中不兼容的那个的队列。声明未携带契约时全部候选都兼容；
// 全部候选都不兼容时返回原候选：任务照常入队，领取时的契约对照拒绝不兼容的 Worker。
func contractCompatibleRoutes(routes []ActivityRoute, ref ActivityRouteRef) []ActivityRoute {
	declared := ref.declaredContract()
	if !declared.HasDeclaration() {
		return routes
	}
	compatible := filterActivityRoutes(routes, func(route ActivityRoute) bool {
		return CheckActivityContract(declared, route).Compatible()
	})
	if len(compatible) == 0 {
		return routes
	}
	return compatible
}

// DeclaredActivityContracts 按 ActivityType 索引 Workflow 静态声明的契约。
// 同一个 Type 只保留第一次带声明的那条引用：契约属于协议身份，与步骤 ID 无关。
func DeclaredActivityContracts(wf *program.WorkflowConfig) map[string]protocol.ActivityContract {
	refs := ActivityRouteRefsForWorkflow(wf)
	var descriptors map[string]program.WLangTypeDescriptor
	if wf != nil {
		descriptors = wf.Program.Types
	}
	contracts := make(map[string]protocol.ActivityContract, len(refs))
	for _, ref := range refs {
		contract := protocol.ActivityContract{Type: ref.Type, Input: ref.Input, Output: ref.Output}
		if !contract.HasDeclaration() {
			continue
		}
		// 声明侧存的是 Program.Types 的 descriptor ID；对照必须基于 wire 结构，
		// 因此先转成与 Go 包名/类型名无关的 wire 指纹。
		if fingerprint, err := program.WLangWireFingerprint(contract.Input, descriptors); err == nil {
			contract.Input = fingerprint
		}
		if fingerprint, err := program.WLangWireFingerprint(contract.Output, descriptors); err == nil {
			contract.Output = fingerprint
		}
		if _, ok := contracts[ref.Type]; ok {
			continue
		}
		contracts[ref.Type] = contract
	}
	return contracts
}

// DeclaredActivityContract 从 Workflow 的静态引用中取出 activityType 的契约声明；没有声明时返回 nil。
func DeclaredActivityContract(wf *program.WorkflowConfig, activityType string) *protocol.ActivityContract {
	if wf == nil || activityType == "" {
		return nil
	}
	contract, ok := DeclaredActivityContracts(wf)[activityType]
	if !ok {
		return nil
	}
	return &contract
}

// CheckDeclaredActivityContract 对照 Workflow 对 activityType 的契约声明与 route 上 Worker 的上报。
// Workflow 没有声明契约时不对照；声明了而上报的值不同或缺失时返回错误。领取时经 CheckClaimingWorkerContract
// 用它校验领取的那个 Worker。
func CheckDeclaredActivityContract(wf *program.WorkflowConfig, route ActivityRoute, activityType string) error {
	declared := DeclaredActivityContract(wf, activityType)
	if declared == nil {
		return nil
	}
	report := CheckActivityContract(*declared, route)
	if !report.Compatible() {
		return report.MismatchError()
	}
	return nil
}

// CheckClaimingWorkerContract 在 Worker 开始一次 attempt 之前，对照 Workflow 的契约声明与这个 Worker 自己的上报。
// registrations 是 identity 这个 Worker 的全部登记（包括心跳已过期的），buildID 是它领取时报告的 build。
// 这个 build 没有为 activityType 登记时按没有上报对照：Workflow 声明了契约就判为不兼容。
func CheckClaimingWorkerContract(wf *program.WorkflowConfig, registrations []protocol.ActivityWorkerVersion, namespace, identity, buildID, taskQueue, activityType string) error {
	route := ActivityRoute{Namespace: namespace, Identity: identity, Type: activityType, TaskQueue: taskQueue, BuildID: buildID}
	for _, worker := range registrations {
		if worker.BuildID == buildID && slices.Contains(worker.ActivityTypes, activityType) {
			route.Contracts = reportedContractsFor(worker.Contracts, activityType)
			break
		}
	}
	return CheckDeclaredActivityContract(wf, route, activityType)
}
