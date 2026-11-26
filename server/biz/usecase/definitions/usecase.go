// Package definitions 编排 Workflow 配置的注册、启动期登记，以及注册期的 Activity 契约校验。
package definitions

import (
	"context"
	"encoding/json"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
)

// maxTargetWorkerIdentityLength 是 target_worker_identity 列的长度上限。
const maxTargetWorkerIdentityLength = 191

// ActivityRoutes 是注册期 Activity 契约校验用到的路由目录，由持有目录的执行核心提供。
type ActivityRoutes interface {
	// RefreshActivityRoutes 按 now 时刻活跃的 Worker 版本重建路由目录。
	RefreshActivityRoutes(now time.Time) error
	// ValidateWorkflowConfigInNamespace 校验 workflow 引用的 Activity 路由与契约；返回错误时配置不得激活。
	ValidateWorkflowConfigInNamespace(namespace string, workflow *programpkg.WorkflowConfig) error
}

// WorkerVersions 提供活跃的 Activity Worker 版本，用于判定目标 Worker 是否在线。
type WorkerVersions interface {
	ListActiveActivityWorkerVersions(now time.Time) ([]protocol.ActivityWorkerVersion, error)
}

// UseCase 负责 workflow 配置的校验、版本分配与持久化；Run 启动消费它返回的不可变引用。
type UseCase struct {
	configs definition.ConfigRepository
	routes  ActivityRoutes
	workers WorkerVersions
	clock   clock.Clock
}

func NewUseCase(configs definition.ConfigRepository, routes ActivityRoutes, workers WorkerVersions, clk clock.Clock) *UseCase {
	return &UseCase{configs: configs, routes: routes, workers: workers, clock: clk}
}

// RegisterWorkflowInNamespace 保存内容版本并把它设为 active。配置指定了目标 Worker 时，
// 该 Worker 必须在 namespace 内在线。
func (u *UseCase) RegisterWorkflowInNamespace(namespace string, workflow *programpkg.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	if err := u.routes.RefreshActivityRoutes(u.clock.Now()); err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	if workflow != nil && workflow.TargetWorkerIdentity != "" {
		if len(workflow.TargetWorkerIdentity) > maxTargetWorkerIdentityLength {
			return definition.WorkflowConfigRef{}, protocol.InvalidRequestf("target_worker_identity exceeds 191 characters")
		}
		workers, err := u.workers.ListActiveActivityWorkerVersions(u.clock.Now())
		if err != nil {
			return definition.WorkflowConfigRef{}, err
		}
		found := false
		for _, worker := range workers {
			if protocol.NormalizeNamespace(worker.Namespace) == namespace && worker.Identity == workflow.TargetWorkerIdentity {
				found = true
				break
			}
		}
		if !found {
			return definition.WorkflowConfigRef{}, protocol.InvalidRequestf("target worker %q is offline or unknown in namespace %q", workflow.TargetWorkerIdentity, namespace)
		}
	}
	return u.activateConfig(namespace, workflow)
}

// activateConfig 校验配置与 Activity 路由后保存并激活；校验失败时 active 保持不变。
func (u *UseCase) activateConfig(namespace string, workflow *programpkg.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	if err := definition.ValidateWorkflowConfig(namespace, workflow); err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	if err := u.routes.ValidateWorkflowConfigInNamespace(namespace, workflow); err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	return u.configs.SaveWorkflowConfigInNamespace(namespace, workflow)
}

// RegisterWorkflowArtifactInNamespace 只登记内容版本（幂等、按 hash 去重），active 保持不变。
// Worker 与应用启动加载既有产物走这条路径，旧版本重启时 active 留在原版本。
func (u *UseCase) RegisterWorkflowArtifactInNamespace(namespace string, workflow *programpkg.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	if err := definition.ValidateWorkflowConfig(namespace, workflow); err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	return u.configs.SaveWorkflowConfigArtifactInNamespace(namespace, workflow)
}

// RegisterWorkflowJSON 登记 Worker 随二进制发布的 workflow.json（启动期自动登记），返回内容版本。
func (u *UseCase) RegisterWorkflowJSON(ctx context.Context, namespace string, workflowJSON []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var workflow programpkg.WorkflowConfig
	if err := json.Unmarshal(workflowJSON, &workflow); err != nil {
		return "", protocol.InvalidRequestf("decode workflow artifact: %w", err)
	}
	ref, err := u.EnrollWorkflowInNamespace(namespace, &workflow)
	if err != nil {
		return "", err
	}
	return ref.Version, nil
}

// EnrollWorkflowInNamespace 登记启动期产物：新内容首次出现时激活，旧内容重复出现时
// 保持 active；active 尚未建立时激活已登记内容。
//
// 校验在写入前完成，校验失败时 active 保持不变。Activity 路由校验只在本次会产生激活时执行：
// 已存在内容 + 已有 active 的重复登记只做静态校验后幂等返回，旧 binary 重启在
// Activity worker 离线（路由 TTL 过期）时照常完成。
func (u *UseCase) EnrollWorkflowInNamespace(namespace string, workflow *programpkg.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	if workflow != nil && len(workflow.TargetWorkerIdentity) > maxTargetWorkerIdentityLength {
		return definition.WorkflowConfigRef{}, protocol.InvalidRequestf("target_worker_identity exceeds 191 characters")
	}
	if err := u.routes.RefreshActivityRoutes(u.clock.Now()); err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	namespace = protocol.NormalizeNamespace(namespace)
	if err := definition.ValidateWorkflowConfig(namespace, workflow); err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	version, err := programpkg.WorkflowConfigVersion(workflow)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	activating, err := u.configs.WorkflowConfigEnrollStateInNamespace(namespace, definition.WorkflowConfigRef{
		Namespace: namespace, WorkflowType: workflow.Type, Version: version,
	})
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	if activating {
		if err := u.routes.ValidateWorkflowConfigInNamespace(namespace, workflow); err != nil {
			return definition.WorkflowConfigRef{}, err
		}
	}
	ref, _, err := u.configs.EnrollWorkflowConfigInNamespace(namespace, workflow)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	return ref, nil
}

// SetWorkflowWorkerTarget 为新 Run 发布一个指向 identity 的不可变配置版本。
func (u *UseCase) SetWorkflowWorkerTarget(namespace, workflowType, identity string) (definition.WorkflowConfigRef, error) {
	if workflowType == "" {
		return definition.WorkflowConfigRef{}, protocol.InvalidRequestf("workflow_type is required")
	}
	ref, err := definition.ActiveWorkflowRef(u.configs, namespace, workflowType)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	workflow, err := u.configs.GetWorkflowConfig(ref)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	targeted := *workflow
	targeted.TargetWorkerIdentity = identity
	return u.RegisterWorkflowInNamespace(namespace, &targeted)
}
