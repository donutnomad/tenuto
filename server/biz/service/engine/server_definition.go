package engine

import (
	"context"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// activityRouteCacheTTL 是 Activity 路由目录快照的有效期。
//
// 目录是数据库里 Worker 登记的快照。本副本上的登记与注销立即重建它；调度 Activity 时快照超过这个时长才重建，
// 其他副本上的登记与注销最多晚这么久生效。心跳只延长到期时间，不重建：登记的 TTL（默认 30s）远大于这个时长，
// 正常心跳的 Worker 在快照有效期内不会被判为过期。
const activityRouteCacheTTL = 5 * time.Second

// definitionRoutes 把 Server 持有的 Activity 路由目录提供给 definitions 用例。
// 校验每次都读取当前目录，目录由 loadActivityRoutes 替换。
type definitionRoutes struct{ server *Server }

func (r definitionRoutes) RefreshActivityRoutes(now time.Time) error {
	return r.server.refreshActivityWorkerVersions(now)
}

func (r definitionRoutes) ValidateWorkflowConfigInNamespace(namespace string, workflow *programpkg.WorkflowConfig) error {
	r.server.mu.Lock()
	catalog := r.server.activityRoutes
	r.server.mu.Unlock()
	return catalog.ValidateWorkflowConfigInNamespace(namespace, workflow)
}

// RegisterActivityWorkerVersion 登记一个 Activity Worker 的路由并立即重建路由目录。同进程 Worker 经它登记：Server 直接满足
// worker.ActivityWorkerVersionRegistryAPI。
func (s *Server) RegisterActivityWorkerVersion(ctx context.Context, worker protocol.ActivityWorkerVersion) error {
	worker.Namespace = protocol.NormalizeNamespace(worker.Namespace)
	ttl, err := dispatch.WorkerVersionTTL(worker)
	if err != nil {
		return err
	}
	if _, err := s.workerVersions.UpsertActivityWorkerVersionInNamespace(worker.Namespace, worker, s.clock.Now(), ttl); err != nil {
		return err
	}
	return s.refreshActivityWorkerVersions(s.clock.Now())
}

func (s *Server) HeartbeatActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error {
	namespace = protocol.NormalizeNamespace(namespace)
	return s.workerVersions.HeartbeatActivityWorkerVersionInNamespace(namespace, identity, buildID, s.clock.Now(), protocol.DefaultActivityWorkerVersionTTL)
}

// DeregisterActivityWorkerVersionInNamespace 注销一个 Activity Worker：它的路由立刻退出选路。
// Worker 正常停止时调用；进程崩溃时路由靠心跳 TTL 到期。
func (s *Server) DeregisterActivityWorkerVersionInNamespace(ctx context.Context, namespace, identity, buildID string) error {
	now := s.clock.Now()
	if err := s.workerVersions.DeregisterActivityWorkerVersionInNamespace(protocol.NormalizeNamespace(namespace), identity, buildID, now); err != nil {
		return err
	}
	return s.refreshActivityWorkerVersions(now)
}

// RegisterWorkflowJSON 登记 Worker 随二进制发布的 workflow.json（启动期自动登记）。Server 借它满足
// registration.Registrar，同进程 Worker 经这个接口登记。
//
// 新内容首次出现时激活 latest；同 hash 重复启动（旧二进制重启）不动 latest；
// latest 尚未建立时激活已登记内容。校验失败不改动 latest。
func (s *Server) RegisterWorkflowJSON(ctx context.Context, namespace string, workflowJSON []byte) (string, error) {
	return s.definitions.RegisterWorkflowJSON(ctx, namespace, workflowJSON)
}

func (s *Server) resolveActivityCommand(runID, programVersion string, cmd history.Command) (history.Command, error) {
	// Namespace、目标 Worker 与配置引用取自同一次读到的 Run。
	namespace := protocol.DefaultNamespace
	var wf *programpkg.WorkflowConfig
	run, wfErr := s.mgr.Get(runID)
	if wfErr == nil && run == nil {
		wfErr = execution.ErrRunNotFound
	}
	if wfErr == nil {
		namespace = protocol.NormalizeNamespace(run.Namespace)
		// target 只取 Run 自己的路由快照：AutoUpgrade 换内容不能顺带换 worker。
		cmd.TargetWorkerIdentity = run.TargetWorkerIdentity
		wf, wfErr = s.workflowConfigs.GetWorkflowConfig(runConfigRef(run, programVersion))
	}
	ref := dispatch.ActivityRouteRef{
		Namespace:            namespace,
		TargetWorkerIdentity: cmd.TargetWorkerIdentity,
		ActivityID:           cmd.ActivityID,
		Type:                 cmd.ActivityType,
		TaskQueue:            cmd.TaskQueue,
		RoutingKey:           activityCommandRoutingKey(namespace, runID, cmd.ActivityID, cmd.ActivityType),
	}
	// 声明契约参与选路：同一 type/queue 下有多个活跃 build 时，用契约兼容的 Worker 的上报对照。
	if wfErr == nil {
		if declared := dispatch.DeclaredActivityContract(wf, cmd.ActivityType); declared != nil {
			ref.Input, ref.Output = declared.Input, declared.Output
		}
	}
	route, err := s.resolveActivityRoute(ref)
	if err != nil {
		return history.Command{}, err
	}
	// 路由只确定队列：执行它的 build 不在这里决定，也不随 Activity 保存，任务投到队列由领取的 Worker 执行。
	cmd.TaskQueue = route.TaskQueue
	// 调度时确认声明的契约能被至少一个在线 Worker 满足：没有时命令不派发，WFT 失败保留 history，
	// 修复 worker 上报后既有 WFT retry 重新执行同一条命令即可恢复。领取时再按领取 Worker 自己的上报校验。
	if wfErr == nil {
		if err := dispatch.CheckDeclaredActivityContract(wf, route, cmd.ActivityType); err != nil {
			// 确定性契约冲突：任务不派发，暂停 Run 等待人工修复；同一配置不再无限重试。
			s.pauseRunForConfigError(runID, err)
			return history.Command{}, err
		}
	}
	return cmd, nil
}

func (s *Server) resolveActivityRoute(ref dispatch.ActivityRouteRef) (dispatch.ActivityRoute, error) {
	if err := s.loadActivityRoutes(s.clock.Now(), false); err != nil {
		return dispatch.ActivityRoute{}, err
	}
	s.mu.Lock()
	activityRoutes := s.activityRoutes
	s.mu.Unlock()
	if ref.TargetWorkerIdentity != "" && s.workerVersions != nil {
		return s.resolveTargetedActivityRoute(ref)
	}
	return activityRoutes.ResolveActivityRoute(ref)
}

// resolveTargetedActivityRoute 读取目标 Worker 的全部登记并按 dispatch.ResolveTargetedActivityRoute 选路。
func (s *Server) resolveTargetedActivityRoute(ref dispatch.ActivityRouteRef) (dispatch.ActivityRoute, error) {
	registrations, err := s.workerVersions.ListActivityWorkerVersionsOfWorker(ref.Namespace, ref.TargetWorkerIdentity)
	if err != nil {
		return dispatch.ActivityRoute{}, err
	}
	return dispatch.ResolveTargetedActivityRoute(registrations, ref)
}

func activityCommandRoutingKey(namespace, runID, activityID, activityType string) string {
	return protocol.NormalizeNamespace(namespace) + ":" + runID + ":" + activityID + ":" + activityType
}

// refreshActivityWorkerVersions 立即从数据库重建路由目录。
func (s *Server) refreshActivityWorkerVersions(now time.Time) error {
	return s.loadActivityRoutes(now, true)
}

// loadActivityRoutes 从数据库重建路由目录；force 为 false 时快照未超过 activityRouteCacheTTL 就沿用。
// 重建串行执行：每次读取都发生在上一次安装之后，后安装的快照不会比先安装的旧。
func (s *Server) loadActivityRoutes(now time.Time, force bool) error {
	if s.workerVersions == nil {
		return nil
	}
	s.activityRoutesLoadMu.Lock()
	defer s.activityRoutesLoadMu.Unlock()
	s.mu.Lock()
	loadedAt := s.activityRoutesLoadedAt
	s.mu.Unlock()
	if !force && !loadedAt.IsZero() && !now.Before(loadedAt) && now.Sub(loadedAt) < activityRouteCacheTTL {
		return nil
	}
	workers, err := s.workerVersions.ListActiveActivityWorkerVersions(now)
	if err != nil {
		return err
	}
	activityRoutes := dispatch.NewActivityRouteCatalog(s.clock)
	for _, worker := range workers {
		if err := activityRoutes.RegisterActivityWorkerVersion(worker); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activityRoutesLoadedAt = now
	s.activityRoutes = activityRoutes
	return nil
}

func (s *Server) activeWorkflowRefInNamespace(namespace, workflowType string) (definition.WorkflowConfigRef, error) {
	return definition.ActiveWorkflowRef(s.workflowConfigs, namespace, workflowType)
}

func (s *Server) childWorkflowConfigRef(namespace, workflowType string) (definition.WorkflowConfigRef, error) {
	if workflowType == "" {
		return definition.WorkflowConfigRef{}, protocol.InvalidRequestf("child workflow type is empty")
	}
	return s.workflowConfigRef(namespace, workflowType, "")
}

// checkEntryInput 校验跨 workflow 边界（child / continue-as-new）的载荷能被目标
// workflow 的入口类型接受。
//
// 为什么在这里而不在 executor 里：目标 workflow 的 config version 是服务端解析
// 命令时才确定的（空 version 取 latest），executor 产命令时并不知道目标是谁的哪个
// 版本，也就拿不到它的 Entry.Input。
//
// ⚠️ 必须在 RunInTx 之前调用。它走 GetWorkflowConfig（非事务读），而 gorm 单连接
// 后端在事务内再开非事务读会自我死锁——同 runBranchInfo 被提到事务外的原因。
//
// 目标没有 typed 入参时直接放行：标量入参、无入参的 workflow 完全合法。
func (s *Server) checkEntryInput(ref definition.WorkflowConfigRef, input map[string]any) error {
	return definition.CheckEntryInput(s.workflowConfigs, ref, input)
}

func (s *Server) workflowConfigRef(namespace, workflowType, version string) (definition.WorkflowConfigRef, error) {
	return definition.ResolveWorkflowConfigRef(s.workflowConfigs, namespace, workflowType, version)
}

// GetWorkflowConfig 读取 Run 在 programVersion 这一版 Program 下的配置，供 WFT 在 task 内固定内容快照；
// programVersion 为空时取 Run 当前采用的版本。
func (s *Server) GetWorkflowConfig(ctx context.Context, runID, programVersion string) (*programpkg.WorkflowConfig, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	run, err := s.mgr.Get(runID)
	if err != nil {
		return nil, err
	}
	return s.workflowConfigs.GetWorkflowConfig(runConfigRef(run, programVersion))
}

// runConfigRef 是 run 在 programVersion 这一版 Program 下的配置引用；programVersion 为空时取 run 当前采用的版本。
func runConfigRef(run *execution.Run, programVersion string) definition.WorkflowConfigRef {
	if programVersion == "" {
		programVersion = run.WorkflowConfigVersion
	}
	return definition.WorkflowConfigRef{Namespace: run.Namespace, WorkflowType: run.WorkflowType, Version: programVersion}
}

// runWorkflowConfig 读取 Run 当前采用的配置。
func (s *Server) runWorkflowConfig(runID string) (*programpkg.WorkflowConfig, error) {
	return s.GetWorkflowConfig(context.Background(), runID, "")
}
