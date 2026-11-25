package dispatch

import (
	"errors"
	"hash/fnv"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
)

// ErrActivityRouteUnavailable 表示目录中暂无可用路由。Worker 上线后即可恢复，
// 发布阶段记 warning，运行阶段由 Workflow Task 退避重试。
var ErrActivityRouteUnavailable = errors.New("activity route unavailable")

type ActivityRoute struct {
	Identity  string
	Namespace string
	Type      string
	TaskQueue string
	BuildID   string
	Default   bool
	Weight    int
	ExpiresAt time.Time
	// Contracts 是 Worker 为该 Activity 类型上报的契约。
	Contracts []protocol.ActivityContract
}

// ActivityRouteRef 是一次选路请求：Workflow 对某个 Activity 的引用，以及调度时已知的目标 Worker 与契约声明。
// 选路只确定 Activity 的队列并校验契约能被在线 Worker 满足；不确定由哪个 build 执行。
type ActivityRouteRef struct {
	TargetWorkerIdentity string
	Namespace            string
	// ActivityID 是调度命令的 ActivityID；注册期按 Workflow 的静态引用检查时是 Activity 符号名。
	ActivityID string
	Type       string
	TaskQueue  string
	RoutingKey string
	// Input/Output 是 Workflow 静态声明的契约，用于调度期与 worker 上报对照。
	Input  string
	Output string
}

// declaredContract 是引用声明的契约；没有声明任何维度时 HasDeclaration 为 false。
func (ref ActivityRouteRef) declaredContract() protocol.ActivityContract {
	return protocol.ActivityContract{Type: ref.Type, Input: ref.Input, Output: ref.Output}
}

// unavailableForContract 是「没有在线 Worker 可以对照契约」的错误：Workflow Task 退避重试，Worker 上线后恢复。
func unavailableForContract(ref ActivityRouteRef, taskQueue string) error {
	return protocol.InvalidRequestf("%w: activity %s type %s declares a contract and no online worker on task queue %s reported one",
		ErrActivityRouteUnavailable, ref.ActivityID, ref.Type, taskQueue)
}

type ActivityRouteCatalog struct {
	clock   clock.Clock
	mu      sync.RWMutex
	routes  map[string][]ActivityRoute
	workers map[string][]ActivityRoute
}

// NewActivityRouteCatalog 用 clk 判断路由是否仍在有效期内；clk 为 nil 时不按有效期过滤。
func NewActivityRouteCatalog(clk clock.Clock, routes ...ActivityRoute) *ActivityRouteCatalog {
	catalog := &ActivityRouteCatalog{clock: clk, routes: make(map[string][]ActivityRoute, len(routes))}
	for _, route := range routes {
		catalog.registerRouteLocked(route)
	}
	return catalog
}

// RegisterActivityWorkerVersion 将 worker 能执行的 Activity 固定到队列和版本。
func (c *ActivityRouteCatalog) RegisterActivityWorkerVersion(worker protocol.ActivityWorkerVersion) error {
	if len(worker.ActivityTypes) == 0 {
		return protocol.InvalidRequestf("activity worker version has no activity types")
	}
	taskQueues := WorkerRouteTaskQueues(worker)
	namespace := protocol.NormalizeNamespace(worker.Namespace)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.routes == nil {
		c.routes = make(map[string][]ActivityRoute)
	}
	registered := 0
	for _, activityType := range worker.ActivityTypes {
		if activityType == "" {
			continue
		}
		typeContracts := reportedContractsFor(worker.Contracts, activityType)
		for _, taskQueue := range taskQueues {
			c.registerRouteLocked(ActivityRoute{
				Namespace: namespace,
				Identity:  worker.Identity,
				Type:      activityType,
				TaskQueue: taskQueue,
				BuildID:   worker.BuildID,
				Default:   worker.Default,
				Weight:    worker.Weight,
				ExpiresAt: worker.ExpiresAt,
				Contracts: typeContracts,
			})
			registered++
		}
	}
	if registered == 0 {
		return protocol.InvalidRequestf("activity worker version has no activity types")
	}
	return nil
}

func reportedContractsFor(contracts []protocol.ActivityContract, activityType string) []protocol.ActivityContract {
	for _, contract := range contracts {
		if contract.Type == activityType {
			return []protocol.ActivityContract{contract}
		}
	}
	return nil
}

func (c *ActivityRouteCatalog) registerRouteLocked(route ActivityRoute) {
	if route.Type == "" {
		return
	}
	if c.routes == nil {
		c.routes = make(map[string][]ActivityRoute)
	}
	route.TaskQueue = NormalizeActivityTaskQueue(route.TaskQueue)
	route.Namespace = protocol.NormalizeNamespace(route.Namespace)
	route.Weight = normalizeRouteWeight(route.Weight)
	if route.Identity != "" {
		if c.workers == nil {
			c.workers = make(map[string][]ActivityRoute)
		}
		k := activityRouteKey(route.Namespace, route.Type) + "\x00" + route.Identity
		c.workers[k] = append(c.workers[k], route)
	}
	key := activityRouteKey(route.Namespace, route.Type)
	routes := c.routes[key]
	for i, existing := range routes {
		if sameActivityRouteVersion(existing, route) {
			routes[i] = route
			c.routes[key] = routes
			return
		}
	}
	c.routes[key] = append(routes, route)
}

// ResolveActivityRoute 为配置或命令中的引用确定队列，返回的路由带一个契约兼容的在线 Worker 的上报，供调用方对照契约。
// 路由里的 BuildID 只说明这次对照用了谁的上报：任务投递到队列，由领取时的 Worker 执行，领取时再按它自己的上报对照契约。
//
// 指定了目标 Worker 或 TaskQueue 时，有在线候选就在候选里按契约与权重选择；没有候选时直接投递到
// 指定的队列（与 Temporal 的隐式队列一致，任务在队列里等 Worker 上线）。引用声明了契约时没有候选就无法对照契约，
// 返回 ErrActivityRouteUnavailable，由 Workflow Task 退避等待 Worker。
//
// 两者都没指定时，目录是选择队列的唯一依据：没有活跃路由返回 ErrActivityRouteUnavailable。
func (c *ActivityRouteCatalog) ResolveActivityRoute(ref ActivityRouteRef) (ActivityRoute, error) {
	ref.Namespace = protocol.NormalizeNamespace(ref.Namespace)
	if ref.Type == "" {
		return ActivityRoute{}, protocol.InvalidRequestf("activity %s type is empty", ref.ActivityID)
	}
	if ref.TargetWorkerIdentity != "" {
		c.mu.RLock()
		routes := append([]ActivityRoute(nil), c.workers[activityRouteKey(ref.Namespace, ref.Type)+"\x00"+ref.TargetWorkerIdentity]...)
		c.mu.RUnlock()
		candidates := filterActivityRoutes(activeActivityRoutes(routes, c.now()), func(route ActivityRoute) bool {
			return ref.TaskQueue == "" || route.TaskQueue == ref.TaskQueue
		})
		if len(candidates) > 0 {
			candidates = contractCompatibleRoutes(candidates, ref)
			sortActivityRoutes(candidates)
			return chooseActivityRoute(candidates, ref), nil
		}
		taskQueue := NormalizeActivityTaskQueue(ref.TaskQueue)
		if ref.declaredContract().HasDeclaration() {
			return ActivityRoute{}, unavailableForContract(ref, taskQueue)
		}
		return ActivityRoute{Namespace: ref.Namespace, Type: ref.Type, TaskQueue: taskQueue, Identity: ref.TargetWorkerIdentity}, nil
	}
	if ref.TaskQueue != "" {
		return c.resolveExplicitActivityRoute(ref)
	}
	c.mu.RLock()
	routes := append([]ActivityRoute(nil), c.routes[activityRouteKey(ref.Namespace, ref.Type)]...)
	c.mu.RUnlock()
	if len(routes) == 0 {
		return ActivityRoute{}, protocol.InvalidRequestf("%w: activity %s type %s is not registered in activity route catalog", ErrActivityRouteUnavailable, ref.ActivityID, ref.Type)
	}
	candidates := activeActivityRoutes(routes, c.now())
	if len(candidates) == 0 {
		return ActivityRoute{}, protocol.InvalidRequestf("%w: activity %s type %s has no active route", ErrActivityRouteUnavailable, ref.ActivityID, ref.Type)
	}
	candidates = contractCompatibleRoutes(candidates, ref)
	sortActivityRoutes(candidates)
	// 候选按契约收窄过时，实例的路由也按同一契约过滤；没有兼容候选时 contractCompatibleRoutes 保留全部，实例同样不过滤。
	declared := ref.declaredContract()
	narrowed := declared.HasDeclaration() && CheckActivityContract(declared, candidates[0]).Compatible()
	instances := func() [][]ActivityRoute {
		return c.instanceRoutes(ref.Namespace, ref.Type, func(route ActivityRoute) bool {
			return !narrowed || CheckActivityContract(declared, route).Compatible()
		})
	}
	if pools := ambiguousRoutePools(candidates, instances); pools != nil {
		return ActivityRoute{}, protocol.InvalidRequestf("%w: activity %s type %s declares no task queue and workers serve it on unrelated task queues %q; "+
			"declare the task queue in @Activity(queue=...) or ActivityOptions.TaskQueue, or stop one of them",
			ErrActivityRouteUnavailable, ref.ActivityID, ref.Type, pools)
	}
	return chooseActivityRoute(candidates, ref), nil
}

// ambiguousRoutePools 判断没有声明队列的 Activity 在候选路由里有没有明确的去处，没有时返回互不相关的各组队列。
//
// chooseActivityRoute 在默认路由之间、或既没有默认也没有权重的路由之间按排序取第一个；这些路由分布在
// 互不相关的队列上时（例如两个服务各自的队列），取第一个就是在两个服务之间静默二选一。
//
// 队列按进程分组：同一个队列上的路由算一组（同一服务的多个实例、滚动发布都在组内）；同一个 Worker 实例
// （Identity）拉取的几个队列算一组，这个进程确实同时服务它们。instances 按实例给出在线路由，只在 pool
// 跨多个队列时才读取，按与候选相同的默认规则筛选；没有 Identity 的路由只通过共享队列相连。BuildID 只标识
// 代码版本，可以为空或被不同服务共用，不参与分组。配置了权重而没有默认路由时，按权重分流是显式意图，不算歧义。
func ambiguousRoutePools(candidates []ActivityRoute, instances func() [][]ActivityRoute) [][]string {
	pool := filterActivityRoutes(append([]ActivityRoute(nil), candidates...), func(route ActivityRoute) bool { return route.Default })
	defaultsOnly := len(pool) > 0
	if !defaultsOnly {
		for _, route := range candidates {
			if route.Weight > 0 {
				return nil
			}
		}
		pool = candidates
	}
	// 并查集：节点是 pool 里的队列；同一个实例拉取的 pool 内队列相连。
	parent := map[string]string{}
	for _, route := range pool {
		parent[route.TaskQueue] = route.TaskQueue
	}
	if len(parent) < 2 {
		return nil
	}
	var find func(string) string
	find = func(queue string) string {
		if parent[queue] == queue {
			return queue
		}
		parent[queue] = find(parent[queue])
		return parent[queue]
	}
	for _, routes := range instances() {
		first := ""
		for _, route := range routes {
			queue := route.TaskQueue
			if _, ok := parent[queue]; !ok || (defaultsOnly && !route.Default) {
				continue
			}
			if first == "" {
				first = queue
				continue
			}
			parent[find(queue)] = find(first)
		}
	}
	groups := map[string][]string{}
	for queue := range parent {
		root := find(queue)
		groups[root] = append(groups[root], queue)
	}
	if len(groups) < 2 {
		return nil
	}
	pools := make([][]string, 0, len(groups))
	for _, queues := range groups {
		slices.Sort(queues)
		pools = append(pools, queues)
	}
	slices.SortFunc(pools, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
	return pools
}

// instanceRoutes 按 Worker 实例（Identity）分组返回该 Activity 的在线路由，只保留 keep 接受的路由。
// c.routes 按 (队列, build) 合并、只留最后一个注册者的 Identity，实例信息取自按 Identity 保存的 c.workers。
func (c *ActivityRouteCatalog) instanceRoutes(namespace, activityType string, keep func(ActivityRoute) bool) [][]ActivityRoute {
	prefix := activityRouteKey(namespace, activityType) + "\x00"
	now := c.now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	var instances [][]ActivityRoute
	for key, routes := range c.workers {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if routes = filterActivityRoutes(activeActivityRoutes(routes, now), keep); len(routes) > 1 {
			instances = append(instances, routes)
		}
	}
	return instances
}

// resolveExplicitActivityRoute 处理调用方指定了 TaskQueue 的引用。
//
// 目录里有订阅该队列的活跃 Worker 时在它们之中选择，选中的路由带 Worker 上报的契约。
// 没有时：引用声明了契约则返回 ErrActivityRouteUnavailable；否则原样投递到该队列并记 warning：
// ScheduleToStartTimeout 默认为 0，拼错的队列名会让任务一直留在队列里，warning 是这种情况唯一的线索。
func (c *ActivityRouteCatalog) resolveExplicitActivityRoute(ref ActivityRouteRef) (ActivityRoute, error) {
	taskQueue := NormalizeActivityTaskQueue(ref.TaskQueue)
	c.mu.RLock()
	routes := append([]ActivityRoute(nil), c.routes[activityRouteKey(ref.Namespace, ref.Type)]...)
	c.mu.RUnlock()
	candidates := filterActivityRoutes(activeActivityRoutes(routes, c.now()), func(route ActivityRoute) bool {
		return route.TaskQueue == taskQueue
	})
	if len(candidates) > 0 {
		candidates = contractCompatibleRoutes(candidates, ref)
		sortActivityRoutes(candidates)
		return chooseActivityRoute(candidates, ref), nil
	}
	if ref.declaredContract().HasDeclaration() {
		return ActivityRoute{}, unavailableForContract(ref, taskQueue)
	}
	slog.Warn("activity task queue has no known active worker; task may stall in the queue",
		slog.String("namespace", ref.Namespace),
		slog.String("activity_id", ref.ActivityID),
		slog.String("activity_type", ref.Type),
		slog.String("task_queue", taskQueue),
	)
	return ActivityRoute{Namespace: ref.Namespace, Type: ref.Type, TaskQueue: taskQueue}, nil
}

func (c *ActivityRouteCatalog) now() time.Time {
	if c.clock == nil {
		return time.Time{}
	}
	return c.clock.Now()
}

func (c *ActivityRouteCatalog) ValidateWorkflowConfigInNamespace(namespace string, wf *program.WorkflowConfig) error {
	return c.validateWorkflowConfig(namespace, wf.TargetWorkerIdentity, wf)
}

// TargetWorkerConflict 判定 version 这一版内容 workflow 能否由 Run 固定的目标 Worker target 承接：全部静态 Activity 引用
// （含未选中的分支）都要被 target 登记的队列与 Activity 类型覆盖，规则与发布时的 target 校验相同。
// active 是当前活跃的 Activity Worker 登记。target 为空、target 在 namespace 里没有活跃登记（离线的 target 按等待与超时
// 处理）时不判为冲突。
func (c *ActivityRouteCatalog) TargetWorkerConflict(namespace, target, version string, workflow *program.WorkflowConfig, active []protocol.ActivityWorkerVersion) error {
	if target == "" {
		return nil
	}
	targetActive := false
	for _, worker := range active {
		if protocol.NormalizeNamespace(worker.Namespace) == protocol.NormalizeNamespace(namespace) && worker.Identity == target {
			targetActive = true
			break
		}
	}
	if !targetActive {
		return nil
	}
	if err := c.validateWorkflowConfig(namespace, target, workflow); err != nil {
		return protocol.InvalidRequestf("workflow content %s conflicts with run target %q: %w", version, target, err)
	}
	return nil
}

// ResolveTargetedActivityRoute 为固定了目标 Worker 的 Run 选路。registrations 是目标 Worker 的全部登记，包括心跳已过期的：
// 按这份登记选路，目标离线时任务仍投到它会回来领取的队列。同一 Worker 有多份能承接 ref 的登记时取最近心跳的那份。
func ResolveTargetedActivityRoute(registrations []protocol.ActivityWorkerVersion, ref ActivityRouteRef) (ActivityRoute, error) {
	var latest time.Time
	for _, worker := range registrations {
		if !slices.Contains(worker.ActivityTypes, ref.Type) ||
			(ref.TaskQueue != "" && !slices.Contains(worker.TaskQueues, ref.TaskQueue)) {
			continue
		}
		if worker.LastHeartbeatAt.After(latest) {
			latest = worker.LastHeartbeatAt
		}
	}
	targeted := NewActivityRouteCatalog(nil)
	for _, worker := range registrations {
		if !worker.LastHeartbeatAt.Equal(latest) {
			continue
		}
		worker.ExpiresAt = time.Time{}
		if err := targeted.RegisterActivityWorkerVersion(worker); err != nil {
			return ActivityRoute{}, err
		}
	}
	return targeted.ResolveActivityRoute(ref)
}

// validateWorkflowConfig 校验 workflow 的静态 Activity 引用：指定了 target 时 target 必须有能承接的活跃路由；
// 其余引用缺路由只告警。
func (c *ActivityRouteCatalog) validateWorkflowConfig(namespace, target string, wf *program.WorkflowConfig) error {
	for _, ref := range ActivityRouteRefsForWorkflow(wf) {
		ref.Namespace = protocol.NormalizeNamespace(namespace)
		ref.TargetWorkerIdentity = target
		if ref.TargetWorkerIdentity != "" {
			c.mu.RLock()
			routes := append([]ActivityRoute(nil), c.workers[activityRouteKey(ref.Namespace, ref.Type)+"\x00"+ref.TargetWorkerIdentity]...)
			c.mu.RUnlock()
			matches := filterActivityRoutes(activeActivityRoutes(routes, c.now()), func(route ActivityRoute) bool {
				return ref.TaskQueue == "" || ref.TaskQueue == route.TaskQueue
			})
			if len(matches) == 0 {
				return protocol.InvalidRequestf("target worker %q cannot handle activity %q on the configured queue", ref.TargetWorkerIdentity, ref.Type)
			}
		}
		if _, err := c.ResolveActivityRoute(ref); err != nil {
			if !errors.Is(err, ErrActivityRouteUnavailable) {
				return err
			}
			// 发布与 Worker 上线的先后顺序不作要求：缺路由只告警，调度期由 WFT 退避等待 Worker。
			slog.Warn("activity route not found in catalog at registration time; runs wait for a worker when they reach this activity",
				slog.String("namespace", ref.Namespace),
				slog.String("activity_id", ref.ActivityID),
				slog.String("activity_type", ref.Type),
				slog.String("task_queue", ref.TaskQueue),
				slog.Any("err", err),
			)
		}
	}
	return nil
}

// ActivityRouteRefsForWorkflow 输出 Workflow 程序引用的全部 Activity（含尚未被 GetVersion 选中的分支），
// 按符号排序，步骤 ID 取程序里的符号。设置 target 的静态能力校验和调度期契约对照共用这份引用集。
func ActivityRouteRefsForWorkflow(wf *program.WorkflowConfig) []ActivityRouteRef {
	if wf == nil {
		return nil
	}
	var refs []ActivityRouteRef
	for _, ref := range wf.SymbolRefs() {
		refs = append(refs, ActivityRouteRef{
			ActivityID: ref.Symbol,
			Type:       ref.Type,
			TaskQueue:  ref.TaskQueue,
			Input:      ref.Input,
			Output:     ref.Output,
		})
	}
	slices.SortFunc(refs, func(a, b ActivityRouteRef) int { return strings.Compare(a.ActivityID, b.ActivityID) })
	return refs
}

func WorkerRouteTaskQueues(worker protocol.ActivityWorkerVersion) []string {
	seen := map[string]bool{}
	var taskQueues []string
	for _, taskQueue := range worker.TaskQueues {
		if taskQueue != "" {
			queueName := NormalizeActivityTaskQueue(taskQueue)
			if !seen[queueName] {
				seen[queueName] = true
				taskQueues = append(taskQueues, queueName)
			}
		}
	}
	if worker.App != "" {
		queueName := NormalizeActivityTaskQueue(worker.App)
		if !seen[queueName] {
			taskQueues = append(taskQueues, queueName)
		}
	}
	if len(taskQueues) == 0 {
		taskQueues = append(taskQueues, defaultActivityTaskQueue)
	}
	return taskQueues
}

func sameActivityRouteVersion(left, right ActivityRoute) bool {
	return protocol.NormalizeNamespace(left.Namespace) == protocol.NormalizeNamespace(right.Namespace) && left.Type == right.Type && left.TaskQueue == right.TaskQueue && left.BuildID == right.BuildID
}

func activeActivityRoutes(routes []ActivityRoute, now time.Time) []ActivityRoute {
	active := make([]ActivityRoute, 0, len(routes))
	for _, route := range routes {
		if route.ExpiresAt.IsZero() || now.Before(route.ExpiresAt) {
			active = append(active, route)
		}
	}
	return active
}

func filterActivityRoutes(routes []ActivityRoute, keep func(ActivityRoute) bool) []ActivityRoute {
	filtered := make([]ActivityRoute, 0, len(routes))
	for _, route := range routes {
		if keep(route) {
			filtered = append(filtered, route)
		}
	}
	return filtered
}

func chooseActivityRoute(routes []ActivityRoute, ref ActivityRouteRef) ActivityRoute {
	if len(routes) == 1 {
		return routes[0]
	}
	defaults := filterActivityRoutes(append([]ActivityRoute(nil), routes...), func(route ActivityRoute) bool {
		return route.Default
	})
	if len(defaults) > 0 {
		sortActivityRoutes(defaults)
		return defaults[0]
	}
	totalWeight := 0
	for _, route := range routes {
		if route.Weight > 0 {
			totalWeight += route.Weight
		}
	}
	if totalWeight > 0 {
		// 候选集不变时同一个 activity 的选择稳定；选择只决定队列与对照契约所用的上报，不决定执行它的 build。
		point := int(stableActivityRouteHash(activityRouteSelectionKey(ref)) % uint64(totalWeight))
		cursor := 0
		for _, route := range routes {
			if route.Weight <= 0 {
				continue
			}
			cursor += route.Weight
			if point < cursor {
				return route
			}
		}
	}
	return routes[0]
}

func sortActivityRoutes(routes []ActivityRoute) {
	sort.Slice(routes, func(i, j int) bool {
		left, right := routes[i], routes[j]
		if left.BuildID != right.BuildID {
			return left.BuildID < right.BuildID
		}
		if left.TaskQueue != right.TaskQueue {
			return left.TaskQueue < right.TaskQueue
		}
		return left.Type < right.Type
	})
}

func activityRouteSelectionKey(ref ActivityRouteRef) string {
	if ref.RoutingKey != "" {
		return ref.RoutingKey
	}
	return protocol.NormalizeNamespace(ref.Namespace) + ":" + ref.ActivityID + ":" + ref.Type + ":" + NormalizeActivityTaskQueue(ref.TaskQueue)
}

func activityRouteKey(namespace, activityType string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + activityType
}

func stableActivityRouteHash(key string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return hash.Sum64()
}

func normalizeRouteWeight(weight int) int {
	if weight < 0 {
		return 0
	}
	return weight
}

const defaultActivityTaskQueue = "default"

func NormalizeActivityTaskQueue(taskQueue string) string {
	if taskQueue == "" {
		return defaultActivityTaskQueue
	}
	return taskQueue
}
