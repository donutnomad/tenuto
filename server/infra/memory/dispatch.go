package memory

import (
	"cmp"
	"sort"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/google/uuid"
)

// DispatchRepository 是派发租约的内存仓储。
type DispatchRepository struct {
	mu     sync.Mutex
	states map[taskDispatchKey]dispatch.State
}

var _ dispatch.Repository = (*DispatchRepository)(nil)

func NewDispatchRepository() *DispatchRepository {
	return &DispatchRepository{states: make(map[taskDispatchKey]dispatch.State)}
}

func (s *DispatchRepository) AcquireTaskDispatch(task protocol.Task, leaseTTL time.Duration) (dispatch.Lease, bool, error) {
	key := taskDispatchKeyFromTask(task)
	if key.RunID == "" || key.ScheduledEventID == 0 || key.Type == "" {
		return dispatch.Lease{}, false, nil
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	state, exists := s.states[key]
	if exists {
		switch state.Status {
		case dispatch.StatusDispatched:
			return dispatch.Lease{}, false, nil
		case dispatch.StatusLeased:
			if state.LeaseExpiresAt.After(now) {
				return dispatch.Lease{}, false, nil
			}
		}
	}

	leaseID := uuid.NewString()
	task = dispatch.NormalizeTask(task)
	state.Namespace = task.Namespace
	state.Status = dispatch.StatusLeased
	state.LeaseID = leaseID
	state.LeaseExpiresAt = now.Add(leaseTTL)
	state.Attempts++
	state.UpdatedAt = now
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	s.states[key] = state
	return dispatch.Lease{Task: task, LeaseID: leaseID}, true, nil
}

func (s *DispatchRepository) MarkTaskDispatchDispatched(task protocol.Task, leaseID string) error {
	key := taskDispatchKeyFromTask(task)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.states[key]
	if state.LeaseID != leaseID {
		return nil
	}
	state.Status = dispatch.StatusDispatched
	state.LastError = ""
	state.LastDispatchedAt = now
	state.UpdatedAt = now
	s.states[key] = state
	return nil
}

func (s *DispatchRepository) MarkTaskDispatchFailed(task protocol.Task, leaseID, errMsg string) error {
	key := taskDispatchKeyFromTask(task)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.states[key]
	if state.LeaseID != leaseID {
		return nil
	}
	state.Status = dispatch.StatusFailed
	state.LastError = errMsg
	state.UpdatedAt = now
	s.states[key] = state
	return nil
}

// ReleaseTaskDispatch 与 dispatchrepo.Repository.ReleaseTaskDispatch 语义必须逐条一致：
// 只清"非活跃"的 dispatch 行，绝不清掉他人持有的、未过期的活跃租约
// （否则同一 activity 会被两个 worker 并发执行，理由详见 dispatchrepo 侧注释）。
func (s *DispatchRepository) ReleaseTaskDispatch(task protocol.Task, errMsg string) error {
	key := taskDispatchKeyFromTask(task)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.states[key]
	if !ok {
		return nil // 没有派发记录时不建记录，与 gorm 的 UPDATE 命中 0 行一致。
	}
	// 守卫：leased 且租约未过期且不属于调用方 → 保护，原样返回。
	// 本地实现的调用方均不持 leaseID，故这里等价于"任何活跃租约都不可被清"。
	// 与 gorm 侧的 WHERE 判据取反后完全对应。
	if state.Status == dispatch.StatusLeased && state.LeaseID != "" && state.LeaseExpiresAt.After(now) {
		return nil
	}
	state.Status = dispatch.StatusFailed
	state.LeaseID = ""
	state.LeaseExpiresAt = time.Time{}
	state.LastError = errMsg
	state.UpdatedAt = now
	s.states[key] = state
	return nil
}

// GetTaskDispatch 读取 task 的派发记录。
func (s *DispatchRepository) GetTaskDispatch(task protocol.Task) (dispatch.State, bool, error) {
	key := taskDispatchKeyFromTask(task)
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.states[key]
	return state, ok, nil
}

type taskDispatchKey struct {
	Namespace        string
	Type             protocol.TaskType
	RunID            string
	ScheduledEventID int64
	Attempt          int
}

func taskDispatchKeyFromTask(task protocol.Task) taskDispatchKey {
	task = dispatch.NormalizeTask(task)
	return taskDispatchKey{
		Namespace:        protocol.NormalizeNamespace(task.Namespace),
		Type:             task.Type,
		RunID:            task.RunID,
		ScheduledEventID: task.ScheduledEventID,
		Attempt:          task.Attempt,
	}
}

// WorkerVersionRepository 是 Activity Worker 路由登记的内存仓储。
type WorkerVersionRepository struct {
	mu      sync.RWMutex
	workers map[string]protocol.ActivityWorkerVersion
}

var _ dispatch.WorkerVersionRepository = (*WorkerVersionRepository)(nil)

func NewWorkerVersionRepository() *WorkerVersionRepository {
	return &WorkerVersionRepository{workers: make(map[string]protocol.ActivityWorkerVersion)}
}

func (s *WorkerVersionRepository) UpsertActivityWorkerVersionInNamespace(namespace string, worker protocol.ActivityWorkerVersion, now time.Time, ttl time.Duration) (protocol.ActivityWorkerVersion, error) {
	if len(worker.ActivityTypes) == 0 {
		return protocol.ActivityWorkerVersion{}, protocol.InvalidRequestf("activity worker version has no activity types")
	}
	worker.Namespace = protocol.NormalizeNamespace(namespace)
	saved := worker
	saved.LastHeartbeatAt = now
	saved.ExpiresAt = now.Add(ttl)
	taskQueues := dispatch.WorkerRouteTaskQueues(worker)
	desired := make(map[string]bool)
	for _, activityType := range worker.ActivityTypes {
		if activityType == "" {
			continue
		}
		for _, taskQueue := range taskQueues {
			desired[activityWorkerVersionKey(worker.Namespace, activityType, worker.Identity, worker.BuildID, taskQueue)] = true
		}
	}
	if len(desired) == 0 {
		return protocol.ActivityWorkerVersion{}, protocol.InvalidRequestf("activity worker version has no activity types")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key, existing := range s.workers {
		if protocol.NormalizeNamespace(existing.Namespace) == worker.Namespace && existing.Identity == worker.Identity && existing.BuildID == worker.BuildID && !desired[key] {
			delete(s.workers, key)
		}
	}
	for _, activityType := range worker.ActivityTypes {
		if activityType == "" {
			continue
		}
		for _, taskQueue := range taskQueues {
			row := saved
			row.TaskQueues = []string{taskQueue}
			row.ActivityTypes = []string{activityType}
			s.workers[activityWorkerVersionKey(row.Namespace, activityType, row.Identity, row.BuildID, taskQueue)] = row
		}
	}
	return saved, nil
}

func (s *WorkerVersionRepository) HeartbeatActivityWorkerVersionInNamespace(namespace, identity, buildID string, now time.Time, ttl time.Duration) error {
	namespace = protocol.NormalizeNamespace(namespace)
	if identity == "" {
		return protocol.InvalidRequestf("activity worker identity is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	matched := 0
	for key, worker := range s.workers {
		if protocol.NormalizeNamespace(worker.Namespace) == namespace && worker.Identity == identity && worker.BuildID == buildID {
			effectiveTTL, err := dispatch.ResolveHeartbeatTTL(worker.HeartbeatTTL, ttl)
			if err != nil {
				return err
			}
			worker.LastHeartbeatAt = now
			worker.ExpiresAt = now.Add(effectiveTTL)
			s.workers[key] = worker
			matched++
		}
	}
	if matched == 0 {
		return protocol.InvalidRequestf("activity worker version %s/%s/%s is not registered", namespace, identity, buildID)
	}
	return nil
}

func (s *WorkerVersionRepository) DeregisterActivityWorkerVersionInNamespace(namespace, identity, buildID string, now time.Time) error {
	namespace = protocol.NormalizeNamespace(namespace)
	if identity == "" {
		return protocol.InvalidRequestf("activity worker identity is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, worker := range s.workers {
		if protocol.NormalizeNamespace(worker.Namespace) == namespace && worker.Identity == identity && worker.BuildID == buildID {
			worker.ExpiresAt = now
			s.workers[key] = worker
		}
	}
	return nil
}

func (s *WorkerVersionRepository) DeleteExpiredActivityWorkerVersions(before time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var deleted int64
	for key, worker := range s.workers {
		if !worker.ExpiresAt.IsZero() && worker.ExpiresAt.Before(before) {
			delete(s.workers, key)
			deleted++
		}
	}
	return deleted, nil
}

func (s *WorkerVersionRepository) ListActiveActivityWorkerVersions(now time.Time) ([]protocol.ActivityWorkerVersion, error) {
	return s.listRoutes(func(worker protocol.ActivityWorkerVersion) bool {
		return worker.ExpiresAt.IsZero() || now.Before(worker.ExpiresAt)
	}), nil
}

func (s *WorkerVersionRepository) ListActivityWorkerVersionsOfWorker(namespace, identity string) ([]protocol.ActivityWorkerVersion, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	return s.listRoutes(func(worker protocol.ActivityWorkerVersion) bool {
		return worker.Namespace == namespace && worker.Identity == identity
	}), nil
}

func (s *WorkerVersionRepository) listRoutes(keep func(protocol.ActivityWorkerVersion) bool) []protocol.ActivityWorkerVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()
	workers := make([]protocol.ActivityWorkerVersion, 0, len(s.workers))
	for _, worker := range s.workers {
		if keep(worker) {
			workers = append(workers, worker)
		}
	}
	// 与 gorm 的 ORDER BY namespace, activity_type, build_id, task_queue, identity 一致；每条路由只有一个类型与一个 task queue。
	sort.Slice(workers, func(i, j int) bool {
		a, b := workers[i], workers[j]
		return cmp.Or(
			cmp.Compare(a.Namespace, b.Namespace),
			cmp.Compare(a.ActivityTypes[0], b.ActivityTypes[0]),
			cmp.Compare(a.BuildID, b.BuildID),
			cmp.Compare(a.TaskQueues[0], b.TaskQueues[0]),
			cmp.Compare(a.Identity, b.Identity),
		) < 0
	})
	return workers
}

func activityWorkerVersionKey(namespace, activityType, identity, buildID, taskQueue string) string {
	return protocol.NormalizeNamespace(namespace) + "\x00" + activityType + "\x00" + identity + "\x00" + buildID + "\x00" + taskQueue
}
