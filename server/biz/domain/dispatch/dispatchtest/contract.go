// Package dispatchtest 提供 dispatch.Repository 与 dispatch.WorkerVersionRepository 的契约测试，
// 各实现的测试用同一套断言运行。
package dispatchtest

import (
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

// 过期用负 TTL 表达：租约在获取时就已过期，测试不依赖 sleep。
const (
	activeLease  = time.Hour
	expiredLease = -time.Second
)

// RunRepositoryContract 对 newRepo 返回的实现运行派发租约仓储契约；每个子测试拿到一个空仓储。
func RunRepositoryContract(t *testing.T, newRepo func(t *testing.T) dispatch.Repository) {
	t.Run("Acquire", func(t *testing.T) { testAcquire(t, newRepo) })
	t.Run("Reacquire", func(t *testing.T) { testReacquire(t, newRepo) })
	t.Run("Release", func(t *testing.T) { testRelease(t, newRepo) })
	t.Run("NamespaceIsolation", func(t *testing.T) { testLeaseNamespaceIsolation(t, newRepo(t)) })
}

// RunWorkerVersionRepositoryContract 对 newRepo 返回的实现运行 Worker 版本仓储契约；每个子测试拿到一个空仓储。
func RunWorkerVersionRepositoryContract(t *testing.T, newRepo func(t *testing.T) dispatch.WorkerVersionRepository) {
	t.Run("ContractsRoundTrip", func(t *testing.T) { testContractsRoundTrip(t, newRepo(t)) })
	t.Run("RoutesExpandByTypeAndQueue", func(t *testing.T) { testRoutesExpand(t, newRepo(t)) })
	t.Run("UpsertRejectsWorkerWithoutActivityTypes", func(t *testing.T) { testUpsertRejectsEmptyWorker(t, newRepo(t)) })
	t.Run("UpsertRemovesStaleRoutes", func(t *testing.T) { testUpsertRemovesStaleRoutes(t, newRepo(t)) })
	t.Run("HeartbeatRenewsEveryRouteOfTheWorker", func(t *testing.T) { testHeartbeatRenews(t, newRepo(t)) })
	t.Run("HeartbeatRejectsUnregisteredWorkerAndInvalidTTL", func(t *testing.T) { testHeartbeatRejects(t, newRepo(t)) })
	t.Run("ExpiredRoutesAreNotListed", func(t *testing.T) { testExpiredRoutes(t, newRepo(t)) })
	t.Run("RoutesAreListedInOrder", func(t *testing.T) { testRouteOrder(t, newRepo(t)) })
	t.Run("DeregisterExpiresEveryRouteOfTheWorker", func(t *testing.T) { testDeregister(t, newRepo(t)) })
	t.Run("NamespaceIsolation", func(t *testing.T) { testWorkerNamespaceIsolation(t, newRepo(t)) })
	t.Run("RoutesOfWorkerIncludeExpired", func(t *testing.T) { testRoutesOfWorker(t, newRepo(t)) })
}

func activityTask() protocol.Task {
	return protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-1", ScheduledEventID: 7, Attempt: 1}
}

func mustAcquire(t *testing.T, repo dispatch.Repository, task protocol.Task, ttl time.Duration) dispatch.Lease {
	t.Helper()
	lease, ok, err := repo.AcquireTaskDispatch(task, ttl)
	if err != nil || !ok || lease.LeaseID == "" {
		t.Fatalf("acquire: lease=%+v ok=%v err=%v", lease, ok, err)
	}
	return lease
}

func mustState(t *testing.T, repo dispatch.Repository, task protocol.Task) dispatch.State {
	t.Helper()
	state, ok, err := repo.GetTaskDispatch(task)
	if err != nil || !ok {
		t.Fatalf("state missing: ok=%v err=%v", ok, err)
	}
	return state
}

func testAcquire(t *testing.T, newRepo func(t *testing.T) dispatch.Repository) {
	t.Run("Happy Path: first acquire leases the normalized task", func(t *testing.T) {
		repo := newRepo(t)
		lease := mustAcquire(t, repo, protocol.Task{Type: protocol.TaskTypeActivity, RunID: "run-1", ScheduledEventID: 7}, activeLease)
		if lease.Task.Attempt != 1 || lease.Task.Namespace != protocol.DefaultNamespace || lease.Task.TaskQueue != protocol.DefaultActivityTaskQueue {
			t.Fatalf("leased task = %+v", lease.Task)
		}
		// Attempt 0 与 1 是同一次尝试，读写落在同一条记录上。
		state := mustState(t, repo, activityTask())
		if state.Status != dispatch.StatusLeased || state.LeaseID != lease.LeaseID || state.Attempts != 1 || state.CreatedAt.IsZero() {
			t.Fatalf("state = %+v", state)
		}
	})

	t.Run("Error Case: incomplete task identity is not leased", func(t *testing.T) {
		repo := newRepo(t)
		for name, task := range map[string]protocol.Task{
			"run id":             {Type: protocol.TaskTypeActivity, ScheduledEventID: 7},
			"scheduled event id": {Type: protocol.TaskTypeActivity, RunID: "run-1"},
			"type":               {RunID: "run-1", ScheduledEventID: 7},
		} {
			if _, ok, err := repo.AcquireTaskDispatch(task, activeLease); ok || err != nil {
				t.Fatalf("missing %s: ok=%v err=%v", name, ok, err)
			}
			if _, found, _ := repo.GetTaskDispatch(task); found {
				t.Fatalf("missing %s left a dispatch record", name)
			}
		}
	})

	t.Run("Edge Case: active lease blocks a second acquire", func(t *testing.T) {
		repo := newRepo(t)
		lease := mustAcquire(t, repo, activityTask(), activeLease)
		if _, ok, err := repo.AcquireTaskDispatch(activityTask(), activeLease); ok || err != nil {
			t.Fatalf("second acquire: ok=%v err=%v", ok, err)
		}
		if state := mustState(t, repo, activityTask()); state.LeaseID != lease.LeaseID || state.Attempts != 1 {
			t.Fatalf("state = %+v", state)
		}
	})

	t.Run("Edge Case: each attempt, run and namespace has its own lease", func(t *testing.T) {
		repo := newRepo(t)
		mustAcquire(t, repo, activityTask(), activeLease)
		for name, change := range map[string]func(*protocol.Task){
			"attempt":         func(task *protocol.Task) { task.Attempt = 2 },
			"scheduled event": func(task *protocol.Task) { task.ScheduledEventID = 8 },
			"run":             func(task *protocol.Task) { task.RunID = "run-2" },
			"namespace":       func(task *protocol.Task) { task.Namespace = "tenant" },
		} {
			task := activityTask()
			change(&task)
			if _, ok, err := repo.AcquireTaskDispatch(task, activeLease); !ok || err != nil {
				t.Fatalf("other %s: ok=%v err=%v", name, ok, err)
			}
		}
	})

	t.Run("Edge Case: a new stamp of the same attempt shares the lease", func(t *testing.T) {
		// 去重键含 attempt、不含 stamp：同一 attempt 换 stamp 重派时仍受原租约约束。
		repo := newRepo(t)
		lease := mustAcquire(t, repo, activityTask(), activeLease)
		restamped := activityTask()
		restamped.Stamp = 9
		if _, ok, err := repo.AcquireTaskDispatch(restamped, activeLease); ok || err != nil {
			t.Fatalf("acquire with a new stamp: ok=%v err=%v", ok, err)
		}
		if state := mustState(t, repo, restamped); state.LeaseID != lease.LeaseID {
			t.Fatalf("restamped task read another record: %+v", state)
		}
	})
}

func testReacquire(t *testing.T, newRepo func(t *testing.T) dispatch.Repository) {
	t.Run("Happy Path: expired lease is taken over and the old holder loses its writes", func(t *testing.T) {
		repo := newRepo(t)
		created := mustState(t, repo, mustAcquire(t, repo, activityTask(), expiredLease).Task).CreatedAt
		stale := mustState(t, repo, activityTask()).LeaseID

		lease := mustAcquire(t, repo, activityTask(), activeLease)
		if lease.LeaseID == stale {
			t.Fatal("takeover reused the expired lease id")
		}
		if err := repo.MarkTaskDispatchDispatched(activityTask(), stale); err != nil {
			t.Fatal(err)
		}
		if err := repo.MarkTaskDispatchFailed(activityTask(), stale, "late failure"); err != nil {
			t.Fatal(err)
		}
		state := mustState(t, repo, activityTask())
		if state.Status != dispatch.StatusLeased || state.LeaseID != lease.LeaseID || state.Attempts != 2 || state.LastError != "" || !state.CreatedAt.Equal(created) {
			t.Fatalf("state = %+v", state)
		}
	})

	t.Run("Happy Path: failed dispatch can be acquired again", func(t *testing.T) {
		repo := newRepo(t)
		first := mustAcquire(t, repo, activityTask(), activeLease)
		if err := repo.MarkTaskDispatchFailed(activityTask(), first.LeaseID, "queue down"); err != nil {
			t.Fatal(err)
		}
		if state := mustState(t, repo, activityTask()); state.Status != dispatch.StatusFailed || state.LastError != "queue down" {
			t.Fatalf("failed state = %+v", state)
		}
		second := mustAcquire(t, repo, activityTask(), activeLease)
		if err := repo.MarkTaskDispatchDispatched(activityTask(), second.LeaseID); err != nil {
			t.Fatal(err)
		}
		state := mustState(t, repo, activityTask())
		if state.Status != dispatch.StatusDispatched || state.Attempts != 2 || state.LastError != "" || state.LastDispatchedAt.IsZero() {
			t.Fatalf("dispatched state = %+v", state)
		}
	})

	t.Run("Edge Case: dispatched task stays dispatched after its lease expires", func(t *testing.T) {
		repo := newRepo(t)
		lease := mustAcquire(t, repo, activityTask(), expiredLease)
		if err := repo.MarkTaskDispatchDispatched(activityTask(), lease.LeaseID); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := repo.AcquireTaskDispatch(activityTask(), activeLease); ok || err != nil {
			t.Fatalf("acquire after dispatch: ok=%v err=%v", ok, err)
		}
	})
}

func testRelease(t *testing.T, newRepo func(t *testing.T) dispatch.Repository) {
	for _, test := range []struct {
		name        string
		prepare     func(t *testing.T, repo dispatch.Repository)
		wantStatus  dispatch.Status
		wantRelease bool
	}{
		{
			name:       "Edge Case: active lease is protected",
			prepare:    func(t *testing.T, repo dispatch.Repository) { mustAcquire(t, repo, activityTask(), activeLease) },
			wantStatus: dispatch.StatusLeased,
		},
		{
			name:        "Happy Path: expired lease is released",
			prepare:     func(t *testing.T, repo dispatch.Repository) { mustAcquire(t, repo, activityTask(), expiredLease) },
			wantStatus:  dispatch.StatusFailed,
			wantRelease: true,
		},
		{
			name: "Happy Path: dispatched task is released for redispatch",
			prepare: func(t *testing.T, repo dispatch.Repository) {
				lease := mustAcquire(t, repo, activityTask(), activeLease)
				if err := repo.MarkTaskDispatchDispatched(activityTask(), lease.LeaseID); err != nil {
					t.Fatal(err)
				}
			},
			wantStatus:  dispatch.StatusFailed,
			wantRelease: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := newRepo(t)
			test.prepare(t, repo)
			if err := repo.ReleaseTaskDispatch(activityTask(), "recovery"); err != nil {
				t.Fatal(err)
			}
			state := mustState(t, repo, activityTask())
			if state.Status != test.wantStatus {
				t.Fatalf("status = %q, want %q", state.Status, test.wantStatus)
			}
			_, acquired, err := repo.AcquireTaskDispatch(activityTask(), activeLease)
			if err != nil || acquired != test.wantRelease {
				t.Fatalf("acquire after release = %v err=%v, want %v", acquired, err, test.wantRelease)
			}
			if !test.wantRelease {
				return
			}
			if state.LeaseID != "" || !state.LeaseExpiresAt.IsZero() || state.LastError != "recovery" || state.CreatedAt.IsZero() {
				t.Fatalf("released state = %+v", state)
			}
		})
	}
	t.Run("Edge Case: releasing a task without a dispatch record creates none", func(t *testing.T) {
		repo := newRepo(t)
		if err := repo.ReleaseTaskDispatch(activityTask(), "recovery"); err != nil {
			t.Fatal(err)
		}
		if state, found, err := repo.GetTaskDispatch(activityTask()); err != nil || found {
			t.Fatalf("state after release = %+v found=%v err=%v, want no record", state, found, err)
		}
		mustAcquire(t, repo, activityTask(), activeLease)
	})
}

func testLeaseNamespaceIsolation(t *testing.T, repo dispatch.Repository) {
	taskA := protocol.Task{
		Namespace: "tenant-a", Type: protocol.TaskTypeActivity, RunID: "same-run-id", ScheduledEventID: 42,
		Attempt: 1, ActivityType: "payments.Capture", TaskQueue: "payments",
	}
	taskB := taskA
	taskB.Namespace = "tenant-b"
	mustAcquire(t, repo, taskA, activeLease)
	mustAcquire(t, repo, taskB, activeLease)
	if state := mustState(t, repo, taskA); state.Namespace != "tenant-a" {
		t.Fatalf("tenant-a dispatch state = %+v", state)
	}
	if state := mustState(t, repo, taskB); state.Namespace != "tenant-b" {
		t.Fatalf("tenant-b dispatch state = %+v", state)
	}
}

func testRouteOrder(t *testing.T, repo dispatch.WorkerVersionRepository) {
	// Happy Path: 路由按 (namespace, Activity 类型, build ID, task queue, identity) 升序返回
	now := registrationTime()
	for _, worker := range []struct{ namespace, identity, buildID string }{
		{"tenant-b", "worker-1", "build-1"},
		{"tenant-a", "worker-2", "build-2"},
		{"tenant-a", "worker-1", "build-2"},
		{"tenant-a", "worker-3", "build-1"},
	} {
		mustUpsert(t, repo, worker.namespace, protocol.ActivityWorkerVersion{
			Identity: worker.identity, BuildID: worker.buildID,
			ActivityTypes: []string{"payments.Refund", "payments.Capture"}, TaskQueues: []string{"queue-b", "queue-a"},
		}, now, time.Minute)
	}
	workers, err := repo.ListActiveActivityWorkerVersions(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 16 {
		t.Fatalf("listed %d routes, want 16", len(workers))
	}
	key := func(worker protocol.ActivityWorkerVersion) [5]string {
		return [5]string{worker.Namespace, worker.ActivityTypes[0], worker.BuildID, worker.TaskQueues[0], worker.Identity}
	}
	for i := 1; i < len(workers); i++ {
		previous, current := key(workers[i-1]), key(workers[i])
		if slices.Compare(previous[:], current[:]) >= 0 {
			t.Fatalf("route %d %v is listed after %v", i, current, previous)
		}
	}
}

// 时间取毫秒精度，数据库的 DATETIME(3) 列能原样存取。
func registrationTime() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func mustUpsert(t *testing.T, repo dispatch.WorkerVersionRepository, namespace string, worker protocol.ActivityWorkerVersion, now time.Time, ttl time.Duration) {
	t.Helper()
	if _, err := repo.UpsertActivityWorkerVersionInNamespace(namespace, worker, now, ttl); err != nil {
		t.Fatalf("upsert %s/%s: %v", worker.Identity, worker.BuildID, err)
	}
}

// activeRoutes 返回 now 时刻 identity 的活跃路由，按 Activity 类型与 task queue 排序；identity 为空时返回全部。
func activeRoutes(t *testing.T, repo dispatch.WorkerVersionRepository, now time.Time, identity string) []protocol.ActivityWorkerVersion {
	t.Helper()
	workers, err := repo.ListActiveActivityWorkerVersions(now)
	if err != nil {
		t.Fatalf("list active worker versions: %v", err)
	}
	var routes []protocol.ActivityWorkerVersion
	for _, worker := range workers {
		if len(worker.ActivityTypes) != 1 || len(worker.TaskQueues) != 1 {
			t.Fatalf("route carries %d activity types and %d task queues, want one each: %+v", len(worker.ActivityTypes), len(worker.TaskQueues), worker)
		}
		if identity == "" || worker.Identity == identity {
			routes = append(routes, worker)
		}
	}
	sort.Slice(routes, func(i, j int) bool { return routeName(routes[i]) < routeName(routes[j]) })
	return routes
}

func routeName(route protocol.ActivityWorkerVersion) string {
	return route.Namespace + "/" + route.Identity + "/" + route.ActivityTypes[0] + "@" + route.TaskQueues[0]
}

func routeNames(routes []protocol.ActivityWorkerVersion) []string {
	names := make([]string, len(routes))
	for i, route := range routes {
		names[i] = route.ActivityTypes[0] + "@" + route.TaskQueues[0]
	}
	return names
}

func assertRouteNames(t *testing.T, routes []protocol.ActivityWorkerVersion, want ...string) {
	t.Helper()
	got := routeNames(routes)
	if len(got) != len(want) {
		t.Fatalf("routes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("routes = %v, want %v", got, want)
		}
	}
}

// Worker 上报的契约跨 Upsert 与 ListActive 保留；没有上报契约的 Worker 读回空集合，
// 声明了契约的 Activity 不会派给它。
func testContractsRoundTrip(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	contracts := []protocol.ActivityContract{{Type: "payments.charge.v1", Input: "input@fp1", Output: "output@fp2"}}
	mustUpsert(t, repo, protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "payments-worker", BuildID: "build-1", TaskQueues: []string{"payments"},
		ActivityTypes: []string{"payments.charge.v1"}, Contracts: contracts, Default: true, Weight: 3,
	}, now, time.Hour)
	mustUpsert(t, repo, protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "legacy-worker", BuildID: "build-0", TaskQueues: []string{"payments"},
		ActivityTypes: []string{"payments.charge.v1"},
	}, now, time.Hour)

	reported := activeRoutes(t, repo, now.Add(time.Minute), "payments-worker")
	if len(reported) != 1 {
		t.Fatalf("payments-worker routes = %+v, want 1", reported)
	}
	route := reported[0]
	if len(route.Contracts) != 1 || route.Contracts[0] != contracts[0] {
		t.Fatalf("contracts round-trip = %+v", route.Contracts)
	}
	if route.Namespace != protocol.DefaultNamespace || route.BuildID != "build-1" || !route.Default || route.Weight != 3 ||
		!route.LastHeartbeatAt.Equal(now) || !route.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("route = %+v", route)
	}
	legacy := activeRoutes(t, repo, now.Add(time.Minute), "legacy-worker")
	if len(legacy) != 1 || len(legacy[0].Contracts) != 0 {
		t.Fatalf("legacy worker routes = %+v, want one route with empty contracts", legacy)
	}
}

func testRoutesExpand(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	// 声明的 task queue 与 App 都成为路由；空的 Activity 类型被跳过。
	mustUpsert(t, repo, "", protocol.ActivityWorkerVersion{
		App: "payments", Identity: "worker-a", BuildID: "build-1",
		TaskQueues: []string{"q1", "q1"}, ActivityTypes: []string{"charge", "", "refund"},
	}, now, time.Minute)
	// 没有声明 task queue 与 App 的 Worker 落在默认 task queue。
	mustUpsert(t, repo, "", protocol.ActivityWorkerVersion{
		Identity: "worker-b", BuildID: "build-1", ActivityTypes: []string{"charge"},
	}, now, time.Minute)

	routes := activeRoutes(t, repo, now, "worker-a")
	assertRouteNames(t, routes, "charge@payments", "charge@q1", "refund@payments", "refund@q1")
	for _, route := range routes {
		if route.Namespace != protocol.DefaultNamespace || route.App != "payments" {
			t.Fatalf("route = %+v", route)
		}
	}
	assertRouteNames(t, activeRoutes(t, repo, now, "worker-b"), "charge@"+protocol.DefaultActivityTaskQueue)
}

func testUpsertRejectsEmptyWorker(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	for name, activityTypes := range map[string][]string{"no activity types": nil, "only empty activity types": {""}} {
		_, err := repo.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
			Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: activityTypes,
		}, now, time.Minute)
		if err == nil {
			t.Fatalf("%s: upsert succeeded", name)
		}
	}
	if routes := activeRoutes(t, repo, now, ""); len(routes) != 0 {
		t.Fatalf("rejected upserts left routes: %+v", routes)
	}
}

func testUpsertRemovesStaleRoutes(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	worker := protocol.ActivityWorkerVersion{
		Identity: "custodian-worker-1", BuildID: "custodian@v1",
		TaskQueues: []string{"custodian"}, ActivityTypes: []string{"custodian.Capture", "custodian.Refund"},
	}
	mustUpsert(t, repo, protocol.DefaultNamespace, worker, now, time.Minute)
	// 同一 identity 的另一个 build 是另一个 Worker，重新登记时保持原状。
	other := worker
	other.BuildID = "custodian@v2"
	mustUpsert(t, repo, protocol.DefaultNamespace, other, now, time.Minute)

	worker.ActivityTypes = []string{"custodian.Capture"}
	mustUpsert(t, repo, protocol.DefaultNamespace, worker, now.Add(time.Second), time.Minute)
	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "custodian-worker-1", "custodian@v1", now.Add(2*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	var v1, v2 []protocol.ActivityWorkerVersion
	for _, route := range activeRoutes(t, repo, now.Add(3*time.Second), "custodian-worker-1") {
		if route.BuildID == "custodian@v1" {
			v1 = append(v1, route)
		} else {
			v2 = append(v2, route)
		}
	}
	assertRouteNames(t, v1, "custodian.Capture@custodian")
	assertRouteNames(t, v2, "custodian.Capture@custodian", "custodian.Refund@custodian")
}

// 心跳按登记的 HeartbeatTTL 给 Worker 的全部路由续期，别的 Worker 的路由保持原值；
// 没有登记 TTL 时采用调用方给出的 ttl。
func testHeartbeatRenews(t *testing.T, repo dispatch.WorkerVersionRepository) {
	registeredAt := registrationTime()
	for _, worker := range []protocol.ActivityWorkerVersion{
		{Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q1", "q2"}, ActivityTypes: []string{"charge", "refund"}, HeartbeatTTL: "90s"},
		{Identity: "worker-b", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"}},
	} {
		mustUpsert(t, repo, protocol.DefaultNamespace, worker, registeredAt, time.Minute)
	}

	heartbeatAt := registeredAt.Add(30 * time.Second)
	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-a", "build-1", heartbeatAt, time.Hour); err != nil {
		t.Fatal(err)
	}
	renewed := activeRoutes(t, repo, heartbeatAt, "worker-a")
	assertRouteNames(t, renewed, "charge@q1", "charge@q2", "refund@q1", "refund@q2")
	for _, route := range renewed {
		if !route.LastHeartbeatAt.Equal(heartbeatAt) || !route.ExpiresAt.Equal(heartbeatAt.Add(90*time.Second)) {
			t.Errorf("worker-a route %s: heartbeat = %v expires = %v", routeName(route), route.LastHeartbeatAt, route.ExpiresAt)
		}
	}
	untouched := activeRoutes(t, repo, heartbeatAt, "worker-b")
	if len(untouched) != 1 || !untouched[0].LastHeartbeatAt.Equal(registeredAt) || !untouched[0].ExpiresAt.Equal(registeredAt.Add(time.Minute)) {
		t.Fatalf("worker-b route was touched: %+v", untouched)
	}

	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-b", "build-1", heartbeatAt, time.Hour); err != nil {
		t.Fatal(err)
	}
	fallback := activeRoutes(t, repo, heartbeatAt, "worker-b")
	if len(fallback) != 1 || !fallback[0].ExpiresAt.Equal(heartbeatAt.Add(time.Hour)) {
		t.Fatalf("worker-b routes = %+v, want heartbeat + fallback ttl", fallback)
	}
}

// 未注册的 Worker 心跳报错；登记了无法解析的 TTL 时心跳报错且路由保持原值。
func testHeartbeatRejects(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "ghost", "build-1", now, time.Minute); err == nil {
		t.Fatal("heartbeat of an unregistered worker succeeded")
	}
	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "", "build-1", now, time.Minute); err == nil {
		t.Fatal("heartbeat with an empty identity succeeded")
	}

	mustUpsert(t, repo, protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "worker-bad", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"}, HeartbeatTTL: "soon",
	}, now, time.Minute)
	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-bad", "build-1", now.Add(time.Second), time.Minute); err == nil {
		t.Fatal("heartbeat with an unparsable ttl succeeded")
	}
	// 同一 identity 的另一个 build 没有登记过，心跳同样报错。
	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-bad", "build-2", now.Add(time.Second), time.Minute); err == nil {
		t.Fatal("heartbeat of an unregistered build succeeded")
	}
	routes := activeRoutes(t, repo, now, "worker-bad")
	if len(routes) != 1 || !routes[0].LastHeartbeatAt.Equal(now) || !routes[0].ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("failed heartbeat changed the route: %+v", routes)
	}
}

func testExpiredRoutes(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	mustUpsert(t, repo, protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"},
	}, now, time.Minute)
	if routes := activeRoutes(t, repo, now.Add(59*time.Second), ""); len(routes) != 1 {
		t.Fatalf("routes before expiry = %+v, want 1", routes)
	}
	// 到期时刻本身已经不活跃。
	for _, at := range []time.Time{now.Add(time.Minute), now.Add(time.Hour)} {
		if routes := activeRoutes(t, repo, at, ""); len(routes) != 0 {
			t.Fatalf("routes at %v = %+v, want none", at.Sub(now), routes)
		}
	}
	// 心跳让过期的路由重新活跃。
	if err := repo.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-a", "build-1", now.Add(time.Hour), time.Minute); err != nil {
		t.Fatal(err)
	}
	if routes := activeRoutes(t, repo, now.Add(time.Hour), ""); len(routes) != 1 {
		t.Fatalf("routes after late heartbeat = %+v, want 1", routes)
	}
}

// 一个 Worker 的路由按 (namespace, identity) 列出，过期的也在内；别的 Worker 与别的 Namespace 不在内。
func testRoutesOfWorker(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	mustUpsert(t, repo, "tenant", protocol.ActivityWorkerVersion{
		Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q2", "q1"}, ActivityTypes: []string{"charge"},
	}, now, time.Minute)
	mustUpsert(t, repo, "tenant", protocol.ActivityWorkerVersion{
		Identity: "worker-b", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"},
	}, now, time.Minute)
	mustUpsert(t, repo, protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"},
	}, now, time.Minute)
	if err := repo.DeregisterActivityWorkerVersionInNamespace("tenant", "worker-a", "build-1", now); err != nil {
		t.Fatal(err)
	}
	routes, err := repo.ListActivityWorkerVersionsOfWorker("tenant", "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 2 || routes[0].TaskQueues[0] != "q1" || routes[1].TaskQueues[0] != "q2" {
		t.Fatalf("routes of worker-a in tenant = %+v, want q1 and q2 including expired", routes)
	}
	for _, route := range routes {
		if route.Namespace != "tenant" || route.Identity != "worker-a" {
			t.Fatalf("route of another worker listed: %+v", route)
		}
	}
}

// 注销让 Worker 的全部路由立刻到期，别的 Worker、别的 build、别的 Namespace 保持原状；
// 登记记录保留，重新登记后路由恢复。
func testDeregister(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	stopped := protocol.ActivityWorkerVersion{Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q1", "q2"}, ActivityTypes: []string{"charge"}}
	otherBuild := protocol.ActivityWorkerVersion{Identity: "worker-a", BuildID: "build-2", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"}}
	otherWorker := protocol.ActivityWorkerVersion{Identity: "worker-b", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"}}
	for _, worker := range []protocol.ActivityWorkerVersion{stopped, otherBuild, otherWorker} {
		mustUpsert(t, repo, protocol.DefaultNamespace, worker, now, time.Minute)
	}
	mustUpsert(t, repo, "tenant-a", stopped, now, time.Minute)

	at := now.Add(time.Second)
	if err := repo.DeregisterActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-a", "build-1", at); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, route := range activeRoutes(t, repo, at, "") {
		names = append(names, route.BuildID+" "+routeName(route))
	}
	sort.Strings(names)
	want := []string{
		"build-1 default/worker-b/charge@q1",
		"build-1 tenant-a/worker-a/charge@q1",
		"build-1 tenant-a/worker-a/charge@q2",
		"build-2 default/worker-a/charge@q1",
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("active routes after deregister = %v, want %v", names, want)
	}
	if known := activeRoutes(t, repo, time.Unix(0, 0), "worker-a"); len(known) != 5 {
		t.Fatalf("registration records after deregister = %d, want all 5 kept", len(known))
	}

	// 注销没有登记过的 Worker 是无操作。
	if err := repo.DeregisterActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "ghost", "build-1", at); err != nil {
		t.Fatalf("deregister of an unregistered worker: %v", err)
	}
	if err := repo.DeregisterActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "", "build-1", at); err == nil {
		t.Fatal("deregister with an empty identity succeeded")
	}

	mustUpsert(t, repo, protocol.DefaultNamespace, stopped, at.Add(time.Second), time.Minute)
	if routes := activeRoutes(t, repo, at.Add(2*time.Second), "worker-a"); len(routes) != 5 {
		t.Fatalf("routes after re-registration = %d, want 5", len(routes))
	}
}

func testWorkerNamespaceIsolation(t *testing.T, repo dispatch.WorkerVersionRepository) {
	now := registrationTime()
	worker := protocol.ActivityWorkerVersion{Identity: "worker-1", BuildID: "build-1", TaskQueues: []string{"queue-a"}, ActivityTypes: []string{"payments.Capture"}}
	mustUpsert(t, repo, "tenant-a", worker, now, time.Minute)
	worker.TaskQueues = []string{"queue-b"}
	mustUpsert(t, repo, "tenant-b", worker, now, time.Minute)

	// 同名 Worker 在两个 namespace 各有自己的路由；心跳与重新登记只作用于所在的 namespace。
	heartbeatAt := now.Add(time.Second)
	if err := repo.HeartbeatActivityWorkerVersionInNamespace("tenant-a", "worker-1", "build-1", heartbeatAt, time.Minute); err != nil {
		t.Fatal(err)
	}
	routes := activeRoutes(t, repo, heartbeatAt, "worker-1")
	if len(routes) != 2 {
		t.Fatalf("routes = %+v, want one per namespace", routes)
	}
	a, b := routes[0], routes[1]
	if a.Namespace != "tenant-a" || a.TaskQueues[0] != "queue-a" || !a.LastHeartbeatAt.Equal(heartbeatAt) {
		t.Fatalf("tenant-a route = %+v", a)
	}
	if b.Namespace != "tenant-b" || b.TaskQueues[0] != "queue-b" || !b.LastHeartbeatAt.Equal(now) {
		t.Fatalf("tenant-b route = %+v", b)
	}
	if err := repo.HeartbeatActivityWorkerVersionInNamespace("tenant-c", "worker-1", "build-1", heartbeatAt, time.Minute); err == nil {
		t.Fatal("heartbeat in a namespace without the worker succeeded")
	}
}
