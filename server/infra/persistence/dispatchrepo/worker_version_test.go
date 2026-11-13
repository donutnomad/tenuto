package dispatchrepo

import (
	"os"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

// newTestRepository 返回独立 MySQL 库上的派发仓储；没有开启 MySQL 后端测试时跳过 t。
func newTestRepository(t *testing.T) *Repository {
	t.Helper()
	return New(mysqltest.NewDB(t))
}

// TestContractsRoundTrip 固定 contracts_json 持久化：worker 上报的契约跨 Upsert/ListActive 不丢失，
// 这也是 server 侧契约对照在生产 DB 模式下生效的前提。
func TestContractsRoundTrip(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now()
	contracts := []protocol.ActivityContract{{Type: "payments.charge.v1", Version: "v1", Input: "input@fp1", Output: "output@fp2"}}
	if _, err := repository.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "payments-worker", BuildID: "build-1", TaskQueues: []string{"payments"},
		ActivityTypes: []string{"payments.charge.v1"}, Contracts: contracts, Default: true,
	}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	workers, err := repository.ListActiveActivityWorkerVersions(now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 {
		t.Fatalf("active workers = %d, want 1", len(workers))
	}
	if len(workers[0].Contracts) != 1 || workers[0].Contracts[0] != contracts[0] {
		t.Fatalf("contracts round-trip = %+v", workers[0].Contracts)
	}

	// 登记时没有带契约的 Worker 读回空集合；声明了契约的 Activity 不会派给它。
	if _, err := repository.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "legacy-worker", BuildID: "build-0", TaskQueues: []string{"payments"},
		ActivityTypes: []string{"payments.charge.v1"},
	}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	workers, err = repository.ListActiveActivityWorkerVersions(now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	var legacy *protocol.ActivityWorkerVersion
	for i := range workers {
		if workers[i].Identity == "legacy-worker" {
			legacy = &workers[i]
		}
	}
	if legacy == nil {
		t.Fatal("legacy worker not listed")
	}
	if len(legacy.Contracts) != 0 {
		t.Fatalf("legacy worker contracts = %+v, want empty", legacy.Contracts)
	}
}

// Happy Path: 心跳按行上登记的 heartbeat_ttl 给 worker 的全部行续期，别的 worker 的行保持原值。
func TestHeartbeatRenewsEveryRowOfTheWorker(t *testing.T) {
	repository := newTestRepository(t)
	registeredAt := time.Now().UTC().Truncate(time.Millisecond)
	for _, worker := range []protocol.ActivityWorkerVersion{
		{Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q1", "q2"}, ActivityTypes: []string{"charge", "refund"}, HeartbeatTTL: "90s"},
		{Identity: "worker-b", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"}},
	} {
		if _, err := repository.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, worker, registeredAt, time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	heartbeatAt := registeredAt.Add(30 * time.Second)
	if err := repository.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-a", "build-1", heartbeatAt, time.Hour); err != nil {
		t.Fatal(err)
	}

	var rows []ActivityWorkerVersionPO
	if err := repository.db.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	renewed := 0
	for _, row := range rows {
		switch row.Identity {
		case "worker-a":
			renewed++
			if !row.LastHeartbeatAt.Equal(heartbeatAt) || !row.ExpiresAt.Equal(heartbeatAt.Add(90*time.Second)) {
				t.Errorf("worker-a row %s/%s: heartbeat = %v expires = %v", row.ActivityType, row.TaskQueue, row.LastHeartbeatAt, row.ExpiresAt)
			}
		case "worker-b":
			if !row.LastHeartbeatAt.Equal(registeredAt) || !row.ExpiresAt.Equal(registeredAt.Add(time.Minute)) {
				t.Errorf("worker-b row was touched: heartbeat = %v expires = %v", row.LastHeartbeatAt, row.ExpiresAt)
			}
		}
	}
	if renewed != 4 {
		t.Fatalf("renewed worker-a rows = %d, want 4 (2 activity types x 2 task queues)", renewed)
	}

	// 行上没有登记 TTL 时采用调用方给出的 fallback。
	if err := repository.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-b", "build-1", heartbeatAt, time.Hour); err != nil {
		t.Fatal(err)
	}
	var workerB ActivityWorkerVersionPO
	if err := repository.db.Where("identity = ?", "worker-b").Take(&workerB).Error; err != nil {
		t.Fatal(err)
	}
	if !workerB.ExpiresAt.Equal(heartbeatAt.Add(time.Hour)) {
		t.Fatalf("worker-b expires = %v, want heartbeat + fallback ttl", workerB.ExpiresAt)
	}
}

// Error Case: 未注册的 worker 心跳报错；行上登记了无法解析的 TTL 时报错且不续期。
func TestHeartbeatRejectsUnregisteredWorkerAndInvalidTTL(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := repository.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "ghost", "build-1", now, time.Minute); err == nil {
		t.Fatal("heartbeat of an unregistered worker succeeded")
	}

	if _, err := repository.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "worker-bad", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"}, HeartbeatTTL: "soon",
	}, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := repository.HeartbeatActivityWorkerVersionInNamespace(protocol.DefaultNamespace, "worker-bad", "build-1", now.Add(time.Second), time.Minute); err == nil {
		t.Fatal("heartbeat with an unparsable ttl succeeded")
	}
	var row ActivityWorkerVersionPO
	if err := repository.db.Where("identity = ?", "worker-bad").Take(&row).Error; err != nil {
		t.Fatal(err)
	}
	if !row.LastHeartbeatAt.Equal(now) {
		t.Fatalf("failed heartbeat moved last_heartbeat_at to %v", row.LastHeartbeatAt)
	}
}

// Error Case: contracts_json 损坏时 ListActive 返回错误，契约不会被当成空集合。
func TestListActiveReportsCorruptContracts(t *testing.T) {
	repository := newTestRepository(t)
	now := time.Now()
	if _, err := repository.UpsertActivityWorkerVersionInNamespace(protocol.DefaultNamespace, protocol.ActivityWorkerVersion{
		Identity: "worker-a", BuildID: "build-1", TaskQueues: []string{"q1"}, ActivityTypes: []string{"charge"},
	}, now, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := repository.db.Model(&ActivityWorkerVersionPO{}).Where("identity = ?", "worker-a").
		Update("contracts_json", []byte("{not json")).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ListActiveActivityWorkerVersions(now.Add(time.Minute)); err == nil {
		t.Fatal("corrupt contracts_json was swallowed")
	}
}
