package protocol

import (
	"encoding/json"
	"testing"
)

func TestExecutionCacheSnapshotWireKeys(t *testing.T) {
	// Worker 列表与 Worker 请求头里的缓存快照用 snake_case 键；计数为零的准入、拒绝与驱逐省略。
	data, err := json.Marshal(ExecutionCacheSnapshot{Entries: 2, EstimatedBytes: 64, Hits: 3, Misses: 1, Rejected: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), `{"entries":2,"estimated_bytes":64,"hits":3,"misses":1,"rejected":4}`; got != want {
		t.Fatalf("snapshot JSON = %s, want %s", got, want)
	}
}
