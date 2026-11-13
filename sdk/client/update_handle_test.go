package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/runtimeapi"
)

// fakeUpdateServer 第一次 update 请求返回 Accepted；之后的 Completed 长轮询先阻塞到 release 关闭，再返回结果。
type fakeUpdateServer struct {
	polls   atomic.Int32
	release chan struct{}
}

func (f *fakeUpdateServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req runtimeapi.WorkflowRuntimeUpdateRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	result := protocol.WorkflowUpdateResult{UpdateID: "u-1", RunID: "run-1", Stage: protocol.UpdateReachedStageEnums.Accepted}
	if req.WaitStage == string(protocol.UpdateWaitStageEnums.Completed) {
		f.polls.Add(1)
		select {
		case <-f.release:
		case <-r.Context().Done():
			return
		}
		payload, _ := protocol.NewJSONPayload(map[string]any{"ok": true})
		result.Stage = protocol.UpdateReachedStageEnums.Completed
		result.Outcome = &protocol.UpdateOutcome{Success: &payload}
	}
	_ = json.NewEncoder(w).Encode(result)
}

// 并发的 Get 共用一次 Completed 轮询；ctx 取消的 Get 不影响句柄，之后的 Get 仍拿到结果。
func TestUpdateHandleSharesOnePollAcrossGets(t *testing.T) {
	fake := &fakeUpdateServer{release: make(chan struct{})}
	server := httptest.NewServer(fake)
	defer server.Close()
	c, err := Dial(Options{HostPort: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	handle, err := c.UpdateWorkflow(context.Background(), UpdateWorkflowOptions{WorkflowID: "wf", UpdateName: "op"})
	if err != nil {
		t.Fatal(err)
	}
	if handle.RunID() != "run-1" || handle.UpdateID() != "u-1" {
		t.Fatalf("handle = %s/%s", handle.RunID(), handle.UpdateID())
	}

	canceled, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := handle.Get(canceled, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get with an expired ctx = %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var out map[string]bool
			err := handle.Get(context.Background(), &out)
			if err == nil && !out["ok"] {
				err = errors.New("missing result")
			}
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(fake.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	// 第一次轮询随过期的 ctx 放弃，之后五个并发 Get 只轮询一次。
	if got := fake.polls.Load(); got != 2 {
		t.Fatalf("completed polls = %d, want 2", got)
	}
}
