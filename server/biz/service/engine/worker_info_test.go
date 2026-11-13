package engine

import (
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func TestWorkerInfoHeadersRoundTripWorkflowCapabilities(t *testing.T) {
	request := httptest.NewRequest("POST", "/runtime/workflow-task/poll", nil)
	want := protocol.WorkerInfo{
		Identity:             "worker-1",
		BuildID:              "build-2",
		WorkflowCapabilities: []string{"wlang-program-schema:wlang/program/v2", "wlang-update-handler-abi-v1"},
	}
	protocol.ApplyWorkerInfoHeaders(request, want)
	if got := protocol.WorkerInfoFromRequest(request); !reflect.DeepEqual(got, want) {
		t.Fatalf("worker info = %#v", got)
	}
}
