package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTypedApprovalWorkflowConfig(t *testing.T) {
	wf, err := typedApprovalWorkflowConfig()
	if err != nil {
		t.Fatalf("generate typed approval workflow: %v", err)
	}
	if wf.ID != "typed-approval-workflow" {
		t.Fatalf("unexpected workflow ID %q", wf.ID)
	}
	if len(wf.Program.Program) == 0 {
		t.Fatal("expected a WLang program")
	}
	if _, ok := wf.Activities["activities.CreateRecord"]; !ok {
		t.Fatalf("expected CreateRecord activity symbol, got %#v", wf.Activities)
	}
}

func TestNoUpdateHandlerWorkflowConfig(t *testing.T) {
	wf, err := noUpdateHandlerWorkflowConfig()
	if err != nil {
		t.Fatalf("generate no-handler workflow: %v", err)
	}
	if wf.ID != "no-update-handler-workflow" {
		t.Fatalf("unexpected workflow ID %q", wf.ID)
	}
}

func TestPublicCreateRecordHandlerValidation(t *testing.T) {
	handler := newPublicCreateRecordHandler(nil)
	tests := []struct {
		name   string
		method string
		body   string
		want   int
	}{
		{name: "method", method: http.MethodGet, body: "", want: http.StatusMethodNotAllowed},
		{name: "json", method: http.MethodPost, body: "{", want: http.StatusBadRequest},
		{name: "record id", method: http.MethodPost, body: `{"record_id":""}`, want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/demo/create-record", strings.NewReader(tt.body))
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			if resp.Code != tt.want {
				t.Fatalf("status = %d, want %d", resp.Code, tt.want)
			}
		})
	}
}
