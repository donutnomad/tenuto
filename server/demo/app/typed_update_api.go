package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	wfclient "github.com/donutnomad/tenuto/sdk/client"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

const typedCreateUpdateName = "create-record"

type typedCreateInput struct {
	RecordID string `json:"record_id"`
}

type typedCreateResult struct {
	RecordID string `json:"record_id"`
	Status   string `json:"status"`
}

func typedCreateRecordActivity(ctx context.Context, input typedCreateInput) (typedCreateResult, error) {
	select {
	case <-time.After(250 * time.Millisecond):
		return typedCreateResult{RecordID: input.RecordID, Status: "created"}, nil
	case <-ctx.Done():
		return typedCreateResult{}, ctx.Err()
	}
}

func typedApprovalWorkflow(ctx workflow.Context, input typedCreateInput) error {
	var result typedCreateResult
	createDone := false
	if err := workflow.SetUpdateHandler(ctx, typedCreateUpdateName,
		func(ctx workflow.Context) (typedCreateResult, error) {
			_ = workflow.Await(ctx, func() bool { return createDone })
			return result, nil
		},
	); err != nil {
		return err
	}
	if err := workflow.Sleep(ctx, 3*time.Second); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, typedCreateRecordActivity, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &result); err != nil {
		return err
	}
	createDone = true
	return nil
}

type typedCreateUseCase struct {
	client wfclient.Client
}

func (uc *typedCreateUseCase) Create(ctx context.Context, input typedCreateInput) (typedCreateResult, error) {
	start := uc.client.NewWithStartWorkflowOperation(wfclient.StartWorkflowOptions{
		ID:                       "create-" + input.RecordID,
		WorkflowIDConflictPolicy: wfclient.WorkflowIDConflictPolicyUseExisting,
	}, "typed-approval-workflow", input)
	handle, err := uc.client.UpdateWithStartWorkflow(ctx, wfclient.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: wfclient.UpdateWorkflowOptions{
			UpdateName:   typedCreateUpdateName,
			WaitForStage: wfclient.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return typedCreateResult{}, err
	}
	var result typedCreateResult
	if err := handle.Get(ctx, &result); err != nil {
		return typedCreateResult{}, err
	}
	return result, nil
}

func typedApprovalWorkflowConfig() (*program.WorkflowConfig, error) {
	source := []byte(`package flows
import (
    workflow "github.com/donutnomad/tenuto/sdk/workflow"
    "example.com/activities"
)

type CreateInput struct {
    RecordID string ` + "`json:\"record_id\"`" + `
}

type CreateResult struct {
    RecordID string ` + "`json:\"record_id\"`" + `
    Status string ` + "`json:\"status\"`" + `
}

func ApprovalWorkflow(ctx workflow.Context, input CreateInput) error {
    var result CreateResult
    createDone := false
    if err := workflow.SetUpdateHandler(ctx, "create-record", func(ctx workflow.Context) (CreateResult, error) {
        _ = workflow.Await(ctx, func() bool { return createDone })
        return result, nil
    }); err != nil { return err }
    if err := workflow.Sleep(ctx, 3000000000); err != nil { return err }
    if err := workflow.ExecuteActivity(ctx, activities.CreateRecord, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &result); err != nil { return err }
    createDone = true
    return nil
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "typed-approval-workflow",
		Name:         "Typed Update-With-Start Approval",
		FuncName:     "ApprovalWorkflow",
		Filename:     "typed_approval_workflow.go",
	})
	if err != nil {
		return nil, err
	}
	return generated.Workflow, nil
}

func noUpdateHandlerWorkflowConfig() (*program.WorkflowConfig, error) {
	source := []byte(`package flows
import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func NoUpdateHandlerWorkflow(ctx workflow.Context, input map[string]any) error {
    _ = input
    return workflow.Sleep(ctx, 500000000)
}
`)
	generated, err := sdkgen.GenerateFromSource(source, sdkgen.Options{
		WorkflowType: "no-update-handler-workflow",
		Name:         "No Update Handler (behavior demo)",
		FuncName:     "NoUpdateHandlerWorkflow",
		Filename:     "no_update_handler_workflow.go",
	})
	if err != nil {
		return nil, err
	}
	return generated.Workflow, nil
}

type publicCreateRecordRequest struct {
	RecordID string `json:"record_id"`
}

type publicCreateRecordResponse struct {
	RecordID string `json:"record_id"`
	Status   string `json:"status"`
}

func newPublicCreateRecordHandler(c wfclient.Client) http.Handler {
	uc := &typedCreateUseCase{client: c}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/demo/create-record", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request publicCreateRecordRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		request.RecordID = strings.TrimSpace(request.RecordID)
		if request.RecordID == "" || len(request.RecordID) > 128 {
			http.Error(w, "record_id must contain 1-128 characters", http.StatusBadRequest)
			return
		}
		result, err := uc.Create(r.Context(), typedCreateInput{RecordID: request.RecordID})
		if err != nil {
			http.Error(w, fmt.Sprintf("create record: %v", err), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(publicCreateRecordResponse(result))
	})
	mux.HandleFunc("/api/demo/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/api/demo/no-update-handler", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request publicCreateRecordRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		request.RecordID = strings.TrimSpace(request.RecordID)
		if request.RecordID == "" || len(request.RecordID) > 128 {
			http.Error(w, "record_id must contain 1-128 characters", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		start := c.NewWithStartWorkflowOperation(wfclient.StartWorkflowOptions{
			ID:                       "no-handler-" + request.RecordID,
			WorkflowIDConflictPolicy: wfclient.WorkflowIDConflictPolicyUseExisting,
		}, "no-update-handler-workflow")
		handle, err := c.UpdateWithStartWorkflow(ctx, wfclient.UpdateWithStartWorkflowOptions{
			StartWorkflowOperation: start,
			UpdateOptions:          wfclient.UpdateWorkflowOptions{UpdateName: typedCreateUpdateName, WaitForStage: wfclient.WorkflowUpdateStageCompleted},
		})
		if err == nil {
			err = handle.Get(ctx, nil)
		}
		// 没有 handler 的 Update 以失败结束：把失败原文返回给调用方。
		result := map[string]any{"completed": err == nil}
		if err != nil {
			result["error"] = err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
	return mux
}
