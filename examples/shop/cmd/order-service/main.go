// Command order-service 是下单入口：对外提供 HTTP API，启动 place-order Workflow，
// 并在 orders 队列上执行通知 Activity。
//
//	TENUTO_URL=http://localhost:7400 go run ./cmd/order-service
//
// API（默认监听 :8080，ORDER_HTTP_ADDR 修改）：
//
//	POST /orders                  {"sku":"book","qty":1,"amount":4900,"address":"..."} → 启动 Run
//	GET  /orders/{id}             Run 状态；进行中时附带 Query 读到的阶段，结束时附带结果
//	POST /orders/{id}/delivered   发送 delivered Signal，确认收货
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/donutnomad/tenuto/examples/shop/internal/svc"
	"github.com/donutnomad/tenuto/examples/shop/orders"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	log.SetPrefix("[order] ")
	log.SetFlags(log.Ltime)
	ctx, stop := svc.SignalContext()
	defer stop()

	c, err := svc.Dial()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	autoConfirm, err := strconv.Atoi(svc.Env("AUTO_CONFIRM_SECONDS", "120"))
	if err != nil {
		log.Fatalf("AUTO_CONFIRM_SECONDS: %v", err)
	}
	api := &api{c: c, autoConfirm: autoConfirm, seq: time.Now().Unix() % 100000 * 100}
	server := &http.Server{Addr: svc.Env("ORDER_HTTP_ADDR", ":8080"), Handler: api.routes()}
	go func() {
		log.Printf("HTTP API on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	// order-service 拥有 place-order：Worker 启动时登记 Workflow 定义，并执行 orders.notify。
	registry := activity.NewRegistry()
	if err := orders.RegisterActivities(registry); err != nil {
		log.Fatal(err)
	}
	if err := svc.RunWorker(ctx, "order-service", c, worker.Options{
		Workflows:           []registration.Definition{orders.WorkflowDefinition()},
		Activities:          registry,
		ActivityTaskQueues:  []string{"orders"},
		ActivityConcurrency: 8,
		BuildID:             svc.Env("BUILD_ID", "order-dev"),
	}); err != nil {
		log.Fatal(err)
	}
}

type api struct {
	c           client.Client
	autoConfirm int
	seq         int64
}

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", a.placeOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /orders/{id}/delivered", a.delivered)
	return mux
}

func workflowID(orderID string) string { return "order-" + orderID }

func (a *api) placeOrder(w http.ResponseWriter, r *http.Request) {
	var in orders.OrderInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if in.SKU == "" || in.Qty <= 0 || in.Amount <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "sku, qty and amount are required"})
		return
	}
	if in.OrderID == "" {
		in.OrderID = strconv.FormatInt(atomic.AddInt64(&a.seq, 1), 10)
	}
	if in.AutoConfirmSeconds <= 0 {
		in.AutoConfirmSeconds = a.autoConfirm
	}
	// Workflow ID 用订单号：同一订单重复提交不会启动第二个 Run。
	run, err := a.c.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{ID: workflowID(in.OrderID)}, "place-order", in)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	log.Printf("order %s placed: workflow=%s run=%s", in.OrderID, run.GetID(), run.GetRunID())
	writeJSON(w, http.StatusAccepted, map[string]any{"order_id": in.OrderID, "workflow_id": run.GetID(), "run_id": run.GetRunID()})
}

func (a *api) getOrder(w http.ResponseWriter, r *http.Request) {
	id := workflowID(r.PathValue("id"))
	desc, err := a.c.DescribeWorkflowExecution(r.Context(), id, "")
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
		return
	}
	resp := map[string]any{"order_id": r.PathValue("id"), "run_id": desc.RunID, "run_status": desc.Status}
	if b := desc.Blocked; b != nil {
		// Workflow Task 连续失败，例如下一步 Activity 所在队列没有在线 Worker。
		// Run 按退避重试，原因消除后自动继续。
		resp["blocked"] = map[string]any{"cause": b.Cause, "reason": b.Reason, "failures": b.ConsecutiveFailures}
	}
	switch desc.Status {
	case client.WorkflowExecutionStatusRunning:
		// Query 在第一个 Workflow Task 完成前会被拒绝，此时只返回 Run 状态。
		if value, err := a.c.QueryWorkflow(r.Context(), id, "", "status"); err == nil {
			var progress map[string]any
			if err := value.Get(&progress); err == nil {
				resp["progress"] = progress
			}
		}
	default:
		var out orders.OrderOutput
		if err := a.c.GetWorkflow(r.Context(), id, desc.RunID).Get(r.Context(), &out); err != nil {
			resp["error"] = err.Error()
		} else {
			resp["result"] = out
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *api) delivered(w http.ResponseWriter, r *http.Request) {
	id := workflowID(r.PathValue("id"))
	if err := a.c.SignalWorkflow(r.Context(), id, "", orders.DeliveredSignal, map[string]any{"at": time.Now().Format(time.RFC3339)}); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"order_id": r.PathValue("id"), "signal": orders.DeliveredSignal})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Print(fmt.Errorf("write response: %w", err))
	}
}
