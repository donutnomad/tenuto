# Tenuto Demo

`server/demo/app` 是 `make dev` 启动的 Demo 进程：它连接 Tenuto Server，在 `demo` Namespace 下运行一个 Activity Worker 和一个 Workflow Worker，登记一组演示 Workflow，创建演示 Run 与 Schedule，并开放一个演示 Update-With-Start 的业务 HTTP 接口。它只使用公开 SDK（`sdk/client`、`sdk/worker`、`sdk/activity`、`sdk/registration`），与 Server 之间只走 Runtime HTTP API。

在仓库根目录运行：

```bash
make dev                                              # Server + Demo + Console
make dev-demo TENUTO_SERVER_URL=http://127.0.0.1:7400 # 只运行 Demo，连接已有的 Server
```

Demo 先等待 Server 的 `/health/ready`，再启动 Worker、创建 Run。Server 地址取环境变量 `WORKFLOW_SERVER_URL`（`make` 目标用 `TENUTO_SERVER_URL` 传入）。Server 开启 `WORKFLOW_HTTP_AUTH_TOKEN` 时，环境变量 `TENUTO_AUTH_TOKEN` 给出其中一个 token，Demo 的 Client 与 Worker 随请求带上它。

## 进程内容

- **Activity Worker**（Identity `demo-worker-1`）：订阅 `default` 队列，注册 `custodian.Reserve`、`wallets.Sign`、`check.Balance`、`transfer.Send`、`notify.Send`、`payment.Process`、`payment.Confirm`、`longRunning.Process`、`demo.Flaky`（前两次 Attempt 失败、第三次成功）与 `activities.CreateRecord`。
- **Workflow Worker**（Identity `tenuto-demo-workflow-worker-1`）：Workflow 源码写在 `main.go` 与 `typed_update_api.go` 的字符串里，启动时由 `compiler/sdkgen` 生成 WLang Program，随 Worker 启动登记。
- **演示 Run**：线性流程、循环与分支、240 次循环 Activity、重试、`workflow.Go` fan-out、Child Workflow、Continue-As-New，以及两条运行约 5 分钟、用于取消/终止/Reset 的长 Run。每条 Run 带 Search Attribute `business_id=biz-demo`。
- **Schedule**：`demo-cross-app-flow` 每 5 分钟、`demo-auto-sweep` 每小时（已暂停）、`demo-wlang-order` 工作日每天 9 点。

Workflow 与推荐页面的对照见 [运行示例环境](../../docs/quickstart/demo.md)。

## Update-With-Start 接口

Demo 监听 `DEMO_HTTP_ADDR`（缺省 `0.0.0.0:7403`）：

```bash
curl -X POST http://localhost:7403/api/demo/create-record \
  -H 'Content-Type: application/json' \
  -d '{"record_id":"record-http-001"}'
# {"record_id":"record-http-001","status":"created"}
```

请求以 Workflow ID `create-<record_id>` 和 `WorkflowIDConflictPolicyUseExisting` 调用 `UpdateWithStartWorkflow`。Workflow `typed-approval-workflow` 先 durable Sleep 3 秒，再执行 `activities.CreateRecord`；Update Handler `create-record` 等这个 Activity 完成后返回 Typed Result，HTTP 响应随之返回。重复提交同一个 `record_id` 复用同一个 Workflow。

`POST /api/demo/no-update-handler` 对一个没有注册 Update Handler 的 Workflow 发 Update，响应里是 Update 失败的原文。`GET /api/demo/health` 是健康检查。

## Trace 调试样例

Demo 在日志里打印可以直接打开的 Trace 地址：

```text
TRACE_LOOP_ACTIVITY_URL=http://localhost:7401/demo/runs/<run-id>/trace   # demo-auto-sweep：同一行两次 Activity 调用
TRACE_BULK_ACTIVITY_URL=http://localhost:7401/demo/runs/<run-id>/trace   # demo-activity-bulk-loop：同一行 240 次调用
TRACE_LOOP_GO_URL=http://localhost:7401/demo/runs/<run-id>/trace         # demo-go-loop-fanout：循环内三条 workflow.Go lane
TRACE_TEST_URL=http://localhost:7401/demo/runs/<run-id>/trace            # demo-trace-child-debug-suite：三个 Child
TRACE_CAN_ROOT_URL=http://localhost:7401/demo/runs/<run-id>/trace        # demo-trace-can-root：根 Run 的 Continue-As-New
```

`demo-trace-child-debug-suite` 依次启动三个 Child：`demo-trace-payment-child`（普通 Child，用于 Step Into/Out、源码、栈和变量）、`demo-trace-sequential-child`（顺序 Activity）与 `demo-trace-can-child`（两段 Continue-As-New，用于 Run Segment 前进与回退）。Parent 等待前两个 Child 后完成。`demo-long-parent-child` 的 Child 长时间运行，用于查看运行中 Child 的 frontier。

数据在内存后端里，Server 停止后清空。
