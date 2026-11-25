# 从普通 Go 到可审阅 JSON

日常修改入口为 `v2/workflow.go` 的 `Lifecycle` 函数；注解、typed 桥与当前 CLI 见
[GENERATION.md](GENERATION.md)。`v1/workflow.go` 保留变更前的 fixture，用于比较生成结果与验证历史兼容性。四个 Activity 实现在 `activities/activities.go`，使用普通 Go 的 `context.Context` 与 struct 入参/返回值；worker 按 `workflow.json` 的 `activities` 核对并上报 wire 指纹契约。

## 生成与保存同步

日常主路径是「构建 + 启动」：生成器把 `workflow.json` embed 进二进制，Worker `Start` 时自动登记
（新内容首次出现时激活 latest，旧 binary 重启保持 latest）。最短示例见
`docs/examples/startup-workflow`：struct IO + 注解 + worker 配置里一行
`Workflows: []registration.Definition{startupflow.WorkflowDefinition()}`。

```sh
# 生成 → 构建（定义随二进制）
make build-example
# 或在 CI 里：
go generate ./...
go build -o bin/tenuto-startup-example ./docs/examples/startup-workflow/worker/
```

普通 `go build` 不会执行 `go:generate`：构建入口必须先跑生成，否则二进制里是上一次的定义。

当前 CLI 与注解规则见同目录 [GENERATION.md](GENERATION.md)；下面这些命令用于显式生成/发布/回滚。
作者修改 `v2/workflow.go`、`v2/activities` 或 `remote` 契约后，从仓库根目录执行：

```sh
# 全仓唯一生成入口（gogen + 本示例全部产物）
go generate ./...

# 手动生成（不联网）
go run ./compiler/cmd/tenuto-workflow-gen generate \
  ./docs/examples/versioned-workflow/v1 ./docs/examples/versioned-workflow/v2

# 高级：显式发布 latest（运维发布/回滚；跳过二进制自带定义）
go run ./compiler/cmd/tenuto-workflow-gen generate \
  -watch -url http://localhost:7400 -namespace lifecycle-demo -publish \
  ./docs/examples/versioned-workflow/v2

# 只登记 artifact、不切换 latest（回滚候选/旧 Worker 幂等登记）
go run ./compiler/cmd/tenuto-workflow-gen generate \
  -url http://localhost:7400 -namespace lifecycle-demo \
  ./docs/examples/versioned-workflow/v2
```

`-watch` 只哈希手写依赖源码（入口、同包 DTO、被引用契约包），不监听输出 JSON/`zz_*_contract_gen.go`，
因此不会自循环；生成失败保留上次成功产物，修复后再保存继续。`-url` 非 2xx 打印响应体并失败。
仓库级 `go generate` 只保留 `server/cmd/tenuto-server/main.go` 一处指令。

Go 版本要求见 `go.mod`。编译链为 Go 源码 → WLang
`go2wlang.TranslateFileDetailed` → Tenuto `sdkgen` 封装和内容版本 → JSON。

## JSON 形状与用途

| 文件/请求 | 形状 | 用途 |
| --- | --- | --- |
| `workflow.json` | `id/name/policy/version/program/activities`：`program` 是 WLang 程序，`activities` 把程序里的 Activity 符号映射到类型、队列与契约指纹 | Runtime 接受的 WorkflowConfig |
| 注册 HTTP body | `{"namespace":"...","workflow":<workflow.json>}` | 向服务注册配置 |

`workflow.json` 规范化内容的 hash 就是服务端登记后的配置版本，生成器打印的 `content version` 与注册响应里的版本相同。

## 直接从生成文件注册、启动、释放

从仓库根目录，在第一个终端启动内存服务：

```sh
WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR=127.0.0.1:7400 \
  go run ./server/cmd/tenuto-server
```

该模式的运行数据随服务进程保存，服务退出后重置。持久化服务使用 MySQL 与 taskq，配置见 [`docs/deploy/server.md`](../../deploy/server.md)。内存演示的服务就绪后，在第二个终端启动示例 worker：

```sh
TENUTO_URL=http://localhost:7400 TENUTO_NAMESPACE=lifecycle-demo \
  go run ./docs/examples/versioned-workflow/worker \
  -build-id lifecycle-build-v1 -identity lifecycle-activity-v1
```

第二个 Activity build 可用 `-build-id lifecycle-build-v2 -identity lifecycle-activity-v2 -default=false` 启动：两个 build 消费同一个 `lifecycle` 队列，任务不带 build，由任一 build 领取，契约不一致的领取被拒绝。启动日志包含 Activity BuildID，`ActivityStarted` 记录实际执行的 build。

`worker/main.go` 同时启动 workflow worker 和 `lifecycle` 队列的 Activity worker。本地活动用生成的一行 `activities.RegisterActivities(registry)` 注册 `activities.Reserve`、`activities.Charge`、`activities.ChargeV2`、`activities.Notify`（含 type 与 wire 契约），实现直接来自 `activities/activities.go`；`payments.settle` 这个 remote 契约的实现由 worker 自己提供，是唯一一条手写注册。日志显示 `ready` 后，在另一终端运行以下命令（需要 `curl` 和 `jq`）：

```sh
export TENUTO_URL=http://127.0.0.1:7400
export TENUTO_NAMESPACE=lifecycle-demo
# 先保留一个 v1 旧运行；它将等待 release。
jq -n --arg namespace "$TENUTO_NAMESPACE" \
  --slurpfile workflow docs/examples/versioned-workflow/v1/workflow.json \
  '{namespace:$namespace,workflow:$workflow[0]}' > /tmp/lifecycle-register-v1.json
curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/register" \
  -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-register-v1.json
jq -n --arg namespace "$TENUTO_NAMESPACE" \
  '{namespace:$namespace,workflow_type:"lifecycle-demo",workflow_id:"lifecycle-demo",input:{order_id:"order-old"}}' \
  > /tmp/lifecycle-start-v1.json
curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/start" \
  -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-start-v1.json \
  > /tmp/lifecycle-run-v1.json
export TENUTO_OLD_RUN_ID=$(jq -r .run_id /tmp/lifecycle-run-v1.json)
# 先确认旧run已完成首个WFT并进入signal等待。
jq -n --arg namespace "$TENUTO_NAMESPACE" --arg run_id "$TENUTO_OLD_RUN_ID" \
  '{namespace:$namespace,run_id:$run_id}' > /tmp/lifecycle-history-v1-request.json
for attempt in $(seq 1 100); do
  curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/history" \
    -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-history-v1-request.json \
    > /tmp/lifecycle-history-v1.json
  jq -e 'any(.[]; has("workflowTaskCompleted"))' /tmp/lifecycle-history-v1.json >/dev/null && break
  sleep 0.2
done
jq -e 'any(.[]; has("workflowTaskCompleted"))' /tmp/lifecycle-history-v1.json >/dev/null || exit 1
# 注册 v2 后的新运行使用新配置；旧运行继续引用 v1。
jq -n --arg namespace "$TENUTO_NAMESPACE" \
  --slurpfile workflow docs/examples/versioned-workflow/v2/workflow.json \
  '{namespace:$namespace, workflow:$workflow[0]}' > /tmp/lifecycle-register.json
curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/register" \
  -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-register.json
# 当前API允许同namespace/workflow_id保留一个active run；先让旧run完成。
jq -n --arg namespace "$TENUTO_NAMESPACE" --arg run_id "$TENUTO_OLD_RUN_ID" \
  '{namespace:$namespace,run_id:$run_id,name:"release",input:{}}' \
  > /tmp/lifecycle-signal-v1.json
curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/signal" \
  -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-signal-v1.json
jq -n --arg namespace "$TENUTO_NAMESPACE" --arg run_id "$TENUTO_OLD_RUN_ID" \
  '{namespace:$namespace,run_id:$run_id}' > /tmp/lifecycle-get-v1.json
for attempt in $(seq 1 100); do
  curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/get-run" \
    -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-get-v1.json \
    > /tmp/lifecycle-result-v1.json
  [ "$(jq -r .run.status /tmp/lifecycle-result-v1.json)" = completed ] && break
  sleep 0.2
done
# 验证通过后再启动使用v2的新run。
test "$(jq -r .run.status /tmp/lifecycle-result-v1.json)" = completed || exit 1
jq .run.output /tmp/lifecycle-result-v1.json
jq -n --arg namespace "$TENUTO_NAMESPACE" \
  '{namespace:$namespace,workflow_type:"lifecycle-demo",workflow_id:"lifecycle-demo",input:{order_id:"order-1"}}' \
  > /tmp/lifecycle-start.json
curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/start" \
  -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-start.json \
  | tee /tmp/lifecycle-run.json
export TENUTO_RUN_ID=$(jq -r .run_id /tmp/lifecycle-run.json)
# 等首个WFT完成、工作流进入signal等待，再发送release。
jq -n --arg namespace "$TENUTO_NAMESPACE" --arg run_id "$TENUTO_RUN_ID" \
  '{namespace:$namespace,run_id:$run_id}' > /tmp/lifecycle-history-request.json
for attempt in $(seq 1 100); do
  curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/history" \
    -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-history-request.json \
    > /tmp/lifecycle-history.json
  jq -e 'any(.[]; has("workflowTaskCompleted"))' /tmp/lifecycle-history.json >/dev/null && break
  sleep 0.2
done
jq -e 'any(.[]; has("workflowTaskCompleted"))' /tmp/lifecycle-history.json >/dev/null || exit 1
jq -n --arg namespace "$TENUTO_NAMESPACE" --arg run_id "$TENUTO_RUN_ID" \
  '{namespace:$namespace,run_id:$run_id,name:"release",input:{}}' \
  > /tmp/lifecycle-signal.json
curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/signal" \
  -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-signal.json
jq -n --arg namespace "$TENUTO_NAMESPACE" --arg run_id "$TENUTO_RUN_ID" \
  '{namespace:$namespace,run_id:$run_id}' > /tmp/lifecycle-get.json
for attempt in $(seq 1 100); do
  curl --fail-with-body -sS "$TENUTO_URL/runtime/workflow/get-run" \
    -H 'Content-Type: application/json' --data-binary @/tmp/lifecycle-get.json \
    > /tmp/lifecycle-result.json
  [ "$(jq -r .run.status /tmp/lifecycle-result.json)" = completed ] && break
  sleep 0.2
done
test "$(jq -r .run.status /tmp/lifecycle-result.json)" = completed || exit 1
jq .run.output /tmp/lifecycle-result.json
```

上述命令在发送每个运行的 `release` 前等待首个 WFT 完成。实测信号紧随 start、落在首个 WFT 内时，运行可能停止调度；随后补发信号可能触发 replay mismatch。稳定演示先确认历史出现 `workflowTaskCompleted`，再发信号。

v1/v2 都先等待 `release`，用于保留观察中的运行。同一 namespace/workflow_id 的第二个 active run 会收到 `400 current run conflict`，上述命令先结束旧 run 再启动新 run。v1 顺序执行 Reserve → Charge → Notify，结果 `policy=legacy`。v2 新运行执行 Reserve → ChargeV2 → Notify，结果 `policy=v2`。

## 三层版本与旧分支生命周期

1. **源码兼容分支**：v1 的 `GetVersion(ctx, "charge-policy", 3, 4)` 在新执行记录版本 4；v2 的 `GetVersion(ctx, "charge-policy", 3, 6)` 在新执行记录版本 6，重放 v1 历史时读回 4，选择原 Charge 分支。历史越过该调用却没有 marker 时得到 `workflow.DefaultVersion`（-1），落在 `[3, 6]` 之外，Workflow Task 以非确定性错误失败（`TestMarkerlessHistoryIsOutsideSupportedRange`）；要接住这类历史，把 `minSupported` 写成 `workflow.DefaultVersion`。保持 change ID 稳定，并保留旧历史需要的活动顺序和结果语义。
2. **服务端配置版本与 active**：注册新版配置让后续按 workflow ID 启动的运行选中当前 active；已创建运行固定引用其配置。旧运行继续使用已注册旧 Program 的现象由版本固定实现。`example_test.go` 额外把 v1 历史直接交给 v2 Program，验证源码兼容分支本身，测试结果分别为 legacy / legacy / v2。
3. **Activity worker build**：活动类型与队列决定任务投到哪里，队列上契约一致的任一 build 执行它。worker 的 BuildID 在启动配置中管理，单个 Activity 调用不指定 build。

清理旧分支前，应确认所有会交给当前 Program 重放的旧历史均已退出保留/重放范围，包含恢复、reset 和保留历史的运维路径；记录版本 1 或缺少 marker 的历史仍可能依赖旧 Charge 逻辑。运行通过服务端固定旧 Program 继续执行，与当前源码里是否保留分支是两项独立事实。

删除本地旧 Go 或 JSON 只影响本地生成输入/产物；服务端已注册配置及运行历史独立存储。当前 Runtime 接口提供注册、查询和按版本启动；本例使用重新注册切换 active 来演示发布。当前 Runtime 路由表提供注册、查询、启动与 worker target 配置。定义的独立启用、停用、归档和永久删除操作属于当前功能缺口；重新注册旧哈希会将 active 切回旧版本。

删除 `v1/*.json` 后，保留 v1 源码和 main.go 中生成指令时，下一次 `go generate ./...` 会重建它们。删除 `v1/workflow.go` 时，应同步移除对应生成指令。watch 对源文件删除输出读取错误，并保留已有 JSON；watch 对输出删除会在下一次源码内容变化或重启时重建。watch 的职责为生成与刷新，产物清理由显式文件操作完成。

## 验证与差异审阅

```sh
go generate ./...
go test -v ./docs/examples/versioned-workflow/... ./compiler/cmd/tenuto-workflow-gen
go test ./compiler/...
go vet ./...
diff -u docs/examples/versioned-workflow/v1/workflow.go docs/examples/versioned-workflow/v2/workflow.go
diff -u docs/examples/versioned-workflow/v1/workflow.json docs/examples/versioned-workflow/v2/workflow.json
```

`TestSourceVersionCompatibility` 使用真实 sdkgen 与 interpreter，执行普通 Go activities 并保存历史，随后将旧历史交给 v2 Program 重放。`TestWatchRegeneratesAfterSaveAndRecovers` 验证保存刷新、语法错误保留产物、修复后继续同步。
同包 typed authoring（`return CallCharge(ctx, in)`，结构体变量入参）由
`compiler/sdkgen/samepkg_test.go` 用解释器回归。
