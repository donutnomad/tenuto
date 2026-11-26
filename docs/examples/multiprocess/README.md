# 多进程 Workflow 示例

示例通过 HTTP 连接三个独立进程：

- `tenuto-server`：Workflow 引擎、HTTP API、内嵌 taskq matching、内嵌 workflow worker（执行全部 Workflow Task）。
- `app-1`：Activity 进程，提供 `orders.finalize` 并随启动登记订单 Workflow 定义，脚本按 v1、v2、v3 重启。
- `app-2`：Activity 进程，提供库存、风控和账务 Activity，供 Workflow 跨进程调用。

Console 是独立的网页进程。数据写入本地 MySQL。

## 运行

在项目根目录执行：

```bash
MP_MYSQL_CNF="$HOME/.my.cnf" ./docs/examples/multiprocess/run-demo.sh
```

MySQL 配置使用 `[client]` 段的 `host`、`port`、`user` 和 `password`。也可以通过 `MP_MYSQL_HOST`、`MP_MYSQL_PORT`、`MP_MYSQL_USER`、`MP_MYSQL_PASSWORD` 覆盖。凭据通过环境和权限为 `0600` 的临时客户端配置传递，退出时删除临时配置。

脚本每次重建 `mp_demo_workflow` 和 `mp_demo_taskq`。`MP_DB_PREFIX` 可修改这两个示例库的前缀。

首次执行生成并构建二进制；后续可设置 `MP_SKIP_BUILD=1` 复用 `.run/multiprocess/bin`。修改 Go 代码后重新构建。

```bash
MP_MYSQL_CNF="$HOME/.my.cnf" MP_SKIP_BUILD=1 MP_CONSOLE=0 \
  ./docs/examples/multiprocess/run-demo.sh
```

默认监听地址为 `localhost`，API 端口为 `7400`，Console 端口为 `7421`。脚本输出完整 Console URL，结果日志位于 `.run/multiprocess/logs/driver.log`，各进程日志位于同目录。启动 Console 时，验证结束后保持运行，按 Ctrl-C 清理进程；`MP_HOLD_SECONDS` 可设置自动结束时间。

## 版本场景

v1 支持版本 marker 3–4，v2 支持 3–5，v3 仅支持 7。v1/v2 的历史 marker 因而超出 v3 支持范围。

driver 验证 pinned、各版本正常完成，以及 v1/v2 在途 Run 升级到 v3 的状态。Activity gate 让旧 Run 在已产生历史的明确位置暂停，发布 v3 后释放，随后读取 Run 状态和 WorkflowTaskFailed 历史作为结果依据。

重新登记已有内容会保持现有 latest，因此验证按 v1 → v2 → v3 的发布顺序进行；每个旧版本阶段先保留在途 Run，再发布下一版本。所有 Run 的 Workflow Type 都是 `order-flow`；同一 Workflow ID 同时保留一个 current Run，两个升级观察对象用 `order-flow-incompat-v1-*`、`order-flow-incompat-v2-*` 前缀的 Workflow ID 与正常订单区分。app-1 每次只登记 `order-flow` 一个 Workflow Type，Workflow Task 由 tenuto-server 执行，app-1 重启期间 Workflow Task 照常推进。app-2 使用 4 个 Activity 执行槽，让两条 gate 暂停任务与正常订单可以并行执行。脚本为 app-1 固定 `APP1_BUILD_ID=app1-demo`，各版本本地 Finalize 使用相同实现与契约；Workflow 内容版本与 Activity build 路由分别管理。
