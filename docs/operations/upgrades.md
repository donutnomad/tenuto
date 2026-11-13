---
title: Server 与 Worker 升级
description: 数据库迁移、Capability 顺序、兼容发布和回滚验证
status: Stable
---

# Server 与 Worker 升级

<FeatureStatus status="Stable" />

Tenuto 升级同时涉及 Server schema、Runtime 协议、Worker capability、Workflow Program 和 Console WASM。

## 推荐顺序

1. 备份数据库并验证恢复点。
2. 应用 additive schema migration。
3. 发布兼容新旧 Worker 的 Server。
4. 发布支持新 capability 的 canary Workflow Worker。
5. 对存量 History 执行 Replay 和增量 activation 验证。
6. 滚动发布全部 Worker。
7. 发布新 Workflow Program。
8. 发布匹配版本的 Console 与 replay WASM。

## 独立 Workflow Worker 的升级顺序

默认内嵌部署中 Workflow Worker 随 Server 一起升级，不受本节影响。`WORKFLOW_WORKER_ENABLED=false` 的部署先升级全部 Server，再升级独立 Workflow Worker。

旧 Server 不识别 `CmdUpsertSearchAttributes`。新 Worker 首次走到 GetVersion 的 Workflow Task 会提交该命令，在旧 Server 上报 `workflow task command result missing` 并持续重试，直到 Server 升级完成。

## Capability 握手

新 Program 依赖的 WLang capability 必须先出现在 Server 和可领取任务的 Worker 中。Worker 会声明 executor ABI、incremental activation 和 sticky execution capability。

```bash
curl -sS -X POST http://tenuto-server:7400/runtime/workflow/capabilities \
  -H 'Content-Type: application/json' -d '{}'
```

## Sticky 版本边界

Artifact fingerprint、Executor ABI、Build ID 或 generation 变化会触发 session 驱逐。升级窗口内 cold restore 增加属于预期现象，Worker CPU 和数据库读取容量应保留余量。

## 回滚准备

- 新旧 Server 都能读取迁移后的 schema。
- 回滚 Worker 可以解释升级期间写入的 Marker 和 Payload。
- Console WASM 与 Server artifact capability 保持配套。
- Schedule leader 和 TaskQ membership 在滚动期间保持多数可用。

## 验证

检查 readiness、Run 启动、Signal/Update、Activity completion、Timer fire、Child、Continue-As-New、Sticky fallback、History pagination、Trace 和 Schedule trigger。

相关内容：[Workflow 版本管理](/develop/versioning)、[预生产测试](/operations/pre-production-testing)。
