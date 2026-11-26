---
title: 安全发布最佳实践
description: Workflow 兼容发布、Worker 灰度、Sticky Cache 和回滚策略
status: Stable
---

# 安全发布最佳实践

<FeatureStatus status="Stable" />

## 发布单元

Server、Workflow Worker、Activity Worker、Workflow Program 和 Console 具有不同兼容边界。镜像和构建产物记录版本、commit、Build ID 和 capability。

## Workflow 代码

命令序列变化使用 `GetVersion`。发布前用真实 History 样本 Replay，并覆盖 Child、Continue-As-New、并发、Handler 和 Side Effect。

仓库 `docs/examples/upgrade-demo` 用三个独立进程演示一次完整发布：新旧 build 并存、Activity 契约变化、旧 build 下线后新 build 承接入参新增了字段的 Activity、没有版本保护的 Program 触发 `UpgradeError`。

## Worker 灰度

先用独立的 Task Queue 验证 canary Build ID 的 Activity：同一队列上的任务不带 build，任何在线 build 都可能领取。旧 build 下线前先排空：Program 声明的契约只有旧 build 满足的 Activity（`pinned` 在旧 Program 上的 Run）一直等它，旧 build 下线后这些任务留在队列里，新 build 每次领取都被拒绝，直到旧 build 回来或 `ScheduleToStartTimeout` 到期；契约不变、或只做兼容改动（入参新增字段）的发布没有这个限制。旧 build 下线时让进程走完 `Worker.Stop()`：它停止拉取，在 `GracefulStopTimeout` 内等在途任务结束，把因停止而结束的 Activity Attempt 交还给 Server 重新派发（不消耗重试次数），再注销 Activity 路由，这个 build 立刻退出选路；被强杀的进程靠心跳 TTL（默认 30s）到期。Workflow Worker canary 关注 replay error、blocked Run、cache miss 和 Workflow Task latency。

## Sticky Cache

滚动发布期间 Artifact 或 Build ID 变化会触发 generation mismatch eviction。预留 cold restore CPU 和 History 读取容量，观察 owner fallback。

## 数据库迁移

采用 additive migration：先增加表、列和索引，再发布读取与写入代码。收敛旧 schema 发生在所有回滚窗口关闭之后。

## 回滚判据

- Workflow Task failure 或 blocked Run 持续增长。
- Activity failure 与 timeout 超过基线。
- Sticky rejection、cold restore latency 或数据库负载超出预算。
- Update Completed 延迟和 Schedule miss 超出业务 SLO。

回滚版本需要理解发布期间写入的 Payload、Marker 和 Event。发布记录保存迁移版本、Worker Build ID、Program Version 和 Console WASM 版本。
