---
title: Task 与 Worker
description: Workflow Task、Activity Task、Worker poll 和任务确认模型
status: Stable
---

# Task 与 Worker

<FeatureStatus status="Stable" />

Tenuto Server 保存状态并生成 Task，Worker 通过长轮询领取任务。Task Queue 将逻辑工作负载映射到 TaskQ physical queue。

## 两类 Task

| 类型 | 执行者 | 工作内容 | 完成结果 |
| --- | --- | --- | --- |
| Workflow Task | Workflow Worker | 重放确定性代码并提交 Command | Workflow Task Completed |
| Activity Task | Activity Worker | 执行外部副作用 | Activity Completed / Failed |

Workflow Task 的输入包含 Program、History 或增量 activation。Activity Task 的输入包含 Activity Type、Payload、Attempt、Timeout 和路由信息。

## Lease 与 Task Token

- TaskQ lease token 用于队列 `ack`、`nack` 和租约管理。
- Tenuto Task Token 用于 Runtime API，绑定 Namespace、Run、Scheduled Event、Attempt、Stamp 和路由信息。
- Worker 完成 Runtime 回报后确认队列 lease。
- 过期 Attempt 或旧 generation 的回报通过 stamp 校验进入幂等路径。

## Worker 身份

Identity 标识实例，Build ID 标识代码版本。生产环境通常使用 Pod 名称作为 Identity，使用镜像版本或 Git commit 作为 Build ID。

Sticky Execution 会把后续 Workflow Task 优先路由到持有 execution session 的 Worker。owner 到期时，Server 清理 assignment 并回退到普通 Task Queue。

## 故障恢复

队列投递属于可恢复副作用。Server 启动时读取 open mutable state 和 dispatch 状态，重新投递仍需处理的 Task。Worker 依靠 Event ID、Attempt 和 Stamp 安全处理重复投递。

- 派发记录已是「已投递」、任务却超过等待时长仍没有被领取（open 且未 started 的 Workflow Task、Scheduled 的 Activity attempt）时，leader 副本的 reconciler 再投递一份：投递在队列里丢失时任务不会永久停住。等待时长从 30 秒宽限期起按这条记录的派发次数翻倍，最长 1 小时（约 7 次翻倍后到达）；任务只是在等空闲 Worker 时，多出的份数随等待时长对数增长：Worker 全部下线 8 小时，一个待领取任务约多出 13 份投递，之后每小时最多再多一份。
- 一个 Workflow Task 只开始一次。不再属于它的投递（WFT 已关闭、Run 已终态，或另一份投递正在执行）由 Server 在领取时确认丢弃，不交给 Worker、不重投。
- sticky 投递在交到目标 Worker 之后、开始之前 sticky 会话被清除（schedule-to-start 期限到期、Worker 更替），这份投递按普通投递开始仍 open 的 Workflow Task。

相关内容：[运行 Worker](/develop/workers)、[Worker 性能调优](/operations/worker-performance)、[Sticky Execution Cache](/operations/sticky-cache)。
