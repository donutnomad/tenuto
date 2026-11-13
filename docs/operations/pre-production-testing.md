---
title: 预生产测试与故障演练
description: Worker 重启、负载、依赖故障、版本回滚和 Game Day 流程
status: Stable
---

# 预生产测试与故障演练

<FeatureStatus status="Stable" />

预生产验证应证明 Workflow 在进程、网络、数据库和依赖故障后继续保持可恢复与幂等。

## Worker 测试

- 终止全部 Workflow Worker，再启动新实例，验证 cold restore。
- 在 Activity 执行中重启 Worker，验证 Retry 和幂等键。
- 高频滚动 Worker，观察 Sticky fallback 与 cache generation。
- 使用两个 Build ID 验证滚动发布：旧 build 停止后，重试与排队中的 Activity 由新 build 领取；契约变化时旧 Program 上的 Run 由旧 build 执行。

## Server 与存储

多副本 Server 的进程故障、数据库连接中断与卡死、副本间网络分区、长时间浸泡、滚动升级与压测用仓库自带的 `tenuto-cluster`，见[多副本故障注入与压测](./cluster-testing.md)；已测得的结果与通过标准见 [生产就绪与验证结果](./production-readiness.md)。

- 在 Workflow Task 提交前后分别终止 Server。
- 中断 MySQL 连接，恢复后验证 pending state 与 dispatch。
- 中断 TaskQ，恢复后验证 open task 补投递。
- 备份并恢复到隔离环境，核对 Run、History、mutable state 和 Schedule。

## 业务依赖

- 注入 HTTP timeout、限流、5xx 和慢响应。
- 注入 Activity Heartbeat 丢失和取消请求。
- 验证 NonRetryable Error、重试耗尽和 Saga 补偿。
- 验证重复 Signal、Update ID 和异步完成回调。

## 版本与确定性

- 新代码 Replay 旧 History。
- 回滚代码读取升级期间产生的 Marker 和 Payload。
- 人为引入确定性失配，验证 Blocked State、告警、回滚与恢复。
- Child、Continue-As-New 和并发 lane 各保留黄金 History。

## 负载测试

`tenuto-cluster bench` 给出启动与完成延迟、调度延迟、吞吐、MySQL 计数器和进程资源，见[多副本故障注入与压测](./cluster-testing.md#bench)。定义成功标准：吞吐、p95/p99 延迟、Task Queue backlog、数据库连接、History 写入、Sticky hit rate 和下游饱和度。压测包含突发 Schedule 与重试风暴。

## Game Day

演练前固定负责人、停止条件、业务窗口和恢复点。演练中记录事件时间线、指标和决策。恢复后验证 current Run、pending task、业务幂等记录和告警关闭，并形成行动项。
