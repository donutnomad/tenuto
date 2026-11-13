---
title: 数据模型与一致性
description: Mutable State、Event History、事务和恢复模型
status: Internal
---

# 数据模型与一致性

<FeatureStatus status="Internal" />

Tenuto 同时保存当前状态和 append-only Event History。当前状态用于恢复与调度，History 用于审计、重放和调试。

## 核心对象

| 对象 | 作用 |
| --- | --- |
| Workflow Definition | Workflow Type 对应的配置或 WLang Program |
| Run | 一次 Workflow Execution，包含 Workflow ID、Run ID 和状态 |
| Event History | Run 的事件序列与 History Branch |
| Activity State | Activity 调度、Attempt、重试和终态 |
| Timer State | Timer 的计划、触发和取消状态 |
| Child Workflow State | 父子 Run 的关联、父关闭策略和关闭状态 |
| Schedule | 周期触发定义与 next_run_at |

## 原子边界

History append 与 mutable state upsert 在同一个 RunInTx 中提交。任务投递属于可恢复副作用，Server 重启后从 open state 和 outbox 重新投递。

## Event ID 与 Scheduled Event ID

Event ID 在单个 Run 或 Branch 内递增。Activity State、Timer State 和 Child Workflow State 使用对应 Scheduled Event ID 作为关联键。Reset 创建新的 History Branch，旧分支保留用于审计和回溯。

## 幂等

Workflow Task close、Activity close、Timer fire、Child close 和终态事件使用 first-wins 规则。过期 Task Token 通过 generation stamp 校验，旧 Attempt 回调无法改变新状态。
