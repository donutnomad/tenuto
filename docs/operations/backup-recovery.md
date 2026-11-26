---
title: 备份、恢复与灾难演练
description: Tenuto 数据一致性、恢复顺序和灾难演练流程
status: Stable
---

# 备份、恢复与灾难演练

<FeatureStatus status="Stable" />

Tenuto 的恢复目标是保持 Run、Event History、mutable state 和待投递任务之间的一致性。

## 必须备份的数据

- Run 与 Workflow 配置版本
- Event History 与 History Branch
- Activity、Timer、Child Workflow State
- Schedule 定义与 next_run_at
- Namespace、Search Attributes 和 Worker Build 元数据
- TaskQ 的待投递任务或可重建的 outbox

## 恢复顺序

```text
1. 恢复数据库
2. 校验 Schema 与索引
3. 启动 Tenuto Server
4. Server readiness 完成 state recovery
5. 连接 TaskQ Frontend
6. 启动 Workflow Worker
7. 启动 Activity Worker
8. 验证 Run、Queue backlog、Timer 和 Schedule
```

Server 恢复会读取 mutable state 重建 pending Activity、Timer 和 Child Workflow。历史只用于重放 Workflow 逻辑和生成下一批命令。

## 副本下线

定时器在副本进程内运行，下线不需要人工恢复。存活副本中当选 leader 的周期 reconciler 每轮按到期索引选出截止时间、timer 或 Activity 超时即将到期，或有待派发任务、open Workflow Task、blocked 的 Running Run，把两个周期内到期的定时器挂到本进程，触发持久化状态已过期超过 30s 宽限期的定时器与超时，补调度退避到期的失败 WFT，并再投递一份已入队却超过等待时长仍没有被领取的 open WFT 与 Scheduled Activity attempt（等待时长从宽限期起按派发次数翻倍，最长 8 分钟）。宽限期让持有定时器的存活副本优先触发。重复触发由 handler 校验收敛：durable timer 与 Run 超时校验持久化状态，Activity 超时在事务内校验 attempt/stamp 和本次超时类型的持久化截止时间，未到期的触发被忽略；ScheduleToClose 到期后 Activity 直接终态失败，扫描不再重复命中。最长接管延迟约为宽限期加一个 `WORKFLOW_RECONCILE_INTERVAL`。

## 演练项目

每季度演练数据库恢复、Server 重启、TaskQ 暂停、Worker 全部退出、网络分区和单个 Run Reset。演练记录 RTO、RPO、丢失任务数、重复 Attempt 数和人工操作步骤。
