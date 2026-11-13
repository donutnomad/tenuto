---
title: 能力与成熟度
description: Tenuto 功能状态、兼容承诺、验证覆盖和产品边界
status: Stable
---

# 能力与成熟度

<FeatureStatus status="Stable" note="能力目录本身属于稳定文档契约" />

本页是 Tenuto 能力范围的权威入口。功能状态依据公开 API、持久化语义、自动化测试和生产装配路径确定。

## 状态定义

| 状态 | 使用建议 | 兼容承诺 | 验证要求 |
| --- | --- | --- | --- |
| **Stable** | 生产工作流 | 公共语义保持兼容，破坏性变化经过迁移方案 | 单元、集成和关键端到端场景 |
| **Preview** | 受控生产与试点 | API 和运维策略仍可能演进 | 主路径、失败路径和恢复测试 |
| **Internal** | Tenuto 组件之间 | 由同版本 Server、SDK、Worker 和 Console 协同使用 | 协议、存储或组件集成测试 |

## Workflow 与开发模型

| 能力 | 状态 | 主要入口 | 关键边界 |
| --- | --- | --- | --- |
| Workflow 与 Activity | Stable | `workflow.ExecuteActivity` | Workflow 保持确定性，外部副作用进入 Activity |
| Durable Timer | Stable | `workflow.Sleep`、`workflow.Now` | deadline 和结果进入持久化状态与 History |
| Side Effect | Stable | `SideEffect`、`MutableSideEffect` | 返回值通过 Marker 固化 |
| 代码版本 | Stable | `workflow.GetVersion` | change ID 是长期兼容契约 |
| 确定性随机值 | Stable | `RandomUUID`、`RandomInt` | 首次值写入 Marker，重放读取同一结果 |
| 协作式并发 | Stable | `Go`、`WaitGroup`、`Channel`、`Selector` | 声明顺序和 History 决定调度结果 |
| Child Workflow | Stable | `ExecuteChildWorkflow` | 父子 Run 分别保存状态与 History |
| Continue-As-New | Stable | `ContinueAsNew`、`GetInfo` | 新 Run 继承 Workflow ID 与 Search Attributes |
| Workflow Reset | Preview | Runtime Reset API、Console | reset point 使用 Workflow Task 边界 |
| Activity Reset | Preview | Runtime Reset Activity API、Console | 新 generation 使旧 task 失效，Attempt 从 1 开始 |

## 外部消息

| 能力 | 状态 | 返回语义 | 幂等键 |
| --- | --- | --- | --- |
| Signal | Stable | 返回写入 History 的 Event ID | 业务消息 ID |
| Query | Stable | 返回重放到最新状态后的只读结果 | 查询天然只读 |
| Typed Update | Stable | Handler 返回强类型结果或 Application Error | Update ID |
| Update-With-Start | Stable | 原子解析 current Run 并接受 Update | Workflow ID + Update ID |
| Signal-With-Start | Preview | 原子解析 current Run 并写入 Signal | 业务消息 ID，由 Workflow 去重 |
| Update Validator | Stable | 写入 Accepted 前完成无状态校验 | Update ID |

## Activity 与 Worker

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| Activity Retry 与四类超时 | Stable | 每次调用可配置 Retry Policy 和 Timeout |
| Heartbeat 与协作取消 | Stable | Heartbeat 保存进度并返回 cancel request |
| 异步 Activity 完成 | Preview | 外部系统使用 Task Token 提交结果 |
| Namespace 与 Task Queue | Stable | Run、任务、Worker 路由按 Namespace 隔离 |
| Worker Identity | Stable | 标识具体 Worker 实例 |
| Activity 契约领取校验 | Preview | Activity 任务只按 Task Queue 投递，Worker 开始执行前按它这个 Build ID 上报的契约对照 Program 声明 |
| Sticky Execution Cache | Stable | 命中时恢复增量 activation，失配时执行安全回退 |
| Worker capability 握手 | Internal | Server 根据 WLang 和增量执行能力匹配 Worker |

## 运维与可观测性

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| History 快照分页 | Stable | Token 绑定 Run、branch 与快照上界 |
| Namespace、Worker、Task Queue 列表 | Stable | Console 与运维 API 的资源视图 |
| Search Attributes | Preview | 当前字段为字符串 map，过滤由 Runtime 完成 |
| Blocked State | Preview | 展示确定性错误和当前等待点 |
| Schedule | Preview | Cron、时区、Pause/Resume/Trigger、AllowAll 和 Skip |
| Batch Operation | Preview | Cancel、Terminate、Reset；持久化、后台分页执行、可取消、崩溃后从游标继续 |
| Trace 调试台 | Preview | WLang Run 的断点、变量、栈、Call、Attempt 与跨 Run 导航 |
| Mutable State | Internal | Activity、Timer、Child 和 Execution 当前状态的权威来源 |
| History Branch | Internal | Workflow Reset 使用前缀分支与新 Run |
| Dispatch Outbox | Internal | History/state 事务提交后恢复任务投递 |
| Runtime HTTP 与 Task Token | Internal | Go SDK、Worker 和 Console 的版本协同协议 |

## Temporal 能力对照

Tenuto 当前专注单集群、自托管、Go/WLang Workflow 场景。下列主题保留为产品边界记录：

| 主题 | 当前归属 |
| --- | --- |
| Dynamic Workflow 与 Dynamic Activity | Temporal-only |
| Workflow Streams | Temporal-only |
| Nexus Operations | Temporal-only |
| History Archival | 部署侧数据生命周期 |
| 多集群复制与 Namespace Failover | 基础设施路线图 |
| 托管 Cloud 配额和计费 | 自托管运维职责 |

页面出现 `Preview` 时，发布计划应包含回滚路径和专门监控；页面出现 `Internal` 时，Server、SDK、Worker 与 Console 应使用同一构建版本。
