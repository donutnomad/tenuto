---
title: 术语表
description: Tenuto Workflow 平台常用术语
status: Stable
---

# 术语表

<FeatureStatus status="Stable" />

| 术语 | 含义 |
| --- | --- |
| Workflow Definition | 可注册、可版本化的编排定义 |
| Workflow Type | 部署稳定的流程类型名称 |
| Workflow ID | 业务侧稳定身份 |
| Run ID | 一次具体 Workflow Execution 的唯一标识 |
| Run Segment | Continue-As-New 链中的一段 Run |
| Event History | Run 的 append-only 已提交事实记录 |
| Mutable State | 当前 Activity、Timer、Child、状态和可见性投影 |
| Workflow Task | 驱动 Workflow 重放和 command 生成的决策任务 |
| Activity | 执行数据库、RPC、MQ 等副作用的业务函数 |
| Call | Workflow 源码中的一次逻辑 Activity 调用 |
| Attempt | Activity Call 的一次实际执行尝试 |
| Task Queue | Worker 领取任务的逻辑队列 |
| Worker Identity | 具体 Worker 实例标识 |
| Build ID | Worker 代码版本标识 |
| Signal | 写入历史的异步外部事件 |
| Query | 读取最新 Workflow 内存状态的只读请求 |
| Update | 写入历史、由 Handler 处理并返回结果的业务命令 |
| Update-With-Start | 原子启动或复用 Workflow 并发送 Update |
| Child Workflow | 由父 Workflow 启动的独立 Workflow Execution |
| Continue-As-New | 关闭当前 Run 并创建下一 Run Segment |
| Reset | 从 Workflow Task 边界创建新的执行分支 |
| Lane | `workflow.Go` 创建的确定性并发执行线 |
| RoutineID | Trace 中区分并发协程实例的稳定标识 |
| Sticky Execution | 把 Run 后续 Workflow Task 定向到缓存 session 的 Worker |
| Cold Replay | 从 Event History 恢复完整 execution session |
| Incremental Activation | 缓存命中后只消费新增 History 的执行方式 |
| Marker | 固化 SideEffect、Random、Version 等确定性值的历史事件 |
