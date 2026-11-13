---
title: 生产上线检查清单
description: Workflow、Worker、Server、存储与可观测性上线检查
---

# 生产上线检查清单

Server 侧的必需配置、正确性保证、实测数字与已知限制见 [生产就绪与验证结果](/operations/production-readiness)。

## 代码与版本

- Workflow Type、Program capability 和 Worker Build ID 已登记。
- Workflow 代码保持确定性，版本变更有兼容策略。
- Activity 输入输出具备稳定 schema 和幂等键。
- Retry Policy、超时、取消和补偿路径经过测试。

## 基础设施

- MySQL 已完成 Schema、索引和备份配置。
- TaskQ Frontend 已配置 TLS、连接池和容量告警。
- Tenuto Server 至少两个副本，readiness 与优雅退出已验证。
- Activity Worker 独立部署并设置并发上限；Workflow Task 默认在 Tenuto Server 内执行（`WORKFLOW_WORKER_CONCURRENCY`），使用独立 Workflow Worker 时同样设置并发上限。

## 运行安全

- Namespace、Task Queue 和访问权限已确认。
- Task Token Secret 在所有 Server 副本中一致。
- Console 放在 SSO 或鉴权代理之后，Reset、Terminate 等运维写操作由代理按路径分组授权（见 [Console 权限](/deploy/security#console-权限)）。
- 输入、History、日志和 Trace 中的敏感数据已脱敏。

## 发布与回滚

1. 发布 Server。
2. 发布兼容 Worker。
3. 观察新旧 Build 的任务分布和失败率。
4. 验证新 Run、旧 Run、Activity Retry、Child Workflow 和 Continue-As-New。
5. 保留旧 Worker，直到存量 Run 完成或迁移。
6. 回滚时先恢复 Worker 兼容版本，再处理未完成 Run。
