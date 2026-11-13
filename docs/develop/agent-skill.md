---
title: Agent Skill
description: 让编码 Agent 掌握 Tenuto 的 Workflow 编写、生成、运行、升级与排障
status: Stable
---

# Agent Skill

<FeatureStatus status="Stable" />

仓库在 `docs/skills/tenuto/` 提供一份 Agent Skill。它把本站的开发、升级与排障内容整理成编码 Agent 可以按需加载的形式：Agent 在没有任何 Tenuto 背景的情况下，依靠它编写 Workflow 与 Activity、运行生成器、接上 Worker、升级已上线的定义并诊断暂停或失败的 Run。

## 内容

```text
docs/skills/tenuto/
├── SKILL.md                         入口：心智模型、编写流程、硬规则、API 索引、升级决策表、故障排查表
└── references/
    ├── codegen.md                   注解、生成命令、产物、生成器报错
    ├── go-subset.md                 Workflow 函数体可用的 Go 语法与符号
    ├── activities.md                调度、超时、重试、心跳、异步完成、幂等
    ├── errors.md                    应用错误的构造、分支与读取
    ├── concurrency.md               Go、WaitGroup、Channel、Selector、Await
    ├── messaging.md                 Signal、Query、Update
    ├── time-and-randomness.md       Now、Sleep、随机值、SideEffect
    ├── child-and-continue-as-new.md Child Workflow 与 Continue-As-New
    ├── cancellation-timeouts.md     取消、终止、超时
    ├── versioning.md                policy、GetVersion、契约、BuildID、发布顺序
    ├── workers.md                   Worker 选项、拓扑、启动顺序
    ├── client-api.md                Go 客户端与 Runtime HTTP API
    ├── schedules.md                 Cron Schedule
    ├── search-attributes.md         搜索属性与 Run 检索
    └── operations.md                诊断、恢复暂停的 Run、Reset、Batch
```

`SKILL.md` 的 frontmatter 声明触发条件：编写或修改 Workflow、Activity、Worker，启动 Run 或发送 Signal / Query / Update，升级已上线的定义，诊断 `ConfigError`、`UpgradeError`、非确定性错误等。Agent 先读入口文件，再按其中的索引表打开对应的 reference。

## 安装

Skill 是一个自包含的目录，放进 Agent 的 skill 搜索路径即可。在使用 Tenuto 的业务仓库根目录执行其一：

```bash
# 复制（随业务仓库提交，版本固定）
mkdir -p .claude/skills
cp -R /path/to/tenuto/docs/skills/tenuto .claude/skills/tenuto

# 软链接（跟随本地 Tenuto 仓库更新）
mkdir -p .claude/skills
ln -s /path/to/tenuto/docs/skills/tenuto .claude/skills/tenuto
```

| Agent | 目录 |
| --- | --- |
| Claude Code | 项目级 `.claude/skills/tenuto/`，用户级 `~/.claude/skills/tenuto/` |
| 读取 `.agents/skills/` 的 Agent | `.agents/skills/tenuto/` |

安装后目录结构是 `<skills 目录>/tenuto/SKILL.md` 与 `<skills 目录>/tenuto/references/*.md`。

## 与 Tenuto 版本对齐

Skill 的内容描述它所在提交的行为。升级业务仓库依赖的 Tenuto 版本时，同步更新这份目录（重新复制，或让软链接指向对应版本的检出）。

Skill 里的示例路径（`docs/examples/startup-workflow/` 等）指向 Tenuto 仓库；Agent 需要完整可运行的工程时到 Tenuto 仓库读取这些目录。

## 维护

- 修改 SDK、生成器或 Runtime API 的行为时，同步更新 `docs/skills/tenuto/` 中对应的文件。
- `SKILL.md` 保持为入口与索引，细节放进 `references/`。
- `docs/skills/**` 不参与站点构建，文件之间用相对文件名互相引用。

相关内容：[编写 Workflow](/develop/workflows)、[注解与代码生成](/develop/code-generation)、[Workflow 版本管理](/develop/versioning)。
