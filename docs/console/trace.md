---
title: Trace 调试台
description: 断点、Step、栈、变量、Activity Attempt 与跨 Workflow 调试
status: Preview
---

# Trace 调试台

<FeatureStatus status="Preview" />

Trace 使用浏览器 WASM 对 Workflow Program 和 Event History 进行安全重放，提供接近 GoLand 的源码调试体验。

## 调试布局

```text
┌─────────────────────────────────────────────┐
│                 Source Code                 │
│  line number · breakpoint · current line   │
├─────────────────────────────────────────────┤
│ Restart · Step · Continue · Next Breakpoint │
├───────────────────┬─────────────────────────┤
│ Execution / Stack │ Variables / Inspector   │
└───────────────────┴─────────────────────────┘
```

代码区域固定高度并在内部滚动。当前执行行使用 `#2F3F55` 背景，gutter 独立展示行号和断点。

## 调试控制

- **Restart**：回到本次 Trace 的第一步。
- **Step**：执行到下一个可观察源码位置。
- **Continue**：运行到下一个断点或终点。
- **Next Breakpoint**：跳转到执行序列中的下一个断点命中。
- **Step Into**：进入 Child Workflow 或可进入调用。
- **Step Out**：返回父栈帧。

## Execution Lines 与 Stack

左侧选择当前执行线。`workflow.Go`、循环 lane 和 Update Handler 都有独立 RoutineID。选择执行线后，代码焦点、栈和变量同步切换。

栈列表展示当前 Workflow、Child Workflow 和父调用边界。选择任意栈帧会更新代码位置和该帧局部变量。

## 变量树

变量面板采用紧凑的 JetBrains 风格：

- 基础类型在同一行展示名称、等号、类型和值。
- map、struct、slice 和 array 可以逐级展开。
- 空 map、空 slice、nil 和基础类型保持叶子节点。
- 闭包帧展示当前闭包内部变量。
- 字符串、数字、nil、变量名和类型使用不同语义颜色。

## Activity Calls 与 Attempts

同一代码行在循环中执行多次 Activity 时，代码行展示 `N calls`。Inspector 支持：

- 上一条、下一条 Call。
- Call 下的 Attempt 列表。
- Failed only、耗时排序和异常快捷定位。
- 输入、输出、错误、Worker、Task Queue、Build ID 与耗时。
- URL 保存 Call、Attempt、阶段、搜索与筛选状态。

## Child Workflow 调试

执行到 Child 调用点时，Step Into 会加载子 Run 的 Program、History 和栈。Continue-As-New Child 可以继续进入下一 Run Segment。Step Out 返回父 Workflow 的调用现场。

## WASM 重放边界

Trace 的 Workflow host 使用 History 满足 Activity、Timer、Signal、Update 和 Child 结果。重放期间不会触发真实业务 Activity。
