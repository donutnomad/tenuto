---
title: Tenuto 文档
description: Tenuto 持久化 Workflow 编排平台中文文档
aside: false
outline: false
pageClass: tenuto-home-page
---

<div class="tenuto-home">
  <div class="tenuto-hero">
    <div>
      <div class="tenuto-eyebrow">Tenuto Documentation</div>
      <h1>让业务流程在故障后<br><span>准确地继续执行</span></h1>
      <p class="tenuto-hero-copy">
        Tenuto 是面向 Go 应用的持久化 Workflow 编排平台。Workflow 状态、Event History、重试、Timer、Child Workflow 与并发执行线均由平台管理，业务进程重启后可以从已提交的确定性状态继续运行。
      </p>
      <div class="tenuto-actions">
        <a class="tenuto-action primary" href="/quickstart/">五分钟快速开始 →</a>
        <a class="tenuto-action" href="/concepts/architecture">了解系统架构</a>
      </div>
    </div>
    <div class="tenuto-paths">
      <a class="tenuto-path-card" href="/quickstart/">
        <span class="tenuto-path-icon">⚡</span>
        <span><strong>运行第一个 Workflow</strong><small>启动 Server、Worker、Console 和完整 Demo。</small></span>
        <span class="tenuto-path-arrow">›</span>
      </a>
      <a class="tenuto-path-card" href="/develop/workflows">
        <span class="tenuto-path-icon">⌘</span>
        <span><strong>Go 开发指南</strong><small>编写 Workflow、Activity、并发 lane 与 Update。</small></span>
        <span class="tenuto-path-arrow">›</span>
      </a>
      <a class="tenuto-path-card" href="/console/trace">
        <span class="tenuto-path-icon">◎</span>
        <span><strong>Trace 调试台</strong><small>断点、调用栈、变量树、Child Workflow Step Into。</small></span>
        <span class="tenuto-path-arrow">›</span>
      </a>
      <a class="tenuto-path-card" href="/deploy/server">
        <span class="tenuto-path-icon">△</span>
        <span><strong>部署到生产环境</strong><small>MySQL、TaskQ、Sticky Cache、健康检查与升级。</small></span>
        <span class="tenuto-path-arrow">›</span>
      </a>
    </div>
  </div>

  <section class="tenuto-section">
    <div class="tenuto-section-heading">
      <h2>围绕 Event History 构建的执行模型</h2>
      <p>服务端保存权威状态，Worker 负责确定性执行。每个外部结果都进入历史，重放始终得到相同命令。</p>
    </div>
    <div class="tenuto-feature-grid">
      <div class="tenuto-feature"><b>持久化执行</b><p>Activity、Timer、Signal、Update 与 Child Workflow 的状态跨进程保存。</p></div>
      <div class="tenuto-feature"><b>确定性重放</b><p>Workflow 使用历史结果恢复内存状态，业务副作用只由 Activity 执行。</p></div>
      <div class="tenuto-feature"><b>Go 并发语义</b><p>Future、workflow.Go、WaitGroup、Channel 与 Selector 由协作式调度器执行。</p></div>
      <div class="tenuto-feature"><b>强类型 Update</b><p>Handler 返回 Result 与 error，Update-With-Start 支持同步业务响应。</p></div>
      <div class="tenuto-feature"><b>IDE 级 Trace</b><p>逐步执行、断点、栈切换、变量树、Attempt 导航与跨 Workflow 调试。</p></div>
      <div class="tenuto-feature"><b>生产运行时</b><p>MySQL mutable state、TaskQ matching、Sticky Cache、恢复与幂等屏障。</p></div>
    </div>
    <div class="tenuto-scenario-grid">
      <a href="/concepts/namespaces-task-queues"><b>我需要规划租户和队列</b><span>Namespace、Task Queue、Worker 匹配和容量隔离</span></a>
      <a href="/develop/cancellation-timeouts"><b>我需要处理失败和停止</b><span>取消、超时、终止、重试、补偿和过期 Attempt</span></a>
      <a href="/develop/testing"><b>我需要验证可靠性</b><span>Replay、故障注入、存储集成和端到端测试</span></a>
      <a href="/operations/backup-recovery"><b>我需要做生产恢复</b><span>备份、恢复顺序、灾难演练和 RTO/RPO</span></a>
      <a href="/reference/errors"><b>我遇到了一个错误</b><span>错误分类、Application Error、Update 失败和排查入口</span></a>
      <a href="/quickstart/production-checklist"><b>我准备上线</b><span>代码、基础设施、安全、发布和回滚检查清单</span></a>
    </div>
    <div class="tenuto-code-strip">
      <div><code>workflow.ExecuteActivity</code><span>把外部副作用交给 Activity Worker</span></div>
      <div><code>workflow.SetUpdateHandler</code><span>接收可等待、可返回结果的业务命令</span></div>
      <div><code>workflow.ContinueAsNew</code><span>为长期运行流程开启新的 Run Segment</span></div>
    </div>
  </section>
</div>
