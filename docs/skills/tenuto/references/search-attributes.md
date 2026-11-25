# 搜索属性与 Run 检索

搜索属性是挂在 Run 上的 `map[string]string` 业务索引，用于列表过滤。Event History 负责审计与重放，搜索属性负责「找到那些 Run」。

## 写入

由调用端写入：

```go
err := c.UpsertSearchAttributes(ctx, workflowID, runID, map[string]string{ // c 是 sdk/client 的 Client
    "order_id": "order-1042",
    "region":   "ap-southeast-1",
})
```

HTTP：`POST /runtime/workflow/search-attributes`，body `{"namespace","workflow_id","run_id","search_attributes":{...}}`。

- 参数依次是 Workflow ID、Run ID、属性，Namespace 取自 Client；`runID` 为空时解析 current Run。
- 写入是 upsert：同名键覆盖，其他键保留。每次写入在 History 里记一条 `SearchAttributesUpserted`。
- 键与值都是字符串。
- 保留键由引擎维护，外部写入返回 `client.ErrInvalidArgument`（`<key> is a reserved key that cannot be set, please use other key`）：`TemporalChangeVersion`（`workflow.GetVersion`）、`TemporalScheduledById` 与 `TemporalScheduledStartTime`（Schedule 启动 Run 时写入 Schedule ID 与名义触发时间，见 `schedules.md`）。
- Workflow 函数体内写搜索属性的入口只有 `GetVersion`（自动维护保留键）。启动请求不带搜索属性：业务属性由启动 Run 的调用端在启动后写入，或由一个 Activity 用 `sdk/client` 写入（Activity 从 `activity.GetInfo` 取得 Run 标识）。

## 检索

```go
result, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    Statuses:         []client.WorkflowExecutionStatus{client.WorkflowExecutionStatusRunning},
    SearchAttributes: map[string]string{"region": "ap-southeast-1"},
    Limit:            100,
    NewestFirst:      true,
})
for _, execution := range result.Executions {
    // Run 摘要：WorkflowID、RunID、Status、SearchAttributes ...
}
```

`client.ListWorkflowOptions` 的字段：

| 字段 | 含义 |
| --- | --- |
| `WorkflowID` | 按 Workflow ID（含同一 ID 的全部 Run） |
| `Statuses` | `WorkflowExecutionStatusRunning` / `Completed` / `Failed` / `Canceled` / `Terminated` / `ContinuedAsNew`；为空时不按状态过滤 |
| `SearchAttributes` | 每个键值都要匹配 |
| `Offset`、`Limit`、`NewestFirst` | 分页与排序 |

HTTP 的过滤器另有 `run_ids`（指定 Run ID 集合）、`blocked_only`（只看 Blocked 的 Run）、`include_terminal`（不给 `statuses` 时是否包含已结束的 Run，缺省 false；Go Client 总是包含）。HTTP：`POST /runtime/workflow/list`，body `{"filter":{"namespace":"orders","search_attributes":{"region":"ap-southeast-1"},"limit":100}}`（字段名是上表的 snake_case：`run_ids`、`workflow_id`、`statuses`、`search_attributes`、`include_terminal`、`blocked_only`、`offset`、`limit`、`order_desc`）。

同一个过滤器也是批量取消、终止、Reset 的目标选择器（见 `operations.md`）。

## 按 GetVersion 版本找 Run

新执行经过 `GetVersion` 时，`TemporalChangeVersion` 记录 `changeID-version`：

```go
result, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"TemporalChangeVersion": "charge-policy-4"},
})
```

用于提高 `minSupported` 之前确认存量。重放时得到 `workflow.DefaultVersion` 的 Run（History 早于该版本点）不带这个值，按 Workflow ID 与启动时间确认。

## 按 Schedule 找 Run

```go
result, err := c.ListWorkflow(ctx, client.ListWorkflowOptions{
    SearchAttributes: map[string]string{"TemporalScheduledById": scheduleID},
})
```

## 派生 Run 的继承

| 派生方式 | 新 Run 的搜索属性 |
| --- | --- |
| Continue-As-New | 上一段 Run 的当前属性，包括 `TemporalChangeVersion` |
| 重试 | 失败 Run 启动时的属性；运行期写入的键不继承 |
| Reset 到某个事件 | 启动时的属性加上该事件及之前的全部写入 |
| 按 `reset_type` 的 Reset | 基准 Run 启动时的属性 |

## 设计建议

- 键名保持业务稳定（`order_id`、`tenant`、`region`）；高基数字段用于精确定位，低基数字段用于列表过滤。
- 敏感数据用脱敏后的标识。
- 一个 Run 需要按多个业务键查找时写入搜索属性；Workflow ID 只承载一个业务键，留空启动时是生成的 UUIDv7 文本。
