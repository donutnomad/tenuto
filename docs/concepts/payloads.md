---
title: Payload 与数据转换
description: Tenuto 参数、结果、错误详情和持久化数据的编码契约
status: Stable
---

# Payload 与数据转换

<FeatureStatus status="Stable" />

Tenuto 使用版本化 Payload 在 Client、Server、Worker、History 和 DataConverter 之间传递数据。默认编码为 `json/plain-v1`。

## Payload 结构

```text
Payload {
  encoding: "json/plain-v1"
  data:     <JSON bytes>
}
```

Workflow 输入、Activity 输入输出、Update 参数与结果、Application Error details 都应使用可长期兼容的数据结构。

## Go 类型建议

```go
type CreateOrderInput struct {
    OrderID string `json:"order_id"`
    Amount  int64  `json:"amount"`
}

type CreateOrderResult struct {
    Status string `json:"status"`
}
```

- 结构体字段使用稳定 JSON 名称。
- 新字段使用零值兼容的可选语义。
- 枚举值采用稳定字符串。
- 大整数在 Payload 路径中保留整数宽度；Side Effect Marker 中的精度敏感值推荐编码为字符串。
- map 使用字符串键，业务协议优先使用命名结构体。

## 客户端编码

[`sdk/client`](/sdk/client) 的编码与 Worker、Server 相同，不可替换：

- Workflow 的输入与结果、Signal 与 Query 的参数、Query 的结果是 JSON 对象，传结构体或 map。
- Update 的参数与结果、应用错误的 details 是 `json/plain-v1` payload，每个参数一个。
- 解码时整数是 `int64`（超过 `int64` 的是 `uint64`），带小数的是 `float64`，2^53 以上的整数不丢精度。

结果的解码目标必须是非空指针：

```go
var result ChangeLimitResult
if err := handle.Get(ctx, &result); err != nil {
    return err
}
```

## Schema 演进

| 变化 | 兼容策略 |
| --- | --- |
| 增加输出字段 | 旧调用方忽略新字段 |
| 增加输入字段 | Workflow 为缺省值定义业务含义 |
| 字段重命名 | 保留旧字段并增加新字段，完成存量 Run 后收敛 |
| 枚举扩展 | 调用方提供 unknown 分支 |
| 类型变化 | 引入新字段或新 Workflow Type |

History 会长期保存旧 Payload。发布新 Worker 时，应使用存量 History 执行 Replay 测试。
