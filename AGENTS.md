Before adding complexity for a speculative or vanishingly unlikely race or security edge case, explain the concrete failure mode, likelihood, and complexity cost to the user and get their buy-in. Do not silently expand scope for theoretical robustness.

## 沟通方式

<!-- talk-normal 0.6.2 -->

- 直接、具体地表达，先给答案，再补充理解答案所需的背景。信息量与任务复杂度相匹配。
- 所有语言、句子位置和表达顺序都采用直接肯定的陈述。需要区分概念时，分别写出双方的正向定义；逻辑、数学和形式证明中的必要条件与充分条件按其精确定义表达。
- 省去铺垫、客套、空话和问题复述；同一个解释一次讲清楚。
- 是非问题先给结论，再用一句话解释原因。概念问题通常用 3–5 句话说明核心。
- 比较方案时先给推荐和简短理由；每侧的优缺点控制在最重要的 3–4 项。
- 列表用于实际的步骤或并列内容；复杂问题保持结构清晰、篇幅紧凑。
- 代码问题直接给代码；用法复杂时补充使用示例。
- 结尾直接给出适用的具体建议或下一步。行动请求按已授权范围继续执行，后续行动直接落地。
- 省去总结标签、重复换句话解释、假设性邀请和等待用户触发的后续菜单。

## 代码组织与简洁性

- 代码易懂、简洁、职责清晰优先，性能其次。拆文件和增加抽象都需要有实际职责依据。
- 每个仓储保留一个实际实现类型，由该类型直接持有数据库依赖、实现领域接口。构造函数与实现类型放在同一个文件中，仓储方法按实际职责组织。
- 仅嵌入另一个实现类型的空壳 `Repository`、仅转发一次调用的包装、重复的 PO/schema 别名，应合并或直接使用原有类型与方法。
- 只有类型别名、短构造函数或一条生成指令的文件应合并到相关实现或项目入口。文件数量按职责需要控制。
- 仓储直接复用公共数据库抽象；数据库连接、DSN、驱动选择和连接池配置由基础设施与应用启动入口负责。
- 反射、泛型和公共辅助函数用于消除实际重复；已有库方法能直接完成的操作直接调用。

## DDD 与 Clean Architecture

- 修改某一分层前，先读同层已有的实现（领域模型与仓储接口在 `server/biz/domain`，用例在 `server/biz/usecase`，持久化在 `server/infra/persistence`），按它们的结构、命名、依赖方向与错误处理实现。
- 持久层按本节下列规则组织，修改前逐项核对。
- 领域模型与仓储接口由领域层定义；用例依赖领域接口；基础设施实现接口；应用入口组合具体实现。源码依赖指向领域与用例核心。
- 按领域或聚合组织持久层。同一个 `<domain>repo` 包包含该领域的 PO、mapper、查询和实际仓储实现。
- PO 带 `@Gsql` 与 `@Setter(setter=false, patch=v2)`；GORM tag 仅保留 `column`；表约束、索引和类型写在对应 PO 的 `MysqlCreateTable()` 中。
- `MysqlCreateTable()` 直接返回该表的完整 SQL。PO、建表 SQL、mapper 与仓储查询一起维护。
- 查询使用 `github.com/donutnomad/gsql` 及其生成的 schema，列表查询也遵守同一规则；实体更新使用 `ToPatch`；实体字段变更使用生成的 setter 跟踪。
- 跨仓储的业务编排由用例或 view 承担；仓储组合负责依赖装配；数据库技术细节留在基础设施。

## 统一代码生成

- 全仓库只保留一处 gogen 的 `//go:generate` 指令，放在现有命令入口 `server/cmd/tenuto-server/main.go`。
- 统一指令从项目根目录递归扫描 `./...`，一次调用覆盖领域 setter、PO patch、gsql schema 和其他注解生成目标。
- 开发者从项目根目录执行 `go generate ./...`；各领域包只保留注解及生成产物。
- `generate.go` 等生成产物由生成器维护，字段与注解变化通过统一命令重新生成。
- 本节是本项目对各 skill 中示例生成路径的统一约定，执行时采用这里的入口与命令。

## 文档与 skill 同步例程

每次功能变化（SDK API、注解、编译器接受的写法、worker 选项、服务端行为、console 操作、部署配置）在同一次提交内完成三件事：

1. 更新 `docs/` 中对应的用户文档页，示例代码与当前签名一致。
2. 更新 `docs/skills/tenuto/` 中对应的内容：`SKILL.md` 的索引与要点，以及 `references/` 下的专题文件。
3. 在 `docs/` 目录运行 `node_modules/.bin/vitepress build .`，构建通过后提交。

新增功能时文档页、skill 条目与代码一起新增；删除功能时三处一起删除。文档与 skill 只描述当前代码的行为。

## 验证

- 先运行统一生成命令并检查生成差异，再执行受影响包的测试与 `go vet ./...`。
- 按变更涉及的接口和行为选择验证范围。检查通过后，继续交付可审阅的代码和文档。
