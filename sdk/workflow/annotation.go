package workflow

// Policy 是 `@Workflow(policy=...)` 声明的版本策略取值。
//
// 生成器把注解里的字面量原样写进 WorkflowConfig.Policy；这里只固定合法词表，
// 让注解、生成产物和服务端配置共用同一组字符串。
type Policy string

const (
	// PolicyAutoUpgrade 缺省策略：注册新版本后后续启动自动升到最新。
	PolicyAutoUpgrade Policy = "auto_upgrade"
	// PolicyPinned 固定作者声明的 version，不随新注册自动升级。
	PolicyPinned Policy = "pinned"
)

// DefaultPolicy 是注解省略 policy 时的缺省策略。
const DefaultPolicy = PolicyAutoUpgrade

// Version 是 `@Workflow(version=...)` / `@Activity(version=...)` 声明的发布标签。
// 它与内容 hash 分开维护：hash 描述产物内容，Version 描述作者声明的协议标签。
type Version string
