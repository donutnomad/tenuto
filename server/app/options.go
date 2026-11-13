package app

import (
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/tenuto/server/infra/db"

	"github.com/donutnomad/tenuto/telemetry"
)

type runOptions struct {
	telemetry       telemetry.Providers
	workflowDB      db.DB
	taskQDB         db.DB
	membershipStore membership.MemberStore
}

// RunOption configures process dependencies owned by the caller.
type RunOption func(*runOptions)

// WithTelemetry injects OpenTelemetry providers into the server application.
func WithTelemetry(providers telemetry.Providers) RunOption {
	return func(options *runOptions) {
		options.telemetry = providers
	}
}

// WithDatabase 注入调用方持有的 workflow 数据库句柄，替代按 Config.Database.DSN 自行拨号。
// 注入后 Config.Database 的 DSN 与连接池设置不再生效（连接池由调用方管理），
// HistoryPageTokenKey 仍然生效。
// 该连接的生命周期归调用方：Run 返回时不会关闭它。句柄必须以 gorm.Config{TranslateError: true} 打开，
// 否则启动失败。库必须已执行完 Tenuto 的迁移（tenuto-server migrate，或 tenutosql.TenutoMigrations.Migrate），
// 否则启动失败。
func WithDatabase(db db.DB) RunOption {
	return func(options *runOptions) {
		options.workflowDB = db
	}
}

// WithTaskQDatabase 注入调用方持有的 TaskQ 数据库句柄，替代按 Config.TaskQ.DSN 自行拨号。
// 语义与 WithDatabase 一致，库必须已执行完 TaskQ 的迁移（tenutosql.TaskQMigrations）；未注入时仍按 Config.TaskQ.DSN 打开。
func WithTaskQDatabase(db db.DB) RunOption {
	return func(options *runOptions) {
		options.taskQDB = db
	}
}

// WithMembershipStore 注入调用方自己的服务发现后端，替代 TaskQ 自带的 DB 轮询 membership。
//
// 用途：调用方已有 nacos / consul / k8s endpoints 之类的注册中心时，实现 membership.MemberStore
// 的两个方法（UpsertMember / GetActiveMembers）即可把 Tenuto 的 matching 节点发现接到上面，
// 不必再让 TaskQ 去轮询 `taskq_membership_members` 表。
//
// 注入后 Tenuto 仍自行创建 Monitor，轮询间隔、副本点数等参数继续由 Config.TaskQ 控制
// （MembershipPollInterval / MembershipReplicaPoints）——只有**数据来源**被替换。
// 未注入时行为完全不变：仍使用 TaskQ 的 DB membership store。
//
// 注意 Tenuto 内嵌 matching（TASKQ_MATCHING_ENABLED=true，默认）时会注册本 node
// （AdvertisedServices），因此会调用 UpsertMember；注入的 store 必须同时支持读写。
// TASKQ_MATCHING_ENABLED=false 只观察不注册，此时只会调用 GetActiveMembers。
//
// 该 store 的生命周期归调用方：Run 返回时不会关闭它。
func WithMembershipStore(store membership.MemberStore) RunOption {
	return func(options *runOptions) {
		options.membershipStore = store
	}
}

func resolveRunOptions(options []RunOption) runOptions {
	resolved := runOptions{}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	resolved.telemetry = telemetry.Resolve(resolved.telemetry)
	return resolved
}
