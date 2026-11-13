package history

// Schedule 启动的 Run 带这两个保留搜索属性，与 Temporal 同名同义：ScheduledByIDSearchAttribute 是 Schedule ID，
// ScheduledStartTimeSearchAttribute 是这次触发的名义时间（cron 到期点，手动触发取触发时刻），UTC RFC 3339 秒级文本。
// continue-as-new 与重试产生的 Run 照常继承它们，所以按 Schedule ID 过滤能找到这个 Schedule 的全部 Run。
const (
	ScheduledByIDSearchAttribute      = "TemporalScheduledById"
	ScheduledStartTimeSearchAttribute = "TemporalScheduledStartTime"
)

// IsReservedSearchAttribute 报告 key 是否由 Server 维护：调用端写入这些键被拒绝。
func IsReservedSearchAttribute(key string) bool {
	switch key {
	case ChangeVersionSearchAttribute, ScheduledByIDSearchAttribute, ScheduledStartTimeSearchAttribute:
		return true
	}
	return false
}
