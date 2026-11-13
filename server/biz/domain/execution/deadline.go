package execution

import "time"

// ConfiguredTimeout 解析 Workflow 配置声明的超时；没有声明、非法或非正的值是 0，表示没有超时。
func ConfiguredTimeout(value string) time.Duration {
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return 0
	}
	return timeout
}

// ApplyTimeouts 补齐还没有设定的截止时间，返回 Run 是否有改动；超时为 0 表示没有声明。执行截止时间覆盖整条
// continue-as-new 与重试链，派生 Run 继承已有的值；Run 截止时间从本 Run 开始计时。新 Run 在建 Run 的事务之前调用，
// 截止时间随 Run 一起写入。
func (r *Run) ApplyTimeouts(executionTimeout, runTimeout time.Duration, now time.Time) bool {
	changed := false
	if executionTimeout > 0 && r.DeadlineAt == nil {
		deadline := now.Add(executionTimeout)
		r.setDeadlineAt(&deadline)
		changed = true
	}
	if runTimeout > 0 && r.RunDeadlineAt == nil {
		deadline := now.Add(runTimeout)
		r.setRunDeadlineAt(&deadline)
		changed = true
	}
	return changed
}

// RestoreRunTimeouts 在恢复时补齐 Run 缺少的截止时间并写回，例如 Run 升级到了声明超时的 Program。
func RestoreRunTimeouts(tx ExecutionTx, run *Run, executionTimeout, runTimeout time.Duration, now time.Time) error {
	if !run.ApplyTimeouts(executionTimeout, runTimeout, now) {
		return nil
	}
	return tx.UpdateRun(run)
}

func WorkflowDeadlineExpired(current *Run, now time.Time) bool {
	return current.DeadlineAt != nil && !now.Before(*current.DeadlineAt)
}

func RunDeadlineExpired(current *Run, now time.Time) bool {
	return current.RunDeadlineAt != nil && !now.Before(*current.RunDeadlineAt)
}
