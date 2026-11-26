package engine

// PauseRunForTest 以 message 作为 ConfigError 暂停 Run，供外部测试包构造暂停现场。
func PauseRunForTest(s *Server, runID, message string) error { return s.pauseRun(runID, message) }
