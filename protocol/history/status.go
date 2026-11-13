package history

type Status string

const (
	StatusRunning        Status = "running"
	StatusCompleted      Status = "completed"
	StatusFailed         Status = "failed"
	StatusCancelled      Status = "cancelled"
	StatusTerminated     Status = "terminated"
	StatusContinuedAsNew Status = "continued_as_new"
)

// IsTerminalStatus is the single definition of a closed Run status.
func IsTerminalStatus(status Status) bool {
	switch status {
	case StatusCompleted, StatusFailed, StatusCancelled, StatusTerminated, StatusContinuedAsNew:
		return true
	default:
		return false
	}
}
