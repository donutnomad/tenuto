package history

type WorkflowSignalRecord struct {
	EventID int64          `json:"event_id"`
	Name    string         `json:"name"`
	Input   map[string]any `json:"input,omitempty"`
}

type WorkflowUpdateRecord struct {
	UpdateID         string         `json:"update_id"`
	Name             string         `json:"name"`
	Args             *Payloads      `json:"args,omitempty"`
	Outcome          *UpdateOutcome `json:"outcome,omitempty"`
	AcceptedEventID  int64          `json:"accepted_event_id,omitempty"`
	CompletedEventID int64          `json:"completed_event_id,omitempty"`
}
