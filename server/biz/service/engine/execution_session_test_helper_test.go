package engine

import (
	"strings"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

func executionSessionEvent(id int64, event history.Event) history.Event {
	event.ID = id
	event.Timestamp = time.Unix(id, 0).UTC()
	return event
}

func wftCompletedEvent(id int64) history.Event {
	return history.Event{ID: id, Timestamp: time.Now(), WorkflowTaskCompleted: &history.WorkflowTaskCompletedEvent{}}
}

func wftc(id int64) history.Event { return wftCompletedEvent(id) }

func contains(value, sub string) bool { return strings.Contains(value, sub) }
