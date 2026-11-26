package history

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/donutnomad/tenuto/protocol"
)

// maxDedupeKeyLength 是 tenuto_events.dedupe_key 的宽度（VARCHAR(400)，按字符计）。
const maxDedupeKeyLength = 400

// 长度上限内最长的用户标识拼出的去重键不超过 dedupe_key 的列宽。
func TestLongestUserIdentifiersFitDedupeKey(t *testing.T) {
	for name, event := range map[string]Event{
		"marker": {MarkerRecorded: &MarkerRecordedEvent{
			MarkerName: strings.Repeat("界", protocol.MaxMarkerNameLength),
			MarkerID:   strings.Repeat("界", protocol.MaxMarkerIDLength),
		}},
		"signal":           {WorkflowSignaled: &WorkflowSignaledEvent{Name: "pay", RequestID: strings.Repeat("r", protocol.MaxRequestIDLength)}},
		"update accepted":  {WorkflowUpdateAccepted: &WorkflowUpdateAcceptedEvent{UpdateID: strings.Repeat("界", protocol.MaxUpdateIDLength)}},
		"update completed": {WorkflowUpdateCompleted: &WorkflowUpdateCompletedEvent{UpdateID: strings.Repeat("界", protocol.MaxUpdateIDLength)}},
		"timer":            {TimerScheduled: &TimerScheduledEvent{TimerID: strings.Repeat("界", protocol.MaxTimerIDLength)}},
	} {
		key, ok := DedupeKey(event)
		if !ok {
			t.Fatalf("%s has no dedupe key", name)
		}
		if n := utf8.RuneCountInString(key); n > maxDedupeKeyLength {
			t.Errorf("%s dedupe key has %d characters, the column holds %d", name, n, maxDedupeKeyLength)
		}
	}
}
