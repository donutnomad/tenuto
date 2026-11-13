package history

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// ValidateEventSequence 校验 events 的 ID 从 after+1 起连续递增。
func ValidateEventSequence(after int64, events []Event) error {
	expected := after + 1
	for i, event := range events {
		if event.ID != expected {
			return fmt.Errorf("event at index %d has ID %d, want %d", i, event.ID, expected)
		}
		expected++
	}
	return nil
}

// DigestCommands 返回命令列表的内容摘要；Worker 上报与 Server 校验用同一算法。
func DigestCommands(commands []Command) (string, error) {
	data, err := json.Marshal(commands)
	if err != nil {
		return "", fmt.Errorf("encode workflow commands for digest: %w", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
