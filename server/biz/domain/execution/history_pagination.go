package execution

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

type HistoryDirection string

const (
	HistoryDirectionAscending  HistoryDirection = "ascending"
	HistoryDirectionDescending HistoryDirection = "descending"
	DefaultHistoryPageSize                      = 200
	MaxHistoryPageSize                          = 1000
)

type HistoryPageRequest struct {
	RunID     string           `json:"run_id"`
	PageSize  int              `json:"page_size,omitempty"`
	Direction HistoryDirection `json:"direction,omitempty"`
	PageToken string           `json:"page_token,omitempty"`
}

type HistoryPage struct {
	Events             []history.Event `json:"events"`
	NextPageToken      string          `json:"next_page_token,omitempty"`
	SnapshotMaxEventID int64           `json:"snapshot_max_event_id"`
	HistoryEventCount  int64           `json:"history_event_count"`
	HistorySizeBytes   int64           `json:"history_size_bytes"`
}

// CloseEventScan 是读取 Run 结束事件时从最后一个事件向前查看的事件数：结束事件之后不再追加事件，留出余量即可。
const CloseEventScan = 8

type HistoryPageStore interface {
	GetEventPage(request HistoryPageRequest) (HistoryPage, error)
	GetEventsRange(runID string, afterExclusive, throughInclusive int64) ([]history.Event, error)
	// GetEventsByDedupeKeys 返回 Run 可见 History 里去重键（history.DedupeKey）属于 keys 的事件，按事件 ID 升序。
	// 范围与 GetEvents 相同（含 reset 从祖先 fork 来的前缀），读取走去重键索引，与 History 长度无关。
	GetEventsByDedupeKeys(runID string, keys []string) ([]history.Event, error)
}

var (
	ErrInvalidHistoryPageToken = errors.New("invalid history page token")
	ErrExpiredHistoryPageToken = errors.New("expired history page token")
)

type HistoryPageCursor struct {
	Version             int              `json:"v"`
	RunID               string           `json:"run_id"`
	BranchFingerprint   string           `json:"branch_fingerprint"`
	Direction           HistoryDirection `json:"direction"`
	RangeOrdinal        int              `json:"range_ordinal"`
	NodeID              int64            `json:"node_id"`
	SnapshotUpperBounds []int64          `json:"snapshot_upper_bounds"`
	// HistoryEventCount 与 HistorySizeBytes 是首页按快照上界算出的统计值，后续页沿用，不再重算。
	HistoryEventCount int64 `json:"history_event_count"`
	HistorySizeBytes  int64 `json:"history_size_bytes"`
	IssuedAtUnix      int64 `json:"issued_at"`
	ExpiresAtUnix     int64 `json:"expires_at"`
}

func NormalizeHistoryPageRequest(request HistoryPageRequest) (HistoryPageRequest, error) {
	if request.RunID == "" {
		return request, protocol.InvalidRequestf("history page run id is required")
	}
	if request.PageSize == 0 {
		request.PageSize = DefaultHistoryPageSize
	}
	if request.PageSize < 1 || request.PageSize > MaxHistoryPageSize {
		return request, protocol.InvalidRequestf("history page size must be between 1 and %d", MaxHistoryPageSize)
	}
	if request.Direction == "" {
		request.Direction = HistoryDirectionAscending
	}
	if request.Direction != HistoryDirectionAscending && request.Direction != HistoryDirectionDescending {
		return request, protocol.InvalidRequestf("unsupported history direction %q", request.Direction)
	}
	return request, nil
}

func EncodeHistoryPageToken(secret []byte, cursor HistoryPageCursor) (string, error) {
	if len(secret) < 32 {
		return "", errors.New("history page token secret must contain at least 32 bytes")
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	data := append(payload, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func DecodeHistoryPageToken(secret []byte, token string, now time.Time) (HistoryPageCursor, error) {
	var cursor HistoryPageCursor
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(data) <= sha256.Size {
		return cursor, ErrInvalidHistoryPageToken
	}
	payload, signature := data[:len(data)-sha256.Size], data[len(data)-sha256.Size:]
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return cursor, ErrInvalidHistoryPageToken
	}
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.Version != 1 {
		return HistoryPageCursor{}, ErrInvalidHistoryPageToken
	}
	if cursor.ExpiresAtUnix <= now.Unix() {
		return HistoryPageCursor{}, ErrExpiredHistoryPageToken
	}
	return cursor, nil
}

func HistoryBranchFingerprint(token []byte, runID string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(runID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(token)
	return base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}
