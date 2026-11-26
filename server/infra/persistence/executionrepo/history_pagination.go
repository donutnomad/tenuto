package executionrepo

import (
	"fmt"
	"math"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// MySQL SUM(BIGINT) returns DECIMAL; its driver value can contain a fractional zero suffix.
type historyMetrics struct {
	Count int64
	Size  decimal.Decimal
}

type visibleHistoryRange struct {
	treeID    string
	branchID  string
	minNodeID int64
	maxNodeID int64
}

func visibleHistoryRanges(run *execution.Run) ([]visibleHistoryRange, error) {
	branch, err := runBranch(run)
	if err != nil {
		return nil, err
	}
	ranges := make([]visibleHistoryRange, 0, len(branch.Ancestors)+1)
	for _, ancestor := range branch.Ancestors {
		ranges = append(ranges, visibleHistoryRange{treeID: branch.TreeID, branchID: ancestor.BranchID, minNodeID: ancestor.BeginNodeID, maxNodeID: ancestor.EndNodeID - 1})
	}
	ranges = append(ranges, visibleHistoryRange{treeID: branch.TreeID, branchID: branch.BranchID, minNodeID: execution.GetBeginNodeID(branch), maxNodeID: math.MaxInt64})
	return ranges, nil
}

func historyRangeCondition(historyRange visibleHistoryRange) gsql.Condition {
	conditions := []gsql.Expression{
		EventSchema.TreeID.Eq(historyRange.treeID),
		EventSchema.BranchID.Eq(historyRange.branchID),
		EventSchema.NodeID.Gte(historyRange.minNodeID),
	}
	if historyRange.maxNodeID != math.MaxInt64 {
		conditions = append(conditions, EventSchema.NodeID.Lte(historyRange.maxNodeID))
	}
	return gsql.And(conditions...)
}

func rangeQuery(historyRange visibleHistoryRange) *gsql.QueryBuilderG[EventPO] {
	return gsql.From[EventPO](EventSchema).Where(historyRangeCondition(historyRange))
}

func snapshotUpperBounds(db gsql.IDB, ranges []visibleHistoryRange) ([]int64, error) {
	upperBounds := make([]int64, len(ranges))
	for i, historyRange := range ranges {
		row, err := gsql.SelectG[EventPO](EventSchema.NodeID.Max().As("node_id")).
			From(EventSchema).
			Where(historyRangeCondition(historyRange)).
			Take(db)
		if err != nil {
			return nil, err
		}
		if row != nil {
			upperBounds[i] = row.NodeID
		}
	}
	return upperBounds, nil
}

type ordinalEventRow struct {
	ordinal int
	row     EventPO
}

func (s *Repository) GetEventPage(request execution.HistoryPageRequest) (execution.HistoryPage, error) {
	request, err := execution.NormalizeHistoryPageRequest(request)
	if err != nil {
		return execution.HistoryPage{}, err
	}
	var page execution.HistoryPage
	err = s.db.Transaction(func(db *gorm.DB) error {
		runRow, err := gsql.From[RunPO](RunSchema).Where(RunSchema.ID.Eq(request.RunID)).Take(db)
		if err := runRowError(request.RunID, runRow, err); err != nil {
			return err
		}
		run, err := rowToRun(runRow)
		if err != nil {
			return err
		}
		ranges, err := visibleHistoryRanges(run)
		if err != nil {
			return err
		}
		fingerprint := execution.HistoryBranchFingerprint(run.BranchToken, run.ID)
		upperBounds, err := snapshotUpperBounds(db, ranges)
		if err != nil {
			return err
		}
		cursorOrdinal, cursorNodeID := 0, int64(0)
		if request.Direction == execution.HistoryDirectionDescending {
			cursorOrdinal = len(ranges) - 1
			cursorNodeID = math.MaxInt64
		}
		if request.PageToken != "" {
			cursor, err := execution.DecodeHistoryPageToken(s.historyPageTokenKey, request.PageToken, s.clock.Now())
			if err != nil {
				return err
			}
			if cursor.RunID != run.ID || cursor.BranchFingerprint != fingerprint || cursor.Direction != request.Direction || len(cursor.SnapshotUpperBounds) != len(ranges) || cursor.RangeOrdinal < 0 || cursor.RangeOrdinal >= len(ranges) {
				return execution.ErrInvalidHistoryPageToken
			}
			upperBounds = cursor.SnapshotUpperBounds
			cursorOrdinal, cursorNodeID = cursor.RangeOrdinal, cursor.NodeID
		}

		rows, err := readHistoryPageRows(db, ranges, upperBounds, request.Direction, cursorOrdinal, cursorNodeID, request.PageSize+1)
		if err != nil {
			return err
		}
		hasMore := len(rows) > request.PageSize
		if hasMore {
			rows = rows[:request.PageSize]
		}
		page.Events = make([]history.Event, 0, len(rows))
		for _, item := range rows {
			event, err := rowToEvent(&item.row)
			if err != nil {
				return err
			}
			page.Events = append(page.Events, *event)
		}
		for _, upperBound := range upperBounds {
			if upperBound > page.SnapshotMaxEventID {
				page.SnapshotMaxEventID = upperBound
			}
		}
		page.HistoryEventCount, page.HistorySizeBytes, err = snapshotMetrics(db, ranges, upperBounds)
		if err != nil {
			return err
		}
		if hasMore {
			last := rows[len(rows)-1]
			now := s.clock.Now()
			page.NextPageToken, err = execution.EncodeHistoryPageToken(s.historyPageTokenKey, execution.HistoryPageCursor{
				Version: 1, RunID: run.ID, BranchFingerprint: fingerprint, Direction: request.Direction,
				RangeOrdinal: last.ordinal, NodeID: last.row.NodeID, SnapshotUpperBounds: upperBounds,
				IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(15 * time.Minute).Unix(),
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return page, err
}

func readHistoryPageRows(db gsql.IDB, ranges []visibleHistoryRange, upperBounds []int64, direction execution.HistoryDirection, cursorOrdinal int, cursorNodeID int64, limit int) ([]ordinalEventRow, error) {
	result := make([]ordinalEventRow, 0, limit)
	step, end := 1, len(ranges)
	if direction == execution.HistoryDirectionDescending {
		step, end = -1, -1
	}
	for ordinal := cursorOrdinal; ordinal != end && len(result) < limit; ordinal += step {
		if upperBounds[ordinal] == 0 {
			cursorNodeID = 0
			continue
		}
		query := rangeQuery(ranges[ordinal]).Where(EventSchema.NodeID.Lte(upperBounds[ordinal]))
		if ordinal == cursorOrdinal {
			if direction == execution.HistoryDirectionAscending && cursorNodeID > 0 {
				query = query.Where(EventSchema.NodeID.Gt(cursorNodeID))
			}
			if direction == execution.HistoryDirectionDescending && cursorNodeID != math.MaxInt64 {
				query = query.Where(EventSchema.NodeID.Lt(cursorNodeID))
			}
		}
		rows, err := infradb.Find(db, query.
			Order(EventSchema.NodeID, direction == execution.HistoryDirectionAscending).
			Limit(limit-len(result)))
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			result = append(result, ordinalEventRow{ordinal: ordinal, row: *row})
		}
		cursorNodeID = 0
		if direction == execution.HistoryDirectionDescending {
			cursorNodeID = math.MaxInt64
		}
	}
	return result, nil
}

func snapshotMetrics(db gsql.IDB, ranges []visibleHistoryRange, upperBounds []int64) (int64, int64, error) {
	var count, size int64
	for i, historyRange := range ranges {
		if upperBounds[i] == 0 {
			continue
		}
		value, err := gsql.SelectG[historyMetrics](
			EventSchema.ID.Count().As("count"),
			EventSchema.EventSizeBytes.Sum().IfNull(0).As("size"),
		).From(EventSchema).
			Where(historyRangeCondition(historyRange), EventSchema.NodeID.Lte(upperBounds[i])).Take(db)
		if err != nil {
			return 0, 0, err
		}
		if value != nil {
			count += value.Count
			size += value.Size.IntPart()
		}
	}
	return count, size, nil
}

// GetEventsRange 返回 (afterExclusive, throughInclusive] 区间内的可见事件。
// throughInclusive == 0 表示不限上界。
//
// 范围过滤**下推到 SQL**（见 getEventsRangeWithDB）。原实现是 GetEvents 全量加载整个
// history 再在 Go 里过滤——本方法位于增量激活最热路径上
// （server_incremental_activation.go 的 GetWorkflowActivation 每次都调，只要一小段增量），
// 全量加载使单个 run 随自身历史增长呈 O(H²)。下推后单次调用只传输实际返回的行。
func (s *Repository) GetEventsRange(runID string, afterExclusive, throughInclusive int64) ([]history.Event, error) {
	if afterExclusive < 0 || throughInclusive < 0 || (throughInclusive > 0 && throughInclusive < afterExclusive) {
		return nil, fmt.Errorf("invalid history range (%d, %d]", afterExclusive, throughInclusive)
	}
	run, err := s.Get(runID)
	if err != nil {
		return nil, err
	}
	return s.getEventsRangeWithDB(s.db, run, afterExclusive, throughInclusive)
}
