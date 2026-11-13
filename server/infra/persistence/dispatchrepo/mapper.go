package dispatchrepo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

func taskDispatchRowFromTask(task protocol.Task, now time.Time) *TaskDispatchPO {
	task = dispatch.NormalizeTask(task)
	return &TaskDispatchPO{
		Namespace:        task.Namespace,
		TaskType:         string(task.Type),
		RunID:            task.RunID,
		ScheduledEventID: task.ScheduledEventID,
		Attempt:          task.Attempt,
		ActivityID:       task.ActivityID,
		ActivityType:     task.ActivityType,
		TaskQueue:        task.TaskQueue,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func taskDispatchRowToState(row *TaskDispatchPO) dispatch.State {
	state := dispatch.State{
		Namespace: protocol.NormalizeNamespace(row.Namespace),
		Status:    dispatch.Status(row.Status),
		LeaseID:   row.LeaseID,
		Attempts:  row.Attempts,
		LastError: row.LastError,
		CreatedAt: row.CreatedAt,
		UpdatedAt: row.UpdatedAt,
	}

	if row.LeaseExpiresAt != nil {
		state.LeaseExpiresAt = *row.LeaseExpiresAt
	}
	if row.LastDispatchedAt != nil {
		state.LastDispatchedAt = *row.LastDispatchedAt
	}
	return state
}

func activityWorkerVersionRowToWorker(row *ActivityWorkerVersionPO) (protocol.ActivityWorkerVersion, error) {
	var contracts []protocol.ActivityContract
	if len(row.ContractsJSON) > 0 {
		if err := json.Unmarshal(row.ContractsJSON, &contracts); err != nil {
			return protocol.ActivityWorkerVersion{}, fmt.Errorf("activity worker version %s/%s/%s contracts: %w", row.Namespace, row.Identity, row.BuildID, err)
		}
	}
	return protocol.ActivityWorkerVersion{
		Contracts:       contracts,
		Namespace:       protocol.NormalizeNamespace(row.Namespace),
		App:             row.App,
		Identity:        row.Identity,
		BuildID:         row.BuildID,
		TaskQueues:      []string{row.TaskQueue},
		ActivityTypes:   []string{row.ActivityType},
		Default:         row.IsDefault,
		Weight:          row.Weight,
		HeartbeatTTL:    row.HeartbeatTTL,
		LastHeartbeatAt: row.LastHeartbeatAt,
		ExpiresAt:       row.ExpiresAt,
	}, nil
}
