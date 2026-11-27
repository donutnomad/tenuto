package integration

import (
	"testing"
	"time"

	"github.com/donutnomad/taskq/queue"
	taskqgormstore "github.com/donutnomad/taskq/store/gormstore"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
)

// TaskQ 的表是 utf8mb4_0900_bin：只差大小写或尾随空格的 queue name 是各自的分区，读不到对方的任务，
// 确认（DELETE task_id <= ack level）也不删对方的任务。
func TestTaskQQueueNamesCompareByBytesOnMySQL(t *testing.T) {
	store, err := taskqgormstore.New(mysqltest.NewTaskQDB(t), taskqgormstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	names := []string{"activity:default/Payments", "activity:default/payments", "activity:default/payments "}
	for _, name := range names {
		if err := store.CreateQueue(ctx, &queue.QueueInfo{QueueName: name, RangeID: 1, NextTaskID: 1}); err != nil {
			t.Fatalf("create queue %q: %v", name, err)
		}
		if err := store.CreateTasks(ctx, []*queue.TaskRow{{TaskID: 1, QueueName: name, Data: []byte(name), CreatedAt: time.Now()}}); err != nil {
			t.Fatalf("create task in %q: %v", name, err)
		}
	}
	for _, name := range names {
		tasks, err := store.GetTasks(ctx, name, 0, 0, 100, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) != 1 || string(tasks[0].Data) != name {
			t.Fatalf("queue %q reads %d tasks, want only its own", name, len(tasks))
		}
	}
	if _, _, err := store.AdvanceAckLevelAndDeleteTasks(ctx, names[0], 0, 1, 1); err != nil {
		t.Fatal(err)
	}
	for _, name := range names[1:] {
		tasks, err := store.GetTasks(ctx, name, 0, 0, 100, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(tasks) != 1 {
			t.Fatalf("acknowledging %q deleted the task of %q", names[0], name)
		}
	}
}
