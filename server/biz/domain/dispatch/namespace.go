package dispatch

import (
	"github.com/donutnomad/tenuto/protocol"
)

func ActivityTaskQueueKey(namespace, taskQueue string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + NormalizeActivityTaskQueue(taskQueue)
}
