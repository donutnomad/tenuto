package taskq

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"github.com/donutnomad/tenuto/protocol"
)

const (
	stickyRoutingKeyScope         = "workflow-sticky:v1:"
	activityRoutingKeyScope       = "activity-route:v3:"
	activityTargetRoutingKeyScope = "activity-worker:v2:"
)

var errEmptyStickyWorkerIdentity = errors.New("sticky workflow task worker identity is empty")

func stickyRoutingKey(workerIdentity string) (string, error) {
	if workerIdentity == "" {
		return "", errEmptyStickyWorkerIdentity
	}
	return scopedRoutingKey(stickyRoutingKeyScope, workerIdentity), nil
}

// activityRoutingKey 是队列的通用 routing key：订阅这个队列的任何 Worker 都能领取，与它的 build 无关。
func activityRoutingKey(namespace, taskQueue string) string {
	return scopedRoutingKey(activityRoutingKeyScope, namespace+"\x00"+taskQueue)
}

// activityRoutingKeysForWorker 是 Worker 在一个队列上接受的 routing key：队列的通用 key，以及发给它自己 Identity 的 key。
func activityRoutingKeysForWorker(namespace, taskQueue, identity string) []string {
	keys := []string{activityRoutingKey(namespace, taskQueue)}
	if identity != "" {
		keys = append(keys, targetedActivityRoutingKey(namespace, taskQueue, identity))
	}
	return keys
}

func scopedRoutingKey(scope, value string) string {
	digest := sha256.Sum256([]byte(value))
	return scope + base64.RawURLEncoding.EncodeToString(digest[:])
}

// targetedActivityRoutingKey 是发给 Run 固定的目标 Worker 的 routing key，只有这个 Identity 能领取。
func targetedActivityRoutingKey(namespace, taskQueue, identity string) string {
	return scopedRoutingKey(activityTargetRoutingKeyScope, namespace+"\x00"+taskQueue+"\x00"+identity)
}

// taskActivityRoutingKey 是一份 Activity 投递的 routing key：Run 固定了目标 Worker 时发给它，否则用队列的通用 key。
func taskActivityRoutingKey(task protocol.Task) string {
	if task.TargetWorkerIdentity != "" {
		return targetedActivityRoutingKey(task.Namespace, task.TaskQueue, task.TargetWorkerIdentity)
	}
	return activityRoutingKey(task.Namespace, task.TaskQueue)
}
