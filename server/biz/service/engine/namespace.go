package engine

import (
	"github.com/donutnomad/tenuto/protocol"
)

// workflowLockKey 是 s.keyedMu 的唯一 key 构造函数：per-(namespace, workflowID) 粒度。
//
// ⚠️ 全部 keyedMu.Lock 调用点必须共用本函数——若有调用点自行拼 key（或退回裸 workflowID），
// 就会出现「两处用不同 key 锁同一实体 → 互斥失效 → 竞态」，且不会有编译错误。
// 必须经 NormalizeNamespace：否则 "" 与 "default" 会落到两把锁上，制造同样的竞态。
func workflowLockKey(namespace, workflowID string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + workflowID
}
