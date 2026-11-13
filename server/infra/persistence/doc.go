// Package persistence 在同一个数据库上建立工作流执行所需的各个 GORM 仓储，Store 以具名字段持有它们。
//
// 领域模型与仓储接口在 server/biz/domain 下；每个具体仓储与它的 PO、mapper 放在本目录的
// 同名子包里（executionrepo、definitionrepo、dispatchrepo、schedulerepo、batchrepo）。保留期回收操作执行聚合的表，在 executionrepo 里。
package persistence
