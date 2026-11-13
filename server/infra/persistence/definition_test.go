package persistence

import (
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/definition/definitiontest"
)

// 配置版本仓储的契约与内存实现共用 definitiontest；gorm 特有的行数与持久化断言
// 在 definitionrepo 包与本包的其他测试里。

func TestDefinitionConfigRepositoryContract(t *testing.T) {
	definitiontest.RunConfigRepositoryContract(t, func(t *testing.T) definition.ConfigRepository { return newTestStore(t).Definitions })
}
