package definitionrepo

import "time"

// WorkflowConfigPO
// @Gsql
// @Setter(setter=false, patch=v2)
type WorkflowConfigPO struct {
	Namespace  string    `gorm:"column:namespace"`
	WorkflowID string    `gorm:"column:workflow_id"`
	Version    string    `gorm:"column:version"`
	ConfigJSON []byte    `gorm:"column:config_json"`
	CreatedAt  time.Time `gorm:"column:created_at"`
}

func (WorkflowConfigPO) TableName() string { return "tenuto_configs" }

func (WorkflowConfigPO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_configs (
  namespace   VARCHAR(191) NOT NULL DEFAULT 'default',
  workflow_id VARCHAR(191) NOT NULL,
  version     VARCHAR(64)  NOT NULL,
  config_json LONGBLOB     NOT NULL,
  created_at  DATETIME(3)  NOT NULL,
  PRIMARY KEY (namespace, workflow_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
}

// WorkflowConfigActivePO
// @Gsql
// @Setter(setter=false, patch=v2)
type WorkflowConfigActivePO struct {
	Namespace  string    `gorm:"column:namespace"`
	WorkflowID string    `gorm:"column:workflow_id"`
	Version    string    `gorm:"column:version"`
	UpdatedAt  time.Time `gorm:"column:updated_at"`
}

func (WorkflowConfigActivePO) TableName() string { return "tenuto_config_active" }

func (WorkflowConfigActivePO) MysqlCreateTable() string {
	return `
CREATE TABLE IF NOT EXISTS tenuto_config_active (
  namespace   VARCHAR(191) NOT NULL DEFAULT 'default',
  workflow_id VARCHAR(191) NOT NULL,
  version     VARCHAR(64)  NOT NULL,
  updated_at  DATETIME(3)  NOT NULL,
  PRIMARY KEY (namespace, workflow_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
}
