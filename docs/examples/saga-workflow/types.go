package sagaflow

// OrderInput 是下单入参。Scenario 只用于演示失败分支，生产代码里它来自业务事实。
type OrderInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
	// Scenario: "" 正常；"payment_declined"；"payment_unknown"；"payment_pending"；
	// "shipment_rejected"；"shipment_rejected_refund_failed"；"shipment_unknown"。
	Scenario string `json:"scenario"`
}

// OrderOutput 是成功下单的结果。
type OrderOutput struct {
	ReservationID string `json:"reservation_id"`
	PaymentID     string `json:"payment_id"`
	ShipmentID    string `json:"shipment_id"`
}

// ReserveInventoryInput 是预留库存入参。
type ReserveInventoryInput struct {
	OrderID string `json:"order_id"`
}

// ReserveInventoryOutput 是预留结果。
type ReserveInventoryOutput struct {
	ReservationID  string `json:"reservation_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

// ChargePaymentInput 是扣款入参。
type ChargePaymentInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Amount        int64  `json:"amount"`
	Scenario      string `json:"scenario"`
}

// ChargePaymentOutput.Status 表达业务结论："charged" 已扣，"declined" 明确拒绝。
type ChargePaymentOutput struct {
	Status         string `json:"status"`
	PaymentID      string `json:"payment_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

// CreateShipmentInput 是发货入参。
type CreateShipmentInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	PaymentID     string `json:"payment_id"`
	Scenario      string `json:"scenario"`
}

// CreateShipmentOutput.Status 表达业务结论："created" 已创建，"rejected" 明确拒绝。
type CreateShipmentOutput struct {
	Status     string `json:"status"`
	ShipmentID string `json:"shipment_id"`
}

// RefundPaymentInput 是退款入参。
type RefundPaymentInput struct {
	OrderID   string `json:"order_id"`
	PaymentID string `json:"payment_id"`
	Scenario  string `json:"scenario"`
}

// RefundPaymentOutput 的 Refunded=true 表示退款已完成；返回 error 时 Refunded 不可信。
type RefundPaymentOutput struct {
	Refunded       bool   `json:"refunded"`
	IdempotencyKey string `json:"idempotency_key"`
}

// ReleaseInventoryInput 是释放库存入参。
type ReleaseInventoryInput struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"reason"`
}

// ReleaseInventoryOutput 的 Released=true 表示库存已释放；返回 error 时 Released 不可信。
type ReleaseInventoryOutput struct {
	Released       bool   `json:"released"`
	IdempotencyKey string `json:"idempotency_key"`
}
