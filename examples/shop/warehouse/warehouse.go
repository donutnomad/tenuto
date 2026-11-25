// Package warehouse 是 warehouse-service 的 Activity 实现：在 warehouse 队列上预留、释放库存和发货。
package warehouse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/donutnomad/tenuto/examples/shop/contracts"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// Warehouse 是内存里的库存。每个操作都以订单号为幂等键：重复调用返回同一结果，不重复扣减。
type Warehouse struct {
	mu           sync.Mutex
	stock        map[string]int
	reservations map[string]reservation // key: OrderID
	shipments    map[string]string      // OrderID → 运单号
}

type reservation struct {
	sku      string
	qty      int
	released bool
}

// New 返回初始库存为 stock 的仓库。
func New(stock map[string]int) *Warehouse {
	return &Warehouse{stock: stock, reservations: map[string]reservation{}, shipments: map[string]string{}}
}

// Register 把三个 Activity 登记到 registry，契约与 contracts 包里的 remote 声明一致。
func (w *Warehouse) Register(registry *activity.Registry) error {
	return errors.Join(
		activity.RegisterActivityOnQueue(registry, "warehouse", "warehouse.reserve", "v1", w.Reserve),
		activity.RegisterActivityOnQueue(registry, "warehouse", "warehouse.release", "v1", w.Release),
		activity.RegisterActivityOnQueue(registry, "warehouse", "warehouse.ship", "v1", w.Ship),
	)
}

func (w *Warehouse) Reserve(_ context.Context, in contracts.ReserveInput) (contracts.ReserveOutput, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := "rsv_" + in.OrderID
	if _, ok := w.reservations[in.OrderID]; ok {
		return contracts.ReserveOutput{ReservationID: id}, nil
	}
	if left := w.stock[in.SKU]; left < in.Qty {
		log.Printf("reserve order=%s sku=%s qty=%d: out of stock (left %d)", in.OrderID, in.SKU, in.Qty, left)
		return contracts.ReserveOutput{}, apperror.NewNonRetryable(
			fmt.Sprintf("sku %s: want %d, left %d", in.SKU, in.Qty, left), contracts.ErrOutOfStock, nil)
	}
	w.stock[in.SKU] -= in.Qty
	w.reservations[in.OrderID] = reservation{sku: in.SKU, qty: in.Qty}
	log.Printf("reserve order=%s sku=%s qty=%d: %s (left %d)", in.OrderID, in.SKU, in.Qty, id, w.stock[in.SKU])
	return contracts.ReserveOutput{ReservationID: id}, nil
}

func (w *Warehouse) Release(_ context.Context, in contracts.ReleaseInput) (contracts.ReleaseOutput, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	r, ok := w.reservations[in.OrderID]
	if ok && !r.released {
		w.stock[r.sku] += r.qty
		r.released = true
		w.reservations[in.OrderID] = r
		log.Printf("release order=%s %s (%s): sku=%s back to %d", in.OrderID, in.ReservationID, in.Reason, r.sku, w.stock[r.sku])
	}
	return contracts.ReleaseOutput{Released: true}, nil
}

func (w *Warehouse) Ship(_ context.Context, in contracts.ShipInput) (contracts.ShipOutput, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	tracking, ok := w.shipments[in.OrderID]
	if !ok {
		tracking = "SF" + in.OrderID
		w.shipments[in.OrderID] = tracking
		log.Printf("ship order=%s %s to %q: tracking %s", in.OrderID, in.ReservationID, in.Address, tracking)
	}
	return contracts.ShipOutput{TrackingNo: tracking}, nil
}
