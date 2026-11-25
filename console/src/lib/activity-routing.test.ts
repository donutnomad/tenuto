import { describe, expect, it } from "vitest"
import { activityRoutings, taskQueueSource } from "./activity-routing"

describe("activityRoutings", () => {
  it("groups activity types across workers and marks default queues", () => {
    const routings = activityRoutings([
      { identity: "payment-1", build_id: "p1", task_queues: ["payments"], activity_types: ["payments.charge"], default: true },
      { identity: "payment-2", build_id: "p2", task_queues: ["payments"], activity_types: ["payments.charge"], default: true },
      { identity: "billing-1", build_id: "b1", task_queues: ["billing"], activity_types: ["payments.charge", "billing.invoice"] },
    ])
    expect(routings).toEqual([
      { activityType: "billing.invoice", queues: ["billing"], defaultQueues: [], workers: 1 },
      { activityType: "payments.charge", queues: ["billing", "payments"], defaultQueues: ["payments"], workers: 3 },
    ])
  })

  it("treats a worker without queues as polling default", () => {
    expect(activityRoutings([{ identity: "w", activity_types: ["a"] }])).toEqual([
      { activityType: "a", queues: ["default"], defaultQueues: [], workers: 1 },
    ])
  })
})

describe("taskQueueSource", () => {
  const activities = {
    "orders.Charge": { symbol: "orders.Charge", type: "payments.charge", task_queue: "payments" },
    "orders.Notify": { symbol: "orders.Notify", type: "orders.notify" },
  }

  it("reports a queue declared in the workflow definition", () => {
    expect(taskQueueSource("payments", "payments.charge", activities)).toBe("declared")
  })

  it("reports a call-site override when the event queue differs from the declaration", () => {
    expect(taskQueueSource("payments-gpu", "payments.charge", activities)).toBe("call-site")
  })

  it("reports an undeclared queue", () => {
    expect(taskQueueSource("orders", "orders.notify", activities)).toBe("undeclared")
    expect(taskQueueSource("orders", "unknown.type", activities)).toBe("undeclared")
  })

  it("returns null without the definition", () => {
    expect(taskQueueSource("payments", "payments.charge", undefined)).toBeNull()
  })
})
