# Order & Inventory Sync Service: Design

**AI usage note:** AI Usage Note: I used Claude (Anthropic) to explore two architectural approaches—a database-first design prioritizing transactional inventory consistency and a queue-driven design emphasizing asynchronous processing and fault isolation. I provided the business constraints, scalability requirements, cost considerations, and delivery timeline, then submitted the same evaluation criteria to ChatGPT (OpenAI). I compared both outputs across concurrency safety, idempotency, failure recovery, operational complexity, and cost before consolidating the final architecture and trade-offs.

## 1. Summary and key idea

The oversell problem is a **concurrency problem on one number per SKU**. My design makes **PostgreSQL the only authority for stock**. A reservation is a single atomic conditional `UPDATE` that cannot go negative. Everything else (queues, Redis, channel pushes) is derived from Postgres and can be lost and rebuilt without losing an order or overselling.

Sizing: sale day is about 80k orders. Peak is 300/min = **5 orders/s**, about 10 stock changes/s. That is small for Postgres, so I use a **modular monolith** and no extra infrastructure.

## 2. Architecture

![Architecture diagram](architecture.png)

Editable source: [`architecture.svg`](architecture.svg).

**What runs where.** One NestJS codebase deployed on ECS Fargate as two services:
- `api`: webhook receivers, ops API and a small static React page for ops.
- `worker`: BullMQ consumers (ingest and reserve, Flipkart poller, 4 stock pushers, ERP relay, reconcilers).

Both autoscale on CPU and queue depth (min 2, max 6 tasks).
- **RDS PostgreSQL** Multi-AZ is the source of truth.
- **ElastiCache Redis** holds BullMQ and rate-limit buckets only. Losing it loses nothing, because a sweeper re-enqueues pending rows from Postgres.
- **S3** keeps raw payloads for audit and replay.

**Cost estimate** (approximate, ap-south-1): Fargate about $90, RDS `db.t4g.large` Multi-AZ about $200, Redis about $50, ALB, NAT and CloudWatch about $90. Total is roughly **₹38–42k/month**, inside the ₹50k budget.

## 3. Data model (PostgreSQL)

| Table | Key columns | Constraints and purpose |
|---|---|---|
| `inbound_events` | id, channel, dedupe_key, raw_ref (S3), status (`NEW/DONE/QUARANTINED`), received_at | **UNIQUE(channel, dedupe_key)**. Webhook de-dup and replay. |
| `orders` | id, channel, external_order_id, status, last_event_at, total_amount | **UNIQUE(channel, external_order_id)**. Status machine: `RESERVED → ERP_PENDING → ERP_SYNCED`, or `REJECTED_OOS / CANCELLED / RETURNED`. |
| `order_items` | order_id, sku, qty, unit_price | PK(order_id, sku), qty > 0 |
| `stock` | sku PK, on_hand, reserved, buffer, version | `CHECK (on_hand >= 0)`, `CHECK (reserved >= 0)`, `CHECK (reserved <= on_hand)`. The DB itself refuses to oversell. |
| `reservations` | order_id, sku, qty, status (`HELD/COMMITTED/RELEASED`) | PK(order_id, sku). Makes release and commit idempotent. |
| `stock_ledger` | id, sku, d_on_hand, d_reserved, on_hand_after, reserved_after, reason, order_id, actor, at | **Append-only**, written in the same txn as every stock change. Answers "why did stock change?" |
| `stock_channel_state` | sku, channel, desired_qty, desired_version, pushed_qty, pushed_version, last_push_at, last_error | PK(sku, channel). A row is dirty when `desired_version > pushed_version`. |
| `erp_outbox` | id, order_id, seq, type (`CREATE/CANCEL`), status, attempts, next_attempt_at, erp_doc_no | UNIQUE(order_id, seq). Per-order ordering. |
| `channel_cursor` | channel, cursor, updated_at | Flipkart poll position. |

**No duplicate orders.** There are three layers: (1) `inbound_events` unique key drops repeated webhooks, (2) `orders` UNIQUE(channel, external_order_id) with `INSERT … ON CONFLICT DO NOTHING` inside the reservation txn, (3) an ERP idempotency key (section 7).

## 4. Order ingestion

- **Webhooks (website, Nykaa, Blinkit).** Verify the signature, store the raw body in S3, insert into `inbound_events`, return `200`. Target is under 100 ms and **depends only on Postgres**. Normalisation and reservation happen asynchronously on a per-channel queue.
- **Flipkart polling.** One repeatable job every 2 min, using a cursor in `channel_cursor` with a **10-minute overlap**. Re-read orders hit the unique key and are ignored.
- **Duplicates.** Handled by the layers in section 3.
- **Out of order.** The order row is locked (`SELECT … FOR UPDATE`) and an event is applied only if it moves the status forward. A `CANCELLED` that arrives *before* `CREATED` writes a **tombstone** order, so the late `CREATED` is ignored and never reserves stock.
- **Channel down.** Queues, worker pools and circuit breakers are **per channel (bulkheads)**, so one channel stalling cannot block others. Missed webhooks are recovered by a nightly list-orders reconcile for Nykaa and Blinkit, and by polling for Flipkart.
- **Bad data** (unknown SKU, schema failure) goes to `QUARANTINED`, with no retries, and appears on the ops screen.

## 5. Inventory consistency: the last unit

Two orders, Nykaa and Blinkit, buy the last unit of SKU X in the same second.

1. Both webhooks are acked and enqueued on separate channel queues, so they run in parallel workers.
2. Each worker runs **one transaction**:

```sql
-- pseudo-code; SKUs processed in sorted order to avoid deadlocks
BEGIN;
INSERT INTO orders(...) ON CONFLICT (channel, external_order_id) DO NOTHING;  -- 0 rows => duplicate, stop
-- for each item (sorted by sku):
UPDATE stock
   SET reserved = reserved + :qty, version = version + 1
 WHERE sku = :sku AND on_hand - reserved >= :qty
RETURNING on_hand, reserved;                       -- 0 rows => out of stock
-- any item returns 0 rows => ROLLBACK, then record the order as REJECTED_OOS in a new txn
INSERT INTO reservations(...), stock_ledger(...), erp_outbox(...);
UPDATE stock_channel_state SET desired_qty = :new, desired_version = desired_version + 1 ...;
COMMIT;
```

3. Postgres takes a **row lock on `stock[X]`**. The first transaction commits. The second waits, then **re-evaluates `on_hand - reserved >= 1` against the committed row**, finds it false, and updates 0 rows.
4. **Winner:** order confirmed and queued for the ERP. **Loser:** `REJECTED_OOS`. A cancel call goes to the channel (the website also triggers a refund).
5. The commit also marks every channel's displayed quantity dirty. The SKU is now **at or near zero, so it is the highest push priority** (section 6).

No Redis lock or counter is in the correctness path, so there is no dual write and no split-brain.

**Honest caveat.** Marketplaces take the customer's money *before* our webhook arrives, so a rejection is a seller-side cancellation that hurts the rating. The DB guarantees we never *promise* stock we don't have. **Keeping the shown stock accurate** is what keeps cancellations under 0.5%. That is the job of the buffer and the push design below.

## 6. Stock push (4 channels, 60 s, sale day)

**Push the latest absolute number, never deltas.** `stock_channel_state` is coalesced, so 50 changes to one SKU become one push. Pushes are idempotent, so any retry is safe.

**Three levers keep the load inside every rate limit.**
1. **Safety buffer per SKU.** `displayed = available − buffer`, where `buffer = max(1, ceil(1.5 × units_sold_per_minute_last_5min))`. It covers the units that can sell during the 60 s push lag. A job recomputes it every 30 s.
2. **Bucketed display.** The shown quantity is rounded down: exact below 20, steps of 5 up to 99, steps of 20 above. We push only when the *displayed* value changes, so healthy SKUs rarely push.
3. **Priority queue.** Low-stock SKUs go first (lowest displayed qty, then fastest-selling). Full refresh is a low-priority background job.

| Channel | Method and limit | Budget (≈70–80% of limit) | Math |
|---|---|---|---|
| Website | Direct DB/API | none | Immediate, in the same pipeline |
| Nykaa | 1 SKU/call, 5 rps | **4 rps = 240 SKU/min** | A full 1,200-SKU refresh takes 5 min, so the design relies on coalescing, bucketing and priority. Hot and low-stock SKUs (a few hundred at most) update within 60 s. |
| Flipkart | 50 SKUs/call, 10 rps | **7 rps** | 1,200 SKUs = 24 calls, so the whole catalogue refreshes in 1 s. On `429`, honour `Retry-After` and halve the bucket (AIMD), then recover slowly. |
| Blinkit | API (2% timeouts) + daily CSV | **2 rps** | Timeout 5 s, 3 tries with jitter. Per-push failure is about 0.02³ ≈ 0.0008%. The retry is safe because the push is an absolute value. Daily CSV is the baseline truth and re-syncs any drift. |

Rate limits are enforced by a **Redis token bucket per channel** (Lua script) shared by all workers.

## 7. Failure handling

| Case | Behaviour |
|---|---|
| General retries | Exponential backoff with jitter. State lives in Postgres (`attempts`, `next_attempt_at`). BullMQ is only the runner. |
| ERP exactly once | Realistically *effectively-once*. The key is `External Document No = {channel}-{external_order_id}`. Before any retry after an ambiguous timeout, the relay **GETs by that key first**. Only if it is absent does it POST. One in-flight row per order, via `FOR UPDATE SKIP LOCKED`. `CANCEL` events are sent only after `CREATE` succeeded (`seq`). |
| ERP Sunday downtime | Orders are committed to `erp_outbox` **at reservation time**, so none can be lost. A circuit breaker opens after 5 consecutive failures, probes every 30 s, and the outbox accumulates in Postgres. On recovery it drains oldest-first at a configurable rate (default 10 rps). Worst case, a 2 h sale-day backlog (about 36k) clears in about 1 h. Alerts are muted during the known maintenance window. |
| Blinkit timeouts | As in section 6: absolute-value retries, circuit breaker per channel, daily CSV as safety net. |
| Poison or repeatedly failing messages | ERP: 12 attempts (backoff capped at 30 min, about 24 h), then `FAILED`. Quarantine for invalid payloads. Both appear on the ops screen and fire an alarm. |
| Ops retry screen | Lists `erp_outbox`, `inbound_events` and push rows in `FAILED/QUARANTINED`. A **Retry** button resets status and attempts. Each action is logged with the user. Auth is OIDC against the company IdP. |
| Cancel and return | **Cancel before dispatch:** idempotent `reservations HELD→RELEASED`, `reserved −= qty`, ledger row, push. **Return:** stock is added back (`on_hand += qty`) only when the warehouse marks it *received and QC-passed*, not when the customer starts the return. |
| Redis loss or restart | Sweeper re-enqueues `NEW` inbound events, dirty push rows and due outbox rows from Postgres. |

## 8. Observability: day-one alerts

1. **Stock-push lag p95** (stock change → channel ack), per channel. Alert above 45 s.
2. **`REJECTED_OOS` rate.** Warn above 0.3% of orders in 15 min, page above 0.5%.
3. **Oldest pending `erp_outbox` age.** Alert above 15 min (outside the maintenance window).
4. **Failed/DLQ count** (any channel, quarantined events). Alert if above 0 for 10 min.
5. **Ingestion freshness per channel** (age of last webhook or poll success) plus webhook 5xx rate. Silence on a normally busy channel means a missed-order risk.

Also track the daily **reconciliation drift** (our `on_hand` vs Business Central).

## 9. Trade-offs

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Where reservation correctness lives | **Postgres conditional `UPDATE` + CHECK constraints** | Redis atomic counters or Redlock as source of truth | Redis plus DB means a dual write and stock drift if Redis is lost. 5 orders/s is trivial for Postgres row locks. |
| Stock allocation across channels | **One shared pool + dynamic buffer** | Fixed per-channel quotas | Quotas strand stock (Blinkit holds 50 while Nykaa sells out). The buffer protects against push lag at low cost. |
| Messaging | **Postgres outbox/inbox + BullMQ (Redis) as runner** | SQS or Kafka | The stack is already approved and volume is tiny. Durability comes from Postgres, so Redis loss is harmless. One fewer service for 3 devs. |
| Services | **Modular monolith, 2 deployables** | Microservice per channel | 3 developers, 6 weeks. Isolation comes from per-channel queues, workers and breakers. Modules can be split out later. |
| Push model | **Coalesced absolute quantity with bucketed display** | Event per stock change | Fits Nykaa's 5 rps. Retries are idempotent and no ordering problems arise. |
| ERP delivery | **Idempotency key + lookup-before-retry** | Distributed transaction / assume ERP de-dups | The ERP offers no transactional guarantee. This is the realistic route to "once". |

## 10. Left out of v1 (deliberately)

- **Second warehouse** and split shipments (see stretch).
- **Return QC workflow.** v1 takes a "received" signal from ops or the ERP.
- **Price, catalogue and promotion sync.** Only stock and orders are in scope.
- **Demand-forecast-driven buffers.** A simple rolling rate is enough for v1.
- **Rich ops UI** (analytics, bulk edits), **multi-region DR** (Multi-AZ only) and **customer notifications.**

## 11. Assumptions

1. Business Central (BC) owns *physical* stock. Our `on_hand` is seeded from BC, updated by inbound receipts or adjustments (ops or BC import every 15 min), and **reconciled nightly**. Drift above a threshold alerts and writes a `RECONCILE` ledger row. Reservation moves to committed on dispatch confirmation from BC (polled).
2. Webhooks carry an HMAC signature. Nykaa and Blinkit retry on non-2xx responses.
3. Each channel exposes a list-orders API usable for reconciliation. The Flipkart poll returns orders by `modified_since`.
4. A "confirmed" order is one that reserved stock successfully (COD verification is out of scope).
5. BC accepts a custom External Document No and can be queried by it. It can take about 10 rps.
6. Blinkit's daily CSV is delivered via the seller portal or SFTP that we can script.
7. Average order has about 2 line items. Sale day is about 80k orders.
8. Ops authenticate through the company's OIDC IdP (Google Workspace). No Cognito is needed.
9. Cost figures are estimates based on public AWS ap-south-1 pricing and should be re-checked.

## 12. Stretch A: second warehouse (Bengaluru)

- `stock` becomes `(sku, warehouse_id)`. The ledger and reservations gain `warehouse_id`, and lock order becomes `(warehouse, sku)`.
- The reservation step picks **one** warehouse per order: nearest to `shipping_pincode` that can fill **all lines**, else the other, else a flagged split (policy decision for the business).
- ERP routing sends a BC **location code** per order.
- Displayed qty per channel = sum over warehouses minus buffer. Slight split-shipment risk is accepted and monitored.
- Channels that support one inventory location (Nykaa, Flipkart) get the aggregate. Blinkit may need per-warehouse mapping.
- Alerts and reconciliation become per warehouse.
