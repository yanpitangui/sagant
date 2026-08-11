-- Runs once, the first time the "postgres" server container's data volume is created (see
-- OrderFulfillment.AppHost's Program.cs: postgres.WithInitFiles(...) mounts this file under
-- /docker-entrypoint-initdb.d, the official postgres image's own first-time-init hook). At that
-- point the "orders-db" database Aspire's AddDatabase("orders-db") asks for doesn't exist yet —
-- Aspire only creates it later, via a CREATE DATABASE issued from app startup once the container
-- reports ready — so this script creates it itself, then builds the read-model schema inside it in
-- the same script. Aspire's own later CREATE DATABASE attempt against the same name is idempotent
-- (it catches Postgres's "already exists" error and logs, doesn't fail startup).
--
-- Caveat: because this only runs on a *fresh* data volume, a schema change made here after the
-- volume already exists needs the volume wiped (docker volume rm / drop the Aspire-managed volume)
-- to take effect — same as any docker-entrypoint-initdb.d script, not specific to Aspire.
--
-- Akka.Persistence.Sql's own journal/snapshot tables are created separately, by that library's own
-- autoInitialize:true (see OrderFulfillment.Sample's Program.cs) against this same database —
-- unrelated to the read-model tables below, just sharing the connection string.
CREATE DATABASE "orders-db";

\connect "orders-db"

-- One row per workflow instance this read model has ever seen — an order itself
-- (parent_workflow_id null) or one of its item children (parent_workflow_id = the order id). Holds
-- every field that changes over a workflow's life, for either kind uniformly: OrderFulfillmentWorkflow
-- writes OrderStatus names into status, ItemFulfillmentWorkflow writes ItemStatus names — the read
-- side doesn't need to know which.
--
-- Written once, at place-order time, by OrderPlacementService (INSERT, not upserted) — order and
-- every one of its items are all fully known upfront (item ids are pre-assigned before
-- FulfillItemsStep ever runs — see OrderLineItem's own doc comment), so there's no "first sight from
-- a notification" case to handle, unlike step_runs/event_log below. UPDATEs after that come from
-- OrderReadModelRepository.RefreshStatus, driven by WorkflowEventLoggerActor re-querying the
-- workflow's own authoritative state after a step completes/fails or the workflow ends.
CREATE TABLE IF NOT EXISTS workflow_views (
    workflow_id text PRIMARY KEY,
    parent_workflow_id text NULL,
    workflow_type text NOT NULL,
    status text NOT NULL,
    failure_reason text NULL,
    paused boolean NOT NULL DEFAULT false,
    pause_reason text NULL,
    auto_cancel_at timestamptz NULL
);

CREATE INDEX IF NOT EXISTS ix_workflow_views_parent ON workflow_views (parent_workflow_id);

-- Order-only identity/placement facts — everything that's the same for the order's whole life lives
-- here instead of workflow_views, so the list/detail queries don't have to filter workflow_views by
-- workflow_type just to find "the orders".
CREATE TABLE IF NOT EXISTS orders (
    order_id text PRIMARY KEY REFERENCES workflow_views (workflow_id),
    customer_id text NOT NULL,
    amount integer NOT NULL,
    shipping_address text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz NULL
);

-- Item-only identity/placement facts, same reasoning as orders above.
CREATE TABLE IF NOT EXISTS order_items (
    order_id text NOT NULL REFERENCES orders (order_id),
    item_workflow_id text PRIMARY KEY REFERENCES workflow_views (workflow_id),
    amount integer NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_order_items_order_id ON order_items (order_id);

-- One row per (workflow, step, attempt) — an upsert target (ON CONFLICT DO UPDATE), same shape as
-- the old OrderView.StartStep/CompleteStep/FailStep's find-or-append-then-replace logic, just backed
-- by a real constraint instead of an in-process list scan. Every replica processing the same
-- StepStarted/StepCompleted/StepFailed notification writes the identical row, so N-way duplicate
-- delivery (see WorkflowEventPubSubBridge) converges to one row instead of N.
CREATE TABLE IF NOT EXISTS step_runs (
    workflow_id text NOT NULL REFERENCES workflow_views (workflow_id),
    step_name text NOT NULL,
    attempt integer NOT NULL,
    status text NOT NULL,
    error text NULL,
    started_at timestamptz NOT NULL,
    PRIMARY KEY (workflow_id, step_name, attempt)
);

-- One row per log line. WorkflowFeedItem.Timestamp is stamped once by the originating runtime
-- driver, identical across every replica's copy of the same broadcast notification — combined with
-- the line text itself, that's a stable natural key for ON CONFLICT DO NOTHING dedup across
-- replicas, the same role step_runs' primary key plays for step events.
CREATE TABLE IF NOT EXISTS event_log (
    workflow_id text NOT NULL REFERENCES workflow_views (workflow_id),
    at timestamptz NOT NULL,
    line text NOT NULL,
    PRIMARY KEY (workflow_id, at, line)
);

CREATE INDEX IF NOT EXISTS ix_event_log_workflow_id ON event_log (workflow_id, at);
