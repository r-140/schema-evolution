CREATE SCHEMA sales;
CREATE TABLE sales.customers (
 id BIGINT PRIMARY KEY, name TEXT NOT NULL, segment TEXT NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE sales.orders (
 id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES sales.customers(id),
 amount BIGINT NOT NULL, currency TEXT NOT NULL DEFAULT 'PLN',
 status TEXT NOT NULL DEFAULT 'created', ordered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE sales.customers REPLICA IDENTITY FULL;
ALTER TABLE sales.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION evolution_pub FOR TABLE sales.customers, sales.orders;
CREATE SCHEMA control;
CREATE TABLE control.settings (name TEXT PRIMARY KEY, value JSONB NOT NULL);
CREATE TABLE control.changes (
 id BIGSERIAL PRIMARY KEY, entity TEXT NOT NULL, fingerprint TEXT NOT NULL,
 kind TEXT NOT NULL, details JSONB NOT NULL, first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
 resolved_at TIMESTAMPTZ, UNIQUE(entity,fingerprint)
);
CREATE TABLE control.event_schemas (
 fingerprint TEXT PRIMARY KEY, entity TEXT NOT NULL, schema_json JSONB NOT NULL,
 allowed BOOLEAN NOT NULL, first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE control.runs (
 id TEXT PRIMARY KEY, mode TEXT NOT NULL, status TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 finished_at TIMESTAMPTZ, details JSONB NOT NULL DEFAULT '{}'
);
