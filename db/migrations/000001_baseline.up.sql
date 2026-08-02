CREATE TABLE delivery_authorizations (
    tenant_id uuid NOT NULL,
    delivery_authorization_id uuid NOT NULL,
    event_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    target_subscriber_id uuid NOT NULL,
    fencing_token uuid NOT NULL,
    route_version bigint NOT NULL CHECK (route_version > 0),
    delivery_deadline timestamptz NOT NULL,
    traceparent text NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    request_body bytea NOT NULL,
    receipt_id uuid NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    log_sequence bigint NOT NULL,
    issuer_service_id text NOT NULL,
    visibility text NOT NULL CHECK (visibility = 'PENDING_NOT_VISIBLE'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, delivery_authorization_id),
    UNIQUE (tenant_id, event_id),
    UNIQUE (tenant_id, receipt_id),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(delivery_authorization_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(event_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(reservation_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(target_subscriber_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(fencing_token::text FROM 15 FOR 1) = '7'),
    CHECK (substring(receipt_id::text FROM 15 FOR 1) = '7')
);

CREATE TABLE interaction_delivery_log (
    tenant_id uuid NOT NULL,
    log_sequence bigint GENERATED ALWAYS AS IDENTITY,
    interaction_id uuid NOT NULL,
    delivery_authorization_id uuid NOT NULL,
    receipt_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    authorization_body bytea NOT NULL,
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, log_sequence),
    UNIQUE (tenant_id, delivery_authorization_id),
    UNIQUE (tenant_id, receipt_id),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(delivery_authorization_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(receipt_id::text FROM 15 FOR 1) = '7')
);

CREATE TABLE delivery_ack_outbox (
    tenant_id uuid NOT NULL,
    receipt_id uuid NOT NULL,
    delivery_authorization_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    target_subscriber_id uuid NOT NULL,
    expected_route_version bigint NOT NULL CHECK (expected_route_version > 0),
    fencing_token uuid NOT NULL,
    receipt_body bytea NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'claimed', 'completed')),
    claim_token uuid,
    claim_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    result_disposition text,
    result_route_version bigint,
    result_event_id uuid,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, receipt_id),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(receipt_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(delivery_authorization_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(reservation_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(target_subscriber_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(fencing_token::text FROM 15 FOR 1) = '7'),
    CHECK (claim_token IS NULL OR substring(claim_token::text FROM 15 FOR 1) = '7'),
    CHECK (result_event_id IS NULL OR substring(result_event_id::text FROM 15 FOR 1) = '7')
);

CREATE INDEX delivery_ack_outbox_claim_idx ON delivery_ack_outbox (tenant_id, next_attempt_at, receipt_id) WHERE status IN ('pending', 'claimed');

CREATE TABLE route_projection_heads (
    tenant_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    route_version bigint NOT NULL CHECK (route_version >= 0),
    event_id uuid,
    fact_hash bytea CHECK (fact_hash IS NULL OR octet_length(fact_hash) = 32),
    visibility text NOT NULL DEFAULT 'hidden' CHECK (visibility IN ('hidden', 'visible')),
    delivery_authorization_id uuid,
    receipt_id uuid,
    visibility_generation bigint NOT NULL DEFAULT 0 CHECK (visibility_generation >= 0),
    lease_until timestamptz,
    snapshot_history_floor bigint,
    snapshot_provenance text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, interaction_id),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7'),
    CHECK (event_id IS NULL OR substring(event_id::text FROM 15 FOR 1) = '7'),
    CHECK (delivery_authorization_id IS NULL OR substring(delivery_authorization_id::text FROM 15 FOR 1) = '7'),
    CHECK (receipt_id IS NULL OR substring(receipt_id::text FROM 15 FOR 1) = '7')
);

CREATE TABLE route_fact_version_ledger (
    tenant_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    route_version bigint NOT NULL CHECK (route_version > 0),
    event_id uuid NOT NULL,
    fact_hash bytea NOT NULL CHECK (octet_length(fact_hash) = 32),
    fact_body bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, interaction_id, route_version),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(event_id::text FROM 15 FOR 1) = '7')
);

CREATE TABLE route_fact_event_ledger (
    tenant_id uuid NOT NULL,
    event_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    route_version bigint NOT NULL CHECK (route_version > 0),
    fact_hash bytea NOT NULL CHECK (octet_length(fact_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (tenant_id, interaction_id, route_version),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(event_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7')
);

CREATE TABLE projection_reconcile_intents (
    tenant_id uuid NOT NULL,
    reconcile_token uuid NOT NULL,
    interaction_id uuid NOT NULL,
    observed_version bigint NOT NULL CHECK (observed_version >= 0),
    requested_from bigint NOT NULL CHECK (requested_from > 0),
    requested_to bigint NOT NULL CHECK (requested_to >= requested_from),
    held_event_id uuid NOT NULL,
    held_fact_hash bytea NOT NULL CHECK (octet_length(held_fact_hash) = 32),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'installed', 'audited')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, reconcile_token),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(reconcile_token::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(held_event_id::text FROM 15 FOR 1) = '7')
);

CREATE UNIQUE INDEX projection_reconcile_intents_pending_idx ON projection_reconcile_intents (tenant_id, interaction_id) WHERE status = 'pending';

CREATE TABLE route_projection_dlq (
    tenant_id uuid NOT NULL,
    dlq_id uuid NOT NULL,
    reconcile_token uuid,
    interaction_id uuid NOT NULL,
    event_id uuid NOT NULL,
    route_version bigint NOT NULL CHECK (route_version > 0),
    fact_hash bytea NOT NULL CHECK (octet_length(fact_hash) = 32),
    reason text NOT NULL CHECK (reason IN ('DIVERGENT_HISTORY', 'UNKNOWN_STALE_HISTORY')),
    fact_body bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, dlq_id),
    UNIQUE (tenant_id, interaction_id, event_id, reason),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(dlq_id::text FROM 15 FOR 1) = '7'),
    CHECK (reconcile_token IS NULL OR substring(reconcile_token::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(event_id::text FROM 15 FOR 1) = '7')
);

CREATE TABLE projection_invariant_alert_outbox (
    tenant_id uuid NOT NULL,
    alert_id uuid NOT NULL,
    reconcile_token uuid NOT NULL,
    interaction_id uuid NOT NULL,
    reason text NOT NULL CHECK (reason = 'UNKNOWN_STALE_HISTORY'),
    payload bytea NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, alert_id),
    UNIQUE (tenant_id, reconcile_token),
    CHECK (substring(tenant_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(alert_id::text FROM 15 FOR 1) = '7'),
    CHECK (substring(reconcile_token::text FROM 15 FOR 1) = '7'),
    CHECK (substring(interaction_id::text FROM 15 FOR 1) = '7')
);

ALTER TABLE delivery_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE delivery_authorizations FORCE ROW LEVEL SECURITY;
ALTER TABLE interaction_delivery_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE interaction_delivery_log FORCE ROW LEVEL SECURITY;
ALTER TABLE delivery_ack_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE delivery_ack_outbox FORCE ROW LEVEL SECURITY;
ALTER TABLE route_projection_heads ENABLE ROW LEVEL SECURITY;
ALTER TABLE route_projection_heads FORCE ROW LEVEL SECURITY;
ALTER TABLE route_fact_version_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE route_fact_version_ledger FORCE ROW LEVEL SECURITY;
ALTER TABLE route_fact_event_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE route_fact_event_ledger FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_reconcile_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_reconcile_intents FORCE ROW LEVEL SECURITY;
ALTER TABLE route_projection_dlq ENABLE ROW LEVEL SECURITY;
ALTER TABLE route_projection_dlq FORCE ROW LEVEL SECURITY;
ALTER TABLE projection_invariant_alert_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_invariant_alert_outbox FORCE ROW LEVEL SECURITY;

CREATE POLICY delivery_authorizations_tenant ON delivery_authorizations USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY interaction_delivery_log_tenant ON interaction_delivery_log USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY delivery_ack_outbox_tenant ON delivery_ack_outbox USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY route_projection_heads_tenant ON route_projection_heads USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY route_fact_version_ledger_tenant ON route_fact_version_ledger USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY route_fact_event_ledger_tenant ON route_fact_event_ledger USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY projection_reconcile_intents_tenant ON projection_reconcile_intents USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY route_projection_dlq_tenant ON route_projection_dlq USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
CREATE POLICY projection_invariant_alert_outbox_tenant ON projection_invariant_alert_outbox USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
