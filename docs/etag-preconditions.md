# ETag / If-Match: configurable schema-contract preconditions

Two related but different ideas are implemented:

1. **Control plane, real HTTP optimistic concurrency:** GET `/contract` returns a strong ETag for the accepted source contract plus reader mapping (including units/divisor and monotonic version). PUT `/contract` requires `If-Match`; stale reviewers get 412, and no update is applied. Missing validator is 428. Matching `If-None-Match` on GET returns 304.
2. **Data plane, analogous exact schema check:** an observed writer schema is fingerprinted and compared with the accepted writer schema. An additive compatible field changes the token anyway. The configurable `etag` mode can hold Gold, only alert, or ignore this exact-equality check. Raw Bronze continues to preserve events.

These tokens have distinct purposes. The contract ETag protects a control-resource update from another reviewer; the writer-schema ETag identifies an event shape. Neither proves compatibility or validates data values. Existing compatibility checks/data tests still run. A stale token must trigger fetch/diff/review, not blindly overwrite or automatically approve a changed contract.

## Configure without rebuilding

```bash
./bin/dev mode etag
./bin/dev etag-config --policy hold --require-if-match true
./bin/dev etag-config --policy warn --require-if-match true
./bin/dev etag-config --policy off --require-if-match false
./bin/dev mode hybrid
```

| Configuration | Behavior |
|---|---|
| mode `hybrid` | Existing compatible-reader behavior; exact equality gate inactive |
| mode `strict` | Existing sink hold for breaking changes; exact equality gate inactive |
| mode `etag`, policy `hold` | Even compatible source-schema drift holds the next Gold build; Bronze continues |
| mode `etag`, policy `warn` | Exact drift alerts; compatible projection continues |
| mode `etag`, policy `off` | Exact drift check disabled; ordinary compatibility/data gates remain |
| `require_if_match=true` | In etag mode, legacy `/approve` also requires If-Match |
| `require_if_match=false` | Legacy `/approve` can be unconditional; PUT `/contract` remains explicitly conditional |

Settings are persisted and can be inspected at `/config/preconditions` and `/state`. Updates are serialized with builds/approval in the single lab service process.

For first initialization, Compose accepts `.env` variables:

```dotenv
SCHEMA_GUARD_MODE=etag
ETAG_SCHEMA_POLICY=hold
ETAG_REQUIRE_IF_MATCH=true
```

See `.env.example`. Existing persisted settings win on restart; changing `.env` does not overwrite an already-reviewed runtime configuration. Use the configuration API/CLI to change an existing installation.

## Complete automatic demo (fresh stack)

```bash
./bin/dev up
python3 bin/etag-demo.py
```

The script validates: baseline matching schema, cached GET 304, additive reader-compatible change, raw preservation while exact-mode publication is held, missing-token 428, conditional approval, stale second-reviewer 412, and resumed Gold build. It then switches to warn mode, adds another optional field and verifies that compatible publication continues; finally it turns the exact gate off. The full Docker demo has not been executed in the authoring environment; HTTP tests with mocked storage and pure precondition tests cover the new API/decision logic.

## Manual two-reviewer example

```bash
curl -i http://localhost:8090/contract
# Copy the quoted ETag, including the double quotes:
curl -i -X PUT http://localhost:8090/contract \
  -H 'Content-Type: application/json' \
  -H 'If-Match: "REPLACE_WITH_64_HEX_DIGITS"' \
  -d '{"country":true}'
```

After reviewer A succeeds, reviewer B's original token receives 412, even if B's proposed mapping would be technically valid. GET the current resource, examine the schema/mapping diff and make a fresh explicit decision.

All approval writers lock the same PostgreSQL control-setting rows (`SELECT ... FOR UPDATE`) and check/write inside the transaction. This avoids a separate compare-then-write race. Existing `/approve` also uses these locks. A global process lock coordinates with builds. This does not synchronize external source DDL or provide a distributed build lease; those need deployment coordination.

The demo accepts one quoted strong SHA-256 token. Weak tags, wildcard and comma-separated lists are rejected with 400. This is a deliberately restricted API, not a complete generic implementation of the HTTP header grammar. ETags are concurrency/version validators, not authentication or authorization.

## Event-schema analogue

GET `/event-schemas` lists observed fingerprints and reader compatibility. GET `/event-precondition/<schema_id>` shows:

```json
{
  "matched": false,
  "reader_compatible": true,
  "expected_schema_etag": "...",
  "observed_schema_etag": "..."
}
```

This is the interesting additive-change case: “I can still read your event” and “this is the version I reviewed” are different statements.

For PostgreSQL bigint/text/timestamptz the demo maps catalog types to Connect int64/string/string and compares names/types/nullability. Primary-key drift is handled by the separate catalog contract. More complex PostgreSQL logical types need explicit normalization.

The runtime exact gate is evaluated at a **build boundary** against current catalog state; the event endpoint is an explanatory/diagnostic comparison. It does not reject every historical message against the latest fingerprint: old versions must remain replayable with appropriate readers. The raw sink has its existing bounded compatibility cache. No HTTP request is sent per CDC record. To implement per-event optimistic contract checking in production, carry a schema ID/fingerprint and consult a cached immutable per-version reader registry.

## Grafana

`schema_etag_precondition_failed` reports mismatch in hold/warn policies. Existing `schema_publication_blocked` includes hold-mode equality failures. A separate provisioned alert covers warn mode too. Policy off clears this equality signal while leaving ordinary schema-change alerts enabled.

## Interview explanation

> I separated schema compatibility from schema preconditions. A consumer might be able to read a changed event, but it may not have reviewed that exact contract. I used version fingerprints for cheap identification, an ETag/If-Match API to prevent stale contract approvals, and configurable hold-or-alert behavior for drift. Raw capture continues, while compatibility validation, business tests and explicit replay determine whether historical data can safely be published.

Call this **optimistic concurrency for data-contract approval** and **version preconditions for schema drift**. It is an application of established ETags/versioned contracts, not a claim that hashing schemas is a newly invented algorithm. Discuss stale updates, atomic compare-and-set, the difference between equality and compatibility, cache invalidation, historical versions, rollout ownership and backfill costs.

HTTP semantics reference: https://www.rfc-editor.org/rfc/rfc9110.html#name-if-match
