# Phase 1 Implementation Plan: QBO + HubSpot Sync into Postgres

This document is the working plan for the first slice of `muse-backend`: getting QuickBooks Online (QBO) and HubSpot data flowing into PostgreSQL via both scheduled syncs and webhooks.

It is derived from `architecture.md` sections 4, 6, 7, 9, 10, and 16. Where this document and `architecture.md` disagree, `architecture.md` is authoritative for the contract (schemas, endpoint surface, secrets); this document is authoritative for the *order of work* and the *implementation-level details* the architecture doc deliberately leaves open.

---

## 1. Scope

Mapping this slice to the implementation-order table in `architecture.md` §16:

| Area | Architecture steps |
|---|---|
| Foundation (scaffolding, migrations with join tables + pipeline-stage seed, secrets, Celery with `task_routes`) | 1, 2, 3 |
| HubSpot: client → hourly sync + daily pipeline-stage sync → webhook (with pinned `HUBSPOT_WEBHOOK_URL`) | 5, 6, 7 |
| QBO: **CLI OAuth** (`scripts/qbo_connect.py`) → client → sync → webhook | 5a, 10, 11, 12 |
| Mapping `raw_sync` → `core` with join-table association phase | part of 6 (HubSpot) and 11 (QBO) |
| `reconcile_hubspot_deletions` safety net | 8 |

**Out of scope for this phase:**

- **Auth endpoints (magic link, JWT, `/auth/me`)** — architecture step 4. Phase 1 protects QBO OAuth via a CLI tool instead of a public admin endpoint (§5, this doc); other admin endpoints (`/v1/admin/users`, etc.) simply don't exist yet.
- **Public admin QBO OAuth endpoints** (`/v1/admin/qbo/connect`, `/v1/admin/qbo/callback`) — architecture §5.3, deferred to Phase 2 because they require JWT auth.
- Portal-facing read endpoints (`/v1/deals`, `/v1/invoices`, …) — architecture steps 9 and 13.
- OpenAPI/SDK freeze and portal SDK consumption — steps 14, 15.
- Cloud Run deployment — step 16.

Phase 1's validation surface is **not** purely DB assertions: it also covers webhook ingestion (with a forged-`Host`-header ingress test for HMAC, §9), the QBO CLI OAuth flow, worker-restart mid-sync, and duplicate/out-of-order webhook delivery. See §9 for the full acceptance list.

---

## 2. Current State of the Repo

What exists today:

- `architecture.md` — complete design document.
- `phase1.md` — this plan.
- `.cursor/.cursorrules` — per-repo rules, aligned with `architecture.md`.
- `app/main.py` — FastAPI hello-world stub.
- `requirements.txt` — populated with all Phase 1 top-level dependencies (FastAPI, SQLAlchemy, Celery + RedBeat, httpx, structlog, Sentry SDK, GCS client, cryptography, PyJWT, Resend, etc.). See the file for the curated list.
- `requirements-dev.txt` — test / lint / schemathesis / testcontainers tooling, kept out of the production Docker image.
- `Dockerfile` — present but has `ENTRYPOINT ["top", "-b"]` on the last line, which will override `CMD` and prevent the API from starting. **Still needs fixing** (see §3.2).
- `docker-composer.yml` — empty, wrong name (should be `docker-compose.yml`). Still needs renaming and populating.

Everything else from `architecture.md` §8 (project structure) is not yet created: no `app/core/`, `app/db/`, `app/api/v1/` implementations, no `alembic/`, no `app/worker/`, no `app/integrations/`, no `app/services/`, no `tests/`.

---

## 3. Foundation Work

Prerequisites before any integration code. These correspond to `architecture.md` §16 steps 1–3.

### 3.1 Dependencies

**Status:** done. `requirements.txt` at the repo root is already populated with the Phase 1 dependencies listed below (plus a handful required by later sections: RedBeat, Sentry, GCS client, cryptography, PyJWT, Resend). `requirements-dev.txt` covers test/lint tooling and is not copied into the production image. See each file for the authoritative, annotated list.

Phase 1 minimum (superset is in `requirements.txt`):

```text
sqlalchemy[asyncio]==2.0.36
alembic==1.14.0
asyncpg==0.30.0
psycopg2-binary==2.9.10        # Alembic + sync Celery engine
celery[redis]==5.4.0
celery-redbeat==2.3.2          # singleton Beat with Redis-backed schedule (architecture.md §10.1)
redis==5.2.1
structlog==24.4.0
httpx==0.28.1                  # HubSpot/QBO HTTP client
tenacity==9.0.0                # retries outside Celery
orjson==3.10.12                # fast JSON for content hashing
pydantic-settings==2.7.0
cryptography==46.0.6           # Fernet for qbo.oauth_tokens (architecture.md §4.5)
```

### 3.2 Dockerfile Fixes

- Remove `ENTRYPOINT ["top", "-b"]`.
- Ensure the `COPY requirements.txt .` line matches the renamed file.
- Per architecture §10.1, a single image supports three commands (`uvicorn …`, `celery … worker`, `celery … beat`), chosen by the Cloud Run service's command override. `CMD` stays as uvicorn.

### 3.3 `docker-compose.yml`

Rename `docker-composer.yml` → `docker-compose.yml` and populate with the services listed in architecture §10.3:

- `api` — `uvicorn app.main:app --reload`
- `worker` — `celery -A app.worker.celery_app worker -l info`
- `beat` — `celery -A app.worker.celery_app beat -l info`
- `postgres:15`
- `redis:7-alpine`

### 3.4 Config & Secrets

`app/core/config.py` — a Pydantic `BaseSettings` that declares every secret from `architecture.md` §9. The app must fail to start if any required secret is missing.

`app/integrations/gcp_secrets.py` — the `env var → in-memory cache → GCP Secret Manager` resolver. Short-circuit to `os.environ` when `GOOGLE_CLOUD_PROJECT` is unset so local `docker compose up` works without GCP credentials.

### 3.5 SQLAlchemy Models

Layout per architecture §8:

```
app/db/
  session.py          # async engine + sessionmaker + get_db dependency (FastAPI)
                      # + sync engine + session for Celery workers
  base.py             # declarative base
  models/
    __init__.py
    types.py          # Shared TypeDecorators: EncryptedToken (Fernet; architecture.md §4.5)
    auth.py           # User, MagicLink (Phase 2, but tables are created now)
    core.py           # Company, Contact, Deal, DealContact, DealCompany, PipelineStage,
                      # Invoice, Estimate, PurchaseOrder,
                      # MappingBookmark (composite PK (connector, object_type); architecture.md §4.3)
    raw_sync.py       # HubspotSyncRun, HubspotWebhookEvent, HubspotRecord,
                      # QboSyncRun, QboWebhookEvent, QboRecord
    qbo.py            # OauthToken (access_token + refresh_token columns use EncryptedToken)
```

**Every model file sets `__table_args__ = {"schema": "<its_schema>"}` on every model.** Without explicit schema tagging, Alembic's `autogenerate` compares against `public`, misses the tables, and emits drop-and-recreate migrations. Non-optional.

`core.deals.company_id`, `stage_id`, and `contact_id` are **nullable** (architecture.md §4.3) so the mapper can write orphan-safe rows when a deal arrives before its parent. Every `core.*` table also has `source_updated_at`, `last_mapping_source_id`, and `deleted_at` columns.

**`DealContact` and `DealCompany` are the authoritative "who can see this deal" source.** These are many-to-many join tables with composite PK `(deal_id, other_id, association_type)` per architecture.md §4.3. The columns `core.deals.contact_id` / `core.deals.company_id` stay on the `deals` table as a **display-only primary-contact hint** -- do not use them for authorization. The portal's deal-list query in architecture §5.2 is the canonical access-control shape; `test_deals.py` must include a case where a contact is in `core.deal_contacts` but is NOT `core.deals.contact_id` and still sees the deal.

**`PipelineStage` has both a seed and a sync source.** Architecture.md §4.3 and §6.1: stages are seeded via an Alembic data migration at first deploy (so the portal never shows an empty dropdown) AND kept fresh by the daily `sync_hubspot_pipeline_stages` Celery task. The SQLAlchemy model does not need special handling for the seed; the data migration writes rows directly via `op.bulk_insert(...)`.

**Async vs sync.** FastAPI is async (`asyncpg`), Celery is typically sync. Run two SQLAlchemy engines in `session.py` pointing at the same database:

- `async_engine` + `AsyncSessionLocal` — for FastAPI.
- `sync_engine` + `SyncSessionLocal` — for Celery tasks.

### 3.6 Alembic Initial Migration

Two migrations: one schema + tables, one data-only seed for `core.pipeline_stages`.

```
alembic/
  env.py                                # async-mode env, reads DATABASE_URL
  versions/
    001_create_schemas_and_tables.py    # DDL only: schemas, tables, indexes, constraints
    002_seed_pipeline_stages.py         # Data only: initial rows in core.pipeline_stages
  script.py.mako
alembic.ini
```

**Why split the seed into its own migration:** rolling back the schema (`alembic downgrade 001`) when we rename a stage should not also wipe the schema -- keeping data migrations separate makes downgrades tractable, and `002` can be re-run idempotently via `ON CONFLICT DO NOTHING` if the stage list ever needs an update. See also architecture.md §4.3 for the contract that `core.pipeline_stages` must never be empty when the portal renders the deal-stage dropdown.

Critical items easy to miss (DDL migration, `001`):

- `alembic/env.py` sets `include_schemas=True` and `version_table_schema='public'`. Without these, Alembic ignores the non-`public` schemas entirely.
- `CREATE EXTENSION IF NOT EXISTS pgcrypto;` for `gen_random_uuid()`.
- Unique constraint `(object_type, source_id)` on `raw_sync.hubspot_records` and `raw_sync.qbo_records` — it is the upsert key. The chronological guard from §4.4 is enforced at the SQL statement level, not as a DB constraint.
- **Composite PK** on `core.mapping_bookmarks`: `(connector, object_type)`. Per-object-type progress, not per-connector. See architecture.md §4.3. The `object_type` check constraint (if you add one) must list all of `pipeline_stage`, `company`, `contact`, `deal`, `deal_contact`, `deal_company`, `customer`, `invoice`, `estimate`, `purchaseorder`.
- **Composite PK on the join tables:** `core.deal_contacts` = `(deal_id, contact_id, association_type)`; `core.deal_companies` = `(deal_id, company_id, association_type)`. Both have `ON DELETE CASCADE` on their FKs and `deleted_at` for tombstoning -- the cascade handles hard-deletes during tests, the tombstone handles normal upstream de-association events.
- **Nullable FKs** on `core.deals`: `company_id`, `contact_id`, `stage_id`. Emit them as nullable in the Alembic column definition, not NOT NULL. Same goes for `core.invoices.deal_id` and `core.estimates.deal_id` (already nullable in architecture).
- `last_mapping_source_id UUID` (nullable) on all six `core` business tables (companies, contacts, deals, invoices, estimates, purchase_orders) **and** on both join tables. Not an FK -- `raw_sync` rows may be purged, and the referential integrity would defeat the "connector sandbox" isolation.
- Indexes from architecture §4.3 and §4.4:
  - `raw_sync.hubspot_records (object_type, source_updated_at)`
  - `raw_sync.hubspot_records (version)`
  - `GIN (payload_json)`
  - `raw_sync.hubspot_sync_runs (object_type, status, started_at)`
  - same trio on QBO.
  - Partial index on `core.deals (id) WHERE company_id IS NULL OR stage_id IS NULL OR contact_id IS NULL` -- the association phase of the mapper scans this each run.
  - `core.deal_contacts (contact_id, deleted_at) WHERE deleted_at IS NULL` -- hit on every portal deal-list request.
  - `core.deal_contacts (deal_id)` -- admin "who's on this deal?" view.
  - Same pair on `core.deal_companies`.
  - Partial index `(deleted_at) WHERE deleted_at IS NULL` on `core.companies`, `core.contacts`, `core.deals`, `core.invoices`, `core.estimates`, `core.purchase_orders`, `core.deal_contacts`, `core.deal_companies` -- portal queries always filter live rows.
- **Defer `received_at` range partitioning** on `raw_sync.*_webhook_events` — architecture §4.4 calls for monthly partitions, but partition maintenance is non-trivial. Start with a plain table; add a second migration once webhook volume justifies it.
- **Insert an initial `(connector='hubspot', object_type='pipeline_stage')` row** (and the other object types) into `core.mapping_bookmarks` with `last_version=0` inside `001` so the mapper doesn't need to create them on first run.

Seed migration (`002_seed_pipeline_stages.py`):

```python
PIPELINE_STAGES = [
    # (name, display_order, hubspot_stage_id, pipeline_name)
    ("Qualification",       10, "appointmentscheduled", "default"),
    ("Discovery Call",      20, "qualifiedtobuy",       "default"),
    ("Proposal Sent",       30, "presentationscheduled","default"),
    ("Negotiation",         40, "decisionmakerboughtin","default"),
    ("Contract Sent",       50, "contractsent",         "default"),
    ("Closed Won",          60, "closedwon",            "default"),
    ("Closed Lost",         70, "closedlost",           "default"),
]

def upgrade() -> None:
    op.execute(sa.text("""
        INSERT INTO core.pipeline_stages (id, name, display_order, pipeline_name, hubspot_stage_id)
        VALUES (gen_random_uuid(), :name, :display_order, :pipeline_name, :hubspot_stage_id)
        ON CONFLICT (hubspot_stage_id) DO NOTHING
    """), [...])
```

Values above are placeholders -- confirm with HubSpot's current pipeline-stage IDs during step 1 of the build order (§7). The daily `sync_hubspot_pipeline_stages` task (architecture.md §6.1) then keeps the table in sync if stages get renamed or reordered; the seed is only the first-boot safety net.

### 3.7 Celery App

`app/worker/celery_app.py` — wires Celery + Beat.

```python
from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

celery_app = Celery(
    "muse",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        "app.worker.tasks_hubspot_sync",
        "app.worker.tasks_qbo_sync",
        "app.worker.tasks_hubspot_webhook",
        "app.worker.tasks_qbo_webhook",
        "app.worker.tasks_mapping",
        "app.worker.tasks_reconcile",
        "app.worker.tasks_maintenance",
    ],
)

# Queue routing (architecture.md §6.6). task_routes is the primary mechanism;
# a task's per-declaration queue= kwarg (e.g. @shared_task(queue="high_priority"))
# overrides task_routes. Either form is acceptable, but unrouted tasks silently
# land on Celery's default "celery" queue, bypassing the priority pull order --
# that is the failure mode task_routes is here to prevent.
celery_app.conf.task_routes = {
    "app.worker.tasks_hubspot_webhook.*":        {"queue": "high_priority"},
    "app.worker.tasks_qbo_webhook.*":            {"queue": "high_priority"},
    "app.worker.tasks_hubspot_sync.*":           {"queue": "default"},
    "app.worker.tasks_qbo_sync.*":               {"queue": "default"},
    "app.worker.tasks_mapping.*":                {"queue": "default"},
    "app.worker.tasks_reconcile.*":              {"queue": "low_priority"},
    "app.worker.tasks_maintenance.*":            {"queue": "low_priority"},
}
celery_app.conf.task_default_queue = "default"

celery_app.conf.beat_schedule = {
    "hubspot-hourly-sync": {
        "task": "app.worker.tasks_hubspot_sync.run_hourly_hubspot_sync",
        "schedule": crontab(minute=5),
    },
    "qbo-hourly-sync": {
        "task": "app.worker.tasks_qbo_sync.run_hourly_qbo_sync",
        "schedule": crontab(minute=15),
    },
    # Daily at 02:30 UTC (architecture.md §6.1). Stages rarely change; the seed
    # migration (§3.6) covers day-0; this keeps the table fresh afterwards.
    "hubspot-pipeline-stages-daily": {
        "task": "app.worker.tasks_hubspot_sync.sync_hubspot_pipeline_stages",
        "schedule": crontab(hour=2, minute=30),
    },
    # Weekly (architecture.md §6.5)
    "hubspot-deletion-reconcile-weekly": {
        "task": "app.worker.tasks_reconcile.reconcile_hubspot_deletions",
        "schedule": crontab(hour=2, minute=0, day_of_week="sun"),
    },
}

celery_app.conf.task_default_retry_delay = 30
celery_app.conf.task_acks_late = True
celery_app.conf.worker_prefetch_multiplier = 1
```

**RedBeat in dev as well as prod.** `docker-compose.yml` runs `celery ... beat -l info -S redbeat.RedBeatScheduler` (architecture.md §10.3), not plain `celery ... beat`. The default scheduler stores state in a local `celerybeat-schedule` file inside the container -- which works locally but hides the whole class of "container restart replays all scheduled tasks" bugs that RedBeat solves. Running RedBeat locally means those bugs surface in `docker compose up`, not at first prod deploy.

Staggering HubSpot at `:05`, pipeline stages at `02:30`, and QBO at `:15` keeps them from fighting over Redis/Postgres in the same instant.

---

## 4. HubSpot

### 4.1 Integration Client — `app/integrations/hubspot.py`

Refactor of `muse-scripts`' `hsapi/hsapi_token.py`. Responsibilities:

- Build a `Bearer ${HUBSPOT_ACCESS_TOKEN}` session.
- Expose paged iterators per object type:
  - `iter_changed_contacts(since: datetime)`
  - `iter_changed_companies(since: datetime)`
  - `iter_changed_deals(since: datetime)`
  - `iter_associations(...)`
- Use CRM v3 `/search` with filter `hs_lastmodifieddate >= ?` for incremental sync, paginating via the `after` cursor.
- Respect HubSpot rate limits (~100 req / 10 s). Use Celery's `rate_limit="10/s"` plus `tenacity` retries on 429.
- Return raw dicts — normalization happens in the mapper.

### 4.2 Sync Tasks — `app/worker/tasks_hubspot_sync.py`

Per architecture §6.1 and §6.4:

```python
from celery import chain, group, shared_task
from app.worker.tasks_mapping import map_hubspot_to_core

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True,
             retry_backoff_max=600, max_retries=5, rate_limit="10/s")
def sync_hubspot_contacts(self): ...

@shared_task(...)
def sync_hubspot_companies(self): ...

@shared_task(...)
def sync_hubspot_deals(self): ...

@shared_task(...)
def sync_hubspot_associations(self, _prev=None): ...

@shared_task
def run_hourly_hubspot_sync():
    return chain(
        group(
            sync_hubspot_contacts.s(),
            sync_hubspot_companies.s(),
            sync_hubspot_deals.s(),
        ),
        sync_hubspot_associations.s(),
        map_hubspot_to_core.s(),
    )()
```

Per-task algorithm:

1. Open a new `raw_sync.hubspot_sync_runs` row with `status='running'`, `window_start = last_successful_run.window_end or (now - 2h)`, `window_end = now`.
2. Iterate the HubSpot API for that window. Persist `cursor_bookmark` after each page to enable mid-run resume.
3. For each record, upsert into `raw_sync.hubspot_records` with the chronological guard (architecture §4.4):

```sql
INSERT INTO raw_sync.hubspot_records (
    object_type, source_id, source_updated_at, payload_json,
    content_hash, version, created_at, updated_at
) VALUES (:object_type, :source_id, :source_updated_at, :payload,
          :hash, nextval('raw_sync.hubspot_records_version_seq'), now(), now())
ON CONFLICT (object_type, source_id)
DO UPDATE SET
    source_updated_at = EXCLUDED.source_updated_at,
    payload_json      = EXCLUDED.payload_json,
    content_hash      = EXCLUDED.content_hash,
    version           = EXCLUDED.version,
    updated_at        = now()
WHERE raw_sync.hubspot_records.source_updated_at < EXCLUDED.source_updated_at;
```

4. Update the sync run row with `status='completed'`, `records_synced`, `finished_at`. On final failure (Celery autoretry exhausted), write `status='failed'` with the error text.

### 4.3 Webhook Endpoint — `app/api/v1/webhooks.py`

```python
from fastapi import APIRouter, Request, HTTPException, Header
from app.core.config import settings
from app.core.security import verify_hubspot_signature, WebhookSignatureError
from app.worker.tasks_hubspot_webhook import process_hubspot_webhook

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

@router.post("/hubspot", status_code=200)
async def hubspot_webhook(
    request: Request,
    x_hubspot_signature_v3: str = Header(...),
    x_hubspot_request_timestamp: str = Header(...),
):
    body = await request.body()  # raw bytes for HMAC
    if not verify_hubspot_signature(
        method="POST",
        uri=settings.hubspot_webhook_url,   # PINNED, not str(request.url)
        body=body,
        timestamp=x_hubspot_request_timestamp,
        signature=x_hubspot_signature_v3,
    ):
        raise WebhookSignatureError("hubspot")  # → 403, P1 Sentry alert (arch §13.4)
    process_hubspot_webhook.delay(body.decode())
    return {"ok": True}
```

**Why not `str(request.url)`** (architecture.md §7.3). Behind Cloud Run's ingress, `request.url` reconstructs from the internal scheme + host (`http://0.0.0.0:8080/...`) regardless of the external URL HubSpot signed against (`https://api.muse-semi.com/...`). That causes HMAC to fail 100% of the time in production while passing 100% of the time locally. Pinning the URI against `settings.hubspot_webhook_url` (driven by the `HUBSPOT_WEBHOOK_URL` secret, architecture.md §9) removes the ingress dependency from the verification path.

Two belt-and-suspenders still apply:

1. Uvicorn is launched on Cloud Run with `--proxy-headers --forwarded-allow-ips='*'` (architecture.md §10.1) so `request.url` elsewhere in the app (logging, absolute-URL generation) still sees the external URL.
2. `WebhookSignatureError` is a custom exception that maps to 403 via a FastAPI `exception_handler` and is captured by Sentry as a known-type event for the P1 alert described in architecture §13.4.

Behaviors that must match architecture §3.2 Flow 2 and §7.3:

- Verify HMAC **before** parsing JSON. Use raw bytes.
- Return 200 immediately; Celery does the work. HubSpot retries aggressively on non-200.
- Idempotency: the Celery task writes `eventId` as the PK of `raw_sync.hubspot_webhook_events`, so duplicate delivery becomes an `ON CONFLICT DO NOTHING`.

### 4.4 Webhook Processor — `app/worker/tasks_hubspot_webhook.py`

```python
@shared_task(bind=True, autoretry_for=(Exception,),
             retry_backoff=True, max_retries=5)
def process_hubspot_webhook(self, body: str):
    events = json.loads(body)  # HubSpot sends an array
    for evt in events:
        insert_webhook_event(evt)
        fetch_and_upsert_record(evt)
    map_hubspot_to_core.delay(event_ids=[e["eventId"] for e in events])
```

HubSpot webhook payloads do not include the full object, so the processor re-fetches `GET /crm/v3/objects/{type}/{id}` and upserts that — same upsert path as the scheduled sync.

---

## 5. QBO

### 5.1 OAuth — `app/integrations/qbo_oauth.py` + `scripts/qbo_connect.py` (CLI, Phase 1)

**Phase 1 does not expose the OAuth endpoints on Cloud Run** (architecture.md §5.3 Phase-2+ note). A publicly-reachable `/v1/admin/qbo/connect` before JWT auth exists would put the Intuit redirect on an unauthenticated URL -- anyone hitting the host could hijack the flow. We use a CLI tool from a developer's workstation instead, and reuse the same `app/integrations/qbo_oauth.py` token-exchange logic that the Phase-2 HTTP endpoints will later call.

**`app/integrations/qbo_oauth.py`** (used by both the CLI in Phase 1 and `/v1/admin/qbo/*` in Phase 2):

- `build_authorize_url(state) -> str` — construct the Intuit authorize URL.
- `exchange_code_for_tokens(code, realm_id) -> TokenPair` — POST to Intuit's token endpoint, return access+refresh.
- `refresh_access_token(realm_id)` — advisory-locked refresh (code below). **This is what the hourly sync calls, not any of the connect/callback code.**
- `upsert_tokens(session, realm_id, tokens)` — write encrypted `access_token` + `refresh_token` into `qbo.oauth_tokens`. The `EncryptedToken` TypeDecorator (§3.5) does the Fernet round-trip transparently.

**`scripts/qbo_connect.py`** (architecture.md §5.3 CLI flow):

```python
# scripts/qbo_connect.py — run as: python scripts/qbo_connect.py
# Reads DATABASE_URL + QBO_* secrets from the environment, so it can write
# tokens into any environment (dev / staging / prod) by setting the right vars.
import asyncio, secrets, webbrowser
from aiohttp import web
from app.core.config import settings
from app.db.session import sync_engine, SyncSessionLocal
from app.integrations.qbo_oauth import (
    build_authorize_url, exchange_code_for_tokens, upsert_tokens,
)

CALLBACK_PORT = 8080
STATE = secrets.token_urlsafe(32)
REDIRECT_URI = f"http://localhost:{CALLBACK_PORT}/qbo/callback"

async def callback(request):
    if request.query.get("state") != STATE:
        return web.Response(status=400, text="state mismatch")
    code = request.query["code"]
    realm_id = request.query["realmId"]
    tokens = await exchange_code_for_tokens(code, realm_id, REDIRECT_URI)
    with SyncSessionLocal() as session:
        upsert_tokens(session, realm_id, tokens)
        session.commit()
    request.app["done"].set_result(None)
    return web.Response(text="OK -- tokens written. You can close this tab.")

async def main():
    app = web.Application()
    app["done"] = asyncio.get_event_loop().create_future()
    app.router.add_get("/qbo/callback", callback)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "localhost", CALLBACK_PORT).start()

    authorize_url = build_authorize_url(state=STATE, redirect_uri=REDIRECT_URI)
    print(f"Opening browser to: {authorize_url}")
    webbrowser.open(authorize_url)

    await app["done"]
    await runner.cleanup()
    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
```

**Pre-flight:** the target Intuit OAuth app must have `http://localhost:8080/qbo/callback` listed as an allowed redirect URI -- do this in the Intuit developer dashboard once per environment (sandbox + production).

**Runtime token refresh** (architecture.md §4.5 -- unchanged regardless of Phase 1 vs Phase 2):

- Before every outbound QBO call: if `access_token_expires_at` is within 5 minutes, refresh inside a Postgres advisory lock so concurrent workers don't both refresh at once:

```sql
SELECT pg_advisory_xact_lock(hashtext('qbo:refresh:' || :realm_id));
```

- Refresh tokens rotate on each use. Writing the new refresh token back is mandatory — missing this drops the connection within 100 days.
- Re-read the token row **inside** the lock before deciding to refresh: another transaction may have refreshed while you were waiting.

### 5.2 Integration Client — `app/integrations/qbo.py`

Refactor of `modules/qbo_module/`. Exposes:

- `query(realm_id, sql)` — QBO's SQL-like query endpoint.
- Paged iterators: `iter_invoices(since)`, `iter_estimates(since)`, `iter_purchase_orders(since)`, `iter_customers(since)`.
- `fetch_invoice_pdf(invoice_id)` — stub now, used later by `/v1/invoices/{id}/pdf`.
- Transparent token refresh via `qbo_oauth.py`.

Incremental filter syntax: `MetaData.LastUpdatedTime >= 'YYYY-MM-DDTHH:MM:SSZ'`.

### 5.3 Sync Tasks — `app/worker/tasks_qbo_sync.py`

Same shape as HubSpot. Tasks per architecture §6.1:

- `sync_qbo_customers`
- `sync_qbo_invoices`
- `sync_qbo_estimates`
- `sync_qbo_purchase_orders`
- `map_qbo_to_core` (downstream)

Orchestrator:

```python
@shared_task
def run_hourly_qbo_sync():
    return chain(
        group(
            sync_qbo_customers.s(),
            sync_qbo_invoices.s(),
            sync_qbo_estimates.s(),
            sync_qbo_purchase_orders.s(),
        ),
        map_qbo_to_core.s(),
    )()
```

All write into `raw_sync.qbo_records` with the same chronological upsert guard and write run metadata into `raw_sync.qbo_sync_runs`.

**Bootstrap (`query`) vs incremental (`cdc`) -- QBO-specific.** QBO exposes two different endpoints for "what changed":

- **`/cdc?entities=Invoice&changedSince=<ISO>`** -- the Change Data Capture endpoint. Returns inserts, updates, and deletes (`status='Deleted'`). **Bounded to the last 30 days**: `changedSince` older than that is silently truncated. This is the right tool for the hourly incremental sync.
- **`/query`** with `SELECT * FROM Invoice WHERE MetaData.LastUpdatedTime >= '<ISO>'` -- the SQL-like query endpoint. Unbounded history, paginated via `STARTPOSITION` / `MAXRESULTS`. Returns only live entities -- **deletes are not visible**. This is the right tool for the first-ever sync.

Each `sync_qbo_*` task must choose between the two at runtime:

```python
last_successful_run = (
    db.query(QboSyncRun)
      .filter_by(object_type=object_type, status="completed")
      .order_by(QboSyncRun.window_end.desc())
      .first()
)

if last_successful_run is None:
    # Bootstrap: paginate /query from epoch
    iterator = qbo_client.query(f"SELECT * FROM {entity} ORDER BY Id STARTPOSITION {{pos}} MAXRESULTS 1000")
    window_start = None
elif last_successful_run.window_end < datetime.now(timezone.utc) - timedelta(days=29):
    # Gap too wide for CDC -- fall back to /query to avoid missing rows
    iterator = qbo_client.query(...)
    window_start = last_successful_run.window_end
else:
    # Normal hourly path
    iterator = qbo_client.cdc(entity, changed_since=last_successful_run.window_end)
    window_start = last_successful_run.window_end
```

The 30-day guard matters more than it sounds: if sync is paused for a month (outage, holiday freeze, extended Intuit sandbox issue), the next hourly run must NOT silently start at "now minus 30 days" and leak 30 days of changes. Fall back to `/query` and accept one slow bootstrap over silent data loss.

Deletions during the bootstrap are not a concern because there is nothing in `core` yet to go stale. After bootstrap completes, CDC's `status='Deleted'` entries handle tombstoning going forward (architecture.md §4.3).

### 5.4 QBO Webhook — `POST /v1/webhooks/qbo`

Signature: `base64(HMAC-SHA256(QBO_WEBHOOK_VERIFIER_TOKEN, raw_body))`, compared to `intuit-signature` header. Use `hmac.compare_digest`. Fail closed.

Payload is a batch of `entities` per `realmId`. Processor (`process_qbo_webhook`):

1. Insert into `raw_sync.qbo_webhook_events`. QBO doesn't provide a single event id; synthesize `f"{realmId}:{entityName}:{entityId}:{lastUpdated}"` as the PK.
2. For each entity, call the QBO API to fetch current state, upsert into `raw_sync.qbo_records`.
3. `map_qbo_to_core.delay(record_ids=...)` for the changed records.

---

## 6. Mapping `raw_sync` → `core` — `app/worker/tasks_mapping.py`

This is what actually populates the tables the Portal will read. Two tasks, each run as a sequence of per-object-type phases:

### 6.1 `map_hubspot_to_core(since=None, record_ids=None)`

Process object types in dependency order within the same task call:

1. **`pipeline_stage`** → upsert `core.pipeline_stages` on `hubspot_stage_id`. Seed data (§3.6) covers day-0; this phase keeps the table fresh when the daily `sync_hubspot_pipeline_stages` task lands new rows. Runs first so the `deal` phase below can always resolve `stage_id`.
2. **`company`** → upsert `core.companies` on `hubspot_id`. No FK dependencies.
3. **`contact`** → upsert `core.contacts`. Resolve `company_id` via the contact's HubSpot company association; write `NULL` if the parent isn't in `core.companies` yet.
4. **`deal`** → upsert `core.deals`. Resolve `company_id`, `contact_id`, `stage_id` via `hubspot_id` lookup; write `NULL` for any FK whose parent is missing (the column is nullable per architecture §4.3). `contact_id` here is just the **primary contact hint** for display purposes.
5. **`deal_contact`** (part of the association phase) → for each `(deal, contact, association_type)` triple in HubSpot's associations API:
   - If both sides exist in `core`, upsert into `core.deal_contacts` with `deleted_at=NULL`, `source_updated_at = now()`.
   - If either side is missing, skip; the next mapper run retries once its own `deal` / `contact` phase has caught up.
   - For each local `core.deal_contacts` row whose `(deal, contact, association_type)` is NOT present in the fetched association set for that deal, set `deleted_at = now()` (tombstone). This is how removed associations propagate -- HubSpot has no dedicated "association deleted" webhook payload, so every association fetch is a diff-and-tombstone.
6. **`deal_company`** → mirror of `deal_contact` above, writing to `core.deal_companies`.

Unmapped/custom properties go into `properties_json`.

Rewiring `core.deals.company_id` / `stage_id` / `contact_id` (the primary-hint FKs) when the parent row was missing on a prior run happens **inside** the `deal` phase's lookup on every pass -- those nullable FKs are not a separate phase. See §6.3 for why the orphan-safe bookmark advancement makes this work without `needs_association_sync`-style flags.

### 6.2 `map_qbo_to_core(record_ids=None)`

Same dependency-ordered pattern:

1. **`customer`** → upsert `core.companies.qbo_customer_id` (match by `qbo_customer_id`, fall back to name/domain).
2. **`invoice`** → upsert `core.invoices` on `qbo_invoice_id`; `company_id` resolves from the just-mapped customer.
3. **`estimate`** → upsert `core.estimates` on `qbo_estimate_id`.
4. **`purchaseorder`** → upsert `core.purchase_orders` on `qbo_po_id`.

### 6.3 Per-Object-Type Bookmarks (orphan-safe advancement)

`core.mapping_bookmarks` has a composite PK `(connector, object_type)` — one row per object type per connector, not one row per connector (architecture §4.3). Bookmarks exist for `pipeline_stage`, `company`, `contact`, `deal`, `deal_contact`, `deal_company` (HubSpot) and `customer`, `invoice`, `estimate`, `purchaseorder` (QBO).

**Algorithm per phase:**

```python
def map_phase(connector: str, object_type: str):
    bookmark = get_bookmark(connector, object_type)  # last_version, default 0
    rows = fetch_raw_rows_above_version(connector, object_type, bookmark.last_version)

    max_successfully_mapped_version = bookmark.last_version
    for row in rows:  # ordered by version ASC
        try:
            upsert_core_row(row)           # writes NULLs for unresolved FKs
            update_last_mapping_source_id(row.id)
            max_successfully_mapped_version = row.version
        except Exception:
            # Hard failure (not just a missing FK -- that is expected). Stop advancing
            # to preserve replay-from-this-point semantics.
            log_and_reraise_for_celery_retry()

    # Critical: only advance the bookmark on success. Missing FKs are NOT a failure --
    # we wrote NULLs, the row is in core, and the *next* mapper run will rediscover
    # and update it once the parent arrives.
    if max_successfully_mapped_version > bookmark.last_version:
        set_bookmark(connector, object_type, max_successfully_mapped_version)
```

**What this buys us (and what Gemini's original "`needs_association_sync` boolean" suggestion was trying to solve):**

- A `deal` whose `company_id` can't be resolved does **not** freeze the `deal` bookmark. It lands in `core.deals` with `company_id = NULL` and `version` is stored as the new bookmark. Re-resolution of the nullable primary-hint FKs happens **inside** the `deal` phase on the next run (a lookup on every upsert, not a flag), because that run's `company` / `contact` phases will have caught up.
- The `deal_contact` / `deal_company` phases (§6.1 steps 5-6) are skip-and-continue by design: an association whose parent is missing in `core` is simply not written and not tombstoned; it reappears on the next run once the `deal` or `contact` phase lands the missing parent. The mapper never tombstones associations that are absent because *we* haven't mapped them yet; it only tombstones associations HubSpot itself stopped returning.
- The partial index `core.deals WHERE company_id IS NULL OR stage_id IS NULL OR contact_id IS NULL` (architecture §4.3) keeps the per-run scan cheap even as the deal table grows.
- No `needs_association_sync` boolean or `pending_associations JSONB` column is needed. The FK's nullability + the partial index + the per-object-type bookmark together are the state machine.

### 6.4 Rules That Apply to Both Mappers

1. **Phase 2 authority boundary (architecture §11 Phase 2).** Even in Phase 1, structure the mapper so individual columns can be declared "Portal-owned." Use a module-level dict:

```python
HUBSPOT_OWNED_FIELDS = {
    "deals": {"title", "amount", "stage_id", "close_date", ...},
    "companies": {"name", "domain"},
    "contacts": {"first_name", "last_name", "email", "phone"},
}
```

The mapper only writes those columns. When Phase 2 starts you tune the set, not the code.

2. **Chronological guard in `core` too.** `core.*` tables have `source_updated_at` (architecture §4.3). Apply `WHERE old.source_updated_at < EXCLUDED.source_updated_at` in every upsert so a late-arriving webhook can't trample a newer hourly-sync row.

3. **Write the mapping audit trail.** On every successful upsert into `core.*`, set `last_mapping_source_id = <raw_sync row id that drove this write>` (architecture §4.3). When Phase 2 debugging inevitably asks "why does this core row look like this?", the audit trail points directly at the `raw_sync` row (and its `payload_json`) responsible.

---

## 7. Build Order (Vertical Slices)

Rather than build HubSpot fully then QBO fully, interleave for faster feedback:

1. **Foundation** (§3): deps (done), Dockerfile fix, `docker-compose.yml` mirroring prod (RedBeat scheduler + `-Q high_priority,default,low_priority` on the worker, architecture.md §10.3), `config.py` (including `HUBSPOT_WEBHOOK_URL` + `PORTAL_ORIGIN`), `gcp_secrets.py`, `db/models/types.py` (EncryptedToken decorator), `db/session.py`, `db/models/*` (incl. `DealContact`, `DealCompany`, `PipelineStage`), initial Alembic migration + data migration seeding `core.pipeline_stages` (§3.6), `celery_app.py` with `task_routes` (§3.7). Verify end-to-end with a trivial `ping` task that returns `now()` from the DB.
2. **HubSpot vertical slice #1:** integration client + `sync_hubspot_companies` (on the `default` queue) + raw-table upsert + one beat schedule entry. Confirm `raw_sync.hubspot_records` fills up.
3. **QBO OAuth via CLI** (architecture.md §5.3, §16 step 5a): `app/integrations/qbo_oauth.py` + `scripts/qbo_connect.py` + `qbo.oauth_tokens` with Fernet-encrypted token columns + refresh-with-advisory-lock. Run the CLI against the Intuit sandbox before any QBO sync code, so redirect-URI / sandbox-credential quirks surface in week 1. **Do not add public HTTP endpoints for connect/callback in Phase 1** -- those land in Phase 2 with the JWT middleware.
4. **Pipeline stages end-to-end:** `sync_hubspot_pipeline_stages` task + `pipeline_stage` phase in the mapper + the `(hubspot, pipeline_stage)` bookmark. Confirm renaming a stage in HubSpot sandbox propagates into `core.pipeline_stages` within 24 h and that an already-seeded row gets `hubspot_stage_id` populated on the first task run.
5. **Remaining HubSpot sync tasks** (contacts, deals, associations) + the `chain(group(...), associations, mapping)` orchestrator with dependency-ordered phases (§6.1, §6.3). Associations must write `core.deal_contacts` / `core.deal_companies`, not just rewire the primary-hint FKs on `core.deals`.
6. **HubSpot webhook endpoint** + HMAC verification using `settings.hubspot_webhook_url` (§4.3) + `process_hubspot_webhook` task on the `high_priority` queue (reuses the same upsert + mapper as the hourly sync). Include the ingress-realistic test (forged `Host` + `X-Forwarded-Host`, §9) from day 1.
7. **QBO sync tasks** (mirror of the HubSpot shape; bootstrap via `/query`, incremental via `/cdc` per §5.3).
8. **QBO webhook** + processor task on the `high_priority` queue; includes PDF-cache invalidation hooks (no-op in Phase 1 since no PDF cache table writes until Phase 2 portal endpoints exist, but the invalidation columns are already in the schema).
9. **Mapping workers** for `core.*` tables (can begin as early as step 2 for companies). Use per-object-type bookmarks (§6.3); write `last_mapping_source_id` on every successful upsert.
10. **`/v1/sync/status`** — trivial read of `raw_sync.*_sync_runs`. Useful debugging aid while Phase 1 is live. Does not require auth (per §1 scope); gate it behind JWT in Phase 2 when the middleware lands.

---

## 8. Decisions to Lock in Early

Not called out in `architecture.md` but come up immediately in code:

- **Async DB vs sync DB in Celery.** Run two SQLAlchemy engines in `session.py` (async for FastAPI, sync for Celery). Same database, different engines. Simpler than `asyncio.run(...)` inside tasks.
- **Idempotency key for QBO webhooks.** `f"{realmId}:{entityName}:{entityId}:{lastUpdated}"`. QBO doesn't give you a single event id.
- **Bootstrap vs incremental.** The first run of each sync backfills everything. For HubSpot, check for an absent prior `sync_run`; if none, set `window_start = None` and page through all records. For QBO, use `/query` on bootstrap and `/cdc` on the normal incremental path -- and fall back to `/query` if the last successful run is more than 29 days old. See §5.3 for the decision rule.
- **Queue routing.** Primary mechanism is `celery_app.conf.task_routes` in `celery_app.py` (§3.7); per-declaration `@shared_task(queue=...)` is still acceptable and overrides `task_routes`. Webhooks -> `high_priority`, hourly syncs + batch mapping -> `default`, weekly/monthly reconciles and cleanup -> `low_priority` (architecture.md §6.6). An unrouted task goes to Celery's default `celery` queue and bypasses the priority pull order.
- **QBO token encryption.** Tokens in `qbo.oauth_tokens` are app-level encrypted via Fernet at the SQLAlchemy `TypeDecorator` boundary (architecture.md §4.5). Business logic sees plaintext; no `.encrypt()` / `.decrypt()` calls outside `app/db/models/types.py`. `QBO_TOKEN_ENCRYPTION_KEY` must be set before the first OAuth callback, or writes will fail.
- **QBO OAuth in Phase 1 is CLI-only** (architecture.md §5.3, §16). `scripts/qbo_connect.py` on a developer's workstation does the OAuth dance; the public `/v1/admin/qbo/connect` + `/callback` endpoints are Phase 2. No JWT yet, so no public OAuth.
- **Join tables are the association source of truth.** `core.deal_contacts` and `core.deal_companies` drive deal visibility (architecture.md §5.2 canonical query, §7.4). `core.deals.contact_id` is a display-only hint for the primary contact; never use it for authorization.
- **Pipeline stages = seed + sync.** Static seed at migration time (§3.6), daily sync afterwards (§3.7). Portal must never render an empty stage dropdown, even before the first `sync_hubspot_pipeline_stages` has run.
- **Cloud SQL tier at launch is `db-custom-4-15360`** (architecture.md §10.2, §10.4). The ~200-connection cap accommodates the 141-connection peak with ~30% headroom; PgBouncer is deferred. Revisit trigger lives in architecture.md §10.4.
- **JWT session is backend-sole authority** (architecture.md §7.2). `AUTH_SECRET` never ships to Vercel; portal middleware calls `GET /v1/auth/me` instead of decoding the cookie locally. This is relevant to Phase 1 only insofar as it constrains how `auth.py` and `/v1/auth/*` are shaped when they land in Phase 2 -- **don't pre-build a shared-secret JWT flow in Phase 1 that would have to be ripped out for Phase 2.**
- **HubSpot webhook URL is pinned** (architecture.md §7.3). `settings.hubspot_webhook_url` drives HMAC input; never `str(request.url)`. Add an ingress test (§9) with forged `Host` / `X-Forwarded-Host` headers so this regression can't ship silently.
- **Secrets in local dev.** `gcp_secrets.py` should short-circuit to `os.environ` when `GOOGLE_CLOUD_PROJECT` is unset.
- **Content hashing.** Use `orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)` then SHA-256 — deterministic regardless of key order.
- **Timezone discipline.** Every `TIMESTAMPTZ` column and every Python `datetime` is UTC. Force it at the edge (HubSpot/QBO API response parsing).

---

## 9. How to Know Phase 1 Is Done

Acceptance checks, in rough order. DB-state checks are table-stakes; the failure-mode checks below them are what prove Phase 1 is actually safe to run unattended.

**Schema + bring-up:**

- [ ] `alembic upgrade head` against a fresh Postgres creates all four schemas and every table in architecture §4, **including** `core.mapping_bookmarks` with a composite PK `(connector, object_type)`, `core.deal_contacts` + `core.deal_companies` with composite PKs and partial `deleted_at` indexes, nullable `core.deals.company_id` / `stage_id` / `contact_id`, and `last_mapping_source_id` columns on all six `core` business tables plus both join tables.
- [ ] After `alembic upgrade head`, `SELECT COUNT(*) FROM core.pipeline_stages` > 0 -- the seed migration (§3.6) has run. The portal's future stage dropdown always has rows.
- [ ] `docker compose up` starts API + worker + beat + Postgres + Redis cleanly. Worker logs `Celery with queues: ['high_priority', 'default', 'low_priority']`. Beat logs `RedBeatScheduler` (not `PersistentScheduler`). `/v1/health` returns 200.

**Scheduled sync:**

- [ ] Celery Beat fires `run_hourly_hubspot_sync` and `run_hourly_qbo_sync`; rows appear in `raw_sync.hubspot_sync_runs` and `raw_sync.qbo_sync_runs` with `status='completed'`.
- [ ] Celery Beat fires `sync_hubspot_pipeline_stages` daily; renaming a stage in the HubSpot sandbox is reflected in `core.pipeline_stages.name` within 24 h.
- [ ] After a sync, `raw_sync.hubspot_records` and `raw_sync.qbo_records` contain rows whose `payload_json` matches the source API.
- [ ] Re-running a sync does not create duplicate rows (upsert + chronological guard work).
- [ ] A first-ever QBO sync uses `/query` to bootstrap (observable in `raw_sync.qbo_sync_runs.cursor_bookmark`), and subsequent runs use `/cdc`; pausing sync for >29 days and resuming triggers a `/query` fallback (§5.3).

**Webhook ingestion:**

- [ ] Posting a signed HubSpot webhook to `/v1/webhooks/hubspot` returns 200, rows appear in `raw_sync.hubspot_webhook_events`, and the corresponding `raw_sync.hubspot_records` row is updated. The task is picked up by a worker consuming `high_priority`.
- [ ] Posting a signed QBO webhook to `/v1/webhooks/qbo` behaves the same way.
- [ ] Posting an **unsigned** or **mis-signed** webhook returns 403 and writes nothing.
- [ ] **Ingress-realistic HMAC test (forged `Host` + `X-Forwarded-Host`).** Send a legitimate HubSpot-signed webhook (signed against `https://api.muse-semi.com/v1/webhooks/hubspot`) with `Host: attacker.example` and `X-Forwarded-Host: other.example` headers. The endpoint must return **200** (verification succeeded, URI was pulled from `settings.hubspot_webhook_url`, not from the forged request), **not** 403. Conversely, a webhook signed against `https://evil.example/webhooks/hubspot` must return 403 even if the attacker sets `X-Forwarded-Host: api.muse-semi.com`. This catches the whole class of `str(request.url)` regressions that only show up behind ingress.
- [ ] **Duplicate webhook delivery is idempotent.** POSTing the same HubSpot event twice produces exactly one row in `raw_sync.hubspot_webhook_events` (PK conflict → `DO NOTHING`) and exactly one Celery task attempt's effect on `core.*` (chronological guard → the second write's `source_updated_at` is not strictly greater, so the update is rejected).
- [ ] **Out-of-order webhook delivery is handled.** POSTing an update webhook for a deal *before* its create webhook results in a `core.deals` row with nullable FKs populated on the first mapper run after the create; the chronological guard ensures the later "older" create doesn't trample the newer update.

**QBO OAuth (CLI) + token storage:**

- [ ] `python scripts/qbo_connect.py` against the sandbox populates `qbo.oauth_tokens`; inspecting the raw Postgres bytes shows `access_token` and `refresh_token` are **encrypted ciphertext**, not plaintext. Reading the same rows via SQLAlchemy returns plaintext strings.
- [ ] Forcing an expired access token triggers a refresh under an advisory lock with no duplicate refresh calls, and the new refresh token is written back in the same transaction.
- [ ] **QBO OAuth callback retry / resume.** Killing `scripts/qbo_connect.py` between Intuit's redirect and the token-exchange POST leaves `qbo.oauth_tokens` untouched; re-running the script completes successfully (no half-state).

**Mapping correctness:**

- [ ] Mapping tasks produce rows in `core.companies`, `core.contacts`, `core.deals`, `core.deal_contacts`, `core.deal_companies`, `core.invoices`, `core.estimates`, `core.purchase_orders` with `last_mapping_source_id` pointing at the `raw_sync` row that drove the write.
- [ ] A contact that is associated with a deal via `core.deal_contacts` but is NOT `core.deals.contact_id` (the primary-hint column) still shows the deal via the §5.2 canonical query. (Proof that join-table access control doesn't silently fall back to the primary-hint column.)
- [ ] Removing an association in HubSpot propagates as a `deleted_at IS NOT NULL` tombstone on the corresponding `core.deal_contacts` row within one mapper cycle.
- [ ] Feeding the mapper a `deal` whose `company_id` has no matching `core.companies` row results in a `core.deals` row with `company_id IS NULL` (not an FK-constraint error). The `deal` bookmark still advances; the next mapper run's `deal` phase back-fills the FK once the company arrives.
- [ ] `core.mapping_bookmarks` has independent rows for `(hubspot, pipeline_stage)`, `(hubspot, company)`, `(hubspot, contact)`, `(hubspot, deal)`, `(hubspot, deal_contact)`, `(hubspot, deal_company)`, and each QBO object type. Each advances independently when its phase succeeds.
- [ ] A late-arriving webhook with an older `source_updated_at` does NOT overwrite a newer `core` row (chronological guard honored end-to-end).

**Worker resilience:**

- [ ] **Worker restart mid-sync.** Killing `muse-backend-worker` partway through a HubSpot sync leaves a `raw_sync.hubspot_sync_runs` row with `status='running'`, but on the next Beat fire, `cursor_bookmark` is used to resume -- records already persisted are not re-fetched.
- [ ] **Beat restart doesn't double-fire.** Restart the Beat container (or kill + recreate in docker-compose); scheduled tasks don't fire twice because RedBeat's distributed lock on the scheduler prevents concurrent scheduling even across a restart window (architecture.md §10.1).
