# muse-backend

Centralized Python backend for the Muse Semiconductor operations platform.

This service replaces all server-side logic currently in the Next.js Portal (`muse-portal`) and consolidates the legacy Python automation scripts (`muse-scripts`). It is the **sole system** responsible for:

- **Authentication:** Passwordless magic-link login via Resend, JWT session management.
- **HubSpot Sync:** Hourly polling + webhook ingestion of contacts, companies, deals, and associations into PostgreSQL. (Temporary -- HubSpot will be decommissioned in phases.)
- **QBO Sync:** Hourly polling + webhook ingestion of invoices, estimates, purchase orders, and customers into PostgreSQL. Manages the full QBO OAuth lifecycle. (Permanent.)
- **Portal API:** Serves deal, invoice, estimate, and company data to the Next.js frontend.
- **Background Jobs:** Celery workers handle sync, webhook processing, and scheduled maintenance.

## Architecture

See [`src/architecture.md`](src/architecture.md) for the full architecture document, including:

- System component diagram and data flows
- Complete PostgreSQL schema design (4 schemas, 15+ tables)
- API surface (auth, portal data, admin, webhooks)
- Celery task design and scheduling
- 3-phase HubSpot decommissioning plan
- Migration path for legacy Python scripts
- Deployment, observability, and security details

## Tech Stack

- **Python 3.12+** / **FastAPI** / **Celery** + **Redis** / **SQLAlchemy 2.0** / **Alembic** / **Pydantic v2**
- **PostgreSQL 15+** on GCP Cloud SQL
- **GCP:** Cloud Run, Memorystore (Redis), Secret Manager, Artifact Registry

## Local Development

```bash
docker compose up -d
alembic upgrade head
uvicorn app.main:app --reload --port 8080
```
