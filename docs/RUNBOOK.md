# CompScope Pipeline Runbook

## Overview

The CompScope pipeline runs automatically every **Monday at 8am EST** via GitHub Actions.
It pulls new NYC sales data, processes them, and syncs to Airtable (CompScope Beta).

---

## Normal Weekly Run (Automated)

GitHub Actions handles this. No action needed unless something fails.

Check the **Actions** tab in GitHub to see run history and logs.

---

## Manual Runs

Run from the project root (activate venv first):

```bash
# Full run — extract + transform + sync
python pipeline.py

# Skip the slow NYC Open Data pull (use existing staging tables)
python pipeline.py --skip-extract

# Transform only — no Airtable sync
python pipeline.py --skip-extract --skip-sync

# Override the cutoff date manually
python pipeline.py --since 2026-01-01

# Send Slack notification on completion
python pipeline.py --notify

# Dry-run Airtable sync (no writes)
python pipeline.py --dry-run

# Test with limited records
python pipeline.py --skip-extract --limit 10
```

---

## GitHub Actions: Manual Trigger

1. Go to GitHub → **Actions** tab
2. Click **CompScope Weekly Sync**
3. Click **Run workflow**
4. Optionally set `since` date or `skip_extract`
5. Click **Run workflow**

---

## Required GitHub Secrets

Set these in GitHub → Settings → Secrets → Actions:

| Secret | Description |
|--------|-------------|
| `DATABASE_URL` | Supabase connection string |
| `AIRTABLE_PAT` | Airtable personal access token |
| `AIRTABLE_BASE_ID` | Airtable base ID (appXXXXXX) |
| `AIRTABLE_TABLE_NAME` | `CompScope Beta` |
| `AIRTABLE_AREAS_TABLE` | `Areas` |
| `NYC_OPEN_DATA_APP_TOKEN` | NYC Open Data app token |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |

---

## Slack Notifications

Set `SLACK_WEBHOOK_URL` in your `.env` (local) and GitHub Secrets (automated).

To get a webhook URL:
1. Go to https://api.slack.com/apps
2. Create app → Incoming Webhooks → Add New Webhook
3. Choose the channel → copy the webhook URL

---

## How the Incremental Logic Works

1. Reads `etl_ingestion_runs` to find the last successful `sales_rolling` run date
2. Pulls only sales **newer than that date** (with a 3-day buffer for late filings)
3. Joins to PLUTO + ACRIS (which are fully refreshed each run)
4. Inserts only NEW BBL+date combinations into `comps_dev_base_v2`
5. Existing records are **never touched**

---

## Failure Scenarios

### Pipeline fails at extract
```bash
# Retry with existing staging data
python pipeline.py --skip-extract
```

### Pipeline fails at transform
```bash
# Check the SQL error in the logs
# Fix if needed, then retry transform + sync only
python pipeline.py --skip-extract
```

### Airtable sync fails
```bash
# Retry sync only (data is already in DB)
python pipeline.py --skip-extract --since 2026-02-01
```

### Too many records added (looks wrong)
```bash
# Check what was inserted
# In Supabase SQL editor:
SELECT COUNT(*), MIN(sale_date), MAX(sale_date)
FROM comps_dev_base_v2
WHERE sale_date >= '2026-02-01';
```

---

## Key Files

| File | Purpose |
|------|---------|
| `pipeline.py` | Main entry point — run this |
| `src/sql/incremental_update.sql` | SQL that inserts new records |
| `src/airtable/sync_airtable.py` | Airtable upsert logic |
| `src/notifications/slack.py` | Slack notification sender |
| `.github/workflows/weekly_sync.yml` | GitHub Actions schedule |

---

## Logs

- **Local runs**: stdout/stderr in terminal
- **GitHub Actions**: Actions tab → click run → view logs
- **DB run history**: `SELECT * FROM etl_ingestion_runs ORDER BY started_at DESC LIMIT 20;`

---

## Database Tables (never drop these)

| Table | Description |
|-------|-------------|
| `comps_dev_base_v2` | Final filtered dev sites — **production table** |
| `stg_sales_raw` | Raw NYC sales staging |
| `stg_pluto_raw` | Raw PLUTO staging |
| `stg_acris_*` | Raw ACRIS staging |
| `etl_ingestion_runs` | Pipeline run log |
| `zoning_far_official` | FAR lookup table |

---

## Full Rebuild (only when needed)

Only run this if you need to rebuild everything from scratch
(e.g. PLUTO major update, filter logic change, schema change).

```bash
# In Supabase SQL editor — run rebuild_pipeline.sql manually
# WARNING: This drops and rebuilds all derived tables
```

The full rebuild SQL is at: `src/sql/rebuild_pipeline.sql`
