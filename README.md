# GTM Campaign Dashboard (DEPRECATED)

> 🛑 **This repository is deprecated and archived as of 2026-04-30.**

The Streamlit-based GTM Campaign Dashboard has been retired. All functionality
has been migrated to a new Next.js + FastAPI implementation:

**👉 New repository:** https://github.com/JennyZhouJR/Jobright-Campaign-Dashboard

The new dashboard runs on Railway with the same Google Sheet as the source of
truth. Both the per-post views Apify cron (`sync_views.yml`) and the Gmail
auto-follow-up cron (`auto_followup.yml`) now live in that repo.

## Why deprecated

- Streamlit's UI primitives didn't scale once the team needed multi-tab
  navigation, inline-editable tables, and auth-gated Gmail send flows.
- Railway blocks outbound SMTP, so we standardized on the Gmail API for
  interactive sends in the new dashboard. The auto-follow-up cron stays on
  GitHub Actions (which doesn't block SMTP) — but it now lives in the new repo
  with one extra POC password (`GMAIL_FALIDA_PASSWORD`) that this old workflow
  never carried.
- Cleaner type-safety, faster iteration, and a single deploy surface.

## What was migrated

| Old (this repo) | New (Jobright-Campaign-Dashboard) |
|---|---|
| `dashboard.py` (Streamlit) | `frontend/` (Next.js 16 + Tailwind 4) |
| `dashboard_utils/*` | `backend/dashboard_utils/*` (FastAPI) |
| `auto_followup.py` + cron | `backend/auto_followup.py` + same cron |
| `sync_views.py` + cron | `backend/sync_views.py` + same cron |

## Final state

- Final release tagged: `v1.0.0-final`
- Both GitHub Actions workflows disabled 2026-04-25, deleted 2026-04-30
- Streamlit Cloud deployment retired 2026-04-30

## If you need to roll back

In the unlikely event the new dashboard is unavailable and the team needs the
old Streamlit version back temporarily:

1. Un-archive this repo via GitHub Settings → Archives → Unarchive
2. Restore the workflow files from history: `git show v1.0.0-final:.github/workflows/auto_followup.yml > .github/workflows/auto_followup.yml`
3. Run `streamlit run dashboard.py` locally, or redeploy to Streamlit Cloud
4. Re-archive when no longer needed

For active development, only the new repo accepts changes.
