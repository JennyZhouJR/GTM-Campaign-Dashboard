# GTM Influencer Sourcing Pipeline

Instagram influencer sourcing pipeline for the Jobright GTM team. Processes
NanoInf CSV/XLSX exports through automated filtering, Apify enrichment,
deduplication, and writes qualified accounts to a shared Google Sheet.

> **Note:** This repository previously also hosted the Streamlit Campaign
> Dashboard. That dashboard was migrated to
> [`JennyZhouJR/Jobright-Campaign-Dashboard`](https://github.com/JennyZhouJR/Jobright-Campaign-Dashboard)
> (Next.js + FastAPI on Railway) and retired here on 2026-04-30. The last
> commit that contained dashboard code is tagged `v1.0.0-final`.

## What's in this repo

| File / folder | Purpose |
|---|---|
| `influencer_pipeline.py` | Main script — reads inputs, filters by rules, scrapes via Apify, writes to Sheet |
| `rules.txt` | Filter rules document (V1.2). Keyword exclusions, follower ranges, ER thresholds, etc. Read by the script and humans. |
| `.claude/skills/influencer-pipeline/SKILL.md` | Claude Code skill definition — gives Claude context to run the pipeline on demand |
| `requirements.txt` | Python dependencies |
| `dao/` (gitignored) | Google service account credentials |
| `input_csvs/` (gitignored) | Drop new NanoInf exports here |
| `output/` (gitignored) | Per-run CSV backups of qualified rows |
| `archived/` (gitignored) | Processed input files (auto-moved by the script) |
| `.env` (gitignored) | Apify API token (`APIFY_API_TOKEN=...`) |

## Running the pipeline

```bash
# Install dependencies (one-time)
pip install -r requirements.txt

# Drop new exports into input_csvs/ then:
python influencer_pipeline.py
```

Or via Claude Code:

> "跑一下 pipeline" / "run the pipeline" — the `influencer-pipeline` skill
> handles invocation, log monitoring, and result summary.

## Outputs

- **Google Sheet** — qualified rows appended to the team's shared sourcing tab
- **`output/output_<input>_<timestamp>.csv`** — local backup of each run

## Filter rules at a glance

See `rules.txt` for the canonical list. Summary:

- Follower range (configurable, currently nano/micro tiers)
- Country whitelist (US, CA)
- Language detection on bio + recent captions
- Keyword exclusions (industries, account types — see rules.txt §3)
- Bio role exclusion (founder / CEO / etc. checked against Apify bio, not just CSV fields)
- Engagement rate threshold against follower-bucket baseline

## Apify dependency

The pipeline scrapes live profile + recent posts via the
[`apify/instagram-scraper`](https://apify.com/apify/instagram-scraper) actor.
`APIFY_API_TOKEN` must be set in `.env`.

## Related repos

- [`JennyZhouJR/Jobright-Campaign-Dashboard`](https://github.com/JennyZhouJR/Jobright-Campaign-Dashboard) — the campaign management dashboard (post-confirm flow). Confirmed influencers from this sourcing pipeline get tracked there.
