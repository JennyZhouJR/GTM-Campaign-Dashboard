---
name: influencer-pipeline
description: |
  Run the Instagram influencer sourcing pipeline that processes NanoInf CSV/XLSX exports through automated filtering, Apify scraping, dedup, and Google Sheet writing. Use this skill whenever the user mentions: running the pipeline, processing input CSVs, putting files in input, sourcing influencers, filtering influencers, updating pipeline rules, checking pipeline results, modifying keyword lists or exclusion rules, adjusting follower ranges, or anything related to the influencer sourcing workflow. Also trigger when the user says things like "跑一下", "跑一次", "跑一轮", "放了新文件", "放了csv", "开始运行", or asks about pipeline rules, ER calculation, or filtering logic.
---

# Instagram Influencer Sourcing Pipeline

You are operating an automated influencer sourcing pipeline for Jenny. The pipeline processes NanoInf CSV/XLSX exports to find qualified Instagram influencers for GTM outreach, filters them through multiple rule layers, scrapes live data via Apify, and writes qualified accounts to a Google Sheet.

## Quick Reference

- **Script**: `influencer_pipeline.py` in the project root
- **Input**: Place `.csv` or `.xlsx` files in `input_csvs/`
- **Output**: Qualified rows written to Google Sheet + local backup in `output/`
- **Archive**: Processed files moved to `archived/`
- **Rules doc**: `rules.txt` (V1.2)
- **Credentials**: `dao/loyal-glass-384620-45dc1d553712.json` (Google Sheets service account)
- **Apify token**: `.env` file (`APIFY_API_TOKEN`)

---

## Running the Pipeline

When the user says to run the pipeline (e.g. "跑一下", "再跑一次", "放了新文件"):

1. List files in `input_csvs/` to confirm what's there
2. Run `python3 influencer_pipeline.py` with a 10-minute timeout
3. Parse the output and present a summary table with columns: File | Input rows | Step5 passed | Written to Sheet
4. Report the total rows written and the Sheet row range

If the output is truncated, read the saved output file to find the step-by-step results for each file.

---

## Pipeline Architecture (6 Steps)

### Step 1: Basic Threshold Filter (local, no API)
All conditions must pass or the row is dropped:
- **Country**: US or Canada (case-insensitive)
- **Followers**: 8,000 - 200,000
- **Platform**: Instagram (check `instagram.com` in URL field, not CHANNEL field which contains display names)

### Step 2: Local Content Filter (no API)
Sequential checks on CSV `TOPICS` and `AUDIENCES` fields:
- **2A** ER threshold: CSV ER < 0.2% -> Drop
- **2B** Role exclusion (whole-word regex): founder, co-founder, ceo, coo, cfo, cto, owner, entrepreneur, director, managing partner, president, principal, agency
- **2C** Hard-exclude topics (substring match): crypto, fitness, travel, beauty, fashion, personal finance, dating advice, etc. (full list in script `HARD_EXCLUDE_TOPICS`)
- **2D** Brand account signals: official, (TM), (R), "we are", "our product", "shop now", etc.
- **2E** India geo-penetration: India flag emoji, Indian city names in topics/audiences

### Step 3: Dedup Against Google Sheet
- Read all usernames from Sheet column D
- Also track `written_this_run` in-memory set to prevent cross-CSV duplicates within the same batch
- Extract username from URL for comparison, handle both full URLs and plain usernames

### Step 4: Apify Scraping (two actors)
- **Profile Scraper** (`apify/instagram-profile-scraper`): Gets bio, fullName, country, latestPosts. `resultsLimit=1`
- **Reel Scraper** (`apify/instagram-reel-scraper`): Gets reels with `videoPlayCount`. `resultsLimit=15`

### Step 5: Process Apify Data + Final Filters
- **5A** Extract recent 8 reels: exclude pinned, require videoPlayCount > 0, sort by timestamp desc, take top 8. If < 3 valid reels -> Skip (not Drop)
- **5B** Metrics:
  - **Avg Views** = mean of `videoPlayCount` across 8 reels (NOT `videoViewCount`)
  - **ER** = per-video ER first `(likes + comments) / videoPlayCount * 100`, then take **median** of all 8 values (NOT mean, NOT aggregate)
- **5C** Final ER check: median ER < 0.2% -> Drop
- **5D** Activity: Past 30 days must have >= 4 Reels (only `productType == "clips"` or `type == "Video"`, image/carousel posts don't count)
- **5E** Bio brand detection: Check Apify biography for brand signals
- **5E2** Bio role exclusion: Check Apify biography for founder/CEO/etc. keywords (this is critical - CSV fields often miss these, the bio is where they actually appear)
- **5F** Bio India detection: Check bio for India flag emoji, `\bindia\b` whole-word regex (not substring to avoid "Indiana" false positives), Indian city names, and post locationName
- **5G** Language: Bio must be English (langdetect, skip if bio < 10 chars)
- **5H** Country: Use Apify `about.country` first, fallback to CSV COUNTRY

### Step 6: Write to Google Sheet
- Append rows using `gspread.append_rows()`
- Column mapping: A=Date, B="Jenny", C=Name, D=Profile URL, E=Email, H="Instagram", I=Followers, J=Country, O=Avg Views (int), Q=ER (2 decimal), S="Similar-{seed_name}", AA=Topics
- Save local CSV backup to `output/`
- Archive original file to `archived/`

---

## Critical Rules & Lessons Learned

These are hard-won lessons from debugging sessions. Pay close attention:

### ER Calculation
The correct ER formula is: calculate each video's individual ER = (likes + comments) / videoPlayCount * 100, then take the **median** of all 8 values. Previous bugs used mean instead of median, or aggregated all likes/views before dividing — both produce wrong results.

### Avg Impression Source
Use `videoPlayCount` from the **reel scraper** (`apify/instagram-reel-scraper`), NOT `videoViewCount` from the profile scraper's `latestPosts`. The profile scraper's view counts are unreliable and produce significantly different numbers.

### Bio-Level Checks Are Essential
Exclusion keywords (founder, CEO, etc.) must be checked against **both** CSV fields (Step 2) AND Apify biography (Step 5). Many influencers don't have "founder" in their NanoInf topics but do have it in their Instagram bio. Missing this check was a recurring bug.

### India Detection: Whole-Word Matching
Use `re.search(r'\bindia\b', text)` for India detection, not `"india" in text`. The substring match causes false positives for "Indiana".

### Cross-CSV Dedup
The `written_this_run` set must be passed through all file processing in a single batch run. Without this, the same account appearing in multiple CSV files from the same seed cluster gets written multiple times.

### Activity Check: Reels Only
The 30-day / 4-post activity check counts only Reels (video content), not image or carousel posts. Filter by `productType == "clips"` or `type == "Video"`.

### NanoInf CHANNEL Field
The CHANNEL field in NanoInf exports contains the influencer's display name, not "Instagram". Check for `instagram.com` in the URL field instead.

### Google Sheet Column D Mixed Formats
The Sheet contains both full URLs (`https://www.instagram.com/username`) and plain usernames. Dedup logic must try URL extraction first, then fall back to using the raw string.

---

## Modifying Rules

When the user asks to update rules (add keywords, change thresholds, etc.):

1. Edit `influencer_pipeline.py` — update the relevant constant list or threshold
2. Also update `rules.txt` to keep documentation in sync
3. Confirm the change with the user
4. If the user asks to re-check already-written Sheet data against new rules, that's a separate manual task

Common modification requests:
- **Add red-line keywords**: Add to `HARD_EXCLUDE_TOPICS` list
- **Add role exclusion words**: Add to `ROLE_EXCLUDE_RE` regex
- **Change follower range**: Edit the `8000 <= subs <= 200000` line in `step1_basic_filter()`
- **Add brand signals**: Add to `BRAND_SIGNALS` and/or `BRAND_BIO_SIGNALS`
- **Add India cities**: Add to `INDIA_CITIES`

---

## Cost & Timing Reference

- Apify profile scraper: ~$8.30 / 1,000 profiles
- Apify reel scraper: ~$4.00 / 1,000 accounts (15 reels each)
- Typical batch (6-8 files, ~30-40 rows each): 5-8 minutes, ~$1-2 Apify cost
- Typical pass-through rate after all filters: 30-50% of Step 3 survivors

---

## Output Reporting Format

After each run, present results as a markdown table:

```
| File | Input | Step5 passed | Written to Sheet |
|------|-------|-------------|-----------------|
| filename | N | M | **K rows** |

**Total: X rows written**, Sheet rows NNNN-MMMM. All files archived.
```
