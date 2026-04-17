#!/usr/bin/env python3
"""Sync 24hr Views + Post ER + Baseline ER for recent campaign posts.

Usage:
    python3 sync_views.py              # Scrape posts from last 3 days with missing views
    python3 sync_views.py --days 7     # Change the lookback window
    python3 sync_views.py --force      # Re-scrape even if 24hr Views already filled

Conditions for scraping:
    - Post Date within the last N days (default 3)
    - Post Link is non-empty
    - Status == "Confirm"
    - 24hr Views is empty (unless --force)

Writes to Sheet:
    - 24hr Views (AB)
    - Post ER (AM)
    - Baseline ER (AN) — from last 10 reels, excluding pinned reels & campaign post

Requires env var: APIFY_API_TOKEN
"""

import os
import re
import sys
import json
import argparse
import statistics
from datetime import date, datetime, timedelta

# Add project root to path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from dotenv import load_dotenv
from apify_client import ApifyClient

from dashboard_utils.gsheet_client import _get_worksheet, load_dataframe, batch_update_cells
from dashboard_utils.data_model import COL, prepare_dataframe, parse_date

# ─── Config ───────────────────────────────────────────────────────────────────

ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(ENV_PATH)
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def clean_post_url(raw: str) -> str:
    """Extract the first valid Instagram post URL from messy input.

    Handles cases like:
      - 'IG：https://www.instagram.com/reel/DXL...'  (Chinese colon prefix)
      - 'https://...\\nhttps://...'                   (multiple URLs)
      - '   https://...   '                           (whitespace)
    """
    if not raw:
        return ""
    # Find the first instagram.com URL in the text
    match = re.search(r'https?://(?:www\.)?instagram\.com/[^\s,;)\]]+', raw)
    return match.group(0) if match else raw.strip()


def extract_username_from_url(url: str) -> str:
    """Extract Instagram username from a post URL.
    Example: https://www.instagram.com/reel/DXLC39cjasI/?igsh=... → (empty, because no username in this URL)
    Example: https://www.instagram.com/nathanielgibson/reel/DXLC39cjasI/ → nathanielgibson
    For /p/ or /reel/ URLs without username, we return empty and must get username from API.
    """
    if not url:
        return ""
    # Match instagram.com/<username>/reel/ or instagram.com/<username>/p/
    match = re.search(r'instagram\.com/([^/?]+)/(?:reel|p)/', url)
    if match:
        username = match.group(1).strip("/")
        # Filter out known non-username paths
        if username not in ("reel", "p", "stories", "explore"):
            return username.lower()
    return ""


def extract_shortcode_from_url(url: str) -> str:
    """Extract the reel/post shortcode from an Instagram URL.
    e.g. https://www.instagram.com/reel/DXLC39cjasI/?igsh=... → DXLC39cjasI
    """
    if not url:
        return ""
    match = re.search(r'/(?:reel|p|tv)/([A-Za-z0-9_-]+)', url)
    return match.group(1) if match else ""


def compute_er(likes, comments, views) -> float:
    """Compute engagement rate as a percentage: (likes + comments) / views * 100.

    Instagram API sometimes returns -1 for hidden likes/comments; clamp those to 0.
    """
    try:
        likes = max(0, float(likes or 0))
        comments = max(0, float(comments or 0))
        views = float(views or 0)
        if views <= 0:
            return None
        return (likes + comments) / views * 100
    except (ValueError, TypeError):
        return None


# ─── Apify calls (batched) ───────────────────────────────────────────────────

def scrape_posts_batch(client: ApifyClient, urls: list) -> dict:
    """Scrape multiple Instagram posts in one Apify call.

    Returns dict: {url → post_data}.
    """
    if not urls:
        return {}
    run = client.actor("apify/instagram-scraper").call(
        run_input={
            "directUrls": urls,
            "resultsType": "posts",
            "resultsLimit": 1,
            "addParentData": False,
        }
    )
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    # Index by URL and by shortCode (for fuzzy matching against input URLs)
    result = {}
    for item in items:
        input_url = item.get("inputUrl") or item.get("url", "")
        shortcode = item.get("shortCode") or item.get("shortcode", "")
        if input_url:
            result[input_url] = item
        # Also index by shortcode → helps if inputUrl normalized
        if shortcode:
            result[f"__shortcode__{shortcode}"] = item
    return result


def scrape_reels_batch(client: ApifyClient, usernames: list, limit_per_user: int = 15) -> dict:
    """Scrape recent reels for multiple users in one Apify call.

    Returns dict: {username → list of reels}.
    """
    if not usernames:
        return {}
    run = client.actor("apify/instagram-reel-scraper").call(
        run_input={"username": usernames, "resultsLimit": limit_per_user}
    )
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    grouped = {}
    for item in items:
        uname = (item.get("ownerUsername") or "").lower()
        if uname:
            grouped.setdefault(uname, []).append(item)
    return grouped


def _filter_baseline_reels(reels: list, exclude_shortcode: str = "", top_n: int = 10) -> list:
    """Filter and sort reels for baseline calculations (shared by ER + views)."""
    filtered = []
    for reel in reels:
        # Skip pinned
        if reel.get("isPinned"):
            continue
        # Skip campaign post itself
        if exclude_shortcode and reel.get("shortCode") == exclude_shortcode:
            continue
        # Must have views to be useful
        views = reel.get("videoPlayCount") or reel.get("videoViewCount") or 0
        if views <= 0:
            continue
        filtered.append(reel)
    # Sort by timestamp descending (newest first)
    filtered.sort(key=lambda r: r.get("timestamp", "") or "", reverse=True)
    return filtered[:top_n]


def compute_baseline_views(reels: list, exclude_shortcode: str = "", top_n: int = 10) -> int:
    """Compute AVERAGE view count from recent reels (excludes pinned + campaign post)."""
    selected = _filter_baseline_reels(reels, exclude_shortcode, top_n)
    if not selected:
        return None
    views = [r.get("videoPlayCount") or r.get("videoViewCount") or 0 for r in selected]
    views = [v for v in views if v > 0]
    if not views:
        return None
    return int(statistics.mean(views))


def compute_baseline_er(reels: list, exclude_shortcode: str = "", top_n: int = 10) -> float:
    """Compute AVERAGE ER from recent reels, excluding pinned reels and campaign post.

    Uses the exact same reel selection as compute_baseline_views to keep
    ER and Views baselines based on the same 10 reels.
    """
    selected = _filter_baseline_reels(reels, exclude_shortcode, top_n)
    if not selected:
        return None
    er_values = []
    for reel in selected:
        er = compute_er(reel.get("likesCount"), reel.get("commentsCount"),
                        reel.get("videoPlayCount") or reel.get("videoViewCount"))
        if er is not None:
            er_values.append(er)
    if not er_values:
        return None
    return round(statistics.mean(er_values), 2)


# ─── Main sync ────────────────────────────────────────────────────────────────

def sync_views(days: int = 3, force: bool = False):
    """Find recent campaign posts missing data and scrape them."""
    if not APIFY_TOKEN:
        print("❌ APIFY_API_TOKEN not set in environment.")
        sys.exit(1)

    print(f"[{datetime.now():%Y-%m-%d %H:%M}] Starting sync_views (lookback={days}d, force={force})...")

    ws = _get_worksheet()
    df = load_dataframe(ws)
    df = prepare_dataframe(df)

    today = date.today()
    latest_scrape_date = today - timedelta(days=1)   # only scrape posts from yesterday or earlier
    cutoff = today - timedelta(days=days)

    # Filter: Status=Confirm, Post Link non-empty, Post Date between cutoff and yesterday
    # Excludes today's posts (not yet 24h old → data would be premature)
    candidates = df[df["Status"] == "Confirm"].copy()
    candidates = candidates[candidates["Post Link"].str.strip() != ""]
    candidates = candidates[candidates["_post_date_parsed"].apply(
        lambda d: d is not None and cutoff <= d <= latest_scrape_date
    )]

    # Only pick rows with missing 24hr Views unless --force
    if not force:
        candidates = candidates[candidates["24hr Views"].str.strip() == ""]

    if candidates.empty:
        print(f"✅ No posts to scrape (nothing posted in last {days}d with missing views).")
        return

    print(f"Found {len(candidates)} post(s) to scrape.")

    client = ApifyClient(APIFY_TOKEN)

    # Build work items: (sheet_row, name, post_url, shortcode, username_from_url)
    AVG_IMPRESSIONS_COL = "Recent Average Impressions（The Latest 10 Videos\n)"
    work = []
    for _, row in candidates.iterrows():
        post_url = clean_post_url(row["Post Link"])
        if not post_url:
            continue  # Skip if we can't parse a URL
        existing_avg = (row.get(AVG_IMPRESSIONS_COL, "") or "").strip()
        work.append({
            "sheet_row": int(row["_sheet_row"]),
            "name": (row.get("Name", "") or "").strip() or "(no name)",
            "post_url": post_url,
            "shortcode": extract_shortcode_from_url(post_url),
            "username_from_url": extract_username_from_url(post_url),
            "has_avg_impressions": bool(existing_avg),  # True means don't overwrite
        })

    if not work:
        print("⚠️ No valid URLs to scrape.")
        return

    # ═══ STEP 1: Batch-scrape all campaign posts in one call ═══
    print(f"\n🔍 Scraping {len(work)} posts (one batched Apify call)...")
    post_urls = [w["post_url"] for w in work]
    try:
        posts_by_url = scrape_posts_batch(client, post_urls)
    except Exception as e:
        print(f"❌ Post scraper failed: {e}")
        return

    # Attach post data to each work item, collect usernames needed for baseline
    usernames_needed = set()
    for w in work:
        data = posts_by_url.get(w["post_url"])
        if not data:
            # Try shortcode-based lookup
            data = posts_by_url.get(f"__shortcode__{w['shortcode']}")
        w["post_data"] = data or {}
        # Determine username (prefer Apify response → fallback to URL)
        uname = (w["post_data"].get("ownerUsername") or "").lower()
        if not uname:
            uname = w["username_from_url"]
        w["username"] = uname
        if uname:
            usernames_needed.add(uname)

    # ═══ STEP 2: Batch-scrape reels for all unique usernames in one call ═══
    reels_by_user = {}
    if usernames_needed:
        print(f"📦 Scraping recent reels for {len(usernames_needed)} users (one batched Apify call)...")
        try:
            reels_by_user = scrape_reels_batch(client, list(usernames_needed), limit_per_user=15)
        except Exception as e:
            print(f"⚠️ Reel scraper failed: {e} — will skip baseline ER")

    # ═══ STEP 3: Compute metrics & queue updates ═══
    updates = []
    ok_count = 0
    fail_count = 0

    for w in work:
        name = w["name"]
        post_data = w["post_data"]
        sheet_row = w["sheet_row"]

        if not post_data:
            print(f"❌ {name}: no post data returned")
            fail_count += 1
            continue

        play_count = post_data.get("videoPlayCount") or post_data.get("videoViewCount") or 0
        likes = post_data.get("likesCount") or 0
        comments = post_data.get("commentsCount") or 0
        post_er = compute_er(likes, comments, play_count)

        # Baseline ER (+ Baseline Views for auto-fill if Avg Impressions is empty)
        username = w["username"]
        reels = reels_by_user.get(username, []) if username else []
        baseline_er = compute_baseline_er(
            reels, exclude_shortcode=w["shortcode"], top_n=10
        ) if reels else None
        baseline_views = compute_baseline_views(
            reels, exclude_shortcode=w["shortcode"], top_n=10
        ) if reels else None

        # Log result
        log_parts = [f"Views {int(play_count):,}"]
        if post_er is not None:
            log_parts.append(f"Post ER {post_er:.2f}%")
        if baseline_er is not None:
            log_parts.append(f"Baseline ER {baseline_er:.2f}%")
            if post_er is not None and baseline_er > 0:
                delta = (post_er / baseline_er - 1) * 100
                arrow = "↑" if delta >= 0 else "↓"
                log_parts.append(f"{arrow} {delta:+.1f}%")
        print(f"✅ {name}: {' | '.join(log_parts)}")

        # Queue Sheet updates
        if play_count > 0:
            updates.append((sheet_row, COL["views_24hr"] + 1, str(int(play_count))))
        if post_er is not None:
            updates.append((sheet_row, COL["post_er"] + 1, f"{post_er:.2f}%"))
        if baseline_er is not None:
            updates.append((sheet_row, COL["baseline_er"] + 1, f"{baseline_er:.2f}%"))
        # Only fill Avg Impressions if it's currently empty (don't overwrite manually entered values)
        if baseline_views is not None and not w["has_avg_impressions"]:
            updates.append((sheet_row, COL["avg_impressions"] + 1, str(baseline_views)))
        ok_count += 1

    # ═══ STEP 4: Batch write to Sheet ═══
    if updates:
        print(f"\n✍️  Writing {len(updates)} cells to Sheet...")
        batch_update_cells(ws, updates)
        print("✅ Done!")
    else:
        print("\n⚠️ No updates to write.")

    print(f"\nSummary: {ok_count} succeeded, {fail_count} failed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3, help="Lookback window in days (default 3)")
    parser.add_argument("--force", action="store_true", help="Re-scrape even if 24hr Views already filled")
    args = parser.parse_args()
    sync_views(days=args.days, force=args.force)
