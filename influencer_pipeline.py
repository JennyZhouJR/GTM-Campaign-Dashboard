#!/usr/bin/env python3
"""
Influencer Sourcing Pipeline
NanoInf CSV → Filter → Apify → Dedup → Google Sheet
"""

import csv
import os
import re
import shutil
import statistics
import sys
from datetime import datetime, timezone

from apify_client import ApifyClient
from dotenv import load_dotenv
from langdetect import detect, LangDetectException

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "input_csvs")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archived")
ENV_PATH = os.path.join(BASE_DIR, ".env")
GSHEET_CRED = os.path.join(BASE_DIR, "dao", "loyal-glass-384620-45dc1d553712.json")
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1hvAJnBUFdQWyLRE2oAwRwB9Z_Ugu6hVUfFjHdBDsSG0/edit"

load_dotenv(ENV_PATH)
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")

# ─── Role exclusion (whole-word regex) — V1.2 expanded ───────────────────────
ROLE_EXCLUDE_RE = re.compile(
    r'\b(founder|co-founder|ceo|coo|cfo|cto|owner|entrepreneur|director|managing\s+partner|president|principal|agency)\b',
    re.IGNORECASE
)

# ─── Hard-exclude topics (substring match) ────────────────────────────────────
HARD_EXCLUDE_TOPICS = [
    # 金融投机 / 赚钱类
    "crypto", "cryptocurrency", "bitcoin", "nft", "forex", "trading signals",
    "options trading", "dropshipping", "make money online", "passive income",
    "mlm", "network marketing", "affiliate marketing",
    # 个人理财 / 财富类（2026-04-07 补充）
    "personal finance", "wealth building", "investing", "income stream",
    "financial independence", "online money", "real estate investing",
    "saving money", "digital nomad", "online business", "digital products",
    # 身体/健康类
    "fitness", "gym", "workout", "bodybuilding", "weight loss", "diet",
    "nutrition", "supplements", "wellness", "yoga",
    # 生活方式类
    "travel", "food", "foodie", "cooking", "recipe", "beauty", "makeup",
    "skincare", "fashion", "style", "ootd", "dance", "dancer",
    # 情感/关系类（2026-04-07 补充）
    "dating advice", "relationship coaching", "life coaching",
    "mom life", "working mom",
    # 政治/宗教/敏感类
    "politics", "political", "religion", "astrology", "horoscope",
    # 成人内容
    "nsfw", "onlyfans", "adult",
    # 纯财经（非求职方向）
    "financial advisor", "cfp", "certified financial planner",
    "real estate agent", "realtor", "mortgage",
    # 营销类（2026-04-07 补充）
    "social media marketing",
    # Gaming / 硬件类（2026-04-13 补充，Jenny review 后确认）
    "gaming", "pc gaming", "pc build", "pc building", "gaming setup",
    "retro gaming", "tech unboxing", "gadgets", "streetwear", "fragrances",
    # 带货 / 折扣类
    "amazon deals", "promo codes", "discount shopping",
    # 娱乐 / 手工类
    "comedy", "nostalgia", "crochet", "knitting", "felting",
    # 家装 / 摄影类
    "home renovation", "photography", "video editing",
    # 其他不相关
    "quantum computing", "photoshop",
]

# ─── 2D: Brand account signals (substring match on TOPICS/AUDIENCES) ──────────
BRAND_SIGNALS = [
    "official", "™", "®", "we are", "our product", "our mission",
    "download now", "sign up", "founded in", "est.", "since ",
    "our team", "we help", "we build", "our app", "try it free",
    "join us", "shop now",
]

# ─── 2E / 5F: India geo-penetration signals ───────────────────────────────────
INDIA_FLAG = "🇮🇳"
INDIA_CITIES = [
    "mumbai", "delhi", "bangalore", "bengaluru", "hyderabad",
    "chennai", "pune", "kolkata", "ahmedabad", "noida",
    "gurgaon", "gurugram", "jaipur", "lucknow", "chandigarh",
]

# ─── 5E: Bio-level brand signals (Apify biography check) ─────────────────────
BRAND_BIO_SIGNALS = [
    "official", "™", "®", "we are", "our product", "our mission",
    "download now", "sign up", "founded in", "est.", "since ",
    "our team", "we help", "we build", "our app", "try it free",
    "join us", "follow us for", "#ad", "shop now",
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def parse_subscribers(val):
    """Parse subscriber count: '12,345' -> 12345"""
    if not val:
        return 0
    return int(str(val).replace(",", "").strip())


def parse_er(val):
    """Parse ER field to percentage number. '1.5%' -> 1.5, '0.015' -> 1.5"""
    if not val or str(val).strip() == "" or str(val).strip().upper() == "N/A":
        return 0.0
    s = str(val).strip()
    if "%" in s:
        return float(s.replace("%", ""))
    f = float(s)
    # If value < 1, assume it's a ratio (0.015 = 1.5%)
    if f < 1:
        return f * 100
    return f


def extract_username_from_url(url):
    """Extract Instagram username from URL."""
    match = re.search(r'instagram\.com/([^/?]+)', url or '')
    return match.group(1).lower().strip('/') if match else ''


def is_english_bio(bio_text):
    """Check if biography is English using langdetect."""
    if not bio_text or len(bio_text.strip()) < 10:
        return True
    try:
        return detect(bio_text) == 'en'
    except LangDetectException:
        return True


def extract_seed_name(filename):
    """Extract seed account name from filename like 'nanoinf_fatimah.csv'."""
    base = os.path.splitext(os.path.basename(filename))[0]
    if base.lower().startswith("nanoinf_"):
        return base[8:]
    return base


def read_csv_robust(filepath):
    """Read CSV or XLSX file, returning list of dicts with header keys."""
    if filepath.lower().endswith(".xlsx"):
        import openpyxl
        wb = openpyxl.load_workbook(filepath, data_only=True)
        ws = wb.active
        rows_raw = list(ws.iter_rows(values_only=True))
        if not rows_raw:
            return []
        headers = [str(h).strip() if h is not None else "" for h in rows_raw[0]]
        rows = []
        for row in rows_raw[1:]:
            d = {headers[i]: (str(row[i]).strip() if row[i] is not None else "") for i in range(len(headers))}
            rows.append(d)
        return rows
    else:
        rows = []
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows


# ─── Google Sheet connection ──────────────────────────────────────────────────

def get_gsheet():
    """Connect to Google Sheet and return first worksheet."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CRED, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(GSHEET_URL)
    return spreadsheet.get_worksheet(0)


# ─── STEP 1: Basic threshold filter ──────────────────────────────────────────

def step1_basic_filter(rows):
    """Country US/CA, Subscribers 10K-100K, Channel Instagram."""
    passed = []
    dropped = 0
    for row in rows:
        country = (row.get("COUNTRY") or "").strip().lower()
        if country not in ("united states", "canada"):
            dropped += 1
            continue

        subs = parse_subscribers(row.get("SUBSCRIBERS"))
        if not (8000 <= subs <= 200000):
            dropped += 1
            continue

        # CHANNEL field in NanoInf CSV contains display name, not platform.
        # Check URL for instagram.com to confirm platform.
        channel_field = (row.get("CHANNEL") or "").strip().lower()
        url_field = (row.get("URL") or "").strip().lower()
        is_instagram = (channel_field == "instagram" or "instagram.com" in url_field)
        if not is_instagram:
            dropped += 1
            continue

        passed.append(row)

    print(f"[STEP 1] 输入: {len(rows)} | 通过: {len(passed)} | Drop: {dropped}")
    return passed


# ─── STEP 2: Local content filter ────────────────────────────────────────────

def step2_content_filter(rows):
    """ER threshold, role exclusion, topic red lines."""
    passed = []
    dropped = 0
    for row in rows:
        # 2A: ER threshold
        er = parse_er(row.get("ER"))
        if er < 0.2:
            dropped += 1
            continue

        # 2B: Role exclusion (whole word)
        topics = (row.get("TOPICS") or "").lower()
        audiences = (row.get("AUDIENCES") or "").lower()
        combined = topics + " " + audiences
        if ROLE_EXCLUDE_RE.search(combined):
            dropped += 1
            continue

        # 2C: Hard-exclude topics (substring)
        hit = False
        for kw in HARD_EXCLUDE_TOPICS:
            if kw in combined:
                hit = True
                break
        if hit:
            dropped += 1
            continue

        # 2D: Brand account exclusion (V1.2 new)
        brand_hit = False
        for signal in BRAND_SIGNALS:
            if signal.lower() in combined:
                brand_hit = True
                break
        if brand_hit:
            dropped += 1
            continue

        # 2E: India geo-penetration detection (V1.2 new)
        raw_combined = (row.get("TOPICS") or "") + " " + (row.get("AUDIENCES") or "")
        if INDIA_FLAG in raw_combined or re.search(r'\bindia\b', combined):
            dropped += 1
            continue
        if any(city in combined for city in INDIA_CITIES):
            dropped += 1
            continue

        passed.append(row)

    print(f"[STEP 2] 输入: {len(rows)} | 通过: {len(passed)} | Drop: {dropped}")
    return passed


# ─── STEP 3: Dedup against Google Sheet ───────────────────────────────────────

def step3_dedup(rows, sheet, written_this_run):
    """Remove rows whose username already exists in Sheet column D or was written this run."""
    # Force a fresh read to avoid gspread caching stale data
    all_vals = sheet.get_all_values()
    col_d = [r[3] if len(r) > 3 else "" for r in all_vals]

    existing = set(written_this_run)  # include usernames written earlier this run
    for val in col_d:
        val = (val or "").strip().lower().strip("/")
        if not val:
            continue
        uname = extract_username_from_url(val)
        if uname:
            existing.add(uname)
        else:
            existing.add(val)

    passed = []
    dropped = 0
    for row in rows:
        username = (row.get("USERNAME") or "").lower().strip("/")
        if username in existing:
            dropped += 1
            continue
        passed.append(row)

    print(f"[STEP 3] 输入: {len(rows)} | 通过: {len(passed)} | Drop: {dropped}（已存在）")
    return passed


# ─── STEP 4: Apify Profile Scraper ───────────────────────────────────────────

def step4_apify_scrape(rows):
    """Call Apify Profile Scraper + Reel Scraper for all accounts."""
    usernames = [extract_username_from_url(row["URL"]) for row in rows]
    print(f"[STEP 4] 调用 Apify，共 {len(usernames)} 个账号...")

    client = ApifyClient(APIFY_TOKEN)

    # 4A: Profile scraper (for name, bio, followers, country)
    print("  [4A] Profile Scraper...")
    profile_run = client.actor("apify/instagram-profile-scraper").call(
        run_input={"usernames": usernames, "resultsLimit": 1}
    )
    profile_items = list(client.dataset(profile_run["defaultDatasetId"]).iterate_items())

    profile_map = {}
    for item in profile_items:
        uname = (item.get("username") or "").lower()
        if uname:
            profile_map[uname] = item
    print(f"  [4A] Profile 返回 {len(profile_items)} 个结果")

    # 4B: Reel scraper (for reels with videoPlayCount)
    print("  [4B] Reel Scraper...")
    reel_run = client.actor("apify/instagram-reel-scraper").call(
        run_input={"username": usernames, "resultsLimit": 15}
    )
    reel_items = list(client.dataset(reel_run["defaultDatasetId"]).iterate_items())

    # Group reels by owner username
    reels_map = {}
    for item in reel_items:
        uname = (item.get("ownerUsername") or "").lower()
        if uname:
            reels_map.setdefault(uname, []).append(item)
    print(f"  [4B] Reel 返回 {len(reel_items)} 条 Reels，涉及 {len(reels_map)} 个账号")

    return profile_map, reels_map


# ─── STEP 5: Process Apify data ──────────────────────────────────────────────

def step5_process(rows, profile_map, reels_map):
    """Extract reels, compute metrics, apply final filters."""
    passed = []
    dropped = 0
    skipped = 0

    for row in rows:
        username = (row.get("USERNAME") or "").lower().strip("/")
        profile = profile_map.get(username)
        reels_raw = reels_map.get(username, [])

        if not profile:
            print(f"  ⚠ Profile 无数据: {username}")
            skipped += 1
            continue

        # 5A: Extract recent 8 reels (exclude pinned, require valid playCount)
        reels = []
        for p in reels_raw:
            if p.get("isPinned"):
                continue
            play_count = p.get("videoPlayCount")
            if play_count is None or play_count <= 0:
                continue
            reels.append(p)

        # Sort by timestamp descending
        reels.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        reels_8 = reels[:8]

        if len(reels_8) < 3:
            skipped += 1
            continue

        # 5B: Compute metrics using videoPlayCount
        avg_views = sum(p["videoPlayCount"] for p in reels_8) / len(reels_8)

        # ER: calculate per-video ER first, then take median
        individual_ers = []
        for p in reels_8:
            likes = max(p.get("likesCount", 0) or 0, 0)
            comments = max(p.get("commentsCount", 0) or 0, 0)
            er_i = (likes + comments) / p["videoPlayCount"] * 100
            individual_ers.append(er_i)
        recent_er = statistics.median(individual_ers)

        # 5C: Final ER check
        if recent_er < 0.2:
            dropped += 1
            continue

        # 5D: Activity check — HARD RULE (V1.2 upgraded)
        # Past 30 days must have >= 4 Reels (video only, exclude image/carousel posts)
        recent_30d = 0
        for p in reels_raw:
            # Only count actual Reels: productType == "clips" or type == "Video"
            product_type = (p.get("productType") or "").lower()
            post_type = (p.get("type") or "").lower()
            if product_type != "clips" and post_type != "video":
                continue
            ts = p.get("timestamp", "")
            if not ts:
                continue
            try:
                post_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - post_dt).days <= 30:
                    recent_30d += 1
            except (ValueError, TypeError):
                pass
        if recent_30d < 4:
            dropped += 1
            continue

        bio = profile.get("biography") or ""

        # 5E: Bio-level brand account detection (V1.2 new)
        if any(signal.lower() in bio.lower() for signal in BRAND_BIO_SIGNALS):
            dropped += 1
            continue

        # 5E2: Bio-level role exclusion (founder, CEO, etc.)
        if ROLE_EXCLUDE_RE.search(bio.lower()):
            dropped += 1
            continue

        # 5F: Bio-level India geo-penetration detection (V1.2 new)
        if INDIA_FLAG in bio:
            dropped += 1
            continue
        bio_lower = bio.lower()
        if re.search(r'\bindia\b', bio_lower) or any(city in bio_lower for city in INDIA_CITIES):
            dropped += 1
            continue
        # Also check post locationName from profile scraper latestPosts
        profile_posts = profile.get("latestPosts") or []
        india_location = False
        for p in profile_posts:
            loc = (p.get("locationName") or "").lower()
            if re.search(r'\bindia\b', loc) or any(city in loc for city in INDIA_CITIES):
                india_location = True
                break
        if india_location:
            dropped += 1
            continue

        # 5G: Language detection (bio must be English)
        if not is_english_bio(bio):
            dropped += 1
            continue

        # 5H: Country (Apify first, fallback CSV)
        notes_parts = []
        about = profile.get("about") or {}
        apify_country = about.get("country") or ""
        csv_country = (row.get("COUNTRY") or "").strip()
        country = apify_country if apify_country else csv_country
        if not country:
            notes_parts.append("Country unverified")

        # Build seed note
        seed_name = row.get("_seed_name", "unknown")
        notes_prefix = f"Similar-{seed_name}"
        if notes_parts:
            notes = notes_prefix + " | " + ", ".join(notes_parts)
        else:
            notes = notes_prefix

        # Get name
        full_name = profile.get("fullName") or ""
        if not full_name:
            full_name = row.get("USERNAME", "")

        # Get email
        valid_email = (row.get("VALID EMAIL") or "").strip()
        email_field = (row.get("EMAIL") or "").strip()
        # Handle multiline emails - take first line
        email = valid_email.split("\n")[0].strip() if valid_email else (email_field.split("\n")[0].strip() if email_field else "")

        # Get subscribers from CSV
        subs = parse_subscribers(row.get("SUBSCRIBERS"))

        # Topics
        topics = (row.get("TOPICS") or "").strip()

        # Profile URL
        profile_url = (row.get("URL") or "").strip()

        today = datetime.today().strftime("%Y-%m-%d")

        sheet_row = [
            today,                          # A(0) Date
            "Jenny",                        # B(1) POC
            full_name,                      # C(2) Name
            profile_url,                    # D(3) Profile Link
            email,                          # E(4) Contact
            "",                             # F(5) Seniority
            "",                             # G(6) Job Function
            "Instagram",                    # H(7) Channel
            subs,                           # I(8) Followers
            country,                        # J(9) Country
            "",                             # K(10)
            "",                             # L(11)
            "",                             # M(12)
            "",                             # N(13)
            int(round(avg_views)),          # O(14) Avg Views
            "",                             # P(15) Audience Geo
            round(recent_er, 2),            # Q(16) ER
            "",                             # R(17) Confirm Date
            notes,                          # S(18) Notes
            "",                             # T(19)
            "",                             # U(20)
            "",                             # V(21)
            "",                             # W(22)
            "",                             # X(23)
            "",                             # Y(24)
            "",                             # Z(25)
            topics,                         # AA(26) Topics
        ]

        passed.append(sheet_row)

    print(f"[STEP 5] 输入: {len(rows)} | 通过: {len(passed)} | Drop: {dropped} | Skip: {skipped}")
    return passed


# ─── STEP 6: Write to Google Sheet ───────────────────────────────────────────

def step6_write_sheet(sheet, rows_to_write):
    """Append rows to Google Sheet."""
    if not rows_to_write:
        print("[STEP 6] 无数据写入")
        return

    # Get current last row dynamically
    all_values = sheet.get_all_values()
    next_row = len(all_values) + 1

    sheet.append_rows(rows_to_write, value_input_option="USER_ENTERED")
    print(f"[STEP 6] 成功写入 Google Sheet {len(rows_to_write)} 行（从第 {next_row} 行开始）")


# ─── Save local backup ───────────────────────────────────────────────────────

def save_local_backup(rows_to_write, seed_name):
    """Save a local CSV backup of passed rows."""
    if not rows_to_write:
        return

    headers = [
        "Date", "POC", "Name", "Profile Link", "Contact", "Seniority",
        "Job Function", "Channel", "Followers", "Country", "K", "L", "M", "N",
        "Avg Views", "Audience Geo", "ER", "Confirm Date", "Notes",
        "T", "U", "V", "W", "X", "Y", "Z", "Topics"
    ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = os.path.join(OUTPUT_DIR, f"output_{seed_name}_{timestamp}.csv")

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows_to_write)

    print(f"本地备份已保存: {outfile}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    """
    Batched pipeline:
      Phase A (per-file): Steps 1-3 — local filter + dedup
      Phase B (once):     Step 4  — single batched Apify call for ALL accounts
      Phase C (per-file): Steps 5-6 — process metrics, write Sheet, archive
    """
    import time

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    csv_files = sorted(
        [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR)
         if f.endswith(".csv") or f.endswith(".xlsx")]
    )

    if not csv_files:
        print("input_csvs/ 目录下没有 CSV/XLSX 文件")
        sys.exit(0)

    print(f"找到 {len(csv_files)} 个文件")

    # ── Phase A: Steps 1-3 for every file ──────────────────────────────────────
    print("\n─── Phase A: 本地过滤 + 去重（所有文件）───")
    print("连接 Google Sheet...")
    sheet = get_gsheet()
    written_this_run = set()

    # file_data: list of (csv_path, seed_name, rows_after_dedup)
    file_data = []

    for csv_path in csv_files:
        seed_name = extract_seed_name(csv_path)
        print(f"\n{'='*60}")
        print(f"处理文件: {os.path.basename(csv_path)} (seed: {seed_name})")
        print(f"{'='*60}")

        rows = read_csv_robust(csv_path)
        print(f"读取 {len(rows)} 行数据")

        for row in rows:
            row["_seed_name"] = seed_name

        rows = step1_basic_filter(rows)
        if not rows:
            print("STEP 1 后无剩余数据，归档跳过")
            file_data.append((csv_path, seed_name, []))
            continue

        rows = step2_content_filter(rows)
        if not rows:
            print("STEP 2 后无剩余数据，归档跳过")
            file_data.append((csv_path, seed_name, []))
            continue

        rows = step3_dedup(rows, sheet, written_this_run)
        if not rows:
            print("STEP 3 后无剩余数据，归档跳过")
            file_data.append((csv_path, seed_name, []))
            continue

        # Reserve usernames now so later files don't duplicate
        for row in rows:
            uname = extract_username_from_url(row.get("URL", ""))
            if uname:
                written_this_run.add(uname)

        file_data.append((csv_path, seed_name, rows))

    # ── Phase B: Single batched Apify call ─────────────────────────────────────
    all_rows = [row for (_, _, rows) in file_data for row in rows]

    if not all_rows:
        print("\n全部文件在 Phase A 已被过滤，无需调用 Apify")
        print("\n✅ 全部处理完成")
        # Archive files that had no survivors
        for csv_path, _, _ in file_data:
            dest = os.path.join(ARCHIVE_DIR, os.path.basename(csv_path))
            try:
                if os.path.exists(csv_path):
                    shutil.move(csv_path, dest)
            except Exception:
                pass
        return

    print(f"\n─── Phase B: 单次 Apify 调用（共 {len(all_rows)} 个账号）───")
    profile_map, reels_map = step4_apify_scrape(all_rows)

    # ── Phase C: Steps 5-6 per file ────────────────────────────────────────────
    print("\n─── Phase C: 处理指标 + 写入 Sheet（逐文件）───")

    # Reset written_this_run — now track only what actually gets written
    written_this_run = set()

    for csv_path, seed_name, rows in file_data:
        print(f"\n{'='*60}")
        print(f"写入阶段: {os.path.basename(csv_path)}")
        print(f"{'='*60}")

        if not rows:
            print("无候选账号，跳过")
        else:
            rows_to_write = step5_process(rows, profile_map, reels_map)
            step6_write_sheet(sheet, rows_to_write)

            for sheet_row in rows_to_write:
                url = sheet_row[3] if len(sheet_row) > 3 else ""
                uname = extract_username_from_url(url)
                if uname:
                    written_this_run.add(uname)

            save_local_backup(rows_to_write, seed_name)

        # Archive
        dest = os.path.join(ARCHIVE_DIR, os.path.basename(csv_path))
        try:
            if os.path.exists(csv_path):
                shutil.move(csv_path, dest)
                print(f"归档完成: {os.path.basename(csv_path)} → archived/")
            else:
                print(f"归档跳过（文件已不存在）: {os.path.basename(csv_path)}")
        except Exception as e:
            print(f"归档警告: {e}")

    print("\n✅ 全部处理完成")


if __name__ == "__main__":
    main()
