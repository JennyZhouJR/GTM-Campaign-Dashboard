"""Column mappings, constants, and DataFrame helpers for the Campaign Dashboard."""

import re
import pandas as pd
from datetime import datetime

# ─── Column index mapping (0-based, matching Google Sheet) ────────────────────

COL = {
    "date_of_contact": 0,   # A
    "poc": 1,                # B
    "name": 2,               # C
    "profile_link": 3,       # D
    "email": 4,              # E
    "seniority": 5,          # F
    "job_function": 6,       # G
    "channel": 7,            # H
    "followers": 8,          # I
    "country": 9,            # J
    "type": 10,              # K
    "status": 11,            # L
    "collab_stage": 12,      # M
    "price": 13,             # N
    "avg_impressions": 14,   # O
    "audience_geo": 15,      # P
    "er": 16,                # Q
    "confirm_date": 17,      # R
    "notes": 18,             # S
    "payment_account": 19,   # T
    "payment_progress": 20,  # U
    "post_link": 21,         # V
    "content_type": 22,      # W
    "post_date": 23,         # X
    "tracking_link": 24,     # Y
    "dropdown": 25,          # Z (unused)
    "source_hashtag": 26,    # AA
    "views_24hr": 27,        # AB (new)
    "link_signups": 28,      # AC (new)
    "campaign_tag": 29,      # AD (new)
    "retro_notes": 30,       # AE (new)
    "email_msg_id": 31,      # AF (email)
    "last_email_sent": 32,   # AG (email)
    "followup_count": 33,    # AH (email)
    "stage_start_date": 34,  # AI (production timeline)
}

# Reverse: index -> column name
IDX_TO_NAME = {v: k for k, v in COL.items()}

# ─── Header names as they appear in Google Sheet ──────────────────────────────

HEADER_NAMES = [
    "Date of Contact", "POC", "Name", "Profile Link", "Contact",
    "Senority", "Job Function", "Channel", "followers", "Country",
    "Type", "Status", "Collaboration Stage", "Price（$)",
    "Recent Average Impressions（The Latest 10 Videos\n)",
    "Audience Geo", "ER", "Confirm Date", "Notes",
    "Payment Receiving Account", "Payment Progress", "Post Link",
    "Content Type", "Post Date", "Tracking Link", "@dropdown",
    "Source Hashtag", "24hr Views", "Link Signups", "Campaign Tag",
    "Retro Notes", "Email Message-ID", "Last Email Sent", "Follow-Up Count",
    "Stage Start Date",
]

# ─── Dropdown options ─────────────────────────────────────────────────────────

STATUS_OPTIONS = ["", "TBD", "Contacted", "Nego", "Confirm", "Reject", "Drop"]

COLLAB_STAGE_OPTIONS = [
    "",
    "Awaiting brief",
    "Script in progress",
    "Script feedback",
    "Video in progress",
    "Video feedback",
    "Approved for posting",
    "Posted",
]

CONTRACT_OPTIONS = ["", "N/A", "Sent", "Signed"]

PAYMENT_PROGRESS_OPTIONS = ["", "Pending", "Invoiced", "Paid"]

STAGE_DEADLINES = {
    # Awaiting brief — no deadline (we send the brief)
    "Script in progress": 3,       # 3 business days
    "Script feedback": 1,          # 1 business day
    "Video in progress": 5,        # 5 business days
    "Video feedback": 3,           # 1d our feedback + 2d influencer final
    # Approved for posting — terminal state, no deadline
}

CAMPAIGN_TAG_OPTIONS = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# ─── Display columns per tab ─────────────────────────────────────────────────

PIPELINE_DISPLAY_COLS = [
    "POC", "Name", "Profile Link", "Contact", "followers", "Country", "Type",
    "ER", "Status", "Collaboration Stage", "Campaign Tag", "Confirm Date", "Post Date", "Notes",
]

CONTENT_DISPLAY_COLS = [
    "Name", "Profile Link", "Status", "Collaboration Stage",
    "Content Type", "Post Link", "Post Date", "Price（$)",
]

PAYMENT_PERF_DISPLAY_COLS = [
    "Name", "Profile Link", "Payment Receiving Account",
    "Payment Progress", "Post Link", "Post Date",
    "24hr Views", "Link Signups", "ER",
    "Recent Average Impressions（The Latest 10 Videos\n)",
    "Retro Notes",
]

RETRO_DISPLAY_COLS = [
    "Name", "Profile Link", "Status", "Collaboration Stage",
    "Post Link", "Post Date", "24hr Views", "Link Signups",
    "ER", "Retro Notes",
]


# ─── DataFrame helpers ────────────────────────────────────────────────────────

def parse_date(val):
    """Try to parse various date formats from the sheet."""
    if not val or not isinstance(val, str):
        return None
    val = val.strip()
    # Handle format like "3/24/2025100000" (Google Sheets serial suffix)
    # or "3/24/2025" or "2025-03-24"
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            # Strip trailing digits that look like serial numbers
            clean = val
            if len(val) > 10 and "/" in val:
                parts = val.split("/")
                if len(parts) == 3:
                    # Take only first 4 digits of year part
                    year_part = parts[2][:4]
                    clean = f"{parts[0]}/{parts[1]}/{year_part}"
            return datetime.strptime(clean, fmt).date()
        except ValueError:
            continue
    return None


def cast_numeric(val):
    """Convert string to float, extracting the first number from mixed text.

    Handles formats like: "$2400 [2*LI + 1 TT]", "$300 tt+ig",
    "300 per video，need 2 videos", "$1,200", "100K+", "2.35%"
    """
    if not val or not isinstance(val, str):
        return None
    val = val.strip()
    if not val:
        return None
    # Handle "100K+" format
    if val.replace(",", "").replace("$", "").strip().endswith("K+"):
        clean = val.replace(",", "").replace("$", "").strip()[:-2]
        try:
            return float(clean) * 1000
        except ValueError:
            return None
    # Extract the first number (with optional commas and decimal point)
    # from strings like "$2,400 [2*LI + 1 TT]" or "300 per video"
    match = re.search(r'\$?\s*([\d,]+(?:\.\d+)?)', val)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except ValueError:
            return None
    return None


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Add parsed/typed columns for filtering and display."""
    if df.empty:
        return df
    df = df.copy()
    # Strip whitespace from key text columns to prevent matching issues
    for col in ["Status", "Collaboration Stage", "POC", "Name", "Country"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    df["_confirm_date_parsed"] = df.get("Confirm Date", pd.Series(dtype=str)).apply(parse_date)
    # Also parse Date of Contact for broader funnel filtering
    doc_col = "Date of Contact" if "Date of Contact" in df.columns else (
        [c for c in df.columns if "Date of Contact" in c] or [""]
    )
    if isinstance(doc_col, list):
        doc_col = doc_col[0] if doc_col else ""
    if doc_col and doc_col in df.columns:
        df["_date_of_contact_parsed"] = df[doc_col].apply(parse_date)
    else:
        df["_date_of_contact_parsed"] = None
    df["_followers_num"] = df.get("followers", pd.Series(dtype=str)).apply(cast_numeric)
    df["_er_num"] = df.get("ER", pd.Series(dtype=str)).apply(cast_numeric)
    df["_avg_impressions_num"] = df.get(
        "Recent Average Impressions（The Latest 10 Videos\n)", pd.Series(dtype=str)
    ).apply(cast_numeric)
    df["_price_num"] = df.get("Price（$)", pd.Series(dtype=str)).apply(cast_numeric)
    df["_views_24hr_num"] = df.get("24hr Views", pd.Series(dtype=str)).apply(cast_numeric)
    df["_signups_num"] = df.get("Link Signups", pd.Series(dtype=str)).apply(cast_numeric)
    df["_post_date_parsed"] = df.get("Post Date", pd.Series(dtype=str)).apply(parse_date)
    df["_stage_start_parsed"] = df.get("Stage Start Date", pd.Series(dtype=str)).apply(parse_date)
    # Days in current stage (business days)
    from datetime import date as _date
    _today = _date.today()
    df["_days_in_stage"] = df["_stage_start_parsed"].apply(
        lambda d: _business_days_between(d, _today) if d is not None else None
    )
    return df


def _business_days_between(start_date, end_date):
    """Count business days (Mon-Fri) between two dates, excluding start."""
    if start_date is None or end_date is None:
        return 0
    count = 0
    current = start_date
    from datetime import timedelta
    while current < end_date:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon=0 .. Fri=4
            count += 1
    return count


def get_timeline_status(df: pd.DataFrame):
    """Compute overdue and in-progress lists for confirmed influencers.

    Returns (overdue, in_progress, completed):
        overdue: [(name, poc, stage, overdue_days, sheet_row), ...]
        in_progress: [(name, poc, stage, days_left, sheet_row), ...]
        completed: count of people at Approved for posting or Posted
    """
    from datetime import date as date_type
    today = date_type.today()

    confirmed = df[df["Status"] == "Confirm"].copy()
    overdue = []
    in_progress = []
    completed = 0

    for _, row in confirmed.iterrows():
        stage = row.get("Collaboration Stage", "").strip()
        name = row.get("Name", "").strip() or "(no name)"
        poc = row.get("POC", "").strip()
        start = row.get("_stage_start_parsed")

        if stage in ("Approved for posting", "Posted"):
            completed += 1
            continue

        deadline_days = STAGE_DEADLINES.get(stage)
        if deadline_days is None:
            # Awaiting brief or unknown stage — show as in progress, no deadline
            if stage:
                in_progress.append((name, poc, stage, None, int(row.get("_sheet_row", 0))))
            continue

        if start is None:
            # Has a tracked stage but no start date — show as in progress
            in_progress.append((name, poc, stage, None, int(row.get("_sheet_row", 0))))
            continue

        elapsed = _business_days_between(start, today)
        remaining = deadline_days - elapsed

        if remaining < 0:
            overdue.append((name, poc, stage, abs(remaining), int(row.get("_sheet_row", 0))))
        else:
            in_progress.append((name, poc, stage, remaining, int(row.get("_sheet_row", 0))))

    overdue.sort(key=lambda x: -x[3])  # most overdue first
    in_progress.sort(key=lambda x: (x[3] is None, x[3] if x[3] is not None else 999))  # most urgent first
    return overdue, in_progress, completed


def filter_by_confirm_date(df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    """Filter rows where Confirm Date falls within [start_date, end_date]."""
    if df.empty:
        return df
    mask = df["_confirm_date_parsed"].apply(
        lambda d: d is not None and start_date <= d <= end_date
    )
    return df[mask].copy()


def filter_by_contact_date(df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
    """Filter rows where Date of Contact falls within [start_date, end_date]."""
    if df.empty or "_date_of_contact_parsed" not in df.columns:
        return df
    mask = df["_date_of_contact_parsed"].apply(
        lambda d: d is not None and start_date <= d <= end_date
    )
    return df[mask].copy()


def filter_by_status(df: pd.DataFrame, statuses: list) -> pd.DataFrame:
    """Filter rows by Status values."""
    if not statuses or df.empty:
        return df
    return df[df["Status"].isin(statuses)].copy()


def filter_by_campaign_tag(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """Filter rows by Campaign Tag."""
    if not tag or df.empty:
        return df
    return df[df.get("Campaign Tag", "") == tag].copy()
