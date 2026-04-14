#!/usr/bin/env python3
"""Sync Amplitude signups data to Google Sheet.

Usage (run from Claude Code or terminal):
    python3 sync_amplitude.py

Reads each confirmed influencer's Tracking Link, extracts utm_campaign,
matches against Amplitude data, and writes signups to Link Signups column.

Amplitude data must be provided as a dict (updated manually from MCP query
or via API). See AMPLITUDE_SIGNUPS below.
"""

import os
import sys
from urllib.parse import urlparse, parse_qs

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dashboard_utils.gsheet_client import _get_worksheet, load_dataframe, batch_update_cells
from dashboard_utils.data_model import COL, HEADER_NAMES, prepare_dataframe

# ─── Amplitude data (update this from MCP query results) ────────────────────
# Run the Amplitude MCP query to get fresh data, then paste here.
# Key = utm_campaign value (case-sensitive), Value = total signups

AMPLITUDE_SIGNUPS = {
    "owen": 16,
    "jesscia": 15,
    "InstagramYudiJ": 11,
    "ben": 7,
    "DavidChen": 1,
    "emma": 1,
    "gary": 1,
    "skyler": 1,
}


def extract_utm_campaign(tracking_link: str) -> str:
    """Extract utm_campaign parameter from a tracking link URL."""
    if not tracking_link or not isinstance(tracking_link, str):
        return ""
    tracking_link = tracking_link.strip()
    if not tracking_link:
        return ""
    try:
        # Handle links that might not have a scheme
        if not tracking_link.startswith("http"):
            tracking_link = "https://" + tracking_link
        parsed = urlparse(tracking_link)
        params = parse_qs(parsed.query)
        campaign = params.get("utm_campaign", [""])[0]
        return campaign
    except Exception:
        return ""


def sync_signups():
    """Main sync function."""
    print("Loading Google Sheet data...")
    ws = _get_worksheet()
    df = load_dataframe(ws)
    df = prepare_dataframe(df)

    # Filter to confirmed influencers with tracking links
    confirmed = df[df["Status"] == "Confirm"].copy()
    tracking_col = "Tracking Link"

    if tracking_col not in confirmed.columns:
        print("No Tracking Link column found.")
        return

    updates = []
    matched = 0
    unmatched = []

    for _, row in confirmed.iterrows():
        link = row.get(tracking_col, "")
        utm_campaign = extract_utm_campaign(link)

        if not utm_campaign:
            continue

        # Case-insensitive matching
        signups = None
        for key, val in AMPLITUDE_SIGNUPS.items():
            if key.lower() == utm_campaign.lower():
                signups = val
                break

        sheet_row = int(row["_sheet_row"])
        name = row.get("Name", "unknown")

        if signups is not None:
            updates.append((sheet_row, COL["link_signups"] + 1, str(signups)))
            matched += 1
            print(f"  ✅ {name} (utm_campaign={utm_campaign}) → {signups} signups")
        else:
            unmatched.append((name, utm_campaign))

    if updates:
        print(f"\nWriting {len(updates)} updates to Sheet...")
        batch_update_cells(ws, updates)
        print("Done!")
    else:
        print("No matches found.")

    if unmatched:
        print(f"\n⚠️ {len(unmatched)} influencers with tracking links but no Amplitude data:")
        for name, campaign in unmatched:
            print(f"  - {name} (utm_campaign={campaign})")

    print(f"\nSummary: {matched} matched, {len(unmatched)} unmatched")


if __name__ == "__main__":
    sync_signups()
