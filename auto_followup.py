#!/usr/bin/env python3
"""Auto follow-up script — runs daily via GitHub Actions.

Checks all Contacted influencers, sends follow-ups to those who
haven't replied within the configured time windows.

Requires environment variables:
  GMAIL_JENNY_PASSWORD, GMAIL_DORIS_PASSWORD, GMAIL_JIALIN_PASSWORD
  GSHEET_CREDENTIALS (JSON string of service account)
"""

import os
import json
import time
from datetime import datetime

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from dashboard_utils.email_client import check_reply_status, send_followup, REPLY_YES, REPLY_NO, REPLY_UNKNOWN
from dashboard_utils.data_model import COL, HEADER_NAMES

# ─── Config ──────────────────────────────────────────────────────────────────

GSHEET_URL = "https://docs.google.com/spreadsheets/d/1hvAJnBUFdQWyLRE2oAwRwB9Z_Ugu6hVUfFjHdBDsSG0/edit"

# POC email → App Password mapping (from env vars)
POC_ACCOUNTS = {
    "Jenny": {"email": "jenny@jobright.ai", "env_key": "GMAIL_JENNY_PASSWORD"},
    "Doris": {"email": "doris@jobright.ai", "env_key": "GMAIL_DORIS_PASSWORD"},
    "Jialin": {"email": "jialin@jobright.ai", "env_key": "GMAIL_JIALIN_PASSWORD"},
}

# Follow-up rules
FOLLOWUP_1_DAYS = 2  # days after initial outreach
FOLLOWUP_2_DAYS = 1  # days after follow-up #1


def get_worksheet():
    """Connect to Google Sheet using service account credentials from env."""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    cred_json = os.environ.get("GSHEET_CREDENTIALS")
    if not cred_json:
        raise ValueError("GSHEET_CREDENTIALS environment variable not set")

    cred_dict = json.loads(cred_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(GSHEET_URL)
    return spreadsheet.get_worksheet(0)


def load_data(ws):
    """Load sheet data into a DataFrame."""
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        return pd.DataFrame()

    headers = all_values[0]
    n_cols = max(len(headers), 34)
    padded_headers = headers + [f"Col_{i}" for i in range(len(headers), n_cols)]

    rows = []
    for i, row in enumerate(all_values[1:], start=2):
        padded = row + [""] * (n_cols - len(row))
        rows.append(padded[:n_cols] + [i])

    df = pd.DataFrame(rows, columns=padded_headers[:n_cols] + ["_sheet_row"])
    return df


def run_followups():
    """Main function: check all contacted people and send follow-ups."""
    print(f"[{datetime.now()}] Starting auto follow-up check...")

    ws = get_worksheet()
    df = load_data(ws)

    if df.empty:
        print("No data found.")
        return

    # Get column names
    status_col = HEADER_NAMES[COL["status"]]
    contact_col = HEADER_NAMES[COL["email"]]  # E column = Contact/email
    name_col = HEADER_NAMES[COL["name"]]
    poc_col = HEADER_NAMES[COL["poc"]]
    msg_id_col = "Email Message-ID"
    last_sent_col = "Last Email Sent"
    fu_count_col = "Follow-Up Count"

    # Filter: Status = Contacted, has Message-ID, has email
    contacted = df[df[status_col].str.strip() == "Contacted"].copy()
    contacted = contacted[contacted[msg_id_col].str.strip() != ""]
    contacted = contacted[contacted[contact_col].str.strip() != ""]

    if contacted.empty:
        print("No contacted influencers with tracked emails.")
        return

    now = datetime.now()
    sent_count = 0
    skipped_replied = 0
    skipped_maxed = 0
    errors = 0

    for _, row in contacted.iterrows():
        name = (row[name_col].strip().split()[0] if row[name_col].strip() else "there")
        to_email = row[contact_col].strip()
        poc = row[poc_col].strip()
        msg_id = row[msg_id_col].strip()
        last_sent_str = row[last_sent_col].strip()
        fu_count = int(row[fu_count_col].strip() or "0")
        sheet_row = int(row["_sheet_row"])

        # Already sent both follow-ups
        if fu_count >= 2:
            skipped_maxed += 1
            continue

        # Parse last sent date
        try:
            last_sent = datetime.strptime(last_sent_str, "%Y-%m-%d %H:%M")
        except (ValueError, AttributeError):
            continue

        days_since = (now - last_sent).days

        # Determine if follow-up is due
        if fu_count == 0 and days_since < FOLLOWUP_1_DAYS:
            continue
        if fu_count == 1 and days_since < FOLLOWUP_2_DAYS:
            continue

        # Get POC account
        poc_account = POC_ACCOUNTS.get(poc)
        if not poc_account:
            print(f"  ⚠️ No account configured for POC '{poc}', skipping {name}")
            continue

        app_password = os.environ.get(poc_account["env_key"])
        if not app_password:
            print(f"  ⚠️ No password for {poc} ({poc_account['env_key']}), skipping {name}")
            continue

        sender_email = poc_account["email"]

        # Check if they replied (tri-state: YES/NO/UNKNOWN)
        reply_status = check_reply_status(sender_email, app_password, msg_id)
        if reply_status == REPLY_YES:
            skipped_replied += 1
            print(f"  ⏭️ {name} — already replied, skipping")
            continue
        if reply_status == REPLY_UNKNOWN:
            # Fail-closed: don't send if we couldn't verify — avoids spamming
            # people who already replied when Gmail IMAP is having issues.
            print(f"  ⚠️ {name} — IMAP check failed, skipping this run (will retry tomorrow)")
            continue

        # Send follow-up
        followup_num = fu_count + 1
        try:
            send_followup(
                sender_email, app_password,
                to_email, name, poc, msg_id, followup_num,
            )
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            ws.update_cell(sheet_row, COL["last_email_sent"] + 1, now_str)
            ws.update_cell(sheet_row, COL["followup_count"] + 1, str(followup_num))
            sent_count += 1
            print(f"  ✅ {name} — Follow-Up #{followup_num} sent from {sender_email}")
            time.sleep(1)  # rate limit
        except Exception as e:
            errors += 1
            print(f"  ❌ {name} — Failed: {e}")

    print(f"\n[{datetime.now()}] Done!")
    print(f"  Sent: {sent_count}")
    print(f"  Skipped (replied): {skipped_replied}")
    print(f"  Skipped (max follow-ups): {skipped_maxed}")
    print(f"  Errors: {errors}")


if __name__ == "__main__":
    run_followups()
