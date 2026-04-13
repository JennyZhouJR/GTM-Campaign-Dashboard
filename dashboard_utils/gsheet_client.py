"""Google Sheet read/write layer for the Campaign Dashboard.

Auth strategy:
  - Local dev: reads service account JSON from dao/ folder
  - Streamlit Cloud: reads credentials from st.secrets["gcp_service_account"]
"""

import os
import json
import time

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GSHEET_CRED = os.path.join(BASE_DIR, "dao", "loyal-glass-384620-45dc1d553712.json")
GSHEET_URL = "https://docs.google.com/spreadsheets/d/1hvAJnBUFdQWyLRE2oAwRwB9Z_Ugu6hVUfFjHdBDsSG0/edit"

# New columns to add after the existing 27 (AB-AE)
NEW_HEADERS = {
    28: "24hr Views",          # AB
    29: "Link Signups",        # AC
    30: "Campaign Tag",        # AD
    31: "Retro Notes",         # AE
    32: "Email Message-ID",    # AF
    33: "Last Email Sent",     # AG
    34: "Follow-Up Count",     # AH
}

# Column letters for indices 0-30
COL_LETTERS = [
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
    "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
    "U", "V", "W", "X", "Y", "Z", "AA", "AB", "AC", "AD", "AE",
]


def _get_worksheet():
    """Connect to Google Sheet and return first worksheet.

    Works in two modes:
    1. Local: reads credentials from GSHEET_CRED file path
    2. Streamlit Cloud: reads credentials from st.secrets["gcp_service_account"]
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    # Try Streamlit secrets first (cloud deployment)
    try:
        import streamlit as st
        if "gcp_service_account" in st.secrets:
            cred_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_url(GSHEET_URL)
            return spreadsheet.get_worksheet(0)
    except Exception:
        pass  # Fall through to local file

    # Local fallback: read from file
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSHEET_CRED, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(GSHEET_URL)
    return spreadsheet.get_worksheet(0)


def _retry(fn, retries=3):
    """Retry a gspread call with exponential backoff on rate limit."""
    for attempt in range(retries):
        try:
            return fn()
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429 and attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def ensure_new_columns(ws):
    """Add AB-AE headers if they don't exist yet."""
    row1 = ws.row_values(1)
    if len(row1) >= 34:
        return  # already have all columns
    cells = []
    for col_idx, header in NEW_HEADERS.items():
        if len(row1) < col_idx or (len(row1) >= col_idx and not row1[col_idx - 1].strip()):
            cells.append(gspread.Cell(1, col_idx, header))
    if cells:
        _retry(lambda: ws.update_cells(cells))


def load_dataframe(ws) -> pd.DataFrame:
    """Load all sheet data into a DataFrame with _sheet_row tracking."""
    all_values = _retry(lambda: ws.get_all_values())
    if len(all_values) < 2:
        return pd.DataFrame()

    headers = all_values[0]
    # Pad rows that are shorter than the header row
    n_cols = max(len(headers), 34)  # at least 34 cols (A-AH)
    padded_headers = headers + [NEW_HEADERS.get(i + 1, f"Col_{i}") for i in range(len(headers), n_cols)]

    rows = []
    for i, row in enumerate(all_values[1:], start=2):
        padded = row + [""] * (n_cols - len(row))
        rows.append(padded[:n_cols] + [i])  # append sheet row number

    df = pd.DataFrame(rows, columns=padded_headers[:n_cols] + ["_sheet_row"])
    return df


def update_cell(ws, row: int, col_index: int, value: str):
    """Write a single cell back to Google Sheet. col_index is 1-based."""
    _retry(lambda: ws.update_cell(row, col_index, value))


def batch_update_cells(ws, updates: list):
    """Batch update cells. updates = list of (row, col_1based, value)."""
    if not updates:
        return
    cells = [gspread.Cell(r, c, v) for r, c, v in updates]
    _retry(lambda: ws.update_cells(cells))


def col_letter(index_0based: int) -> str:
    """Return column letter for a 0-based index."""
    if index_0based < len(COL_LETTERS):
        return COL_LETTERS[index_0based]
    return f"Col{index_0based}"
