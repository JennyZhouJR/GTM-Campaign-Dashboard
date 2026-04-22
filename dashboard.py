#!/usr/bin/env python3
"""
Campaign Dashboard — Influencer Sourcing Workflow
Run: streamlit run dashboard.py
Version: 2026-04-13
"""

import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta

from dashboard_utils.gsheet_client import (
    _get_worksheet, ensure_new_columns, load_dataframe,
    update_cell, batch_update_cells,
)
from dashboard_utils.data_model import (
    COL, HEADER_NAMES, STATUS_OPTIONS, COLLAB_STAGE_OPTIONS,
    CONTRACT_OPTIONS, PAYMENT_PROGRESS_OPTIONS, CAMPAIGN_TAG_OPTIONS,
    STAGE_DEADLINES,
    PIPELINE_DISPLAY_COLS, CONTENT_DISPLAY_COLS,
    PAYMENT_PERF_DISPLAY_COLS,
    FOLLOWER_BUCKET_ORDER,
    prepare_dataframe, filter_by_contact_date,
    filter_by_status, parse_date, get_timeline_status,
    follower_bucket, parse_sheet_datetime, parse_sheet_date,
    compute_overall_score, get_today_la,
)
from dashboard_utils.charts import (
    status_distribution_pie, collab_stage_detail, collab_stage_breakdown,
    er_histogram, followers_vs_er_scatter, cost_vs_views_scatter,
    daily_outreach_chart,
    COLLAB_STAGE_COLORS, COLLAB_STAGE_ORDER,
    POC_COLORS,
)

# ─── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Campaign Dashboard", page_icon="\U0001f4ca", layout="wide")

# ─── Password gate ────────────────────────────────────────────────────────────

def _check_password() -> bool:
    """Returns True if authenticated. st.stop() must be called by caller."""

    # No secret configured → local dev, skip auth
    app_pw = st.secrets.get("app_password", None)
    if not app_pw:
        return True

    # Already authenticated this session
    if st.session_state.get("_pw_ok"):
        return True

    # Show login UI
    st.markdown("""
    <div style="max-width:380px; margin:80px auto; text-align:center;">
        <div style="font-size:2.5em; margin-bottom:8px;">📊</div>
        <h2 style="font-family:'DM Sans',sans-serif; color:#1F2937; margin-bottom:4px;">Campaign Dashboard</h2>
        <p style="color:#6B7280; font-size:0.9em; margin-bottom:28px;">Internal use only · Enter password to continue</p>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        pw = st.text_input("Password", type="password",
                           placeholder="Enter password...", label_visibility="collapsed")
        if st.button("Continue →", use_container_width=True, type="primary"):
            if pw == app_pw:
                st.session_state["_pw_ok"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
    return False

# Gate: stop the entire script if not authenticated
if not _check_password():
    st.stop()

# ─── Playful confetti-inspired CSS ───────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    /* ─── Design tokens ────────────────────────────────────────── */
    :root {
        --bg: #ffffff;
        --bg-alt: #f6f5f4;
        --bg-subtle: #fafaf9;
        --text: rgba(0,0,0,0.95);
        --text-2: #615d59;
        --text-3: #a39e98;
        --border: rgba(0,0,0,0.1);
        --border-subtle: rgba(0,0,0,0.05);

        --blue: #0075de;
        --navy: #213183;
        --teal: #2a9d99;
        --green: #1aae39;
        --orange: #dd5b00;
        --pink: #ff64c8;
        --red: #dc2626;
        --amber: #d97706;

        --shadow-card: rgba(0,0,0,0.04) 0 4px 18px,
                       rgba(0,0,0,0.027) 0 2.025px 7.847px,
                       rgba(0,0,0,0.02) 0 0.8px 2.925px,
                       rgba(0,0,0,0.01) 0 0.175px 1.04px;
        --shadow-subtle: rgba(0,0,0,0.02) 0 1px 3px;
    }

    /* ─── Global font ──────────────────────────────────────────── */
    html, body {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        font-feature-settings: 'lnum', 'locl';
        -webkit-font-smoothing: antialiased;
    }

    /* ─── Page container ───────────────────────────────────────── */
    .block-container { padding-top: 1.4rem; padding-bottom: 2rem; }

    /* ─── Metric cards (used by st.metric) ─────────────────────── */
    [data-testid="stMetric"] {
        background: var(--bg);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 18px 20px;
        box-shadow: var(--shadow-card);
        transition: box-shadow 0.2s ease;
    }
    [data-testid="stMetric"]:hover {
        box-shadow: rgba(0,0,0,0.06) 0 6px 22px, rgba(0,0,0,0.03) 0 2.5px 9px;
    }

    /* ─── Caption ─────────────────────────────────────────────── */
    .stCaption, [data-testid="stCaption"] {
        color: var(--text-2) !important;
        font-size: 13px !important;
    }

    /* ─── Horizontal rule ─────────────────────────────────────── */
    hr {
        border: none !important;
        border-top: 1px solid var(--border-subtle) !important;
        margin: 24px 0 !important;
    }

    /* ─── Pills navigation (top nav) ──────────────────────────── */
    div[data-testid="stPills"] button {
        border-radius: 5px !important;
        padding: 6px 14px !important;
        font-weight: 500 !important;
        font-size: 14px !important;
        border: 1px solid transparent !important;
        color: var(--text-2) !important;
        background: transparent !important;
        transition: background 120ms !important;
    }
    div[data-testid="stPills"] button:hover {
        background: rgba(0,0,0,0.04) !important;
        border-color: transparent !important;
    }
    div[data-testid="stPills"] button[aria-checked="true"] {
        background: rgba(0,0,0,0.05) !important;
        color: var(--text) !important;
        border-color: transparent !important;
        font-weight: 600 !important;
    }

    /* ─── POC badge pills (Today's Activity) ──────────────────── */
    .activity-strip {
        display: flex;
        align-items: center;
        gap: 14px;
        flex-wrap: wrap;
        padding: 14px 18px;
        background: var(--bg-alt);
        border: 1px solid var(--border-subtle);
        border-radius: 12px;
        margin: 4px 0 20px;
    }
    .activity-label {
        font-weight: 600;
        font-size: 11px;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        color: var(--text-2);
    }
    .poc-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 12px;
        border-radius: 9999px;
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.125px;
        font-variant-numeric: tabular-nums;
    }
    .poc-badge .poc-dot {
        width: 6px; height: 6px; border-radius: 50%;
        flex-shrink: 0;
    }
    .poc-badge.poc-jenny  { background: #e8efff; color: #3b5bdb; }
    .poc-badge.poc-jenny .poc-dot { background: #3b5bdb; }
    .poc-badge.poc-doris  { background: #fff4e6; color: #c6590e; }
    .poc-badge.poc-doris .poc-dot { background: #c6590e; }
    .poc-badge.poc-jialin { background: #ffe3f2; color: #c13c91; }
    .poc-badge.poc-jialin .poc-dot { background: #c13c91; }
    .poc-badge.poc-falida { background: #daf0ef; color: #0f7b7b; }
    .poc-badge.poc-falida .poc-dot { background: #0f7b7b; }
    .poc-badge.poc-other  { background: #eef0f3; color: #5c5f66; }
    .poc-badge.poc-other .poc-dot { background: #5c5f66; }
    .poc-badge.badge-overdue { background: #fee4e2; color: #b42318; }
    .poc-badge.badge-overdue .poc-dot { background: #b42318; }

    /* Today subtle helper text */
    .activity-total {
        font-size: 13px;
        color: var(--text-2);
        margin-left: auto;
        font-weight: 500;
    }

    /* ─── Section heading tighten ─────────────────────────────── */
    .section-heading {
        font-size: 18px;
        font-weight: 700;
        letter-spacing: -0.2px;
        color: var(--text);
        margin: 20px 0 2px;
    }
    .section-caption {
        font-size: 13px;
        color: var(--text-2);
        margin-bottom: 14px;
    }

    /* ─── Section header with inline scope badge ────────────── */
    .section-header-row {
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
        margin: 24px 0 8px;
        padding: 0;
    }
    .section-header-row h2 {
        margin: 0 !important;
        padding: 0 !important;
        font-size: 20px !important;
        font-weight: 700 !important;
        letter-spacing: -0.25px !important;
        line-height: 1.3 !important;
        color: var(--text) !important;
    }
    .section-header-row h3 {
        margin: 0 !important;
        padding: 0 !important;
        font-size: 14px !important;
        font-weight: 600 !important;
        line-height: 1.3 !important;
        color: var(--text) !important;
        letter-spacing: -0.1px !important;
    }

    /* ─── Scope badges — make filter scope visible inline ────── */
    .scope-badge {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 2px 8px;
        border-radius: 5px;
        font-size: 11px;
        font-weight: 500;
        letter-spacing: 0.1px;
        color: var(--text-2);
        background: rgba(0,0,0,0.04);
        border: 1px solid transparent;
        font-variant-numeric: tabular-nums;
        vertical-align: middle;
    }
    /* Scope variants — color indicates how widget is filtered */
    .scope-badge.scope-campaign {
        color: #0060b3;
        background: rgba(0,117,222,0.08);
    }
    .scope-badge.scope-date {
        color: var(--text-2);
        background: rgba(0,0,0,0.04);
    }
    .scope-badge.scope-global {
        color: var(--text-2);
        background: rgba(0,0,0,0.04);
    }
    .scope-badge.scope-today {
        color: #0f7b2e;
        background: rgba(26,174,57,0.10);
    }
    .scope-badge.scope-independent {
        color: #a16207;
        background: rgba(245,158,11,0.10);
    }

    /* ─── Kanban cards (kept, polished) ───────────────────────── */
    .kanban-card {
        background: var(--bg);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 10px 12px;
        margin: 6px 0;
        font-size: 13px;
        box-shadow: var(--shadow-subtle);
        transition: all 0.15s ease;
    }
    .kanban-card:hover {
        border-color: rgba(0,0,0,0.15);
        box-shadow: rgba(0,0,0,0.05) 0 2px 8px;
    }
    .kanban-card .name { font-weight: 600; color: var(--text); }
    .kanban-card .poc { color: var(--text-2); font-size: 12px; margin-top: 2px; font-weight: 500; }
    .stage-header {
        font-weight: 700;
        font-size: 13px;
        padding: 8px 0 6px;
        margin-bottom: 6px;
        border-bottom: 2px solid var(--border);
        color: var(--text);
        letter-spacing: -0.1px;
    }

    /* ─── POC text color utilities (still used elsewhere) ─────── */
    .poc-jenny  { color: #3b5bdb; }
    .poc-doris  { color: #c6590e; }
    .poc-jialin { color: #c13c91; }
    .poc-falida { color: #0f7b7b; }
    .poc-other  { color: #5c5f66; }

    /* ─── Data editor/frame ───────────────────────────────────── */
    [data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid var(--border-subtle);
    }

    /* ─── Expander polish ────────────────────────────────────── */
    [data-testid="stExpander"] {
        border: 1px solid var(--border-subtle) !important;
        border-radius: 8px !important;
        background: var(--bg) !important;
    }

    /* ─── Primary button ─────────────────────────────────────── */
    [data-testid="baseButton-primary"] {
        background: var(--text) !important;
        border: none !important;
        border-radius: 6px !important;
        font-weight: 500 !important;
    }

    /* ─── Sidebar subtle polish ──────────────────────────────── */
    [data-testid="stSidebar"] {
        background: var(--bg-alt);
        border-right: 1px solid var(--border-subtle);
    }
    [data-testid="stSidebar"] h1 { font-size: 18px !important; }

    /* ─── Select / input polish ──────────────────────────────── */
    [data-baseweb="select"] > div {
        border-radius: 6px !important;
    }
</style>
""", unsafe_allow_html=True)


# ─── Session state & data loading ─────────────────────────────────────────────

def load_data(force=False):
    if force or "df" not in st.session_state or "ws" not in st.session_state:
        with st.spinner("Loading data from Google Sheet..."):
            ws = _get_worksheet()
            ensure_new_columns(ws)
            df = load_dataframe(ws)
            df = prepare_dataframe(df)
            st.session_state["ws"] = ws
            st.session_state["df"] = df
    return st.session_state["df"]


# ─── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.title("Campaign Filter")

if st.sidebar.button("\U0001f504 Refresh Data", use_container_width=True):
    load_data(force=True)
    st.rerun()

df_all = load_data()

# Campaign Tag first (primary selector)
st.sidebar.markdown("---")
st.sidebar.subheader("Campaign")
if "Campaign Tag" in df_all.columns:
    data_tags = set(t.strip() for t in df_all["Campaign Tag"].unique() if t.strip())
    all_month_tags = [t for t in CAMPAIGN_TAG_OPTIONS if t]
    all_tags = sorted(set(list(data_tags) + all_month_tags), key=lambda t: CAMPAIGN_TAG_OPTIONS.index(t) if t in CAMPAIGN_TAG_OPTIONS else 99)
    # Default to most used Campaign Tag in the data
    if data_tags:
        tag_counts = df_all["Campaign Tag"].value_counts()
        tag_counts = tag_counts[tag_counts.index.str.strip() != ""]
        default_tag = tag_counts.index[0] if not tag_counts.empty else "(All)"
        default_idx = (["(All)"] + all_tags).index(default_tag) if default_tag in all_tags else 0
    else:
        default_idx = 0
    selected_tag = st.sidebar.selectbox("Campaign Tag", ["(All)"] + all_tags, index=default_idx)
else:
    selected_tag = "(All)"

# Date range (optional)
st.sidebar.markdown("---")
use_date_filter = st.sidebar.checkbox("Filter by date range", value=False)
if use_date_filter:
    _today = date.today()
    _default_start = _today - timedelta(days=14)
    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input("From", value=_default_start)
    end_date = col2.date_input("To", value=_today)

st.sidebar.markdown("---")
all_statuses = sorted(set(s.strip() for s in df_all["Status"].unique() if s.strip()))
selected_statuses = st.sidebar.multiselect("Status Filter", options=all_statuses, default=[])

# ─── Data filtering ───────────────────────────────────────────────────────────
# Campaign Tag = primary filter. Date range = optional secondary filter.

df_filtered = df_all.copy()

# Campaign Tag filter
if selected_tag != "(All)" and "Campaign Tag" in df_filtered.columns:
    df_filtered = df_filtered[df_filtered["Campaign Tag"] == selected_tag]

# Date range filter (only when checkbox is checked)
if use_date_filter:
    df_filtered = filter_by_contact_date(df_filtered, start_date, end_date)
    df_by_contact = filter_by_contact_date(df_all, start_date, end_date)
else:
    df_by_contact = df_all

confirmed = df_filtered[df_filtered["Status"] == "Confirm"]

# Status filter
if selected_statuses:
    df_filtered = filter_by_status(df_filtered, selected_statuses)

st.sidebar.markdown("---")
st.sidebar.metric("In view", len(df_filtered))
st.sidebar.caption(f"Confirmed: {len(confirmed)}")


# ─── Helpers ──────────────────────────────────────────────────────────────────

# Map of Sheet column names → user-facing display labels.
# Use this to fix typos in the Google Sheet schema without renaming columns.
DISPLAY_LABELS = {
    "Senority": "Seniority",  # Sheet typo — don't rename schema, just display correctly
}


def _col_label(col_name):
    """Return user-facing label for a column, applying DISPLAY_LABELS overrides."""
    return DISPLAY_LABELS.get(col_name, col_name)


def make_column_config(editable_cols):
    config = {}
    for col_name, col_type in editable_cols.items():
        label = _col_label(col_name)
        if col_type == "select_status":
            config[col_name] = st.column_config.SelectboxColumn(label, options=STATUS_OPTIONS)
        elif col_type == "select_collab":
            config[col_name] = st.column_config.SelectboxColumn(label, options=COLLAB_STAGE_OPTIONS)
        elif col_type == "select_payment":
            config[col_name] = st.column_config.SelectboxColumn(label, options=PAYMENT_PROGRESS_OPTIONS)
        elif col_type == "select_campaign":
            config[col_name] = st.column_config.SelectboxColumn(label, options=CAMPAIGN_TAG_OPTIONS)
        elif col_type == "select_contract":
            config[col_name] = st.column_config.SelectboxColumn(label, options=CONTRACT_OPTIONS)
        elif col_type == "text":
            config[col_name] = st.column_config.TextColumn(label)
        elif col_type == "link":
            config[col_name] = st.column_config.LinkColumn(label, display_text="📝 Edit")
    config["Profile Link"] = st.column_config.LinkColumn("Profile Link")
    return config


def header_to_col_index(header_name):
    for i, h in enumerate(HEADER_NAMES):
        if h == header_name:
            return i
    return -1


def show_editable_table(df_view, display_cols, editable_cols, key_prefix):
    available = [c for c in display_cols if c in df_view.columns]
    if not available:
        return

    # Before rendering new editor, save any edits from the PREVIOUS render.
    # We stored the previous df_view's _sheet_row mapping in session state.
    prev_key = f"{key_prefix}_prev_rows"
    edit_key = f"{key_prefix}_editor"
    changes = st.session_state.get(edit_key, {})
    edited_rows = changes.get("edited_rows", {})
    prev_rows = st.session_state.get(prev_key, {})
    if edited_rows and prev_rows:
        updates = []
        edit_details = []  # (sheet_row, col_name, new_value) for memory update
        payment_email_tasks = []  # [(sheet_row, ...)] for payment confirmation emails
        for row_idx_str, row_changes in edited_rows.items():
            sheet_row = prev_rows.get(int(row_idx_str))
            if sheet_row is None:
                continue
            for col_name, new_value in row_changes.items():
                col_idx = header_to_col_index(col_name)
                if col_idx >= 0:
                    val = str(new_value) if new_value is not None else ""
                    updates.append((sheet_row, col_idx + 1, val))
                    edit_details.append((sheet_row, col_name, val))
                    # Auto-record Stage Start Date when Collaboration Stage changes
                    if col_name == "Collaboration Stage" and val.strip():
                        today_str = date.today().strftime("%m/%d/%Y")
                        updates.append((sheet_row, COL["stage_start_date"] + 1, today_str))
                        edit_details.append((sheet_row, "Stage Start Date", today_str))
                    # Auto-record Contract Signed Date + auto-start production when Contract Status → Signed
                    if col_name == "Contract Status" and val.strip() == "Signed":
                        today_str = date.today().strftime("%m/%d/%Y")
                        # 1. Record contract signed date
                        updates.append((sheet_row, COL["contract_signed_date"] + 1, today_str))
                        edit_details.append((sheet_row, "Contract Signed Date", today_str))
                        # 2. Auto-set Collaboration Stage → Script in progress
                        updates.append((sheet_row, COL["collab_stage"] + 1, "Script in progress"))
                        edit_details.append((sheet_row, "Collaboration Stage", "Script in progress"))
                        # 3. Record Stage Start Date (3-day countdown starts now)
                        updates.append((sheet_row, COL["stage_start_date"] + 1, today_str))
                        edit_details.append((sheet_row, "Stage Start Date", today_str))
                    # Queue payment confirmation email when Payment Progress → Paid
                    if col_name == "Payment Progress" and val.strip() == "Paid":
                        payment_email_tasks.append(sheet_row)
        if updates:
            try:
                batch_update_cells(st.session_state["ws"], updates)
                # Update in-memory DataFrame so changes show immediately
                if "df" in st.session_state:
                    df_mem = st.session_state["df"]
                    for s_row, col_name, val in edit_details:
                        mask = df_mem["_sheet_row"] == s_row
                        if mask.any():
                            df_mem.loc[mask, col_name] = val
                    # Re-run prepare_dataframe so parsed numeric/date columns
                    # (e.g. _views_24hr_num, _price_num, _post_er_num) stay in sync
                    # with the edited string values — otherwise CPM / ER / vs-baseline
                    # computations show stale data until user manually refreshes.
                    st.session_state["df"] = prepare_dataframe(df_mem)
                st.toast(f"Saved {len(updates)} change(s)")
            except Exception as e:
                st.error(f"Save failed: {e}")

            # Send payment confirmation emails (after successful save)
            if payment_email_tasks and st.session_state.get("gmail_connected"):
                try:
                    from dashboard_utils.email_client import send_payment_confirmation as _send_pay_confirm
                    import re as _re
                    gmail_email = st.session_state["gmail_email"]
                    gmail_pw = st.session_state["gmail_password"]
                    sender_name = gmail_email.split("@")[0].capitalize()
                    df_mem = st.session_state.get("df", pd.DataFrame())
                    for s_row in payment_email_tasks:
                        row_data = df_mem[df_mem["_sheet_row"] == s_row]
                        if row_data.empty:
                            continue
                        row_data = row_data.iloc[0]
                        # ENFORCE: only send if connected Gmail matches this row's POC
                        row_poc = (row_data.get("POC", "") or "").strip()
                        if row_poc != sender_name:
                            st.warning(f"⚠️ Payment saved for {row_data.get('Name', '?')} but email NOT sent — this row's POC is '{row_poc}' but you're connected as '{sender_name}'. Connect as {row_poc}@jobright.ai to send.")
                            continue
                        to_email = (row_data.get("Contact", "") or "").strip()
                        name = (row_data.get("Name", "") or "").strip()
                        price = (row_data.get("Price（$)", "") or "").strip()
                        if not to_email or not name:
                            continue
                        # Clean price for display (extract number)
                        price_match = _re.search(r'[\d,]+(?:\.\d+)?', price)
                        amount = price_match.group(0) if price_match else price
                        try:
                            _send_pay_confirm(
                                gmail_email, gmail_pw, to_email, name, sender_name, amount,
                            )
                            st.toast(f"💰 Payment confirmation sent to {name}")
                        except Exception as e:
                            st.warning(f"Payment saved but email failed for {name}: {e}")
                except ImportError:
                    st.warning("Payment saved but email module not available. Please redeploy.")
            elif payment_email_tasks and not st.session_state.get("gmail_connected"):
                st.info("💡 Connect Gmail in Pipeline tab to auto-send payment confirmation emails.")

    # Store current df_view's row index -> sheet_row mapping for next cycle
    st.session_state[prev_key] = {i: int(row["_sheet_row"]) for i, (_, row) in enumerate(df_view.iterrows())}

    col_config = make_column_config(editable_cols)
    disabled = [c for c in available if c not in editable_cols]
    st.data_editor(
        df_view[available], column_config=col_config, disabled=disabled,
        use_container_width=True, num_rows="fixed", key=f"{key_prefix}_editor",
    )


POC_HEX = {"jenny": "#748FFC", "doris": "#FF922B", "jialin": "#F06595", "falida": "#63E6BE"}


def poc_class(poc):
    p = poc.strip().lower()
    return f"poc-{p}" if p in POC_HEX else "poc-other"


def poc_color(poc):
    return POC_HEX.get(poc.strip().lower(), "#B197FC")


# ─── Google Sheet deep-link helper ─────────────────────────────────────────
# Lets users jump from any dashboard view to the corresponding row in the
# campaign tracker Sheet with a single click.
SHEET_BASE_URL = "https://docs.google.com/spreadsheets/d/1hvAJnBUFdQWyLRE2oAwRwB9Z_Ugu6hVUfFjHdBDsSG0/edit"


def sheet_row_link(sheet_row) -> str:
    """Return deep link to a specific row in the campaign tracker Sheet."""
    try:
        return f"{SHEET_BASE_URL}#gid=0&range=A{int(sheet_row)}"
    except (TypeError, ValueError):
        return SHEET_BASE_URL


def sheet_row_icon(sheet_row, size_em=0.85, margin_left_px=3) -> str:
    """Return a small grey 📝 link HTML element pointing at a specific Sheet row."""
    link = sheet_row_link(sheet_row)
    return (
        f'<a href="{link}" target="_blank" '
        f'style="color:#9CA3AF; text-decoration:none; font-size:{size_em}em; '
        f'margin-left:{margin_left_px}px;" title="Open in Google Sheet">📝</a>'
    )


def render_delivery_progress(stages_series, show_total=True):
    """Return HTML for the Confirm → Post delivery progress bar.

    `stages_series` is a pandas Series of Collaboration Stage strings.
    Shared between Payment & Performance and Report tabs.
    """
    _stages = stages_series.str.strip()
    _n_total = len(stages_series)
    _n_posted = ((_stages == "Posted") | (_stages == "Approved for posting")).sum()
    _n_production = ((_stages != "") & (_stages != "Posted") & (_stages != "Approved for posting")).sum()
    _n_pending = _n_total - _n_posted - _n_production

    _pct_posted = (_n_posted / _n_total * 100) if _n_total > 0 else 0
    _pct_prod = (_n_production / _n_total * 100) if _n_total > 0 else 0
    _pct_pend = (_n_pending / _n_total * 100) if _n_total > 0 else 0

    total_span = (
        f'&nbsp;&nbsp;·&nbsp;&nbsp;Total: {_n_total}' if show_total else ''
    )
    return (
        f'<div style="margin-bottom:6px; font-size:0.82em; color:#6B7280;">'
        f'<span style="color:#63E6BE; font-weight:600;">📦 Posted: {_n_posted}</span>'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;'
        f'<span style="color:#3B82F6; font-weight:600;">🎬 In production: {_n_production}</span>'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;'
        f'<span style="color:#F59E0B; font-weight:600;">📝 Pending: {_n_pending}</span>'
        f'{total_span}'
        f'</div>'
        f'<div style="display:flex; height:12px; border-radius:6px; overflow:hidden; background:#F3F4F6; margin-bottom:16px;">'
        f'<div style="width:{_pct_posted}%; background:#63E6BE;"></div>'
        f'<div style="width:{_pct_prod}%; background:#3B82F6;"></div>'
        f'<div style="width:{_pct_pend}%; background:#F59E0B;"></div>'
        f'</div>'
    )


def render_kanban(df_src, show_poc_prefix=True):
    """Render collaboration stage kanban cards."""
    breakdown = collab_stage_breakdown(df_src)
    if not breakdown:
        st.info("No Collaboration Stage data yet.")
        return
    cols = st.columns(len(breakdown))
    for i, (stage, people) in enumerate(breakdown.items()):
        stage_color = COLLAB_STAGE_COLORS.get(stage, "#D5CFC7")
        with cols[i]:
            st.markdown(
                f'<div style="font-weight:700; font-size:0.85em; color:#374151; '
                f'padding:8px 0 6px; margin-bottom:8px; text-transform:uppercase; '
                f'letter-spacing:0.02em; border-bottom:3px solid {stage_color};">'
                f'{stage} ({len(people)})</div>', unsafe_allow_html=True)
            chips = ""
            for person in people:
                # Backward-compatible unpack: support both (name, poc) and (name, poc, sheet_row)
                if len(person) == 3:
                    name, poc, sr = person
                else:
                    name, poc = person[0], person[1]
                    sr = 0
                pc = poc_color(poc)
                chips += (
                    f'<div style="display:flex; align-items:center; gap:6px; padding:3px 0; font-size:0.82em;">'
                    f'<span style="width:8px; height:8px; border-radius:50%; background:{pc}; flex-shrink:0; display:inline-block;"></span>'
                    f'<span style="color:#1F2937; font-weight:500;">{name or "(no name)"}</span>'
                    f'{sheet_row_icon(sr)}'
                    f'</div>'
                )
            st.markdown(chips, unsafe_allow_html=True)
    # POC color legend
    legend_items = "".join(
        f'<span style="display:inline-flex; align-items:center; gap:4px; margin-right:16px; font-size:0.78em; color:#6B7280;">'
        f'<span style="width:8px; height:8px; border-radius:50%; background:{c}; display:inline-block;"></span>{p}</span>'
        for p, c in POC_HEX.items()
    )
    st.markdown(f'<div style="margin-top:8px;">{legend_items}</div>', unsafe_allow_html=True)


# ─── Title ────────────────────────────────────────────────────────────────────

st.title("Campaign Dashboard")
_subtitle_parts = []
if use_date_filter:
    _subtitle_parts.append(f"{start_date.strftime('%m/%d')} \u2013 {end_date.strftime('%m/%d')}")
if selected_tag != "(All)":
    _subtitle_parts.append(f"{selected_tag} Campaign")
_subtitle_parts.append(f"{len(confirmed)} confirmed  \u00b7  {len(df_filtered)} in view")
st.caption("  \u00b7  ".join(_subtitle_parts))

# ─── Navigation ──────────────────────────────────────────────────────────────

NAV_OPTIONS = ["Overview", "Pipeline", "Outreach", "Content & Delivery", "Payment", "Report"]

nav = st.pills(
    "nav", NAV_OPTIONS,
    label_visibility="collapsed", key="nav",
    default="Overview",
)
# Fallback if pills returns None
if nav is None:
    nav = "Overview"

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# Overview
# ═══════════════════════════════════════════════════════════════════════════════

if nav == "Overview":
    if df_by_contact.empty:
        st.info("No influencers found for the selected date range.")
    else:
        # Each section below carries a scope badge indicating how filters apply.
        # This replaces the "How filters work" expander — scope should be visible
        # inline, not hidden in a collapse.

        # Confirmed follows Campaign Tag filter; Contacted is always the full date range
        ov_confirmed = df_filtered[df_filtered["Status"] == "Confirm"]

        # ─── Today's Activity ────────────────────────────────────────────
        _today_local = get_today_la()

        today_contacts = df_all[df_all["_date_of_contact_parsed"] == _today_local]
        today_by_poc = today_contacts["POC"].value_counts()
        today_total = len(today_contacts)

        overdue_list_ov, _, _ = get_timeline_status(df_filtered)
        overdue_count = len(overdue_list_ov)

        st.markdown(
            '<div class="section-header-row">'
            "<h2>Today's Activity</h2>"
            '<span class="scope-badge scope-today">🗓️ Today · All POCs</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        if today_total > 0:
            poc_badges = ""
            for poc_name, count in today_by_poc.items():
                if not poc_name.strip():
                    continue
                _poc_class = poc_class(poc_name)
                poc_badges += (
                    f'<span class="poc-badge {_poc_class}">'
                    f'<span class="poc-dot"></span>'
                    f'{poc_name} +{count}'
                    f'</span>'
                )
            overdue_html = ""
            if overdue_count > 0:
                overdue_html = (
                    f'<span class="poc-badge badge-overdue">'
                    f'<span class="poc-dot"></span>'
                    f'{overdue_count} overdue'
                    f'</span>'
                )
            st.markdown(
                f'<div class="activity-strip">'
                f'<span class="activity-label">Today</span>'
                f'{poc_badges}'
                f'{overdue_html}'
                f'<span class="activity-total">{today_total} new contacts</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            overdue_html = ""
            if overdue_count > 0:
                overdue_html = (
                    f'<span class="poc-badge badge-overdue" style="margin-left:12px;">'
                    f'<span class="poc-dot"></span>'
                    f'{overdue_count} overdue'
                    f'</span>'
                )
            st.markdown(
                f'<div class="activity-strip">'
                f'<span class="activity-label">Today</span>'
                f'<span style="color:var(--text-3); font-size:13px;">No outreach recorded</span>'
                f'{overdue_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        # 7-day outreach chart
        st.markdown(
            '<div class="section-header-row">'
            '<h3>7-day Outreach</h3>'
            '<span class="scope-badge scope-global">🌐 All POCs · Past 7 days</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        fig_daily = daily_outreach_chart(df_all)
        if fig_daily:
            st.plotly_chart(fig_daily, use_container_width=True, key="ov_daily")
        else:
            st.markdown(
                '<div style="color:#9CA3AF; font-size:0.85em; padding:4px 0;">'
                'No outreach recorded in the last 7 days.</div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")

        # ─── KPI Metrics ─────────────────────────────────────────────────
        st.markdown(
            '<div class="section-header-row">'
            '<h2>KPIs</h2>'
            '<span class="scope-badge scope-campaign">🏷️ Campaign + date range</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric(
            "Contacted",
            len(df_by_contact),
            help="Scope: date range only (Campaign Tag not applied — contacts happen before a campaign tag is assigned).",
        )
        k2.metric(
            "Confirmed",
            len(ov_confirmed),
            help="Scope: Campaign Tag + date range. Confirmed influencers for the selected campaign.",
        )
        avg_er = ov_confirmed["_er_num"].dropna().mean()
        k3.metric(
            "Avg ER%",
            f"{avg_er:.2f}%" if pd.notna(avg_er) else "N/A",
            help="Scope: Campaign Tag + date range · confirmed only.",
        )
        total_price = ov_confirmed["_price_num"].dropna().sum()
        k4.metric(
            "Total Cost",
            f"${total_price:,.0f}" if total_price > 0 else "N/A",
            help="Scope: Campaign Tag + date range · sum of confirmed prices.",
        )
        avg_fol = ov_confirmed["_followers_num"].dropna().mean()
        k5.metric(
            "Avg Followers",
            f"{avg_fol:,.0f}" if pd.notna(avg_fol) else "N/A",
            help="Scope: Campaign Tag + date range · average follower count of confirmed influencers.",
        )

        # ─── Contact → Confirm → Post Trends ──────────────────────────────
        st.markdown("---")
        st.markdown(
            '<div class="section-header-row">'
            '<h2>Contact → Confirm → Post Trends</h2>'
            '<span class="scope-badge scope-independent">⚙️ Own time controls · split by POC</span>'
            '</div>',
            unsafe_allow_html=True,
        )

        # Controls
        _tc1, _tc2, _tc3 = st.columns([1.2, 2, 1.5])
        with _tc1:
            _period_mode = st.pills("Period", ["Week", "Month"], default="Week", key="trend_period")
            if _period_mode is None:
                _period_mode = "Week"
        with _tc2:
            if _period_mode == "Week":
                _range_opts = ["Last 4 weeks", "Last 8 weeks", "Last 12 weeks", "Last 26 weeks"]
                _range_default = "Last 8 weeks"
            else:
                _range_opts = ["Last 3 months", "Last 6 months", "Last 12 months"]
                _range_default = "Last 6 months"
            _range_sel = st.selectbox("Range", _range_opts, index=_range_opts.index(_range_default), key="trend_range")
        with _tc3:
            _use_custom = st.checkbox("Custom dates", key="trend_custom")

        _tc4, _tc5 = st.columns(2)
        if _use_custom:
            with _tc4:
                _trend_start = st.date_input("From", value=date.today() - timedelta(days=60), key="trend_start")
            with _tc5:
                _trend_end = st.date_input("To", value=date.today(), key="trend_end")
        else:
            _n = int(_range_sel.split()[1])
            _trend_end = date.today()
            if _period_mode == "Week":
                _trend_start = _trend_end - timedelta(weeks=_n)
            else:
                # Go back N months (approximate via 30.44 days/month)
                _trend_start = _trend_end - timedelta(days=int(_n * 30.44))

        # Helper functions
        def _period_key(d, mode):
            if d is None or pd.isna(d):
                return None
            # Handle datetime too
            if hasattr(d, "date"):
                d = d.date()
            if mode == "Week":
                return d - timedelta(days=d.weekday())  # Monday of that week
            return date(d.year, d.month, 1)

        def _gen_period_keys(start, end, mode):
            """Generate ordered list of period keys spanning start to end."""
            keys = []
            if mode == "Week":
                cur = start - timedelta(days=start.weekday())
                while cur <= end:
                    keys.append(cur)
                    cur = cur + timedelta(weeks=1)
            else:
                cur = date(start.year, start.month, 1)
                end_key = date(end.year, end.month, 1)
                while cur <= end_key:
                    keys.append(cur)
                    # Next month
                    if cur.month == 12:
                        cur = date(cur.year + 1, 1, 1)
                    else:
                        cur = date(cur.year, cur.month + 1, 1)
            return keys

        _period_keys = _gen_period_keys(_trend_start, _trend_end, _period_mode)

        def _count_by_period_poc(date_col):
            """Build a grouped DataFrame: _period, POC, count."""
            if date_col not in df_all.columns:
                return pd.DataFrame(columns=["_period", "_poc_clean", "count"])
            d = df_all[df_all[date_col].notna()].copy()
            d["_period"] = d[date_col].apply(lambda x: _period_key(x, _period_mode))
            d = d[d["_period"].isin(_period_keys)]
            d["_poc_clean"] = d["POC"].astype(str).str.strip()
            return d.groupby(["_period", "_poc_clean"]).size().reset_index(name="count")

        contacted_g = _count_by_period_poc("_date_of_contact_parsed")
        confirmed_g = _count_by_period_poc("_confirm_date_parsed")
        posted_g = _count_by_period_poc("_post_date_parsed")

        # Period-over-period comparison (current vs previous period)
        def _period_total(grouped, key):
            if grouped.empty or key is None:
                return 0
            return int(grouped[grouped["_period"] == key]["count"].sum())

        if len(_period_keys) >= 2:
            _cur_key = _period_keys[-1]
            _prev_key = _period_keys[-2]
        else:
            _cur_key = _period_keys[-1] if _period_keys else None
            _prev_key = None

        def _delta_parts(cur, prev):
            if prev == 0 and cur == 0:
                return "→", "#9CA3AF", "no change"
            if prev == 0:
                return "↑", "#059669", f"(new, prev 0)"
            pct = (cur / prev - 1) * 100
            if pct >= 10: return "↑", "#059669", f"{pct:+.0f}% vs prev {prev}"
            if pct <= -10: return "↓", "#DC2626", f"{pct:+.0f}% vs prev {prev}"
            return "→", "#9CA3AF", f"{pct:+.0f}% vs prev {prev}"

        _metrics = [
            ("📬", "Contacted", contacted_g, "#748FFC"),
            ("✅", "Confirmed", confirmed_g, "#63E6BE"),
            ("🎬", "Posted", posted_g, "#22D3EE"),
        ]

        # Compact comparison cards
        _cmp_cols = st.columns(3)
        for (emoji, name, g, _color), col in zip(_metrics, _cmp_cols):
            with col:
                cur = _period_total(g, _cur_key)
                prev = _period_total(g, _prev_key) if _prev_key else 0
                arrow, color, note = _delta_parts(cur, prev)
                _period_word = "week" if _period_mode == "Week" else "month"
                st.markdown(
                    f'<div style="background:#FAFBFC; border-radius:8px; padding:12px 14px; border-left:3px solid {_color};">'
                    f'<div style="font-size:0.75em; color:#9CA3AF; text-transform:uppercase; letter-spacing:0.03em;">{emoji} {name} — this {_period_word}</div>'
                    f'<div style="font-size:1.6em; font-weight:700; color:#1F2937; margin:4px 0 2px;">{cur}</div>'
                    f'<div style="font-size:0.8em; color:{color}; font-weight:600;">{arrow} {note}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        # Stacked bar charts (3 side by side)
        import plotly.graph_objects as go

        def _build_trend_bar(grouped, title, emoji):
            """Stacked bar chart by POC for a time trend."""
            fig = go.Figure()
            if grouped.empty:
                # Empty chart
                fig.add_trace(go.Bar(x=[], y=[]))
            else:
                pocs = sorted([p for p in grouped["_poc_clean"].unique() if p])
                if not pocs:
                    pocs = [""]
                for poc in pocs:
                    sub = grouped[grouped["_poc_clean"] == poc]
                    counts = sub.set_index("_period")["count"].reindex(_period_keys, fill_value=0)
                    fig.add_trace(go.Bar(
                        x=[k.strftime("%m/%d") if _period_mode == "Week" else k.strftime("%b %Y")
                           for k in _period_keys],
                        y=counts.values.tolist(),
                        name=poc or "(unassigned)",
                        marker_color=POC_COLORS.get(poc, "#B197FC"),
                        hovertemplate="<b>%{x}</b><br>" + (poc or "(unassigned)") + ": %{y}<extra></extra>",
                    ))
            fig.update_layout(
                title=dict(text=f"{emoji} {title}", font=dict(size=13)),
                barmode="stack",
                margin=dict(t=36, b=30, l=10, r=10),
                height=280,
                font=dict(family="DM Sans, Inter, sans-serif"),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=True,
                legend=dict(orientation="h", y=-0.2, font=dict(size=10)),
                xaxis=dict(tickangle=-30, tickfont=dict(size=9)),
                yaxis=dict(gridcolor="#EDEDED", tickfont=dict(size=10)),
            )
            return fig

        _trend_cols = st.columns(3)
        for (emoji, name, g, _color), col in zip(_metrics, _trend_cols):
            with col:
                st.plotly_chart(
                    _build_trend_bar(g, f"{name} by {_period_mode}", emoji),
                    use_container_width=True,
                    key=f"trend_{name.lower()}",
                )

        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(
                '<div class="section-header-row">'
                '<h3>Sourcing Funnel — Status Distribution</h3>'
                '<span class="scope-badge scope-date">🗓️ All · Date range</span>'
                '</div>',
                unsafe_allow_html=True,
            )
            fig = status_distribution_pie(df_by_contact)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_status")
        with c2:
            st.markdown(
                '<div class="section-header-row">'
                '<h3>Campaign Execution — Collaboration Stage</h3>'
                '<span class="scope-badge scope-campaign">🏷️ Campaign + date · Confirmed</span>'
                '</div>',
                unsafe_allow_html=True,
            )
            fig = collab_stage_detail(ov_confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_collab")

# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline
# ═══════════════════════════════════════════════════════════════════════════════

elif nav == "Pipeline":
    if df_filtered.empty:
        st.info("No influencers found.")
    else:
        # POC Overview
        campaign_mode = selected_tag != "(All)"
        df_confirmed_filtered = df_filtered[df_filtered["Status"] == "Confirm"]

        if campaign_mode:
            st.subheader(f"{selected_tag} Campaign")
            st.metric("Total Confirmed", len(df_confirmed_filtered))
            st.markdown("")

        st.subheader("POC Overview")
        POC_COLOR_MAP = {"jenny": "#748FFC", "doris": "#FF922B", "jialin": "#F06595", "falida": "#63E6BE"}
        poc_counts = df_by_contact["POC"].value_counts()
        poc_confirmed_counts = df_confirmed_filtered["POC"].value_counts()
        # Today's outreach per POC
        _today_pipe = get_today_la()
        today_by_poc_pipe = df_all[df_all["_date_of_contact_parsed"] == _today_pipe]["POC"].value_counts()

        poc_list = [(p, c) for p, c in poc_counts.items() if p.strip()]
        poc_cols = st.columns(min(len(poc_list), 6)) if poc_list else []
        for i, (p, c) in enumerate(poc_list):
            color = POC_COLOR_MAP.get(p.lower(), "#B197FC")
            confirmed_count = poc_confirmed_counts.get(p, 0)
            today_count = today_by_poc_pipe.get(p, 0)
            today_label = f'<span style="color:#9CA3AF; font-size:0.8em; margin-left:6px;">today {today_count}</span>' if today_count > 0 else ''
            poc_cols[i % len(poc_cols)].markdown(
                f"""<div style="background:#FAFBFC; border-radius:10px; padding:12px 16px;
                    box-shadow:0 1px 3px rgba(0,0,0,0.05); border-left:4px solid {color};">
                    <div style="font-weight:700; font-size:0.95em; color:{color}; margin-bottom:6px;">{p}{today_label}</div>
                    <div style="font-size:1.1em; color:#1F2937; font-weight:600; letter-spacing:0.01em;">
                        📬 {c}&nbsp;&nbsp;✅ {confirmed_count}
                    </div>
                </div>""",
                unsafe_allow_html=True,
            )
        st.caption("📬 Contacted&nbsp;&nbsp;&nbsp;✅ Confirmed")

        st.markdown("---")

        # ── Missed Posts detection (merged into Production Timeline below) ─
        # Post Date has passed but Collaboration Stage isn't Posted → needs attention.
        # We no longer render a separate red alert; instead, people in this set get
        # a "🔴 MISSED POST" pill next to their name in Production Timeline.
        _tl_today = get_today_la()
        _confirmed_with_pd = df_filtered[
            (df_filtered["Status"] == "Confirm")
            & (df_filtered["_post_date_parsed"].notna())
        ].copy()
        _missed_posts = _confirmed_with_pd[
            (_confirmed_with_pd["_post_date_parsed"] <= _tl_today)
            & (_confirmed_with_pd["Collaboration Stage"].str.strip() != "Posted")
        ]
        # Map sheet_row → post_date for fast pill rendering
        _missed_pd_by_row = {
            int(r["_sheet_row"]): r["_post_date_parsed"]
            for _, r in _missed_posts.iterrows()
        } if not _missed_posts.empty else {}

        # Production Timeline Status
        overdue_list, in_progress_list, completed_count = get_timeline_status(df_filtered)
        st.subheader("Production Timeline")

        # Merge overdue + in_progress into one view, grouped by stage
        all_people = {}  # stage -> [(name, poc, days, is_overdue, sheet_row), ...]
        for name, poc, stage, days, sr in overdue_list:
            all_people.setdefault(stage, []).append((name, poc, days, True, sr))
        for name, poc, stage, days, sr in in_progress_list:
            all_people.setdefault(stage, []).append((name, poc, days, False, sr))

        # Order by COLLAB_STAGE_ORDER
        from dashboard_utils.charts import COLLAB_STAGE_ORDER
        ordered_stages = [s for s in COLLAB_STAGE_ORDER if s in all_people]
        ordered_stages += [s for s in all_people if s not in ordered_stages]

        if ordered_stages:
            html = '<div style="background:#F9FAFB; border-radius:8px; padding:14px 18px; margin-bottom:12px;">'
            for stage in ordered_stages:
                people = all_people[stage]
                html += (
                    f'<div style="font-weight:600; font-size:0.82em; color:#6B7280; '
                    f'padding:8px 0 4px; margin-top:4px;">{stage}</div>'
                )
                html += '<div style="display:flex; flex-wrap:wrap; gap:4px 18px; padding-bottom:6px;">'
                for name, poc, days, is_over, sr in people:
                    pc = poc_color(poc)
                    if is_over:
                        icon = f'<span style="color:#DC2626; font-size:0.85em;">⚠️</span>'
                        day_label = f'<span style="color:#DC2626; font-weight:600; font-size:0.88em;">{days}d</span>'
                    elif days is not None:
                        icon = f'<span style="width:7px; height:7px; border-radius:50%; background:{pc}; display:inline-block;"></span>'
                        day_label = f'<span style="color:#059669; font-size:0.88em;">{days}d</span>'
                    else:
                        icon = f'<span style="width:7px; height:7px; border-radius:50%; background:{pc}; display:inline-block;"></span>'
                        day_label = ''
                    # "🔴 MISSED POST" pill if Post Date has passed (merged from
                    # former Missed Posts alert block — see computation above)
                    missed_pill = ""
                    if sr in _missed_pd_by_row:
                        _md = _missed_pd_by_row[sr]
                        _md_str = _md.strftime("%m/%d") if _md else ""
                        missed_pill = (
                            f'<span style="background:#FEE2E2; color:#B91C1C; '
                            f'padding:1px 6px; border-radius:3px; font-size:0.72em; '
                            f'font-weight:600; letter-spacing:0.2px; margin-left:4px;">'
                            f'🔴 MISSED {_md_str}</span>'
                        )
                    html += (
                        f'<span style="display:inline-flex; align-items:center; gap:4px; font-size:0.84em;">'
                        f'{icon}'
                        f'<span style="color:#1F2937; font-weight:500;">{name}</span>'
                        f'{day_label}'
                        f'{sheet_row_icon(sr)}'
                        f'{missed_pill}'
                        f'</span>'
                    )
                html += '</div>'
            html += '</div>'
            st.markdown(html, unsafe_allow_html=True)

        if completed_count > 0:
            st.markdown(
                f'<div style="font-size:0.83em; color:#059669; font-weight:600; padding:4px 0;">'
                f'✅ {completed_count} at Approved for posting</div>',
                unsafe_allow_html=True)

        if not overdue_list and not in_progress_list and completed_count == 0:
            st.info("No production timeline data yet. Set Collaboration Stage to start tracking.")

        st.markdown("---")

        # Kanban
        st.subheader("Collaboration Stage")
        render_kanban(df_filtered)

        no_stage = df_filtered[df_filtered["Collaboration Stage"].str.strip() == ""]
        if not no_stage.empty:
            with st.expander(f"No stage assigned ({len(no_stage)})"):
                for _, r in no_stage.iterrows():
                    st.write(f"- **{r.get('Name', '')}** (POC: {r.get('POC', 'N/A')})")


# ═══════════════════════════════════════════════════════════════════════════════
# Outreach (Act — email sending + full edit table)
# ═══════════════════════════════════════════════════════════════════════════════

elif nav == "Outreach":
    if df_filtered.empty:
        st.info("No influencers found.")
    else:
        # ─── Metric computations (section headers + summary row) ───
        _unsent_count = len(df_filtered[(df_filtered["Status"].str.strip() == "") & (df_filtered["Contact"].str.strip() != "")])
        _contacted_for_fu = df_filtered[df_filtered["Status"] == "Contacted"]
        _fu_count = 0
        if "Email Message-ID" in _contacted_for_fu.columns and "Last Email Sent" in _contacted_for_fu.columns:
            _trackable = _contacted_for_fu[_contacted_for_fu["Email Message-ID"].str.strip() != ""]
            _now = datetime.now()
            for _, _r in _trackable.iterrows():
                try:
                    _ls = datetime.strptime(_r["Last Email Sent"].strip(), "%Y-%m-%d %H:%M")
                    _fc = int(_r.get("Follow-Up Count", "0") or "0")
                    _ds = (_now - _ls).days
                    if (_fc == 0 and _ds >= 2) or (_fc == 1 and _ds >= 1):
                        _fu_count += 1
                except (ValueError, AttributeError):
                    pass

        # Compute open rate stats — use df_all (not filtered by Campaign Tag)
        # Exclude emails sent BEFORE the tracking pixel was deployed (2026-04-16).
        TRACKING_START_DATE = date(2026, 4, 16)

        _sent_tracked_all = df_all[df_all["Email Message-ID"].str.strip() != ""] if "Email Message-ID" in df_all.columns else pd.DataFrame()
        if not _sent_tracked_all.empty and "Last Email Sent" in _sent_tracked_all.columns:
            _sent_tracked_all = _sent_tracked_all[
                _sent_tracked_all["Last Email Sent"].apply(parse_sheet_date).apply(
                    lambda d: d is not None and d >= TRACKING_START_DATE
                )
            ]
        _sent_count = len(_sent_tracked_all)
        _opened_count = 0
        if _sent_count > 0 and "Email Opened" in _sent_tracked_all.columns:
            _opened_count = (_sent_tracked_all["Email Opened"].str.strip().str.lower() == "yes").sum()
        _open_pct = (_opened_count / _sent_count * 100) if _sent_count else 0
        _open_rate_str = f", {_opened_count}/{_sent_count} opened ({_open_pct:.0f}%)" if _sent_count else ""

        # ─── Summary row (3 metrics at top) ────────────────────
        _sc1, _sc2, _sc3 = st.columns(3)
        _sc1.metric(
            "Unsent",
            _unsent_count,
            help="Candidates with no Status yet, ready for first outreach (Campaign + date filtered)",
        )
        _sc2.metric(
            "Follow-ups due",
            _fu_count,
            help="Contacted people awaiting follow-up based on days since last email (Campaign + date filtered)",
        )
        _sc3.metric(
            "Opened",
            f"{_opened_count}/{_sent_count} ({_open_pct:.0f}%)" if _sent_count else "0/0",
            help=f"Opened / Sent emails since tracking pixel deployed {TRACKING_START_DATE:%Y-%m-%d}. Global scope (all POCs).",
        )

        # ─── Email Sender (Gmail auth + send actions) ──────────────
        st.markdown("---")
        st.markdown(
            '<div class="section-header-row">'
            '<h2>Email Sender</h2>'
            f'<span class="scope-badge scope-date">🔒 POC-scoped · {_unsent_count} unsent · {_fu_count} follow-ups due</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        if "gmail_connected" not in st.session_state:
            st.session_state["gmail_connected"] = False

        if not st.session_state["gmail_connected"]:
            st.caption("Connect Gmail to send outreach and follow-ups. You'll only see your own POC's rows.")
            gc1, gc2 = st.columns(2)
            gmail_addr = gc1.text_input("Gmail address", placeholder="you@jobright.ai", key="gmail_addr")
            gmail_pw = gc2.text_input("App Password", type="password", placeholder="xxxx xxxx xxxx xxxx", key="gmail_pw")
            if st.button("🔗 Connect Gmail"):
                if gmail_addr and gmail_pw:
                    from dashboard_utils.email_client import test_smtp_connection
                    if test_smtp_connection(gmail_addr, gmail_pw):
                        st.session_state["gmail_connected"] = True
                        st.session_state["gmail_email"] = gmail_addr
                        st.session_state["gmail_password"] = gmail_pw
                        st.toast(f"Connected as {gmail_addr}")
                        st.rerun()
                    else:
                        st.error("Connection failed. Check your email and App Password.")
                else:
                    st.warning("Enter both email and App Password.")
        else:
            gmail_email = st.session_state["gmail_email"]
            # Extract POC name from Gmail address (jenny@jobright.ai → "Jenny")
            connected_poc = gmail_email.split("@")[0].capitalize()
            dc1, dc2 = st.columns([3, 1])
            dc1.caption(f"✅ Connected as **{gmail_email}** — will send as POC **{connected_poc}**")
            if dc2.button("🔌 Disconnect", key="gmail_disconnect"):
                st.session_state["gmail_connected"] = False
                st.session_state.pop("gmail_email", None)
                st.session_state.pop("gmail_password", None)
                st.rerun()

            # Send outreach section — STRICTLY filtered to connected POC's rows only
            df_unsent = df_all[df_all["Status"].str.strip() == ""]
            df_unsent = df_unsent[df_unsent["Contact"].str.strip() != ""]
            # ENFORCE: only show rows where POC matches connected Gmail
            df_unsent = df_unsent[df_unsent["POC"].str.strip() == connected_poc]

            if not df_unsent.empty:
                with st.expander(f"📤 Send Outreach ({len(df_unsent)} {connected_poc}'s unsent people)", expanded=False):
                    st.caption(f"🔒 Only {connected_poc}'s rows shown. Switch Gmail to send as another POC.")

                    # Show candidates
                    send_display = df_unsent[["Name", "Contact", "POC"]].copy()
                    send_display.insert(0, "Send", True)
                    edited_send = st.data_editor(
                        send_display, use_container_width=True,
                        hide_index=True, key="send_editor",
                    )

                    selected = edited_send[edited_send["Send"] == True]
                    st.caption(f"{len(selected)} selected")

                    if st.button(f"📧 Send Outreach ({len(selected)} emails)", type="primary", disabled=len(selected) == 0):
                        from dashboard_utils.email_client import batch_send_outreach

                        # Get sender name from POC column of the connected user
                        sender_name = gmail_email.split("@")[0].capitalize()

                        # Build recipients list
                        recipients = []
                        for idx, row in selected.iterrows():
                            orig_row = df_unsent.loc[idx]
                            recipients.append({
                                "to_email": orig_row["Contact"].strip(),
                                "name": (orig_row["Name"].strip().split()[0] if orig_row["Name"].strip() else "there"),
                                "sheet_row": int(orig_row["_sheet_row"]),
                            })

                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        def update_progress(current, total, name, success):
                            progress_bar.progress(current / total)
                            icon = "✅" if success else "❌"
                            status_text.caption(f"{icon} {name} ({current}/{total})")

                        results = batch_send_outreach(
                            gmail_email, st.session_state["gmail_password"],
                            sender_name, recipients, update_progress,
                        )

                        # Write results to Sheet
                        ws = st.session_state["ws"]
                        updates = []
                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                        success_count = 0
                        for r in results:
                            if r["success"]:
                                success_count += 1
                                sr = r["sheet_row"]
                                # Status → Contacted
                                updates.append((sr, COL["status"] + 1, "Contacted"))
                                # Email Message-ID
                                updates.append((sr, COL["email_msg_id"] + 1, r["msg_id"]))
                                # Last Email Sent
                                updates.append((sr, COL["last_email_sent"] + 1, now_str))
                                # Follow-Up Count = 0
                                updates.append((sr, COL["followup_count"] + 1, "0"))

                        if updates:
                            batch_update_cells(ws, updates)
                            # Update in-memory DataFrame
                            if "df" in st.session_state:
                                df_mem = st.session_state["df"]
                                for r in results:
                                    if r["success"]:
                                        mask = df_mem["_sheet_row"] == r["sheet_row"]
                                        if mask.any():
                                            df_mem.loc[mask, "Status"] = "Contacted"
                                            df_mem.loc[mask, "Email Message-ID"] = r["msg_id"]
                                            df_mem.loc[mask, "Last Email Sent"] = now_str
                                            df_mem.loc[mask, "Follow-Up Count"] = "0"

                        failed_count = len(results) - success_count
                        st.success(f"Done! ✅ Sent {success_count} / ❌ Failed {failed_count}")
            else:
                st.caption(f"No unsent contacts for {connected_poc} (all have a Status or no email).")

            # Follow-up section
            df_contacted = df_filtered[df_filtered["Status"] == "Contacted"].copy()
            # ENFORCE: only show rows where POC matches connected Gmail
            df_contacted = df_contacted[df_contacted["POC"].str.strip() == connected_poc]
            if "Email Message-ID" in df_contacted.columns and "Last Email Sent" in df_contacted.columns:
                df_followable = df_contacted[df_contacted["Email Message-ID"].str.strip() != ""]
                if not df_followable.empty:
                    with st.expander(f"🔄 Follow-Ups ({len(df_followable)} of {connected_poc}'s people awaiting reply)", expanded=False):
                        st.caption(f"🔒 Only {connected_poc}'s rows shown.")

                        from dashboard_utils.email_client import (
                            check_reply_status, send_followup as send_fu,
                            REPLY_YES, REPLY_UNKNOWN,
                        )

                        now = datetime.now()
                        fu_candidates = []
                        for _, row in df_followable.iterrows():
                            try:
                                last_sent = datetime.strptime(row["Last Email Sent"].strip(), "%Y-%m-%d %H:%M")
                            except (ValueError, AttributeError):
                                continue
                            fu_count = int(row.get("Follow-Up Count", "0") or "0")
                            days_since = (now - last_sent).days

                            if fu_count == 0 and days_since >= 2:
                                fu_candidates.append({"row": row, "followup_num": 1, "days": days_since})
                            elif fu_count == 1 and days_since >= 1:
                                fu_candidates.append({"row": row, "followup_num": 2, "days": days_since})

                        if fu_candidates:
                            st.write(f"**{len(fu_candidates)}** people need follow-up:")
                            for c in fu_candidates:
                                r = c["row"]
                                st.write(f"- **{r['Name']}** → Follow-Up #{c['followup_num']} ({c['days']} days since last email)")

                            if st.button(f"📧 Send {len(fu_candidates)} Follow-Ups", type="primary"):
                                ws = st.session_state["ws"]
                                sent = 0
                                skipped = 0
                                for c in fu_candidates:
                                    r = c["row"]
                                    msg_id = r["Email Message-ID"]

                                    # Check if they already replied (tri-state)
                                    _reply_status = check_reply_status(
                                        gmail_email, st.session_state["gmail_password"], msg_id
                                    )
                                    if _reply_status == REPLY_YES:
                                        skipped += 1
                                        continue
                                    if _reply_status == REPLY_UNKNOWN:
                                        # Fail-closed: don't send if we can't verify
                                        st.warning(f"⚠️ {r['Name']} — IMAP check failed, skipping to avoid duplicate send.")
                                        skipped += 1
                                        continue

                                    try:
                                        sender_name = gmail_email.split("@")[0].capitalize()
                                        new_msg_id = send_fu(
                                            gmail_email, st.session_state["gmail_password"],
                                            r["Contact"].strip(),
                                            (r["Name"].strip().split()[0] if r["Name"].strip() else "there"),
                                            sender_name, msg_id, c["followup_num"],
                                        )
                                        sr = int(r["_sheet_row"])
                                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                                        batch_update_cells(ws, [
                                            (sr, COL["last_email_sent"] + 1, now_str),
                                            (sr, COL["followup_count"] + 1, str(c["followup_num"])),
                                            # Update Message-ID to the follow-up's new ID so
                                            # tracking pixel (which uses this ID) matches on open
                                            (sr, COL["email_msg_id"] + 1, new_msg_id),
                                        ])
                                        sent += 1
                                    except Exception as e:
                                        st.error(f"Failed to send to {r['Name']}: {e}")

                                st.success(f"Follow-ups done! ✅ Sent {sent} / ⏭️ Skipped {skipped} (already replied)")
                        else:
                            st.info("No follow-ups needed right now.")
                else:
                    st.caption(f"No emails tracked yet for {connected_poc}. Send outreach first.")

        # ─── Email Tracking Panel (after Email Sender — you connect first, then track) ─
        st.markdown("---")
        st.markdown(
            '<div class="section-header-row">'
            '<h2>Email Tracking</h2>'
            f'<span class="scope-badge scope-global">🌐 All POCs · {_sent_count} sent{_open_rate_str}</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        if _sent_count > 0:
            with st.expander("Expand tracking details", expanded=True):
                # ── Time range filter (scopes everything below) ──
                _tr1, _tr2 = st.columns([2, 1])
                _range_opts = ["Last 7 days", "Last 14 days", "Last 30 days", "All time"]
                _sel_range = _tr1.selectbox(
                    "🗓️ Time range",
                    _range_opts, index=0, key="tracking_range",
                    help="Defaults to Last 7 days. All time shows historical data including pre-tracking emails (which appear as 'not opened').",
                )
                _use_custom = _tr2.checkbox("Custom dates", key="tracking_custom_dates")

                if _use_custom:
                    _cc1, _cc2 = st.columns(2)
                    _t_start = _cc1.date_input("From", value=date.today() - timedelta(days=7), key="tracking_start_d")
                    _t_end = _cc2.date_input("To", value=date.today(), key="tracking_end_d")
                else:
                    _t_end = date.today()
                    if _sel_range == "Last 7 days":
                        _t_start = _t_end - timedelta(days=7)
                    elif _sel_range == "Last 14 days":
                        _t_start = _t_end - timedelta(days=14)
                    elif _sel_range == "Last 30 days":
                        _t_start = _t_end - timedelta(days=30)
                    else:
                        _t_start = TRACKING_START_DATE  # "All time" = since tracking started

                # Clip start to tracking pixel deployment date so pre-tracking emails
                # aren't included (they'd show 0% open because they have no pixel).
                if _t_start is not None and _t_start < TRACKING_START_DATE:
                    _t_start = TRACKING_START_DATE

                # Apply time filter to _sent_tracked_all
                if _t_start is not None:
                    _sent_tracked_all = _sent_tracked_all[
                        _sent_tracked_all["Last Email Sent"].apply(parse_sheet_date).apply(
                            lambda d: d is not None and _t_start <= d <= _t_end
                        )
                    ]
                    # Recompute summary stats from filtered data
                    _sent_count = len(_sent_tracked_all)
                    _opened_count = (_sent_tracked_all["Email Opened"].str.strip().str.lower() == "yes").sum() if _sent_count > 0 else 0
                    _open_pct = (_opened_count / _sent_count * 100) if _sent_count else 0

                # Show range summary — always has a start (clipped to TRACKING_START_DATE)
                st.caption(
                    f"Showing emails sent between **{_t_start.strftime('%m/%d/%Y')}** and **{_t_end.strftime('%m/%d/%Y')}** — {_sent_count} emails in range. "
                    f"(Tracking pixel deployed {TRACKING_START_DATE.strftime('%m/%d/%Y')} — earlier emails excluded.)"
                )

                # If no data in range, show warning and bail
                if _sent_count == 0:
                    st.info("No emails in this time range. Try a wider window or 'All time'.")
                    st.stop() if False else None

                # Summary metrics
                _unopened = _sent_count - _opened_count
                _tm1, _tm2, _tm3, _tm4 = st.columns(4)
                _tm1.metric("Total Sent", _sent_count)
                _tm2.metric("Opened", _opened_count)
                _tm3.metric("Unopened", _unopened)
                _tm4.metric("Open Rate", f"{_open_pct:.0f}%" if _sent_count > 0 else "N/A")

                # ─── POC Breakdown — compare each person's open + reply rates ─────
                st.markdown("##### 📊 Open Rate & Reply Rate by POC")
                _poc_stats = []
                for _poc_name in sorted(set(_sent_tracked_all["POC"].dropna().str.strip().unique()) - {""}):
                    _poc_rows = _sent_tracked_all[_sent_tracked_all["POC"].str.strip() == _poc_name]
                    _poc_sent = len(_poc_rows)
                    if _poc_sent == 0:
                        continue
                    _poc_opened = (_poc_rows["Email Opened"].str.strip().str.lower() == "yes").sum() if "Email Opened" in _poc_rows.columns else 0
                    _poc_rate = (_poc_opened / _poc_sent * 100) if _poc_sent else 0
                    # Reply count — uses Email Replied column (populated by auto_followup cron)
                    _poc_replied = (_poc_rows["Email Replied"].str.strip().str.lower() == "yes").sum() if "Email Replied" in _poc_rows.columns else 0
                    _poc_reply_rate = (_poc_replied / _poc_sent * 100) if _poc_sent else 0
                    # Avg opens per email (count column)
                    _open_counts = _poc_rows["Open Count"].apply(
                        lambda x: int(str(x).strip()) if str(x).strip().isdigit() else 0
                    ) if "Open Count" in _poc_rows.columns else pd.Series([0])
                    _avg_opens = _open_counts.mean() if len(_open_counts) else 0
                    _poc_stats.append({
                        "poc": _poc_name,
                        "sent": _poc_sent,
                        "opened": _poc_opened,
                        "rate": _poc_rate,
                        "replied": _poc_replied,
                        "reply_rate": _poc_reply_rate,
                        "avg_opens": _avg_opens,
                    })
                # Sort by open rate descending
                _poc_stats.sort(key=lambda x: x["rate"], reverse=True)

                if _poc_stats:
                    # Compute overall benchmarks
                    _overall_rate = (_opened_count / _sent_count * 100) if _sent_count else 0
                    _overall_replied = sum(s["replied"] for s in _poc_stats)
                    _overall_reply_rate = (_overall_replied / _sent_count * 100) if _sent_count else 0
                    _cols = st.columns(len(_poc_stats))
                    for i, _p in enumerate(_poc_stats):
                        with _cols[i]:
                            _pc = poc_color(_p["poc"])
                            # Open rate color: green ≥70%, amber 50-69%, red <50%
                            if _p["rate"] >= 70: _rate_color = "#059669"
                            elif _p["rate"] >= 50: _rate_color = "#F59E0B"
                            else: _rate_color = "#DC2626"
                            # Reply rate color: green ≥15%, amber 5-14%, red <5% (industry benchmarks)
                            if _p["reply_rate"] >= 15: _reply_color = "#059669"
                            elif _p["reply_rate"] >= 5: _reply_color = "#F59E0B"
                            else: _reply_color = "#DC2626"
                            # Vs team arrows
                            _diff = _p["rate"] - _overall_rate
                            if _diff >= 3: _vs_color, _vs_arrow = "#059669", "↑"
                            elif _diff <= -3: _vs_color, _vs_arrow = "#DC2626", "↓"
                            else: _vs_color, _vs_arrow = "#9CA3AF", "→"
                            _vs_label = f"{_vs_arrow} {_diff:+.1f}pp vs team"
                            st.markdown(
                                f'<div style="background:#FAFBFC; border-radius:10px; padding:14px 16px; border-left:4px solid {_pc};">'
                                f'<div style="display:flex; align-items:center; gap:6px; font-size:0.82em; color:#6B7280; font-weight:600;">'
                                f'<span style="width:9px; height:9px; border-radius:50%; background:{_pc}; display:inline-block;"></span>'
                                f'{_p["poc"]}</div>'
                                # Open Rate (big)
                                f'<div style="font-size:1.7em; font-weight:700; color:{_rate_color}; margin:8px 0 0;">{_p["rate"]:.0f}%</div>'
                                f'<div style="font-size:0.72em; color:#9CA3AF; text-transform:uppercase; letter-spacing:0.03em;">open rate</div>'
                                f'<div style="font-size:0.78em; color:#6B7280;">{_p["opened"]} / {_p["sent"]} opened · {_p["avg_opens"]:.1f} avg opens</div>'
                                # Reply Rate (medium) — divider
                                f'<div style="border-top:1px solid #E5E7EB; margin:8px 0 6px;"></div>'
                                f'<div style="font-size:1.3em; font-weight:700; color:{_reply_color};">{_p["reply_rate"]:.0f}%</div>'
                                f'<div style="font-size:0.72em; color:#9CA3AF; text-transform:uppercase; letter-spacing:0.03em;">reply rate</div>'
                                f'<div style="font-size:0.78em; color:#6B7280;">{_p["replied"]} / {_p["sent"]} replied</div>'
                                # Vs team
                                f'<div style="margin-top:8px; font-size:0.74em; color:{_vs_color}; font-weight:600;">{_vs_label}</div>'
                                f'</div>',
                                unsafe_allow_html=True,
                            )
                    st.caption(
                        f"Team avg: **{_overall_rate:.0f}%** open · **{_overall_reply_rate:.0f}%** reply. "
                        f"Open: ≥70% green · 50-69% amber · <50% red. "
                        f"Reply: ≥15% green · 5-14% amber · <5% red. "
                        f"Reply rate counts both recipient replies and manual follow-ups from the sender."
                    )

                st.markdown("---")

                # Filters — default POC filter to the connected Gmail's POC so the
                # detail table shows only your own emails. Clear the multiselect
                # to see all POCs' emails (useful for rollup views).
                _tf1, _tf2 = st.columns([2, 1])
                _poc_opts = sorted(set(_sent_tracked_all["POC"].dropna().str.strip().unique()) - {""})
                _connected_gmail_for_scope = st.session_state.get("gmail_email", "")
                _connected_poc_label = (
                    _connected_gmail_for_scope.split("@")[0].capitalize()
                    if _connected_gmail_for_scope else ""
                )
                _default_poc = (
                    [_connected_poc_label]
                    if _connected_poc_label and _connected_poc_label in _poc_opts
                    else []
                )
                _sel_poc = _tf1.multiselect(
                    "Filter by POC",
                    _poc_opts,
                    default=_default_poc,
                    key="track_poc",
                    help=(
                        f"Defaults to {_connected_poc_label} (your connected Gmail). "
                        "Clear the filter to see all POCs."
                        if _connected_poc_label else "Select POCs to filter the tracking table."
                    ),
                )
                _only_unopened = _tf2.checkbox("Only unopened", key="track_unopened")

                _use_date = st.checkbox("Filter by sent date range", key="track_use_date")
                if _use_date:
                    _td = date.today()
                    _dc1, _dc2 = st.columns(2)
                    _start_track = _dc1.date_input("From", value=_td - timedelta(days=14), key="track_start")
                    _end_track = _dc2.date_input("To", value=_td, key="track_end")

                # Build tracking table
                _tracked = _sent_tracked_all.copy()
                if _sel_poc:
                    _tracked = _tracked[_tracked["POC"].str.strip().isin(_sel_poc)]
                if _only_unopened:
                    _tracked = _tracked[_tracked["Email Opened"].str.strip().str.lower() != "yes"]

                # Parse sent dates
                _tracked["_sent_parsed"] = _tracked["Last Email Sent"].apply(parse_sheet_datetime)
                if _use_date:
                    _tracked = _tracked[_tracked["_sent_parsed"].apply(
                        lambda d: d is not None and _start_track <= d.date() <= _end_track
                    )]

                if _tracked.empty:
                    st.info("No emails match the current filters.")
                else:
                    _now_ts = datetime.now()
                    # Build display table with visual indicators
                    _rows_html = '<table style="width:100%; border-collapse:collapse; font-size:0.87em;">'
                    _rows_html += (
                        '<thead><tr style="background:#F9FAFB; text-align:left;">'
                        '<th style="padding:8px 10px; border-bottom:1px solid #E5E7EB;">Name</th>'
                        '<th style="padding:8px 10px; border-bottom:1px solid #E5E7EB;">POC</th>'
                        '<th style="padding:8px 10px; border-bottom:1px solid #E5E7EB;">Sent</th>'
                        '<th style="padding:8px 10px; border-bottom:1px solid #E5E7EB;">Days ago</th>'
                        '<th style="padding:8px 10px; border-bottom:1px solid #E5E7EB; text-align:center;">Opened</th>'
                        '<th style="padding:8px 10px; border-bottom:1px solid #E5E7EB; text-align:center;">Opens</th>'
                        '</tr></thead><tbody>'
                    )
                    # Sort by most recently sent first
                    _tracked_sorted = _tracked.sort_values("_sent_parsed", ascending=False, na_position="last")
                    for _, _tr in _tracked_sorted.iterrows():
                        _tname = (_tr.get("Name", "") or "").strip() or "(no name)"
                        _tpoc = (_tr.get("POC", "") or "").strip()
                        _tpc = poc_color(_tpoc)
                        _tsent = _tr.get("_sent_parsed")
                        _tsent_str = _tsent.strftime("%m/%d %H:%M") if _tsent else "—"
                        _tdays = (_now_ts - _tsent).days if _tsent else None
                        _topened = (_tr.get("Email Opened", "") or "").strip().lower() == "yes"
                        _tcount = _tr.get("Open Count", "") or "0"
                        try:
                            _tcount_int = int(str(_tcount).strip() or "0")
                        except (ValueError, AttributeError):
                            _tcount_int = 0

                        # Opened icon
                        if _topened:
                            _open_cell = '<span style="color:#10B981; font-weight:600;">✅</span>'
                        else:
                            _open_cell = '<span style="color:#9CA3AF;">❌</span>'

                        # Days badge — orange warning if 3+ days unopened
                        if _tdays is None:
                            _days_cell = '<span style="color:#9CA3AF;">—</span>'
                        elif not _topened and _tdays >= 3:
                            _days_cell = f'<span style="color:#F59E0B; font-weight:600;">⚠️ {_tdays}d</span>'
                        else:
                            _days_cell = f'<span style="color:#6B7280;">{_tdays}d</span>'

                        _rows_html += (
                            f'<tr style="border-bottom:1px solid #F3F4F6;">'
                            f'<td style="padding:8px 10px;">'
                            f'<span style="color:#1F2937; font-weight:500;">{_tname}</span>'
                            f'{sheet_row_icon(_tr.get("_sheet_row", 0))}'
                            f'</td>'
                            f'<td style="padding:8px 10px;">'
                            f'<span style="display:inline-flex; align-items:center; gap:5px;">'
                            f'<span style="width:7px; height:7px; border-radius:50%; background:{_tpc}; display:inline-block;"></span>'
                            f'<span style="color:#374151;">{_tpoc}</span></span></td>'
                            f'<td style="padding:8px 10px; color:#6B7280;">{_tsent_str}</td>'
                            f'<td style="padding:8px 10px;">{_days_cell}</td>'
                            f'<td style="padding:8px 10px; text-align:center;">{_open_cell}</td>'
                            f'<td style="padding:8px 10px; text-align:center; color:#374151;">{_tcount_int}</td>'
                            f'</tr>'
                        )
                    _rows_html += '</tbody></table>'
                    st.markdown(_rows_html, unsafe_allow_html=True)

        # ─── Full Table (searchable, editable) ───────────────────
        st.markdown("---")
        st.markdown(
            '<div class="section-header-row">'
            '<h2>Full Table</h2>'
            '<span class="scope-badge scope-campaign">🏷️ Campaign + date</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        search = st.text_input("Search by name", key="pipe_search")
        df_pipe = df_filtered.copy()
        if search:
            df_pipe = df_pipe[df_pipe["Name"].str.contains(search, case=False, na=False)]

        # Column filters
        fc1, fc2, fc3, fc4, fc5 = st.columns(5)
        f_poc = fc1.multiselect("POC", sorted(set(df_pipe["POC"].dropna().unique()) - {""}), key="pf_poc")
        f_status = fc2.multiselect("Status", sorted(set(df_pipe["Status"].dropna().unique()) - {""}), key="pf_status")
        f_stage = fc3.multiselect("Stage", sorted(set(df_pipe["Collaboration Stage"].dropna().unique()) - {""}), key="pf_stage")
        f_tag = fc4.multiselect("Campaign", sorted(set(df_pipe["Campaign Tag"].dropna().unique()) - {""}), key="pf_tag")
        f_country = fc5.multiselect("Country", sorted(set(df_pipe["Country"].dropna().unique()) - {""}), key="pf_country")

        if f_poc: df_pipe = df_pipe[df_pipe["POC"].isin(f_poc)]
        if f_status: df_pipe = df_pipe[df_pipe["Status"].isin(f_status)]
        if f_stage: df_pipe = df_pipe[df_pipe["Collaboration Stage"].isin(f_stage)]
        if f_tag: df_pipe = df_pipe[df_pipe["Campaign Tag"].isin(f_tag)]
        if f_country: df_pipe = df_pipe[df_pipe["Country"].isin(f_country)]

        # Add Days in Stage display column
        if "_days_in_stage" in df_pipe.columns:
            df_pipe["Days in Stage"] = df_pipe["_days_in_stage"].apply(
                lambda x: f"{int(x)}d" if pd.notna(x) else ""
            )

        st.caption(f"{len(df_pipe)} influencers")
        # Deep-link column so intern can jump from dashboard row to Sheet row.
        df_pipe = df_pipe.copy()
        df_pipe["Sheet"] = df_pipe["_sheet_row"].apply(sheet_row_link)
        pipe_display = PIPELINE_DISPLAY_COLS + (["Days in Stage"] if "Days in Stage" in df_pipe.columns else []) + ["Sheet"]
        show_editable_table(
            df_pipe, pipe_display,
            {"Name": "text", "Contact": "text", "Type": "text",
             "Senority": "text", "Job Function": "text",
             "Status": "select_status",
             "Contract Status": "select_contract",
             "Collaboration Stage": "select_collab",
             "Campaign Tag": "select_campaign",
             "Confirm Date": "text", "POC": "text", "Notes": "text",
             "Sheet": "link"},
            "pipeline",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Content & Delivery
# ═══════════════════════════════════════════════════════════════════════════════

elif nav == "Content & Delivery":
    df_content = df_filtered[df_filtered["Status"] == "Confirm"].copy()
    if df_content.empty:
        st.info("No confirmed influencers.")
    else:
        st.subheader(f"{len(df_content)} confirmed influencers")
        fc1, fc2 = st.columns(2)
        f_stage = fc1.multiselect("Stage", sorted(set(df_content["Collaboration Stage"].dropna().unique()) - {""}), key="cf_stage")
        f_poc = fc2.multiselect("POC", sorted(set(df_content["POC"].dropna().unique()) - {""}), key="cf_poc")
        if f_stage: df_content = df_content[df_content["Collaboration Stage"].isin(f_stage)]
        if f_poc: df_content = df_content[df_content["POC"].isin(f_poc)]

        # Posting Schedule — Notion style
        df_with_date = df_content[df_content["_post_date_parsed"].notna()].copy()
        if not df_with_date.empty:
            _sched_today = get_today_la()

            with st.expander("📅 Posting Schedule", expanded=True):
                # Count missed posts for legend
                _missed_total = 0
                grouped = {}
                for _, row in df_with_date.iterrows():
                    d = row["_post_date_parsed"]
                    stage = (row.get("Collaboration Stage", "") or "").strip()
                    grouped.setdefault(d, []).append((row.get("Name", ""), row.get("POC", ""), stage))
                    if d <= _sched_today and stage != "Posted":
                        _missed_total += 1
                sorted_dates = sorted(grouped.keys())

                # Legend
                if _missed_total > 0:
                    st.markdown(
                        f'<div style="font-size:0.8em; color:#DC2626; font-weight:600; padding:4px 0 8px;">'
                        f'⚠️ {_missed_total} influencer{"s" if _missed_total != 1 else ""} missed their post date</div>',
                        unsafe_allow_html=True,
                    )

                html = ""
                for d in sorted_dates:
                    people = grouped[d]
                    day_label = d.strftime("%m/%d/%Y %a")
                    is_past = d <= _sched_today
                    is_today = d == _sched_today
                    # Count missed in this date
                    missed_in_date = sum(1 for _, _, s in people if is_past and s != "Posted") if is_past else 0
                    # Date header row
                    today_badge = '<span style="background:#3B82F6; color:#fff; font-size:0.75em; padding:1px 8px; border-radius:10px; margin-left:6px;">TODAY</span>' if is_today else ""
                    missed_badge = f'<span style="color:#DC2626; font-size:0.78em; font-weight:600; margin-left:8px;">⚠️ {missed_in_date} missed</span>' if missed_in_date > 0 else ""
                    html += (
                        f'<div style="display:flex; align-items:center; gap:8px; padding:10px 14px; '
                        f'background:#F9FAFB; border-radius:6px; margin-top:12px;">'
                        f'<span style="font-weight:700; font-size:0.88em; color:#1F2937;">{day_label}</span>'
                        f'{today_badge}'
                        f'<span style="font-size:0.78em; color:#9CA3AF;">{len(people)} people</span>'
                        f'{missed_badge}'
                        f'</div>'
                    )
                    # People chips — horizontal wrap
                    html += '<div style="display:flex; flex-wrap:wrap; gap:6px 20px; padding:10px 14px;">'
                    for name, poc, stage in people:
                        pc = poc_color(poc)
                        # Missed: past date + not Posted
                        if is_past and stage != "Posted":
                            html += (
                                f'<div style="display:flex; align-items:center; gap:6px; font-size:0.84em;">'
                                f'<span style="color:#DC2626; font-size:0.85em;">⚠️</span>'
                                f'<span style="color:#DC2626; font-weight:600;">{name or "(no name)"}</span>'
                                f'</div>'
                            )
                        else:
                            html += (
                                f'<div style="display:flex; align-items:center; gap:6px; font-size:0.84em;">'
                                f'<span style="width:7px; height:7px; border-radius:50%; background:{pc}; flex-shrink:0; display:inline-block;"></span>'
                                f'<span style="color:#374151; font-weight:500;">{name or "(no name)"}</span>'
                                f'</div>'
                            )
                    html += '</div>'

                st.markdown(html, unsafe_allow_html=True)

        st.markdown("---")
        show_editable_table(
            df_content, CONTENT_DISPLAY_COLS,
            {"Name": "text", "Collaboration Stage": "select_collab", "Content Type": "text",
             "Post Link": "text", "Post Date": "text", "Tracking Link": "text",
             "Price\uff08$)": "text", "24hr Views": "text", "Link Signups": "text",
             "Type": "text", "Senority": "text", "Job Function": "text"},
            "content",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Payment
# ═══════════════════════════════════════════════════════════════════════════════

elif nav == "Payment":
    df_pay = df_filtered[df_filtered["Status"] == "Confirm"].copy()
    if df_pay.empty:
        st.info("No confirmed influencers.")
    else:
        # ─── Campaign Delivery Progress ─────────────────────────────
        st.markdown(
            render_delivery_progress(df_pay["Collaboration Stage"], show_total=True),
            unsafe_allow_html=True,
        )

        # ─── Summary Metrics ────────────────────────────────────────
        st.subheader("Summary")
        total_cost = df_pay["_price_num"].dropna().sum()
        total_24hr = df_pay["_views_24hr_num"].dropna().sum()
        total_signups = df_pay["_signups_num"].dropna().sum()

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Cost", f"${total_cost:,.0f}" if total_cost > 0 else "N/A")
        m2.metric("Total 24hr Views", f"{total_24hr:,.0f}" if total_24hr > 0 else "N/A")
        m3.metric("Total Signups", f"{total_signups:,.0f}" if total_signups > 0 else "N/A")
        if total_cost > 0 and total_signups > 0:
            m4.metric("Cost/Signup", f"${total_cost / total_signups:.2f}")
        else:
            m4.metric("Cost/Signup", "N/A")
        if total_cost > 0 and total_24hr > 0:
            m5.metric("Avg CPM", f"${total_cost / total_24hr * 1000:.2f}")
        else:
            m5.metric("Avg CPM", "N/A")

        # Detail table
        st.markdown("---")
        st.subheader("Detail Table")
        show_editable_table(
            df_pay, PAYMENT_PERF_DISPLAY_COLS,
            {"Name": "text", "Post Link": "text", "Post Date": "text",
             "Payment Receiving Account": "text", "Payment Progress": "select_payment",
             "24hr Views": "text", "Link Signups": "text", "ER": "text",
             "Post ER": "text", "Baseline ER": "text", "Retro Notes": "text",
             "Type": "text", "Senority": "text", "Job Function": "text", "Content Type": "text"},
            "payment",
        )

        # Export
        st.markdown("---")
        csv = df_pay.drop(columns=[c for c in df_pay.columns if c.startswith("_")]).to_csv(index=False)
        # Build filename — guard against start_date/end_date being undefined when date filter is off
        _fname_parts = ["campaign"]
        if use_date_filter:
            _fname_parts.append(f"{start_date}_{end_date}")
        if selected_tag != "(All)":
            _fname_parts.append(selected_tag.lower())
        _csv_filename = "_".join(_fname_parts) + ".csv"
        st.download_button("\U0001f4e5 Export Campaign Report (CSV)", data=csv,
                           file_name=_csv_filename, mime="text/csv",
                           use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Report — Campaign Retrospective Analysis
# ═══════════════════════════════════════════════════════════════════════════════

elif nav == "Report":
    df_rep = df_filtered[df_filtered["Status"] == "Confirm"].copy()
    if df_rep.empty:
        st.info("No confirmed influencers in this view. Change Campaign Tag or Status filter.")
    else:
        # ─── Prepare derived columns ──────────────────────────────────────
        df_rep = df_rep.copy()
        df_rep["_cpm"] = df_rep.apply(
            lambda r: (r["_price_num"] / r["_views_24hr_num"] * 1000)
            if pd.notna(r.get("_price_num")) and pd.notna(r.get("_views_24hr_num")) and r["_views_24hr_num"] > 0 else None,
            axis=1,
        )
        df_rep["_cost_per_signup"] = df_rep.apply(
            lambda r: (r["_price_num"] / r["_signups_num"])
            if pd.notna(r.get("_price_num")) and pd.notna(r.get("_signups_num")) and r["_signups_num"] > 0 else None,
            axis=1,
        )
        df_rep["_er_uplift"] = df_rep.apply(
            lambda r: ((r["_post_er_num"] / r["_baseline_er_num"]) - 1) * 100
            if pd.notna(r.get("_post_er_num")) and pd.notna(r.get("_baseline_er_num")) and r["_baseline_er_num"] > 0 else None,
            axis=1,
        )
        df_rep["_views_vs_avg"] = df_rep.apply(
            lambda r: (r["_views_24hr_num"] / r["_avg_impressions_num"] - 1) * 100
            if pd.notna(r.get("_views_24hr_num")) and pd.notna(r.get("_avg_impressions_num")) and r["_avg_impressions_num"] > 0 else None,
            axis=1,
        )
        # Overall Score — composite of ER uplift, Views vs Avg, CPM (percentile-ranked)
        # Uses Views vs Avg (not absolute) for cross-tier fairness (Nano vs Macro).
        df_rep["_overall_score"] = compute_overall_score(
            df_rep["_er_uplift"], df_rep["_views_vs_avg"], df_rep["_cpm"]
        )

        # Follower bucket
        df_rep["_follower_bucket"] = df_rep["_followers_num"].apply(follower_bucket)

        # ─── Pre-compute segment stats + best-per-segment + top performers ─
        # Hoisted from What Worked Best / Best Archetype / Top Performers sections
        # so Key Takeaways (which lives at the top now) can reference them.
        # Display below is unchanged — those sections just re-render pre-computed vars.

        # ─── Segment Analysis Helper ─────────────────────────────────
        def _segment_stats(df, by_col, label=None):
            """Aggregate per-segment metrics. Returns DataFrame sorted by Avg CPM."""
            if by_col not in df.columns:
                return pd.DataFrame()
            d = df.copy()
            d[by_col] = d[by_col].astype(str).str.strip()
            d = d[d[by_col] != ""]
            if d.empty:
                return pd.DataFrame()
            g = d.groupby(by_col).agg(
                n=("Name", "count"),
                avg_cpm=("_cpm", "mean"),
                avg_cost_per_signup=("_cost_per_signup", "mean"),
                avg_er_uplift=("_er_uplift", "mean"),
                avg_views=("_views_24hr_num", "mean"),
                avg_score=("_overall_score", "mean"),
            ).reset_index()
            g = g.sort_values("avg_score", ascending=False, na_position="last")
            return g

        def _segment_display(seg_df, label, first_col_name=None):
            """Render a segment table — clean, scannable, best row highlighted.

            Sorted by Overall Score desc (best first). Best row gets 🥇 + green bg.
            """
            if seg_df.empty:
                st.info(f"No data for {label} segmentation.")
                return
            # Sort by Overall Score (desc). Fall back to ER uplift, then CPM.
            if seg_df.get("avg_score", pd.Series(dtype=float)).notna().any():
                sorted_df = seg_df.sort_values("avg_score", ascending=False, na_position="last")
            elif seg_df["avg_er_uplift"].notna().any():
                sorted_df = seg_df.sort_values("avg_er_uplift", ascending=False, na_position="last")
            else:
                sorted_df = seg_df.sort_values("avg_cpm", ascending=True, na_position="last")

            seg_col = first_col_name or seg_df.columns[0]
            # Find index of best row (highest Overall Score)
            best_idx = None
            if sorted_df.get("avg_score", pd.Series(dtype=float)).notna().any():
                best_idx = sorted_df.index[sorted_df["avg_score"].notna()][0]
            elif sorted_df["avg_er_uplift"].notna().any():
                best_idx = sorted_df.index[sorted_df["avg_er_uplift"].notna()][0]

            # Build HTML table (Score column first after segment name)
            html = (
                '<table style="width:100%; border-collapse:collapse; font-size:0.88em; margin-bottom:8px;">'
                '<thead><tr style="background:#F3F4F6; color:#374151;">'
                f'<th style="padding:10px 12px; text-align:left; border-bottom:2px solid #E5E7EB;">{label}</th>'
                '<th style="padding:10px 12px; text-align:center; border-bottom:2px solid #E5E7EB;">#</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">Score</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">ER vs Baseline</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">Avg Views</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">Avg CPM</th>'
                '</tr></thead><tbody>'
            )

            for i, (_, row) in enumerate(sorted_df.iterrows()):
                is_best = (row.name == best_idx)
                bg = "#ECFDF5" if is_best else ("#FAFBFC" if i % 2 else "#FFFFFF")
                crown = "🥇 " if is_best else ""
                name = f'{crown}{row[seg_col]}'

                # Format metrics
                n_val = int(row["n"]) if pd.notna(row.get("n")) else 0
                score = row.get("avg_score")
                if pd.isna(score):
                    score_html = '<span style="color:#9CA3AF;">—</span>'
                else:
                    if score >= 70: score_color = "#059669"
                    elif score >= 50: score_color = "#3B82F6"
                    else: score_color = "#9CA3AF"
                    score_html = f'<span style="color:{score_color}; font-weight:700;">{score:.0f}</span>'
                er = row.get("avg_er_uplift")
                if pd.isna(er):
                    er_html = '<span style="color:#9CA3AF;">—</span>'
                else:
                    color = "#059669" if er >= 0 else "#DC2626"
                    er_html = f'<span style="color:{color}; font-weight:600;">{er:+.1f}%</span>'
                views = f"{int(row['avg_views']):,}" if pd.notna(row.get("avg_views")) else "—"
                cpm = f"${row['avg_cpm']:.2f}" if pd.notna(row.get("avg_cpm")) else "—"

                html += (
                    f'<tr style="background:{bg}; border-bottom:1px solid #F3F4F6;">'
                    f'<td style="padding:9px 12px; color:#1F2937; font-weight:{("600" if is_best else "500")};">{name}</td>'
                    f'<td style="padding:9px 12px; text-align:center; color:#6B7280;">{n_val}</td>'
                    f'<td style="padding:9px 12px; text-align:right;">{score_html}</td>'
                    f'<td style="padding:9px 12px; text-align:right;">{er_html}</td>'
                    f'<td style="padding:9px 12px; text-align:right; color:#6B7280;">{views}</td>'
                    f'<td style="padding:9px 12px; text-align:right; color:#374151;">{cpm}</td>'
                    f'</tr>'
                )
            html += '</tbody></table>'
            st.markdown(html, unsafe_allow_html=True)

        def _best_segment(seg_df, metric="avg_score", ascending=False):
            """Pick the segment with the best value for a given metric (default: Overall Score)."""
            if seg_df.empty or metric not in seg_df.columns:
                return None
            d = seg_df.dropna(subset=[metric])
            if d.empty:
                return None
            d = d.sort_values(metric, ascending=ascending)
            return d.iloc[0]


        # Silent compute (used by Key Takeaways, re-referenced in display sections below)
        fol_stats = _segment_stats(df_rep, "_follower_bucket")
        ct_stats = _segment_stats(df_rep, "Content Type")
        type_stats = _segment_stats(df_rep, "Type")
        sen_stats = _segment_stats(df_rep, "Senority")
        best_fol = _best_segment(fol_stats)
        best_ct = _best_segment(ct_stats)
        best_type = _best_segment(type_stats)
        best_sen = _best_segment(sen_stats)
        top_score = df_rep.dropna(subset=["_overall_score"]).sort_values("_overall_score", ascending=False).head(3)

        # ─── Executive Summary ────────────────────────────────────────
        st.subheader("📊 Executive Summary")
        # Campaign date range
        post_dates = df_rep["_post_date_parsed"].dropna()
        if not post_dates.empty:
            _dmin = min(post_dates).strftime("%m/%d/%Y")
            _dmax = max(post_dates).strftime("%m/%d/%Y")
            _range_str = f"{_dmin} – {_dmax}" if _dmin != _dmax else _dmin
        else:
            _range_str = "—"
        selected_tag_str = selected_tag if selected_tag != "(All)" else "All Campaigns"
        st.caption(f"**{selected_tag_str}** · Posts dated {_range_str} · {len(df_rep)} confirmed influencers")

        # Delivery progress bar
        st.markdown(
            render_delivery_progress(df_rep["Collaboration Stage"], show_total=False),
            unsafe_allow_html=True,
        )

        # 5 summary metrics
        r_total_cost = df_rep["_price_num"].dropna().sum()
        r_total_views = df_rep["_views_24hr_num"].dropna().sum()
        r_total_signups = df_rep["_signups_num"].dropna().sum()
        rm1, rm2, rm3, rm4, rm5 = st.columns(5)
        rm1.metric("Total Cost", f"${r_total_cost:,.0f}" if r_total_cost else "N/A")
        rm2.metric("Total 24hr Views", f"{r_total_views:,.0f}" if r_total_views else "N/A")
        rm3.metric("Total Signups", f"{r_total_signups:,.0f}" if r_total_signups else "N/A")
        rm4.metric("Cost/Signup", f"${r_total_cost / r_total_signups:.2f}" if r_total_cost and r_total_signups else "N/A")
        rm5.metric("Avg CPM", f"${r_total_cost / r_total_views * 1000:.2f}" if r_total_cost and r_total_views else "N/A")

        # ─── Table of Contents (jump links) ──────────────────────────
        st.markdown("""
<div style="padding: 12px 16px; background: rgba(0,0,0,0.03); border-radius: 8px;
            margin: 16px 0 20px; font-size: 13px; line-height: 1.9;">
<strong style="color: #615d59; font-weight: 600; margin-right: 10px; font-size: 11px;
        text-transform: uppercase; letter-spacing: 0.5px;">Jump to</strong>
<a href="#key-takeaways" style="margin-right: 14px; color: #0075de; text-decoration: none; font-weight: 600;">💡 Key Takeaways</a>
<a href="#top-performers" style="margin-right: 14px; color: #0075de; text-decoration: none;">🏆 Top Performers</a>
<a href="#profile-breakdown" style="margin-right: 14px; color: #0075de; text-decoration: none;">👥 Profile</a>
<a href="#what-worked-best" style="margin-right: 14px; color: #0075de; text-decoration: none;">📐 What Worked Best</a>
<a href="#best-archetype" style="margin-right: 14px; color: #0075de; text-decoration: none;">🏅 Archetype</a>
<a href="#performance-charts" style="margin-right: 14px; color: #0075de; text-decoration: none;">📈 Charts</a>
<a href="#export" style="color: #0075de; text-decoration: none;">📥 Export</a>
</div>
""", unsafe_allow_html=True)

        # ─── Key Takeaways (moved to position 2 — most valuable insight block) ─
        st.markdown("---")
        st.markdown('<a name="key-takeaways"></a>', unsafe_allow_html=True)
        st.subheader("💡 Key Takeaways")

        insights = []
        overall_cpm = (r_total_cost / r_total_views * 1000) if r_total_cost and r_total_views else None
        overall_cps = (r_total_cost / r_total_signups) if r_total_cost and r_total_signups else None

        def _pct_diff(a, b):
            if a is None or b is None or b == 0:
                return None
            return (a / b - 1) * 100

        # 0. Top Overall Score winner (campaign MVP)
        if not top_score.empty:
            mvp = top_score.iloc[0]
            insights.append(
                f"🏆 **{mvp.get('Name', '(no name)')}** is this campaign's MVP with Overall Score "
                f"{mvp['_overall_score']:.0f}, balancing content fit, reach lift, and cost efficiency."
            )

        # 1. Best follower bucket
        if best_fol is not None and overall_cpm and pd.notna(best_fol.get("avg_cpm")):
            diff = _pct_diff(best_fol["avg_cpm"], overall_cpm)
            if diff is not None:
                insights.append(
                    f"**{best_fol['_follower_bucket']}** influencers delivered the best CPM at "
                    f"${best_fol['avg_cpm']:.2f}, {abs(diff):.0f}% "
                    f"{'below' if diff < 0 else 'above'} the overall average of ${overall_cpm:.2f}."
                )
        # 2. Best content type
        if best_ct is not None and pd.notna(best_ct.get("avg_er_uplift")):
            insights.append(
                f'"**{best_ct["Content Type"]}**" content had the highest ER uplift: '
                f'{best_ct["avg_er_uplift"]:+.1f}% average vs baseline (n={int(best_ct["n"])}).'
            )
        # 3. Best type
        if best_type is not None and pd.notna(best_type.get("avg_cost_per_signup")) and overall_cps:
            diff = _pct_diff(best_type["avg_cost_per_signup"], overall_cps)
            if diff is not None:
                insights.append(
                    f"**{best_type['Type']}** influencers had the lowest Cost/Signup at "
                    f"${best_type['avg_cost_per_signup']:.2f} — {abs(diff):.0f}% "
                    f"{'below' if diff < 0 else 'above'} the overall ${overall_cps:.2f}."
                )
        # 4. Best seniority
        if best_sen is not None and pd.notna(best_sen.get("avg_er_uplift")):
            insights.append(
                f"**{best_sen['Senority']}**-level influencers showed {best_sen['avg_er_uplift']:+.1f}% "
                f"average ER uplift (n={int(best_sen['n'])})."
            )
        # 5. Recommendation
        rec_parts = []
        if best_fol is not None: rec_parts.append(best_fol["_follower_bucket"])
        if best_ct is not None: rec_parts.append(f'"{best_ct["Content Type"]}"')
        if rec_parts:
            insights.append(
                f"**Recommendation:** prioritize {' + '.join(rec_parts)} combinations "
                f"for next campaign wave."
            )

        if insights:
            for i, t in enumerate(insights, 1):
                st.markdown(f"{i}. {t}")
            st.caption(
                "ℹ️ Overall Score weights ER uplift (30%), Views vs Avg (40%), and CPM (30%) — "
                "we use these 3 because Cost/Signup via UTM attribution is unreliable "
                "(most signups get bucketed as organic). See Methodology at the bottom."
            )
        else:
            st.info("Not enough data for insights yet — fill in more campaign results.")

        # ─── Top Performers ──────────────────────────────────────────
        st.markdown('<a name="top-performers"></a>', unsafe_allow_html=True)
        st.markdown("---")
        st.subheader("🏆 Top Performers")

        _tp_cards, _tp_full = st.tabs(["Top 3 Cards", "Full Ranking"])

        with _tp_cards:

            def _top_card(name, poc, metric_label, metric_value, post_link, emphasize=False):
                pc = poc_color(poc)
                # Ensure URL has a scheme — otherwise browser treats it as relative
                # and navigates within the Streamlit app
                if post_link:
                    _link = post_link.strip()
                    if _link and not _link.lower().startswith(("http://", "https://")):
                        _link = "https://" + _link.lstrip("/")
                    link_html = f'<a href="{_link}" target="_blank" rel="noopener noreferrer" style="color:#3B82F6; font-size:0.78em; text-decoration:none;">View post ↗</a>'
                else:
                    link_html = ""
                bg = "#FFFBEB" if emphasize else "#FAFBFC"
                border = "#F59E0B" if emphasize else pc
                return (
                    f'<div style="background:{bg}; border-radius:10px; padding:12px 14px; margin-bottom:8px; border-left:4px solid {border};">'
                    f'<div style="font-weight:600; color:#1F2937; font-size:0.95em;">{name or "(no name)"}</div>'
                    f'<div style="display:flex; align-items:center; gap:6px; font-size:0.78em; color:#6B7280; margin-top:2px;">'
                    f'<span style="width:7px; height:7px; border-radius:50%; background:{pc}; display:inline-block;"></span>{poc}</div>'
                    f'<div style="margin-top:6px; font-size:1.2em; font-weight:700; color:#1F2937;">{metric_value}</div>'
                    f'<div style="font-size:0.72em; color:#9CA3AF; text-transform:uppercase; letter-spacing:0.03em;">{metric_label}</div>'
                    f'<div style="margin-top:4px;">{link_html}</div>'
                    f'</div>'
                )

            # Primary: Top 3 by Overall Score (the MVP ranking)
            st.markdown("##### 🏆 Campaign MVPs (by Overall Score)")
            st.caption("Composite score — see ℹ️ About Overall Score above for the formula")
            top_score = df_rep.dropna(subset=["_overall_score"]).sort_values("_overall_score", ascending=False).head(3)
            if top_score.empty:
                st.info("No Overall Score data yet — need ER uplift, Views vs Avg, and CPM.")
            else:
                mvp_cols = st.columns(3)
                for i, (_, r) in enumerate(top_score.iterrows()):
                    with mvp_cols[i]:
                        st.markdown(_top_card(
                            r.get("Name", ""), (r.get("POC") or "").strip(),
                            "Overall Score", f"{r['_overall_score']:.0f}",
                            (r.get("Post Link") or "").strip(),
                            emphasize=True,
                        ), unsafe_allow_html=True)

            st.markdown("##### By Individual Dimensions")
            st.caption("Breakdowns if you want to dig into specific signals")

            tp_col1, tp_col2, tp_col3 = st.columns(3)
            # Top 3 by ER vs Baseline
            with tp_col1:
                st.markdown("**🎯 By ER vs Baseline**")
                st.caption("Highest content resonance")
                top_er = df_rep.dropna(subset=["_er_uplift"]).sort_values("_er_uplift", ascending=False).head(3)
                if top_er.empty:
                    st.info("No ER data yet.")
                else:
                    for _, r in top_er.iterrows():
                        st.markdown(_top_card(
                            r.get("Name", ""), (r.get("POC") or "").strip(),
                            "ER vs Baseline", f"{r['_er_uplift']:+.1f}%",
                            (r.get("Post Link") or "").strip(),
                        ), unsafe_allow_html=True)
            # Top 3 by 24hr Views
            with tp_col2:
                st.markdown("**👁️ By 24hr Views**")
                st.caption("Highest absolute reach")
                top_v = df_rep.dropna(subset=["_views_24hr_num"]).sort_values("_views_24hr_num", ascending=False).head(3)
                if top_v.empty:
                    st.info("No views data yet.")
                else:
                    for _, r in top_v.iterrows():
                        st.markdown(_top_card(
                            r.get("Name", ""), (r.get("POC") or "").strip(),
                            "24hr Views", f"{int(r['_views_24hr_num']):,}",
                            (r.get("Post Link") or "").strip(),
                        ), unsafe_allow_html=True)
            # Top 3 by CPM (lowest first)
            with tp_col3:
                st.markdown("**💰 By CPM (lowest)**")
                st.caption("Most cost-efficient reach")
                top_cpm = df_rep.dropna(subset=["_cpm"]).sort_values("_cpm", ascending=True).head(3)
                if top_cpm.empty:
                    st.info("No CPM data yet.")
                else:
                    for _, r in top_cpm.iterrows():
                        st.markdown(_top_card(
                            r.get("Name", ""), (r.get("POC") or "").strip(),
                            "CPM", f"${r['_cpm']:.2f}",
                            (r.get("Post Link") or "").strip(),
                        ), unsafe_allow_html=True)



        with _tp_full:
            # Select only the columns we'll use — avoids duplicate names when
            # renaming (df_rep has BOTH "24hr Views" string col AND _views_24hr_num
            # numeric col; renaming the latter would collide with the former).
            _perf_cols = ["Name", "POC", "Post Link", "Content Type", "Type",
                          "Senority", "Job Function",
                          "_price_num", "_views_24hr_num", "_signups_num",
                          "_avg_impressions_num", "_post_er_num", "_baseline_er_num",
                          "_cpm", "_cost_per_signup", "_views_vs_avg",
                          "_er_uplift", "_overall_score"]
            _perf_available = [c for c in _perf_cols if c in df_rep.columns]
            _perf = df_rep[df_rep["_price_num"].notna()][_perf_available].copy()
            if not _perf.empty:
                # Reuse df_rep's pre-computed columns (no need to recompute).
                # Use pd.to_numeric(errors="coerce") to guard against Python 3.14's
                # stricter round() that rejects None.
                _perf["Score"] = pd.to_numeric(_perf["_overall_score"], errors="coerce").round(0)
                _perf["CPM ($)"] = pd.to_numeric(_perf["_cpm"], errors="coerce").round(2)
                _perf["Cost/Signup ($)"] = pd.to_numeric(_perf["_cost_per_signup"], errors="coerce").round(2)
                _perf["Views vs Avg %"] = pd.to_numeric(_perf["_views_vs_avg"], errors="coerce").round(1)
                _perf["ER vs Baseline %"] = pd.to_numeric(_perf["_er_uplift"], errors="coerce").round(1)
                # Drop the numeric helpers to avoid confusion in the final display
                _perf = _perf.drop(columns=[c for c in ["_cpm", "_cost_per_signup",
                                                         "_views_vs_avg", "_er_uplift",
                                                         "_overall_score"]
                                            if c in _perf.columns])
                _perf = _perf.rename(columns={
                    "_price_num": "Cost ($)",
                    "_views_24hr_num": "24hr Views",
                    "_signups_num": "Signups",
                    "Senority": "Seniority",
                    "_avg_impressions_num": "Avg Views",
                    "_post_er_num": "Post ER",
                    "_baseline_er_num": "Baseline ER",
                })

                _sort_mode = st.pills(
                    "Sort by",
                    ["By Overall Score", "By CPM (best value)", "By 24hr Views"],
                    default="By Overall Score",
                    key="rep_perf_sort",
                )
                if _sort_mode == "By 24hr Views":
                    _perf_sorted = _perf.sort_values("24hr Views", ascending=False, na_position="last")
                elif _sort_mode == "By CPM (best value)":
                    _perf_sorted = _perf.sort_values("CPM ($)", ascending=True, na_position="last")
                else:
                    _perf_sorted = _perf.sort_values("Score", ascending=False, na_position="last")

                _perf_display_order = ["Name", "POC", "Score", "Content Type", "Type", "Seniority", "Job Function",
                                       "Cost ($)", "24hr Views", "Avg Views", "Views vs Avg %",
                                       "Post ER", "Baseline ER", "ER vs Baseline %",
                                       "Signups", "CPM ($)", "Cost/Signup ($)", "Post Link"]
                _perf_display_order = [c for c in _perf_display_order if c in _perf_sorted.columns]
                st.dataframe(
                    _perf_sorted[_perf_display_order],
                    use_container_width=True, hide_index=True,
                    column_config={
                        "Post Link": st.column_config.LinkColumn("Post Link"),
                        "Score": st.column_config.NumberColumn("Score", format="%.0f",
                            help="0-100 composite: 30% ER uplift + 40% Views vs Avg + 30% CPM"),
                        "Views vs Avg %": st.column_config.NumberColumn("Views vs Avg %", format="%+.1f%%"),
                        "ER vs Baseline %": st.column_config.NumberColumn("ER vs Baseline %", format="%+.1f%%"),
                        "Post ER": st.column_config.NumberColumn("Post ER", format="%.2f%%"),
                        "Baseline ER": st.column_config.NumberColumn("Baseline ER", format="%.2f%%"),
                    },
                )
            else:
                st.info("Performance data will appear after cost and views/signups are entered.")
        # ─── Segment Analysis Helper ─────────────────────────────────
        def _segment_stats(df, by_col, label=None):
            """Aggregate per-segment metrics. Returns DataFrame sorted by Avg CPM."""
            if by_col not in df.columns:
                return pd.DataFrame()
            d = df.copy()
            d[by_col] = d[by_col].astype(str).str.strip()
            d = d[d[by_col] != ""]
            if d.empty:
                return pd.DataFrame()
            g = d.groupby(by_col).agg(
                n=("Name", "count"),
                avg_cpm=("_cpm", "mean"),
                avg_cost_per_signup=("_cost_per_signup", "mean"),
                avg_er_uplift=("_er_uplift", "mean"),
                avg_views=("_views_24hr_num", "mean"),
                avg_score=("_overall_score", "mean"),
            ).reset_index()
            g = g.sort_values("avg_score", ascending=False, na_position="last")
            return g

        def _segment_display(seg_df, label, first_col_name=None):
            """Render a segment table — clean, scannable, best row highlighted.

            Sorted by Overall Score desc (best first). Best row gets 🥇 + green bg.
            """
            if seg_df.empty:
                st.info(f"No data for {label} segmentation.")
                return
            # Sort by Overall Score (desc). Fall back to ER uplift, then CPM.
            if seg_df.get("avg_score", pd.Series(dtype=float)).notna().any():
                sorted_df = seg_df.sort_values("avg_score", ascending=False, na_position="last")
            elif seg_df["avg_er_uplift"].notna().any():
                sorted_df = seg_df.sort_values("avg_er_uplift", ascending=False, na_position="last")
            else:
                sorted_df = seg_df.sort_values("avg_cpm", ascending=True, na_position="last")

            seg_col = first_col_name or seg_df.columns[0]
            # Find index of best row (highest Overall Score)
            best_idx = None
            if sorted_df.get("avg_score", pd.Series(dtype=float)).notna().any():
                best_idx = sorted_df.index[sorted_df["avg_score"].notna()][0]
            elif sorted_df["avg_er_uplift"].notna().any():
                best_idx = sorted_df.index[sorted_df["avg_er_uplift"].notna()][0]

            # Build HTML table (Score column first after segment name)
            html = (
                '<table style="width:100%; border-collapse:collapse; font-size:0.88em; margin-bottom:8px;">'
                '<thead><tr style="background:#F3F4F6; color:#374151;">'
                f'<th style="padding:10px 12px; text-align:left; border-bottom:2px solid #E5E7EB;">{label}</th>'
                '<th style="padding:10px 12px; text-align:center; border-bottom:2px solid #E5E7EB;">#</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">Score</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">ER vs Baseline</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">Avg Views</th>'
                '<th style="padding:10px 12px; text-align:right; border-bottom:2px solid #E5E7EB;">Avg CPM</th>'
                '</tr></thead><tbody>'
            )

            for i, (_, row) in enumerate(sorted_df.iterrows()):
                is_best = (row.name == best_idx)
                bg = "#ECFDF5" if is_best else ("#FAFBFC" if i % 2 else "#FFFFFF")
                crown = "🥇 " if is_best else ""
                name = f'{crown}{row[seg_col]}'

                # Format metrics
                n_val = int(row["n"]) if pd.notna(row.get("n")) else 0
                score = row.get("avg_score")
                if pd.isna(score):
                    score_html = '<span style="color:#9CA3AF;">—</span>'
                else:
                    if score >= 70: score_color = "#059669"
                    elif score >= 50: score_color = "#3B82F6"
                    else: score_color = "#9CA3AF"
                    score_html = f'<span style="color:{score_color}; font-weight:700;">{score:.0f}</span>'
                er = row.get("avg_er_uplift")
                if pd.isna(er):
                    er_html = '<span style="color:#9CA3AF;">—</span>'
                else:
                    color = "#059669" if er >= 0 else "#DC2626"
                    er_html = f'<span style="color:{color}; font-weight:600;">{er:+.1f}%</span>'
                views = f"{int(row['avg_views']):,}" if pd.notna(row.get("avg_views")) else "—"
                cpm = f"${row['avg_cpm']:.2f}" if pd.notna(row.get("avg_cpm")) else "—"

                html += (
                    f'<tr style="background:{bg}; border-bottom:1px solid #F3F4F6;">'
                    f'<td style="padding:9px 12px; color:#1F2937; font-weight:{("600" if is_best else "500")};">{name}</td>'
                    f'<td style="padding:9px 12px; text-align:center; color:#6B7280;">{n_val}</td>'
                    f'<td style="padding:9px 12px; text-align:right;">{score_html}</td>'
                    f'<td style="padding:9px 12px; text-align:right;">{er_html}</td>'
                    f'<td style="padding:9px 12px; text-align:right; color:#6B7280;">{views}</td>'
                    f'<td style="padding:9px 12px; text-align:right; color:#374151;">{cpm}</td>'
                    f'</tr>'
                )
            html += '</tbody></table>'
            st.markdown(html, unsafe_allow_html=True)

        # ─── Profile Breakdown (campaign composition) ────────────────
        st.markdown("---")
        st.markdown('<a name="profile-breakdown"></a>', unsafe_allow_html=True)
        st.subheader("👥 Profile Breakdown")
        st.caption("What this campaign looked like — distribution across audience dimensions.")
        _bd1, _bd2, _bd3 = st.columns(3)
        _breakdown_palette = ["#748FFC", "#FF922B", "#63E6BE", "#F06595", "#B197FC",
                              "#FCC419", "#22D3EE", "#A9E34B", "#FF6B6B", "#DDA0DD"]
        for _col_ui, _col_name, _title in [
            (_bd1, "Type", "Type"),
            (_bd2, "Job Function", "Job Function"),
            (_bd3, "Senority", "Seniority"),
        ]:
            with _col_ui:
                if _col_name in df_rep.columns:
                    _vals = df_rep[_col_name].str.strip()
                    _vals = _vals[_vals != ""]
                    if not _vals.empty:
                        _counts = _vals.value_counts().reset_index()
                        _counts.columns = [_title, "Count"]
                        import plotly.graph_objects as go
                        _fig = go.Figure(data=[go.Pie(
                            labels=_counts[_title],
                            values=_counts["Count"],
                            marker=dict(colors=_breakdown_palette[:len(_counts)],
                                        line=dict(color="#fff", width=2)),
                            textinfo="value+percent",
                            texttemplate="%{value} (%{percent})",
                            textposition="inside",
                            insidetextorientation="horizontal",
                            hole=0.45,
                            textfont=dict(size=10, family="DM Sans, Inter, sans-serif", color="#fff"),
                        )])
                        _fig.update_layout(
                            title=dict(text=_title, font=dict(size=13)),
                            margin=dict(t=36, b=60, l=10, r=10),
                            height=320,
                            showlegend=True,
                            legend=dict(orientation="h", yanchor="top", y=-0.05,
                                        xanchor="center", x=0.5, font=dict(size=10)),
                            font=dict(family="DM Sans, Inter, sans-serif"),
                            paper_bgcolor="rgba(0,0,0,0)",
                        )
                        st.plotly_chart(_fig, use_container_width=True, key=f"rep_bd_{_col_name}")
                    else:
                        st.info(f"No {_title} data.")
                else:
                    st.info(f"No {_title} column.")

        # Second row: Content Type + Followers distribution
        _bd4, _bd5 = st.columns(2)
        # Content Type donut
        with _bd4:
            if "Content Type" in df_rep.columns:
                _ct_vals = df_rep["Content Type"].str.strip()
                _ct_vals = _ct_vals[_ct_vals != ""]
                if not _ct_vals.empty:
                    _ct_counts = _ct_vals.value_counts().reset_index()
                    _ct_counts.columns = ["Content Type", "Count"]
                    import plotly.graph_objects as go
                    _fig_ct = go.Figure(data=[go.Pie(
                        labels=_ct_counts["Content Type"],
                        values=_ct_counts["Count"],
                        marker=dict(colors=_breakdown_palette[:len(_ct_counts)],
                                    line=dict(color="#fff", width=2)),
                        textinfo="value+percent",
                        texttemplate="%{value} (%{percent})",
                        textposition="inside",
                        insidetextorientation="horizontal",
                        hole=0.45,
                        textfont=dict(size=10, family="DM Sans, Inter, sans-serif", color="#fff"),
                    )])
                    _fig_ct.update_layout(
                        title=dict(text="Content Type", font=dict(size=13)),
                        margin=dict(t=36, b=60, l=10, r=10),
                        height=320,
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="top", y=-0.05,
                                    xanchor="center", x=0.5, font=dict(size=10)),
                        font=dict(family="DM Sans, Inter, sans-serif"),
                        paper_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(_fig_ct, use_container_width=True, key="rep_bd_content_type")
                else:
                    st.info("No Content Type data.")
        # Followers distribution donut (bucketed)
        with _bd5:
            _fol = df_rep["_followers_num"].dropna()
            if not _fol.empty:
                _fol_buckets = _fol.apply(follower_bucket)
                _fol_counts = _fol_buckets.value_counts().reindex(FOLLOWER_BUCKET_ORDER).dropna().astype(int).reset_index()
                _fol_counts.columns = ["Followers", "Count"]
                import plotly.graph_objects as go
                _fig_fol = go.Figure(data=[go.Pie(
                    labels=_fol_counts["Followers"],
                    values=_fol_counts["Count"],
                    marker=dict(colors=["#B197FC", "#748FFC", "#22D3EE", "#FF922B", "#FF6B6B"][:len(_fol_counts)],
                                line=dict(color="#fff", width=2)),
                    textinfo="value+percent",
                    texttemplate="%{value} (%{percent})",
                    textposition="inside",
                    insidetextorientation="horizontal",
                    hole=0.45,
                    textfont=dict(size=10, family="DM Sans, Inter, sans-serif", color="#fff"),
                )])
                _fig_fol.update_layout(
                    title=dict(text="Followers Distribution", font=dict(size=13)),
                    margin=dict(t=36, b=60, l=10, r=10),
                    height=320,
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="top", y=-0.05,
                                xanchor="center", x=0.5, font=dict(size=10)),
                    font=dict(family="DM Sans, Inter, sans-serif"),
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(_fig_fol, use_container_width=True, key="rep_bd_followers")
            else:
                st.info("No Followers data.")

        # ─── Segment Analysis — 4 dimensions ──────────────────────────
        st.markdown("---")
        st.markdown('<a name="what-worked-best"></a>', unsafe_allow_html=True)
        st.subheader("📐 What Worked Best")
        st.caption("Each table compares how different segments performed. 🥇 marks the best segment (highest Avg Overall Score). Sorted by Score descending.")

        # 4 dimensions as tabs — click to switch instead of stacking all 4
        _ww_fol, _ww_ct, _ww_type, _ww_sen = st.tabs([
            "By Followers Size", "By Content Hook", "By Influencer Type", "By Seniority",
        ])
        with _ww_fol:
            st.caption("🥇 best performer within each tier — use this when sourcing the next round of same-size influencers")
            fol_stats = _segment_stats(df_rep, "_follower_bucket")
            _segment_display(fol_stats, "Followers", first_col_name="_follower_bucket")
        with _ww_ct:
            ct_stats = _segment_stats(df_rep, "Content Type")
            _segment_display(ct_stats, "Content Type", first_col_name="Content Type")
        with _ww_type:
            type_stats = _segment_stats(df_rep, "Type")
            _segment_display(type_stats, "Type", first_col_name="Type")
        with _ww_sen:
            sen_stats = _segment_stats(df_rep, "Senority")
            _segment_display(sen_stats, "Seniority", first_col_name="Senority")

        # ─── Best Archetype ──────────────────────────────────────────
        st.markdown("---")
        st.markdown('<a name="best-archetype"></a>', unsafe_allow_html=True)
        st.subheader("🏅 Best Archetype")

        def _best_segment(seg_df, metric="avg_score", ascending=False):
            """Pick the segment with the best value for a given metric (default: Overall Score)."""
            if seg_df.empty or metric not in seg_df.columns:
                return None
            d = seg_df.dropna(subset=[metric])
            if d.empty:
                return None
            d = d.sort_values(metric, ascending=ascending)
            return d.iloc[0]

        best_fol = _best_segment(fol_stats)
        best_ct = _best_segment(ct_stats)
        best_type = _best_segment(type_stats)
        best_sen = _best_segment(sen_stats)

        archetype_parts = []
        if best_fol is not None: archetype_parts.append(f"**{best_fol['_follower_bucket']}**")
        if best_ct is not None: archetype_parts.append(f"**{best_ct['Content Type']}**")
        if best_type is not None: archetype_parts.append(f"**{best_type['Type']}**")
        if best_sen is not None: archetype_parts.append(f"**{best_sen['Senority']}** seniority")

        if archetype_parts:
            # Use best Content Type segment as proxy for characteristics (most granular)
            proxy = best_ct if best_ct is not None else (best_fol if best_fol is not None else best_type)
            chars = []
            if pd.notna(proxy.get("avg_score")): chars.append(f"avg Score {proxy['avg_score']:.0f}")
            if pd.notna(proxy.get("avg_er_uplift")): chars.append(f"ER uplift {proxy['avg_er_uplift']:+.1f}%")
            if pd.notna(proxy.get("avg_cpm")): chars.append(f"CPM ${proxy['avg_cpm']:.2f}")
            st.markdown(
                f'<div style="background:#F0F9FF; border:1px solid #BAE6FD; border-radius:8px; padding:16px 20px;">'
                f'<div style="font-weight:700; color:#0C4A6E; margin-bottom:6px;">Based on Overall Score, the best-performing archetype is:</div>'
                f'<div style="font-size:1.05em; color:#1F2937; margin-bottom:8px;">{" · ".join(archetype_parts)}</div>'
                f'<div style="font-size:0.88em; color:#6B7280;">Characteristics (best Content Type segment): {" · ".join(chars) if chars else "—"}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("Not enough data to compute best archetype.")

        # ─── Performance Charts (scatter) ─────────────────────────────
        st.markdown("---")
        st.markdown('<a name="performance-charts"></a>', unsafe_allow_html=True)
        st.subheader("📈 Performance Charts")
        pc1, pc2 = st.columns(2)
        with pc1:
            fig = cost_vs_views_scatter(df_rep)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="rep_cost_views")
            else:
                st.info("Cost vs Views will appear after campaign data is entered.")
        with pc2:
            fig = followers_vs_er_scatter(df_rep)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="rep_fver")

        # ─── CSV Export ──────────────────────────────────────────────
        st.markdown("---")
        st.markdown('<a name="export"></a>', unsafe_allow_html=True)
        st.subheader("📥 Export")

        # Build consolidated CSV
        export_lines = []
        export_lines.append("# Campaign Report — Per-Influencer Data")
        export_lines.append("")
        indiv = df_rep.copy()
        indiv["Views vs Avg %"] = indiv.apply(
            lambda r: round((r["_views_24hr_num"] / r["_avg_impressions_num"] - 1) * 100, 1)
            if pd.notna(r.get("_views_24hr_num")) and pd.notna(r.get("_avg_impressions_num")) and r["_avg_impressions_num"] > 0 else None,
            axis=1,
        )
        # Use pd.to_numeric with errors='coerce' so None/NaN don't break .round()
        # Python 3.14's stricter round() rejects None with TypeError
        indiv["ER vs Baseline %"] = pd.to_numeric(indiv["_er_uplift"], errors="coerce").round(1)
        indiv["CPM ($)"] = pd.to_numeric(indiv["_cpm"], errors="coerce").round(2)
        indiv["Cost/Signup ($)"] = pd.to_numeric(indiv["_cost_per_signup"], errors="coerce").round(2)
        indiv_cols = ["Name", "POC", "Post Link", "Content Type", "Type", "Senority", "Job Function",
                      "followers", "Country", "Price（$)", "24hr Views", "Signups",
                      "Post ER", "Baseline ER", "Views vs Avg %", "ER vs Baseline %",
                      "CPM ($)", "Cost/Signup ($)", "Post Date"]
        indiv_cols = [c for c in indiv_cols if c in indiv.columns]
        # Rename display labels before CSV export (fix Sheet typo in output)
        export_lines.append(
            indiv[indiv_cols].rename(columns=DISPLAY_LABELS).to_csv(index=False)
        )
        export_lines.append("")

        # Segment aggregations
        for label, sdf in [
            ("Followers Bucket", fol_stats),
            ("Content Type", ct_stats),
            ("Type", type_stats),
            ("Seniority", sen_stats),
        ]:
            if sdf.empty:
                continue
            export_lines.append(f"# Segment: {label}")
            export_lines.append("")
            export_lines.append(sdf.round(2).to_csv(index=False))
            export_lines.append("")

        full_csv = "\n".join(export_lines)
        st.download_button(
            "\U0001f4e5 Download Full Report (CSV)", data=full_csv,
            file_name=f"report_{selected_tag_str.replace(' ', '_')}.csv", mime="text/csv",
            use_container_width=True,
        )

        # ─── Methodology Appendix (reference only, at bottom) ────────
        st.markdown("---")
        with st.expander("ℹ️ Methodology — about Overall Score", expanded=False):
            st.markdown("""
**Formula:**
```
Overall Score =  0.30 × ER uplift percentile
              +  0.40 × Views vs Avg percentile
              +  0.30 × CPM percentile (reversed)
```
Output is 0–100. Each dimension is percentile-ranked within this campaign so different scales combine fairly.

**Why Views vs Avg instead of absolute Views?**
Each influencer is compared to their OWN last-10-reels baseline, so Nano (<10K) and Macro (100K+) use the same yardstick.
A Nano whose post beat their normal views by +50% scores the same as a Macro who did the same — fair across follower tiers.

**Why not Cost/Signup?**
UTM attribution is unreliable — most signups fall into organic. So we judge on three signals that we CAN measure well:
ER uplift (content fit), Views vs Avg (reach lift), CPM (cost efficiency).

**Weight rationale:**
- **Views vs Avg 40%** — reach matters most, but only in relative terms
- **ER uplift 30%** — did the content actually resonate with their audience?
- **CPM 30%** — cost efficiency, keeps spend honest
            """)
