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
    prepare_dataframe, filter_by_contact_date,
    filter_by_status, parse_date, get_timeline_status,
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
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');

    /* Global font */
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif !important; }

    /* Typography */
    .stMetric label {
        font-size: 0.78em !important; color: #6B7280 !important;
        letter-spacing: 0.03em; text-transform: uppercase; font-weight: 500;
    }
    .stMetric [data-testid="stMetricValue"] {
        font-size: 1.6em !important; color: #1F2937 !important; font-weight: 700;
    }

    /* Kanban cards — playful, rounded */
    .kanban-card {
        background: #FAFBFC;
        border-radius: 10px;
        padding: 12px 16px;
        margin: 6px 0;
        border-left: 4px solid #E5E7EB;
        font-size: 0.84em;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        transition: all 0.15s ease;
    }
    .kanban-card:hover {
        background: #F0F4FF;
        box-shadow: 0 2px 6px rgba(0,0,0,0.08);
        transform: translateY(-1px);
    }
    .kanban-card .name { font-weight: 600; color: #1F2937; }
    .kanban-card .poc { color: #6B7280; font-size: 0.8em; margin-top: 3px; font-weight: 500; }
    .stage-header {
        font-weight: 700; font-size: 0.85em;
        padding: 10px 0; margin-bottom: 8px;
        border-bottom: 3px solid #E5E7EB;
        color: #374151;
        text-transform: uppercase;
        letter-spacing: 0.02em;
    }

    /* POC colors */
    .poc-jenny { color: #748FFC; }
    .poc-doris { color: #FF922B; }
    .poc-jialin { color: #F06595; }
    .poc-falida { color: #63E6BE; }
    .poc-other { color: #B197FC; }

    /* Page container */
    .block-container { padding-top: 1.2rem; }

    /* Subtle divider */
    hr { border-color: #E5E7EB !important; }

    /* Pills navigation styling */
    div[data-testid="stPills"] button {
        border-radius: 20px !important;
        padding: 6px 18px !important;
        font-weight: 500 !important;
        font-size: 0.88em !important;
        border: 1.5px solid #E5E7EB !important;
        transition: all 0.2s ease !important;
    }
    div[data-testid="stPills"] button[aria-checked="true"] {
        background: #1F2937 !important;
        color: white !important;
        border-color: #1F2937 !important;
    }
    div[data-testid="stPills"] button:hover {
        border-color: #9CA3AF !important;
    }

    /* Data editor styling */
    [data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
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

def make_column_config(editable_cols):
    config = {}
    for col_name, col_type in editable_cols.items():
        if col_type == "select_status":
            config[col_name] = st.column_config.SelectboxColumn(col_name, options=STATUS_OPTIONS)
        elif col_type == "select_collab":
            config[col_name] = st.column_config.SelectboxColumn(col_name, options=COLLAB_STAGE_OPTIONS)
        elif col_type == "select_payment":
            config[col_name] = st.column_config.SelectboxColumn(col_name, options=PAYMENT_PROGRESS_OPTIONS)
        elif col_type == "select_campaign":
            config[col_name] = st.column_config.SelectboxColumn(col_name, options=CAMPAIGN_TAG_OPTIONS)
        elif col_type == "select_contract":
            config[col_name] = st.column_config.SelectboxColumn(col_name, options=CONTRACT_OPTIONS)
        elif col_type == "text":
            config[col_name] = st.column_config.TextColumn(col_name)
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
                # Also update the in-memory DataFrame so changes show immediately
                if "df" in st.session_state:
                    df_mem = st.session_state["df"]
                    for s_row, col_name, val in edit_details:
                        mask = df_mem["_sheet_row"] == s_row
                        if mask.any():
                            df_mem.loc[mask, col_name] = val
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
            for name, poc in people:
                pc = poc_color(poc)
                chips += (
                    f'<div style="display:flex; align-items:center; gap:6px; padding:3px 0; font-size:0.82em;">'
                    f'<span style="width:8px; height:8px; border-radius:50%; background:{pc}; flex-shrink:0; display:inline-block;"></span>'
                    f'<span style="color:#1F2937; font-weight:500;">{name or "(no name)"}</span>'
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

st.title("\U0001f4ca Campaign Dashboard")
_subtitle_parts = []
if use_date_filter:
    _subtitle_parts.append(f"{start_date.strftime('%m/%d')} \u2013 {end_date.strftime('%m/%d')}")
if selected_tag != "(All)":
    _subtitle_parts.append(f"{selected_tag} Campaign")
_subtitle_parts.append(f"{len(confirmed)} confirmed  \u00b7  {len(df_filtered)} in view")
st.caption("  \u00b7  ".join(_subtitle_parts))

# ─── Navigation ──────────────────────────────────────────────────────────────

NAV_OPTIONS = ["Overview", "Pipeline", "Content & Delivery", "Payment & Performance"]

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
        # Confirmed follows Campaign Tag filter; Contacted is always the full date range
        ov_confirmed = df_filtered[df_filtered["Status"] == "Confirm"]

        # ─── Today's Activity ────────────────────────────────────────────
        try:
            from zoneinfo import ZoneInfo
            _today_local = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        except Exception:
            _today_local = date.today()

        today_contacts = df_all[df_all["_date_of_contact_parsed"] == _today_local]
        today_by_poc = today_contacts["POC"].value_counts()
        today_total = len(today_contacts)

        overdue_list_ov, _, _ = get_timeline_status(df_filtered)
        overdue_count = len(overdue_list_ov)

        st.subheader("Today's Activity")
        if today_total > 0:
            poc_chips = ""
            for poc_name, count in today_by_poc.items():
                if not poc_name.strip():
                    continue
                pc = poc_color(poc_name)
                poc_chips += (
                    f'<span style="display:inline-flex; align-items:center; gap:5px; margin-right:20px; font-size:0.9em;">'
                    f'<span style="color:{pc}; font-weight:700;">{poc_name}</span>'
                    f'<span style="color:#1F2937; font-weight:600;">{count}</span>'
                    f'</span>'
                )
            overdue_html = ""
            if overdue_count > 0:
                overdue_html = f'<span style="color:#DC2626; font-weight:600; margin-left:20px;">⚠️ {overdue_count} overdue</span>'
            st.markdown(
                f'<div style="display:flex; align-items:center; flex-wrap:wrap; padding:8px 0;">'
                f'{poc_chips}'
                f'<span style="color:#6B7280; font-size:0.85em;">{today_total} new contacts today</span>'
                f'{overdue_html}'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            overdue_html = f' &nbsp;·&nbsp; <span style="color:#DC2626; font-weight:600;">⚠️ {overdue_count} overdue</span>' if overdue_count > 0 else ""
            st.markdown(
                f'<div style="color:#9CA3AF; font-size:0.88em; padding:8px 0;">'
                f'No outreach recorded today{overdue_html}</div>',
                unsafe_allow_html=True,
            )

        # 7-day outreach chart
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
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Contacted", len(df_by_contact))
        k2.metric("Confirmed", len(ov_confirmed))
        avg_er = ov_confirmed["_er_num"].dropna().mean()
        k3.metric("Avg ER%", f"{avg_er:.2f}%" if pd.notna(avg_er) else "N/A")
        total_price = ov_confirmed["_price_num"].dropna().sum()
        k4.metric("Total Cost", f"${total_price:,.0f}" if total_price > 0 else "N/A")
        avg_fol = ov_confirmed["_followers_num"].dropna().mean()
        k5.metric("Avg Followers", f"{avg_fol:,.0f}" if pd.notna(avg_fol) else "N/A")

        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            fig = status_distribution_pie(df_by_contact)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_status")
            st.caption("All people contacted in this period.")
        with c2:
            fig = collab_stage_detail(ov_confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_collab")

        st.markdown("---")
        c3, c4 = st.columns(2)
        with c3:
            fig = er_histogram(ov_confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_er")
            st.caption("X axis = ER%, Y axis = number of influencers in that range. "
                       "Green dashed = median, red dotted = average (pulled up by outliers).")
        with c4:
            fig = followers_vs_er_scatter(ov_confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_fver")
            st.caption("Top-left = high ER with fewer followers (great value).")


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
        try:
            from zoneinfo import ZoneInfo
            _today_pipe = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        except Exception:
            _today_pipe = date.today()
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

        # ── Missed Posts Alert ────────────────────────────────────────
        try:
            from zoneinfo import ZoneInfo
            _tl_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        except Exception:
            _tl_today = date.today()

        _confirmed_with_pd = df_filtered[
            (df_filtered["Status"] == "Confirm")
            & (df_filtered["_post_date_parsed"].notna())
        ].copy()
        _missed_posts = _confirmed_with_pd[
            (_confirmed_with_pd["_post_date_parsed"] <= _tl_today)
            & (_confirmed_with_pd["Collaboration Stage"].str.strip() != "Posted")
        ]
        if not _missed_posts.empty:
            _mp_html = (
                '<div style="background:#FEF2F2; border:1px solid #FECACA; border-radius:8px; '
                'padding:14px 18px; margin-bottom:16px;">'
                f'<div style="font-weight:700; font-size:0.92em; color:#DC2626; margin-bottom:8px;">'
                f'⚠️ {len(_missed_posts)} Missed Post{"s" if len(_missed_posts) != 1 else ""}</div>'
                '<div style="display:flex; flex-wrap:wrap; gap:4px 18px;">'
            )
            for _, _mr in _missed_posts.iterrows():
                _mn = (_mr.get("Name", "") or "").strip() or "(no name)"
                _mp = (_mr.get("POC", "") or "").strip()
                _md = _mr["_post_date_parsed"]
                _md_str = _md.strftime("%m/%d") if _md else ""
                _mpc = poc_color(_mp)
                _mp_html += (
                    f'<span style="display:inline-flex; align-items:center; gap:4px; font-size:0.84em;">'
                    f'<span style="width:7px; height:7px; border-radius:50%; background:{_mpc}; display:inline-block;"></span>'
                    f'<span style="color:#DC2626; font-weight:600;">{_mn}</span>'
                    f'<span style="color:#9CA3AF; font-size:0.85em;">({_md_str})</span>'
                    f'</span>'
                )
            _mp_html += '</div></div>'
            st.markdown(_mp_html, unsafe_allow_html=True)

        # Production Timeline Status
        overdue_list, in_progress_list, completed_count = get_timeline_status(df_filtered)
        st.subheader("Production Timeline")

        # Merge overdue + in_progress into one view, grouped by stage
        all_people = {}  # stage -> [(name, poc, days, is_overdue), ...]
        for name, poc, stage, days, _ in overdue_list:
            all_people.setdefault(stage, []).append((name, poc, days, True))
        for name, poc, stage, days, _ in in_progress_list:
            all_people.setdefault(stage, []).append((name, poc, days, False))

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
                for name, poc, days, is_over in people:
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
                    html += (
                        f'<span style="display:inline-flex; align-items:center; gap:4px; font-size:0.84em;">'
                        f'{icon}'
                        f'<span style="color:#1F2937; font-weight:500;">{name}</span>'
                        f'{day_label}'
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

        st.markdown("---")

        # ── Email Outreach ───────────────────────────────────────────────
        # Compute counts for section header
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
        _sent_tracked_all = df_all[df_all["Email Message-ID"].str.strip() != ""] if "Email Message-ID" in df_all.columns else pd.DataFrame()
        _sent_count = len(_sent_tracked_all)
        _opened_count = 0
        if _sent_count > 0 and "Email Opened" in _sent_tracked_all.columns:
            _opened_count = (_sent_tracked_all["Email Opened"].str.strip().str.lower() == "yes").sum()
        _open_rate_str = ""
        if _sent_count > 0:
            _open_pct = (_opened_count / _sent_count * 100)
            _open_rate_str = f", {_opened_count}/{_sent_count} opened ({_open_pct:.0f}%)"

        # ─── Email Tracking Panel ────────────────────────────────────────
        if _sent_count > 0:
            with st.expander(f"📊 Email Tracking ({_sent_count} sent{_open_rate_str})", expanded=False):
                # Summary metrics
                _unopened = _sent_count - _opened_count
                _tm1, _tm2, _tm3, _tm4 = st.columns(4)
                _tm1.metric("Total Sent", _sent_count)
                _tm2.metric("Opened", _opened_count)
                _tm3.metric("Unopened", _unopened)
                _tm4.metric("Open Rate", f"{_open_pct:.0f}%" if _sent_count > 0 else "N/A")

                # Filters
                _tf1, _tf2 = st.columns([2, 1])
                _poc_opts = sorted(set(_sent_tracked_all["POC"].dropna().str.strip().unique()) - {""})
                _sel_poc = _tf1.multiselect("Filter by POC", _poc_opts, key="track_poc")
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
                def _parse_sent(val):
                    if not val or not isinstance(val, str):
                        return None
                    try:
                        return datetime.strptime(val.strip(), "%Y-%m-%d %H:%M")
                    except (ValueError, AttributeError):
                        return None

                _tracked["_sent_parsed"] = _tracked["Last Email Sent"].apply(_parse_sent)
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
                            f'<td style="padding:8px 10px;"><span style="color:#1F2937; font-weight:500;">{_tname}</span></td>'
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

        with st.expander(f"📧 Email Outreach ({_unsent_count} unsent, {_fu_count} follow-ups needed{_open_rate_str})", expanded=False):
            # Gmail connection
            if "gmail_connected" not in st.session_state:
                st.session_state["gmail_connected"] = False

            if not st.session_state["gmail_connected"]:
                with st.expander("Connect Gmail to send emails", expanded=False):
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
                st.caption(f"✅ Connected as **{gmail_email}**")

                # Send outreach section — uses df_all, not affected by Campaign Tag filter
                df_unsent = df_all[df_all["Status"].str.strip() == ""]
                df_unsent = df_unsent[df_unsent["Contact"].str.strip() != ""]

                if not df_unsent.empty:
                    with st.expander(f"📤 Send Outreach ({len(df_unsent)} people with no status & valid email)", expanded=False):
                        # POC filter
                        poc_options = sorted(set(df_unsent["POC"].dropna().unique()) - {""})
                        send_poc_filter = st.multiselect("Filter by POC", poc_options, key="send_poc_filter")
                        if send_poc_filter:
                            df_unsent = df_unsent[df_unsent["POC"].isin(send_poc_filter)]

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
                    st.info("No unsent contacts (all have a Status or no email).")

                # Follow-up section
                df_contacted = df_filtered[df_filtered["Status"] == "Contacted"].copy()
                if "Email Message-ID" in df_contacted.columns and "Last Email Sent" in df_contacted.columns:
                    df_followable = df_contacted[df_contacted["Email Message-ID"].str.strip() != ""]
                    if not df_followable.empty:
                        with st.expander(f"🔄 Follow-Ups ({len(df_followable)} contacted, awaiting reply)", expanded=False):

                            from dashboard_utils.email_client import check_reply, send_followup as send_fu

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

                                        # Check if they already replied
                                        replied = check_reply(gmail_email, st.session_state["gmail_password"], msg_id)
                                        if replied:
                                            skipped += 1
                                            continue

                                        try:
                                            sender_name = gmail_email.split("@")[0].capitalize()
                                            send_fu(
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
                                            ])
                                            sent += 1
                                        except Exception as e:
                                            st.error(f"Failed to send to {r['Name']}: {e}")

                                    st.success(f"Follow-ups done! ✅ Sent {sent} / ⏭️ Skipped {skipped} (already replied)")
                            else:
                                st.info("No follow-ups needed right now.")
                    else:
                        st.info("No emails tracked yet. Send outreach first.")

        st.markdown("---")

        # Filters + table
        st.subheader("Full Table")
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
        pipe_display = PIPELINE_DISPLAY_COLS + (["Days in Stage"] if "Days in Stage" in df_pipe.columns else [])
        show_editable_table(
            df_pipe, pipe_display,
            {"Name": "text", "Contact": "text", "Type": "text",
             "Senority": "text", "Job Function": "text",
             "Status": "select_status",
             "Contract Status": "select_contract",
             "Collaboration Stage": "select_collab",
             "Campaign Tag": "select_campaign",
             "Confirm Date": "text", "POC": "text", "Notes": "text"},
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
            try:
                from zoneinfo import ZoneInfo
                _sched_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
            except Exception:
                _sched_today = date.today()

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
# Payment & Performance
# ═══════════════════════════════════════════════════════════════════════════════

elif nav == "Payment & Performance":
    df_pay = df_filtered[df_filtered["Status"] == "Confirm"].copy()
    if df_pay.empty:
        st.info("No confirmed influencers.")
    else:
        # ─── Campaign Delivery Progress ─────────────────────────────
        _stages = df_pay["Collaboration Stage"].str.strip()
        _contract = df_pay.get("Contract Status", pd.Series(dtype=str)).str.strip()
        _n_total = len(df_pay)
        _n_posted = ((_stages == "Posted") | (_stages == "Approved for posting")).sum()
        _n_production = ((_stages != "") & (_stages != "Posted") & (_stages != "Approved for posting")).sum()
        _n_pending = _n_total - _n_posted - _n_production

        _pct_posted = (_n_posted / _n_total * 100) if _n_total > 0 else 0
        _pct_prod = (_n_production / _n_total * 100) if _n_total > 0 else 0
        _pct_pend = (_n_pending / _n_total * 100) if _n_total > 0 else 0

        st.markdown(
            f'<div style="margin-bottom:6px; font-size:0.82em; color:#6B7280;">'
            f'<span style="color:#63E6BE; font-weight:600;">📦 Posted: {_n_posted}</span>'
            f'&nbsp;&nbsp;·&nbsp;&nbsp;'
            f'<span style="color:#3B82F6; font-weight:600;">🎬 In production: {_n_production}</span>'
            f'&nbsp;&nbsp;·&nbsp;&nbsp;'
            f'<span style="color:#F59E0B; font-weight:600;">📝 Pending: {_n_pending}</span>'
            f'&nbsp;&nbsp;·&nbsp;&nbsp;'
            f'Total: {_n_total}'
            f'</div>'
            f'<div style="display:flex; height:12px; border-radius:6px; overflow:hidden; background:#F3F4F6; margin-bottom:16px;">'
            f'<div style="width:{_pct_posted}%; background:#63E6BE;"></div>'
            f'<div style="width:{_pct_prod}%; background:#3B82F6;"></div>'
            f'<div style="width:{_pct_pend}%; background:#F59E0B;"></div>'
            f'</div>',
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

        # ─── Influencer Profile Breakdown ────────────────────────────
        st.markdown("---")
        st.subheader("Influencer Profile Breakdown")
        _bd1, _bd2, _bd3 = st.columns(3)
        _breakdown_palette = ["#748FFC", "#FF922B", "#63E6BE", "#F06595", "#B197FC",
                              "#FCC419", "#22D3EE", "#A9E34B", "#FF6B6B", "#DDA0DD"]
        for _col_ui, _col_name, _title in [
            (_bd1, "Type", "Type"),
            (_bd2, "Job Function", "Job Function"),
            (_bd3, "Senority", "Seniority"),
        ]:
            with _col_ui:
                if _col_name in df_pay.columns:
                    _vals = df_pay[_col_name].str.strip()
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
                        st.plotly_chart(_fig, use_container_width=True, key=f"bd_{_col_name}")
                    else:
                        st.info(f"No {_title} data.")
                else:
                    st.info(f"No {_title} column.")

        # Second row: Content Type + Followers distribution
        _bd4, _bd5 = st.columns(2)
        # Content Type donut
        with _bd4:
            if "Content Type" in df_pay.columns:
                _ct_vals = df_pay["Content Type"].str.strip()
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
                    st.plotly_chart(_fig_ct, use_container_width=True, key="bd_content_type")
                else:
                    st.info("No Content Type data.")
        # Followers distribution donut (bucketed)
        with _bd5:
            _fol = df_pay["_followers_num"].dropna()
            if not _fol.empty:
                def _follower_bucket(n):
                    if n < 10000: return "Nano (<10K)"
                    elif n < 50000: return "Micro (10K-50K)"
                    elif n < 100000: return "Mid (50K-100K)"
                    elif n < 500000: return "Macro (100K-500K)"
                    else: return "Mega (500K+)"
                _fol_buckets = _fol.apply(_follower_bucket)
                _bucket_order = ["Nano (<10K)", "Micro (10K-50K)", "Mid (50K-100K)",
                                 "Macro (100K-500K)", "Mega (500K+)"]
                _fol_counts = _fol_buckets.value_counts().reindex(_bucket_order).dropna().astype(int).reset_index()
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
                st.plotly_chart(_fig_fol, use_container_width=True, key="bd_followers")
            else:
                st.info("No Followers data.")

        # ─── Performance Ranking ─────────────────────────────────────
        st.markdown("---")
        st.subheader("Performance Ranking")
        _perf_cols = ["Name", "POC", "Post Link", "Content Type", "Type",
                      "Senority", "Job Function", "_price_num", "_views_24hr_num", "_signups_num"]
        _perf_available = [c for c in _perf_cols if c in df_pay.columns]
        perf = df_pay[_perf_available].copy()
        perf = perf.rename(columns={"_price_num": "Cost ($)", "_views_24hr_num": "24hr Views",
                                     "_signups_num": "Signups", "Senority": "Seniority"})
        # Compute CPM and Cost/Signup
        perf["CPM ($)"] = perf.apply(
            lambda r: round(r["Cost ($)"] / r["24hr Views"] * 1000, 2)
            if pd.notna(r.get("Cost ($)")) and pd.notna(r.get("24hr Views")) and r["24hr Views"] > 0 else None, axis=1)
        perf["Cost/Signup ($)"] = perf.apply(
            lambda r: round(r["Cost ($)"] / r["Signups"], 2)
            if pd.notna(r.get("Cost ($)")) and pd.notna(r.get("Signups")) and r["Signups"] > 0 else None, axis=1)
        # Only show rows with at least some data
        perf_display = perf.dropna(subset=["Cost ($)"])
        if not perf_display.empty:
            # Sort toggle
            _sort_mode = st.pills("Sort by", ["By CPM (best value)", "By 24hr Views"],
                                  default="By CPM (best value)", key="perf_sort")
            if _sort_mode == "By 24hr Views":
                perf_sorted = perf_display.sort_values("24hr Views", ascending=False, na_position="last")
            else:
                perf_sorted = perf_display.sort_values("CPM ($)", ascending=True, na_position="last")
            _perf_display_order = ["Name", "POC", "Content Type", "Type", "Seniority", "Job Function",
                                   "Cost ($)", "24hr Views", "Signups", "CPM ($)", "Cost/Signup ($)", "Post Link"]
            _perf_display_order = [c for c in _perf_display_order if c in perf_sorted.columns]
            st.dataframe(
                perf_sorted[_perf_display_order],
                use_container_width=True, hide_index=True,
                column_config={"Post Link": st.column_config.LinkColumn("Post Link")},
            )
        else:
            st.info("Performance data will appear after cost and views/signups are entered.")

        # Detail table
        st.markdown("---")
        st.subheader("Detail Table")
        show_editable_table(
            df_pay, PAYMENT_PERF_DISPLAY_COLS,
            {"Name": "text", "Post Link": "text", "Post Date": "text",
             "Payment Receiving Account": "text", "Payment Progress": "select_payment",
             "24hr Views": "text", "Link Signups": "text", "ER": "text", "Retro Notes": "text",
             "Type": "text", "Senority": "text", "Job Function": "text", "Content Type": "text"},
            "payment",
        )

        # Performance Charts (merged from Retrospective)
        st.markdown("---")
        st.subheader("Performance Charts")
        pc1, pc2 = st.columns(2)
        with pc1:
            fig = cost_vs_views_scatter(df_pay)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="pay_cost_views")
            else:
                st.info("Cost vs Views will appear after campaign data is entered.")
        with pc2:
            fig = followers_vs_er_scatter(df_pay)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="pay_fver")

        # Export
        st.markdown("---")
        csv = df_pay.drop(columns=[c for c in df_pay.columns if c.startswith("_")]).to_csv(index=False)
        st.download_button("\U0001f4e5 Export Campaign Report (CSV)", data=csv,
                           file_name=f"campaign_{start_date}_{end_date}.csv", mime="text/csv",
                           use_container_width=True)
