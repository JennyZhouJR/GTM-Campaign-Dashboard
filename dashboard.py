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
    PAYMENT_PROGRESS_OPTIONS, CAMPAIGN_TAG_OPTIONS, STAGE_DEADLINES,
    PIPELINE_DISPLAY_COLS, CONTENT_DISPLAY_COLS,
    PAYMENT_PERF_DISPLAY_COLS, RETRO_DISPLAY_COLS,
    prepare_dataframe, filter_by_contact_date,
    filter_by_status, parse_date, get_timeline_status,
)
from dashboard_utils.charts import (
    status_distribution_pie, collab_stage_detail, collab_stage_breakdown,
    er_histogram, followers_vs_er_scatter, cost_vs_views_scatter,
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

st.sidebar.markdown("---")
st.sidebar.subheader("Date of Contact")
col1, col2 = st.sidebar.columns(2)
start_date = col1.date_input("From", value=date(2026, 3, 24))
end_date = col2.date_input("To", value=date(2026, 4, 7))

st.sidebar.markdown("---")
st.sidebar.subheader("Campaign")
if "Campaign Tag" in df_all.columns:
    # Show tags found in data + all standard month options as fallback
    data_tags = set(t.strip() for t in df_all["Campaign Tag"].unique() if t.strip())
    all_month_tags = [t for t in CAMPAIGN_TAG_OPTIONS if t]  # January-December
    all_tags = sorted(set(list(data_tags) + all_month_tags), key=lambda t: CAMPAIGN_TAG_OPTIONS.index(t) if t in CAMPAIGN_TAG_OPTIONS else 99)
    selected_tag = st.sidebar.selectbox("Campaign Tag", ["(All)"] + all_tags)
else:
    selected_tag = "(All)"

st.sidebar.markdown("---")
all_statuses = sorted(set(s.strip() for s in df_all["Status"].unique() if s.strip()))
selected_statuses = st.sidebar.multiselect("Status Filter", options=all_statuses, default=[])

# ─── Data filtering ───────────────────────────────────────────────────────────
# PRIMARY filter: Date of Contact (column A). All conditions derive from this set.

df_by_contact = filter_by_contact_date(df_all, start_date, end_date)
confirmed = df_by_contact[df_by_contact["Status"] == "Confirm"]

# Full filter for pipeline tabs (pipeline shows ALL contacted, not just confirmed)
df_filtered = df_by_contact.copy()
if selected_statuses:
    df_filtered = filter_by_status(df_filtered, selected_statuses)
if selected_tag != "(All)" and "Campaign Tag" in df_filtered.columns:
    df_filtered = df_filtered[df_filtered["Campaign Tag"] == selected_tag]

st.sidebar.markdown("---")
st.sidebar.metric("In view", len(df_filtered))
st.sidebar.caption(f"Contacted: {len(df_by_contact)} | Confirmed: {len(confirmed)}")


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
st.caption(f"{start_date.strftime('%m/%d')} \u2013 {end_date.strftime('%m/%d')}  \u00b7  "
           f"{len(df_by_contact)} contacted  \u00b7  {len(confirmed)} confirmed  \u00b7  {len(df_filtered)} in view")

# ─── Navigation ──────────────────────────────────────────────────────────────

NAV_OPTIONS = ["Overview", "Pipeline", "Content & Delivery", "Payment & Performance", "Retrospective"]

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
        poc_list = [(p, c) for p, c in poc_counts.items() if p.strip()]
        poc_cols = st.columns(min(len(poc_list), 6)) if poc_list else []
        for i, (p, c) in enumerate(poc_list):
            color = POC_COLOR_MAP.get(p.lower(), "#B197FC")
            confirmed_count = poc_confirmed_counts.get(p, 0)
            poc_cols[i % len(poc_cols)].markdown(
                f"""<div style="background:#FAFBFC; border-radius:10px; padding:12px 16px;
                    box-shadow:0 1px 3px rgba(0,0,0,0.05); border-left:4px solid {color};">
                    <div style="font-weight:700; font-size:0.95em; color:{color}; margin-bottom:6px;">{p}</div>
                    <div style="font-size:1.1em; color:#1F2937; font-weight:600; letter-spacing:0.01em;">
                        📬 {c}&nbsp;&nbsp;✅ {confirmed_count}
                    </div>
                </div>""",
                unsafe_allow_html=True,
            )
        st.caption("📬 Contacted&nbsp;&nbsp;&nbsp;✅ Confirmed")

        st.markdown("---")

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
        st.subheader("📧 Email Outreach")

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

            # Send outreach section
            df_unsent = df_filtered[df_filtered["Status"].str.strip() == ""]
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
                                "name": orig_row["Name"].strip() or "there",
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
                                            r["Name"].strip() or "there",
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

        st.caption(f"{len(df_pipe)} influencers")
        show_editable_table(
            df_pipe, PIPELINE_DISPLAY_COLS,
            {"Status": "select_status", "Collaboration Stage": "select_collab",
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
            with st.expander("📅 Posting Schedule", expanded=True):
                grouped = {}
                for _, row in df_with_date.iterrows():
                    d = row["_post_date_parsed"]
                    grouped.setdefault(d, []).append((row.get("Name", ""), row.get("POC", "")))
                sorted_dates = sorted(grouped.keys())

                html = ""
                for d in sorted_dates:
                    people = grouped[d]
                    day_label = d.strftime("%m/%d %a")
                    # Date header row
                    html += (
                        f'<div style="display:flex; align-items:center; gap:8px; padding:10px 14px; '
                        f'background:#F9FAFB; border-radius:6px; margin-top:12px;">'
                        f'<span style="font-weight:700; font-size:0.88em; color:#1F2937;">{day_label}</span>'
                        f'<span style="font-size:0.78em; color:#9CA3AF;">{len(people)} people</span>'
                        f'</div>'
                    )
                    # People chips — horizontal wrap
                    html += '<div style="display:flex; flex-wrap:wrap; gap:6px 20px; padding:10px 14px;">'
                    for name, poc in people:
                        pc = poc_color(poc)
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
            {"Collaboration Stage": "select_collab", "Content Type": "text",
             "Post Link": "text", "Post Date": "text", "Price\uff08$)": "text"},
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
        st.subheader("Summary")
        m1, m2, m3, m4 = st.columns(4)
        total_cost = df_pay["_price_num"].dropna().sum()
        m1.metric("Total Cost", f"${total_cost:,.0f}" if total_cost > 0 else "N/A")
        total_24hr = df_pay["_views_24hr_num"].dropna().sum()
        m2.metric("Total 24hr Views", f"{total_24hr:,.0f}" if total_24hr > 0 else "N/A")
        total_signups = df_pay["_signups_num"].dropna().sum()
        m3.metric("Total Signups", f"{total_signups:,.0f}" if total_signups > 0 else "N/A")
        if total_cost > 0 and total_signups > 0:
            m4.metric("Avg Cost/Signup", f"${total_cost / total_signups:.2f}")
        else:
            m4.metric("Avg Cost/Signup", "N/A")

        # CPM
        st.markdown("---")
        st.subheader("CPM (Cost per 1K Views)")
        df_cpm = df_pay[["Name", "POC", "_price_num", "_views_24hr_num"]].dropna(subset=["_price_num"]).copy()
        df_cpm["CPM"] = df_cpm.apply(
            lambda r: (r["_price_num"] / r["_views_24hr_num"] * 1000)
            if pd.notna(r["_views_24hr_num"]) and r["_views_24hr_num"] > 0 else None, axis=1)
        if df_cpm["CPM"].notna().any():
            vals = df_cpm["CPM"].dropna()
            cm1, cm2, cm3 = st.columns(3)
            cm1.metric("Avg CPM", f"${vals.mean():.2f}")
            cm2.metric("Min CPM", f"${vals.min():.2f}")
            cm3.metric("Max CPM", f"${vals.max():.2f}")
            disp = df_cpm[df_cpm["CPM"].notna()][["Name", "POC", "_price_num", "_views_24hr_num", "CPM"]]
            disp.columns = ["Name", "POC", "Cost ($)", "24hr Views", "CPM ($)"]
            disp["CPM ($)"] = disp["CPM ($)"].round(2)
            st.dataframe(disp.sort_values("CPM ($)"), use_container_width=True, hide_index=True)
        else:
            st.info("CPM will appear after 24hr Views are entered.")

        # Top Performance
        st.markdown("---")
        st.subheader("Top Performance")
        perf = df_pay[["Name", "POC", "Post Link", "_views_24hr_num", "_signups_num", "_price_num"]].copy()
        perf.columns = ["Name", "POC", "Post Link", "24hr Views", "Signups", "Cost ($)"]
        tp1, tp2 = st.columns(2)
        with tp1:
            st.markdown("**By 24hr Views**")
            v = perf.dropna(subset=["24hr Views"]).sort_values("24hr Views", ascending=False)
            if not v.empty:
                st.dataframe(v[["Name", "POC", "24hr Views", "Cost ($)"]], use_container_width=True, hide_index=True)
            else:
                st.info("No data yet.")
        with tp2:
            st.markdown("**By Signups**")
            s = perf.dropna(subset=["Signups"]).sort_values("Signups", ascending=False)
            if not s.empty:
                st.dataframe(s[["Name", "POC", "Signups", "Cost ($)"]], use_container_width=True, hide_index=True)
            else:
                st.info("No data yet.")

        # Detail table
        st.markdown("---")
        st.subheader("Detail Table")
        show_editable_table(
            df_pay, PAYMENT_PERF_DISPLAY_COLS,
            {"Payment Receiving Account": "text", "Payment Progress": "select_payment",
             "24hr Views": "text", "Link Signups": "text"},
            "payment",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Retrospective
# ═══════════════════════════════════════════════════════════════════════════════

elif nav == "Retrospective":
    if df_by_contact.empty:
        st.info("No data for retrospective.")
    else:
        st.subheader("Campaign Summary")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Contacted", len(df_by_contact))
        s2.metric("Confirmed", len(confirmed))
        s3.metric("Rejected/Dropped", len(df_by_contact[df_by_contact["Status"].isin(["Reject", "Drop"])]))
        avg_er = confirmed["_er_num"].dropna().mean()
        s4.metric("Avg ER%", f"{avg_er:.2f}%" if pd.notna(avg_er) else "N/A")

        s5, s6, s7, s8 = st.columns(4)
        tc = confirmed["_price_num"].dropna().sum()
        s5.metric("Total Cost", f"${tc:,.0f}" if tc > 0 else "N/A")
        tv = confirmed["_views_24hr_num"].dropna().sum()
        s6.metric("Total 24hr Views", f"{tv:,.0f}" if tv > 0 else "N/A")
        ts = confirmed["_signups_num"].dropna().sum()
        s7.metric("Total Signups", f"{ts:,.0f}" if ts > 0 else "N/A")
        if tc > 0 and ts > 0:
            s8.metric("Avg Cost/Signup", f"${tc / ts:.2f}")
        else:
            s8.metric("Avg Cost/Signup", "N/A")

        # Top Performance
        st.markdown("---")
        st.subheader("Top Performance")
        rp = confirmed[["Name", "POC", "_views_24hr_num", "_signups_num", "_price_num"]].copy()
        rp.columns = ["Name", "POC", "24hr Views", "Signups", "Cost ($)"]
        rp1, rp2 = st.columns(2)
        with rp1:
            st.markdown("**By 24hr Views**")
            v = rp.dropna(subset=["24hr Views"]).sort_values("24hr Views", ascending=False)
            if not v.empty:
                st.dataframe(v[["Name", "POC", "24hr Views", "Cost ($)"]], use_container_width=True, hide_index=True)
            else:
                st.info("No data yet.")
        with rp2:
            st.markdown("**By Signups**")
            s = rp.dropna(subset=["Signups"]).sort_values("Signups", ascending=False)
            if not s.empty:
                st.dataframe(s[["Name", "POC", "Signups", "Cost ($)"]], use_container_width=True, hide_index=True)
            else:
                st.info("No data yet.")

        # Charts
        st.markdown("---")
        st.subheader("Performance Charts")
        r1, r2 = st.columns(2)
        with r1:
            fig = cost_vs_views_scatter(confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="retro_cost")
            else:
                st.info("Cost vs Views will appear after campaign data is entered.")
        with r2:
            fig = followers_vs_er_scatter(confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="retro_fver")

        # Retro Notes
        st.markdown("---")
        st.subheader("Retro Notes")
        show_editable_table(
            df_by_contact, RETRO_DISPLAY_COLS, {"Retro Notes": "text"}, "retro",
        )

        # Export
        st.markdown("---")
        csv = df_by_contact.drop(columns=[c for c in df_by_contact.columns if c.startswith("_")]).to_csv(index=False)
        st.download_button("\U0001f4e5 Export Campaign Report (CSV)", data=csv,
                           file_name=f"campaign_{start_date}_{end_date}.csv", mime="text/csv",
                           use_container_width=True)
