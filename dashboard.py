#!/usr/bin/env python3
"""
Campaign Dashboard — Influencer Sourcing Workflow
Run: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
from datetime import date

from dashboard_utils.gsheet_client import (
    _get_worksheet, ensure_new_columns, load_dataframe,
    update_cell, batch_update_cells,
)
from dashboard_utils.data_model import (
    COL, HEADER_NAMES, STATUS_OPTIONS, COLLAB_STAGE_OPTIONS,
    PAYMENT_PROGRESS_OPTIONS,
    PIPELINE_DISPLAY_COLS, CONTENT_DISPLAY_COLS,
    PAYMENT_PERF_DISPLAY_COLS, RETRO_DISPLAY_COLS,
    prepare_dataframe, filter_by_contact_date,
    filter_by_status, parse_date,
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

all_statuses = sorted(set(s.strip() for s in df_all["Status"].unique() if s.strip()))
selected_statuses = st.sidebar.multiselect("Status Filter", options=all_statuses, default=[])

if "Campaign Tag" in df_all.columns:
    all_tags = sorted(set(t.strip() for t in df_all["Campaign Tag"].unique() if t.strip()))
    selected_tag = st.sidebar.selectbox("Campaign Tag", ["(All)"] + all_tags) if all_tags else "(All)"
else:
    selected_tag = "(All)"

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
    col_config = make_column_config(editable_cols)
    disabled = [c for c in available if c not in editable_cols]
    st.data_editor(
        df_view[available], column_config=col_config, disabled=disabled,
        use_container_width=True, num_rows="fixed", key=f"{key_prefix}_editor",
    )
    changes = st.session_state.get(f"{key_prefix}_editor", {})
    edited_rows = changes.get("edited_rows", {})
    if edited_rows:
        updates = []
        for row_idx_str, row_changes in edited_rows.items():
            sheet_row = int(df_view.iloc[int(row_idx_str)]["_sheet_row"])
            for col_name, new_value in row_changes.items():
                col_idx = header_to_col_index(col_name)
                if col_idx >= 0:
                    updates.append((sheet_row, col_idx + 1, str(new_value) if new_value is not None else ""))
        if updates:
            try:
                batch_update_cells(st.session_state["ws"], updates)
                st.toast(f"Saved {len(updates)} change(s)")
            except Exception as e:
                st.error(f"Save failed: {e}")


def poc_class(poc):
    p = poc.strip().lower()
    return f"poc-{p}" if p in ("jenny", "doris", "jialin", "falida") else "poc-other"


def render_kanban(df_src, show_poc_prefix=True):
    """Render collaboration stage kanban cards."""
    breakdown = collab_stage_breakdown(df_src)
    if not breakdown:
        st.info("No Collaboration Stage data yet.")
        return
    cols = st.columns(len(breakdown))
    for i, (stage, people) in enumerate(breakdown.items()):
        color = COLLAB_STAGE_COLORS.get(stage, "#D5CFC7")
        with cols[i]:
            st.markdown(
                f'<div class="stage-header" style="border-bottom-color: {color};">'
                f'{stage} ({len(people)})</div>', unsafe_allow_html=True)
            for name, poc in people:
                css = poc_class(poc)
                poc_label = f"POC: {poc}" if show_poc_prefix else poc
                st.markdown(
                    f'<div class="kanban-card" style="border-left-color: {color};">'
                    f'<div class="name">{name or "(no name)"}</div>'
                    f'<div class="poc {css}">{poc_label}</div></div>',
                    unsafe_allow_html=True)


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
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Contacted", len(df_by_contact))
        k2.metric("Confirmed", len(confirmed))
        avg_er = confirmed["_er_num"].dropna().mean()
        k3.metric("Avg ER%", f"{avg_er:.2f}%" if pd.notna(avg_er) else "N/A")
        total_price = confirmed["_price_num"].dropna().sum()
        k4.metric("Total Cost", f"${total_price:,.0f}" if total_price > 0 else "N/A")
        avg_fol = confirmed["_followers_num"].dropna().mean()
        k5.metric("Avg Followers", f"{avg_fol:,.0f}" if pd.notna(avg_fol) else "N/A")

        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            fig = status_distribution_pie(df_by_contact)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_status")
            st.caption("All people contacted in this period.")
        with c2:
            fig = collab_stage_detail(confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_collab")

        st.markdown("---")
        st.subheader("Collaboration Stage")
        render_kanban(confirmed, show_poc_prefix=False)

        st.markdown("---")
        c3, c4 = st.columns(2)
        with c3:
            fig = er_histogram(confirmed)
            if fig:
                st.plotly_chart(fig, use_container_width=True, key="ov_er")
            st.caption("X axis = ER%, Y axis = number of influencers in that range. "
                       "Green dashed = median, red dotted = average (pulled up by outliers).")
        with c4:
            fig = followers_vs_er_scatter(confirmed)
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
        st.subheader("POC Overview")
        poc_counts = df_filtered["POC"].value_counts()
        poc_confirmed = df_filtered[df_filtered["Status"] == "Confirm"]["POC"].value_counts()
        poc_cols = st.columns(min(len(poc_counts), 6))
        for i, (p, c) in enumerate(poc_counts.items()):
            if not p.strip():
                continue
            confirmed_count = poc_confirmed.get(p, 0)
            css = poc_class(p)
            poc_cols[i % len(poc_cols)].markdown(
                f"""<div style="background:#FAFBFC; border-radius:10px; padding:14px 18px;
                    box-shadow:0 1px 3px rgba(0,0,0,0.05); border-top: 3px solid currentColor;">
                    <div class="{css}" style="font-weight:700; font-size:1em; margin-bottom:8px;">{p}</div>
                    <div style="display:flex; gap:18px; font-size:0.88em; color:#4B5563;">
                        <span>Contacted&nbsp;<b style="color:#1F2937">{c}</b></span>
                        <span>Confirmed&nbsp;<b style="color:#1F2937">{confirmed_count}</b></span>
                    </div>
                </div>""",
                unsafe_allow_html=True,
            )

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

        # Filters + table
        st.subheader("Full Table")
        search = st.text_input("Search by name", key="pipe_search")
        df_pipe = df_filtered.copy()
        if search:
            df_pipe = df_pipe[df_pipe["Name"].str.contains(search, case=False, na=False)]

        # Column filters
        fc1, fc2, fc3, fc4 = st.columns(4)
        f_poc = fc1.multiselect("POC", sorted(set(df_pipe["POC"].dropna().unique()) - {""}), key="pf_poc")
        f_status = fc2.multiselect("Status", sorted(set(df_pipe["Status"].dropna().unique()) - {""}), key="pf_status")
        f_stage = fc3.multiselect("Stage", sorted(set(df_pipe["Collaboration Stage"].dropna().unique()) - {""}), key="pf_stage")
        f_country = fc4.multiselect("Country", sorted(set(df_pipe["Country"].dropna().unique()) - {""}), key="pf_country")

        if f_poc: df_pipe = df_pipe[df_pipe["POC"].isin(f_poc)]
        if f_status: df_pipe = df_pipe[df_pipe["Status"].isin(f_status)]
        if f_stage: df_pipe = df_pipe[df_pipe["Collaboration Stage"].isin(f_stage)]
        if f_country: df_pipe = df_pipe[df_pipe["Country"].isin(f_country)]

        st.caption(f"{len(df_pipe)} influencers")
        show_editable_table(
            df_pipe, PIPELINE_DISPLAY_COLS,
            {"Status": "select_status", "Collaboration Stage": "select_collab",
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
