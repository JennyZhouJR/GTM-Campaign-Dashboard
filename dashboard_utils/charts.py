"""Plotly chart builders for the Campaign Dashboard."""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


# ─── Vibrant confetti palette ────────────────────────────────────────────────
# Bright, lively, modern tones — no dull greens, greys, or muddy blues

STATUS_COLORS = {
    "Confirm": "#63E6BE",     # fresh mint
    "Contacted": "#748FFC",   # periwinkle
    "Nego": "#FF922B",        # tangerine
    "TBD": "#B197FC",         # bright lavender
    "Reject": "#FF6B6B",      # coral red
    "Drop": "#DDA0DD",        # soft plum
}

COLLAB_STAGE_COLORS = {
    "Awaiting brief": "#FCC419",          # bright gold
    "Script in progress": "#FF922B",      # tangerine
    "Script feedback": "#74C0FC",         # sky blue
    "Video in progress": "#A9E34B",       # lime green
    "Video feedback": "#F06595",          # vibrant pink
    "Approved for posting": "#51CF66",    # vivid green
    "Posted": "#22D3EE",                  # cyan
}

COLLAB_STAGE_ORDER = [
    "Awaiting brief",
    "Script in progress",
    "Script feedback",
    "Video in progress",
    "Video feedback",
    "Approved for posting",
    "Posted",
]

POC_COLORS = {
    "Jenny": "#748FFC",       # periwinkle
    "Doris": "#FF922B",       # tangerine
    "Jialin": "#F06595",      # vibrant pink
    "Falida": "#63E6BE",      # fresh mint
}

POC_COLORS_SEQ = ["#748FFC", "#FF922B", "#F06595", "#63E6BE", "#B197FC"]


# ─── Status Distribution ──────────────────────────────────────────────────────

def status_distribution_pie(df: pd.DataFrame):
    """Donut chart of Status distribution with counts and percentages."""
    counts = df["Status"].value_counts().reset_index()
    counts.columns = ["Status", "Count"]
    counts = counts[counts["Status"].str.strip() != ""]
    if counts.empty:
        return None
    total = counts["Count"].sum()

    colors = [STATUS_COLORS.get(s, "#C5B9A8") for s in counts["Status"]]
    fig = go.Figure(data=[go.Pie(
        labels=counts["Status"],
        values=counts["Count"],
        marker=dict(colors=colors, line=dict(color="#fff", width=2)),
        textinfo="value+percent",
        texttemplate="%{value} (%{percent})",
        textposition="inside",
        insidetextorientation="horizontal",
        hole=0.45,
        textfont=dict(size=11, family="DM Sans, Inter, sans-serif", color="#fff"),
    )])
    fig.update_layout(
        title=dict(text="Status Distribution", font=dict(size=14)),
        margin=dict(t=40, b=80, l=20, r=20),
        height=420,
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="top", y=-0.05,
            xanchor="center", x=0.5,
            font=dict(size=11),
        ),
        font=dict(family="DM Sans, Inter, sans-serif"),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ─── Collaboration Stage Detail ───────────────────────────────────────────────

def collab_stage_detail(df: pd.DataFrame):
    """Horizontal bar chart of Collaboration Stage with POC breakdown."""
    col = "Collaboration Stage"
    if col not in df.columns:
        return None

    df_stage = df[df[col].str.strip() != ""].copy()
    if df_stage.empty:
        return None

    grouped = df_stage.groupby([col, "POC"]).size().reset_index(name="Count")
    # Match case-insensitively, preserve original casing from data
    data_stages = grouped[col].unique().tolist()
    stage_order_lower = [s.lower() for s in COLLAB_STAGE_ORDER]
    ordered = [s for sl in stage_order_lower
               for s in data_stages if s.lower() == sl]
    # Add any unknown stages at the end
    ordered += [s for s in data_stages if s.lower() not in stage_order_lower]
    # Plotly horizontal bar: reverse so first stage appears at TOP
    chart_order = list(reversed(ordered))

    fig = px.bar(
        grouped, y=col, x="Count", color="POC",
        orientation="h",
        color_discrete_map=POC_COLORS,
        category_orders={col: chart_order},
        title="Collaboration Stage (by POC)",
        hover_data=["POC", "Count"],
    )
    fig.update_layout(
        margin=dict(t=40, b=20, l=20, r=20),
        height=400,
        xaxis_title="", yaxis_title="",
        legend_title="POC",
        font=dict(family="DM Sans, Inter, sans-serif"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(gridcolor="#EDEDED", gridwidth=1)
    return fig


def collab_stage_breakdown(df: pd.DataFrame) -> dict:
    """Return a dict: {stage: [(name, poc), ...]} for kanban display.
    Shows all stages in COLLAB_STAGE_ORDER first, then any unknown stages."""
    col = "Collaboration Stage"
    if col not in df.columns:
        return {}
    result = {}
    # Build case-insensitive map: lowercase -> actual display label
    known_lower = {s.lower(): s for s in COLLAB_STAGE_ORDER}
    # Known stages in order — match case-insensitively against actual Sheet values
    stage_to_rows = {}
    for actual_val in df[col].dropna().unique():
        sv = actual_val.strip()
        if not sv:
            continue
        matched_key = known_lower.get(sv.lower())
        if matched_key:
            stage_to_rows.setdefault(matched_key, []).extend(
                [(r.get("Name", ""), r.get("POC", ""))
                 for _, r in df[df[col].str.strip() == sv].iterrows()]
            )
        else:
            # Unknown stage — add as-is
            stage_to_rows.setdefault(sv, []).extend(
                [(r.get("Name", ""), r.get("POC", ""))
                 for _, r in df[df[col].str.strip() == sv].iterrows()]
            )
    # Output in COLLAB_STAGE_ORDER order, then unknowns
    for stage in COLLAB_STAGE_ORDER:
        if stage in stage_to_rows:
            result[stage] = stage_to_rows[stage]
    for stage, people in stage_to_rows.items():
        if stage not in result:
            result[stage] = people
    return result


# ─── ER% Distribution ─────────────────────────────────────────────────────────

def er_histogram(df: pd.DataFrame):
    """Histogram of ER% values with outlier handling."""
    er_vals = df["_er_num"].dropna()
    if er_vals.empty:
        return None

    cap = er_vals.quantile(0.95) if len(er_vals) > 5 else er_vals.max()
    cap = max(cap, 10)
    er_display = er_vals.clip(upper=cap)

    fig = px.histogram(
        er_display, nbins=12,
        title="ER% Distribution",
        labels={"value": "Engagement Rate (%)", "count": "Influencers"},
        color_discrete_sequence=["#74C0FC"],
    )
    median_er = er_vals.median()
    avg_er = er_vals.mean()
    fig.add_vline(x=median_er, line_dash="dash", line_color="#51CF66",
                  annotation_text=f"Median: {median_er:.1f}%",
                  annotation_font_color="#51CF66")
    fig.add_vline(x=avg_er, line_dash="dot", line_color="#FF6B6B",
                  annotation_text=f"Avg: {avg_er:.1f}%",
                  annotation_font_color="#FF6B6B")

    outliers = (er_vals > cap).sum()
    if outliers > 0:
        fig.add_annotation(x=cap, y=0, text=f"+{outliers} outlier(s) >{cap:.0f}%",
                           showarrow=False, yshift=20, font=dict(size=10, color="#A8A8A0"))

    fig.update_layout(
        margin=dict(t=40, b=20, l=20, r=20), height=350,
        font=dict(family="DM Sans, Inter, sans-serif"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(gridcolor="#EDEDED")
    fig.update_yaxes(gridcolor="#EDEDED")
    return fig


# ─── Followers vs ER% ─────────────────────────────────────────────────────────

def followers_vs_er_scatter(df: pd.DataFrame):
    """Scatter plot of Followers vs ER%."""
    plot_df = df[["Name", "_followers_num", "_er_num", "POC"]].dropna(
        subset=["_followers_num", "_er_num"]
    )
    if plot_df.empty:
        return None
    fig = px.scatter(
        plot_df, x="_followers_num", y="_er_num",
        hover_data=["Name"], color="POC",
        color_discrete_map=POC_COLORS,
        title="Followers vs ER%",
        labels={"_followers_num": "Followers", "_er_num": "ER%"},
    )
    fig.update_traces(marker=dict(size=10, opacity=0.8))
    fig.update_layout(
        margin=dict(t=40, b=20, l=20, r=20), height=350,
        font=dict(family="DM Sans, Inter, sans-serif"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(gridcolor="#EDEDED")
    fig.update_yaxes(gridcolor="#EDEDED")
    return fig


# ─── Cost vs Performance ──────────────────────────────────────────────────────

def cost_vs_views_scatter(df: pd.DataFrame):
    """Scatter plot of Cost vs 24hr Views per influencer."""
    plot_df = df[["Name", "_price_num", "_views_24hr_num", "POC"]].dropna(
        subset=["_price_num", "_views_24hr_num"]
    )
    if plot_df.empty:
        return None
    fig = px.scatter(
        plot_df, x="_price_num", y="_views_24hr_num",
        hover_data=["Name"], color="POC",
        color_discrete_map=POC_COLORS,
        title="Cost vs 24hr Views",
        labels={"_price_num": "Cost ($)", "_views_24hr_num": "24hr Views"},
    )
    fig.update_traces(marker=dict(size=10, opacity=0.8))
    fig.update_layout(
        margin=dict(t=40, b=20, l=20, r=20), height=350,
        font=dict(family="DM Sans, Inter, sans-serif"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(gridcolor="#EDEDED")
    fig.update_yaxes(gridcolor="#EDEDED")
    return fig
