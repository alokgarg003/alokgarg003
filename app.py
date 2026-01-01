
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit Dashboard – Incident Partitions (Refactored)
------------------------------------------------------
Global filters apply consistently to:
  1) Group incidents (loaded from outputs_partitioned/run_xxx/group files)
  2) Unique Paths Explorer (unique_outputs/unique_paths_all.csv)

Run:
  streamlit run app.py
"""

from pathlib import Path
import re
import sys
import math
import io
from typing import Optional, Tuple, Dict, Any, List

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

# ---------- Config ----------
DEFAULT_OUT_ROOT = Path("outputs_partitioned")
GROUPS_ORDER = [
    "stuck-files", "ftp-errors", "config-file", "err-file-remove",
    "callout/mfterr03", "callout/mfterr04", "process-alert", "warnings", "misc"
]
PREVIEW_LIMIT = 10000  # max rows to display in table for performance

# ---------- Caching ----------
@st.cache_data(show_spinner=False)
def list_runs(out_root: Path) -> List[Path]:
    if not out_root.exists():
        return []
    runs = sorted([p for p in out_root.iterdir() if p.is_dir() and p.name.startswith("run_")])
    return runs[::-1]  # newest first

@st.cache_data(show_spinner=False)
def read_summary(run_dir: Path) -> pd.DataFrame:
    p = run_dir / "_logs" / "summary.csv"
    if not p.exists():
        return pd.DataFrame(columns=["group", "count", "total_rows", "coverage_pct"])
    df = pd.read_csv(p)
    df["group"] = df["group"].astype(str)
    # Normalize callout naming for UI consistency
    df.loc[df["group"].str.startswith("callout_"), "group"] = df["group"].str.replace("callout_", "callout/", regex=False)
    return df

@st.cache_data(show_spinner=False)
def safe_glob_excel_or_csv(group_dir: Path, base_prefix: str = "ALL_") -> Optional[Path]:
    xl = list(group_dir.glob(f"{base_prefix}*.xlsx"))
    if xl: return xl[0]
    cs = list(group_dir.glob(f"{base_prefix}*.csv"))
    if cs: return cs[0]
    return None

@st.cache_data(show_spinner=False)
def load_group_df(run_dir: Path, group_key: str) -> pd.DataFrame:
    if group_key.startswith("callout/"):
        sub = group_key.split("/", 1)[1]  # e.g., mfterr03 or mfterr04
        group_dir = run_dir / "callout" / sub
    else:
        group_dir = run_dir / group_key
    path = safe_glob_excel_or_csv(group_dir)
    if not path:
        return pd.DataFrame()
    if path.suffix.lower() == ".xlsx":
        return pd.read_excel(path, engine="openpyxl")
    return pd.read_csv(path)

@st.cache_data(show_spinner=False)
def _load_unique_paths_csv(root_dir: Path) -> pd.DataFrame:
    """
    Locate and load unique_paths_all.csv produced by the extractor.
    Robust search through common locations.
    """
    candidates = [
        Path("unique_outputs/unique_paths_all.csv"),
        root_dir.parent / "unique_outputs" / "unique_paths_all.csv",
        root_dir / "unique_paths_all.csv",
    ]
    for p in candidates:
        if p.exists():
            try:
                df = pd.read_csv(p)
                # Basic dtype fixes
                for col in ["incident_count"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                return df
            except Exception:
                pass
    return pd.DataFrame(columns=[
        "path","incident_count","configuration_items","error_families","top_primary_interfaces"
    ])

# ---------- Helpers ----------
def parse_opened_to_datetime(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.Series(dtype="datetime64[ns]")
    dt = pd.to_datetime(s, errors="coerce")
    if dt.notna().sum() > 0:
        return dt
    # Try Excel serial (days since 1899-12-30)
    try:
        return pd.to_datetime(s, unit="d", origin="1899-12-30", errors="coerce")
    except Exception:
        return pd.to_datetime(s, errors="coerce")

def add_time_grains(df: pd.DataFrame) -> pd.DataFrame:
    if "Opened" in df.columns:
        dt = parse_opened_to_datetime(df["Opened"])
        df = df.copy()
        df["Opened_dt"] = dt
        df["month"] = dt.dt.to_period("M").astype(str)
        df["dow"] = dt.dt.day_name()
        df["hour"] = dt.dt.hour
    return df

def build_pareto_df(s: pd.Series, top_n: int = 20) -> pd.DataFrame:
    s = s.dropna().astype(str)
    top = s.value_counts().head(top_n)
    df = top.reset_index()
    df.columns = ["value", "count"]
    total = df["count"].sum()
    df["cum_pct"] = (df["count"].cumsum() / total * 100.0) if total else 0.0
    return df

# ---------- Global Filter Application ----------
def apply_global_filters_df(
    df: pd.DataFrame,
    filters: Dict[str, Any],
) -> pd.DataFrame:
    """
    Apply global filters to the incidents df.
    filters keys:
        interface, errfam, ftp_cmd, ftp_code, text_query,
        months, dows, hour_range (tuple[int, int])
    """
    if df is None or df.empty:
        return df

    m = pd.Series(True, index=df.index)

    # Exact matches
    if filters.get("interface") and "primary_interface" in df.columns:
        m &= (df["primary_interface"].astype(str) == str(filters["interface"]))
    if filters.get("errfam") and "error_family" in df.columns:
        m &= (df["error_family"].astype(str) == str(filters["errfam"]))
    if filters.get("ftp_cmd") and "ftp_command" in df.columns:
        m &= (df["ftp_command"].astype(str).str.upper() == str(filters["ftp_cmd"]).upper())
    if filters.get("ftp_code") and "ftp_code" in df.columns:
        m &= (df["ftp_code"].astype(str) == str(filters["ftp_code"]))

    # Text search across relevant fields
    txt = (filters.get("text_query") or "").strip()
    if txt:
        pool_cols = [c for c in ["Short description", "paths", "error_codes",
                                 "Resolution notes"] if c in df.columns]
        if pool_cols:
            t = df[pool_cols].astype(str).agg(" ".join, axis=1)
            m &= t.str.contains(re.escape(txt), case=False, na=False)

    # Time grains
    months = filters.get("months")
    dows = filters.get("dows")
    hour_range = filters.get("hour_range")

    if months and "month" in df.columns:
        m &= df["month"].isin(months)
    if dows and "dow" in df.columns:
        m &= df["dow"].isin(dows)
    if hour_range and "hour" in df.columns:
        lo, hi = hour_range
        m &= df["hour"].between(int(lo), int(hi))

    return df[m]

def apply_global_filters_up(
    up_df: pd.DataFrame,
    filters: Dict[str, Any],
) -> pd.DataFrame:
    """
    Apply global filters to unique paths dataframe by mapping fields:
      - interface -> 'top_primary_interfaces' contains interface
      - errfam -> 'error_families' contains errfam
      - ftp_cmd/ftp_code -> not available; ignored
      - text_query -> searches across path/CI/error_families/top_primary_interfaces
      - months/dows/hour_range -> not applicable; ignored
    """
    if up_df is None or up_df.empty:
        return up_df
    m = pd.Series(True, index=up_df.index)

    if filters.get("interface") and "top_primary_interfaces" in up_df.columns:
        q = re.escape(str(filters["interface"]))
        m &= up_df["top_primary_interfaces"].astype(str).str.contains(q, case=False, na=False)
    if filters.get("errfam") and "error_families" in up_df.columns:
        q = re.escape(str(filters["errfam"]))
        m &= up_df["error_families"].astype(str).str.contains(q, case=False, na=False)

    txt = (filters.get("text_query") or "").strip()
    if txt:
        pool_cols = [c for c in ["path","configuration_items","error_families","top_primary_interfaces"]
                     if c in up_df.columns]
        if pool_cols:
            t = up_df[pool_cols].astype(str).agg(" ".join, axis=1)
            m &= t.str.contains(re.escape(txt), case=False, na=False)

    return up_df[m]

# ---------- Unique Paths Specific Filters ----------
def apply_unique_paths_specific_filters(
    up_df: pd.DataFrame,
    path_prefix: Optional[str],
    min_incidents: Optional[int],
    ci_sel: Optional[str],
    fam_sel: Optional[str],
    file_name_query: Optional[str],
) -> pd.DataFrame:
    if up_df is None or up_df.empty:
        return up_df

    m = pd.Series(True, index=up_df.index)

    if path_prefix:
        m &= up_df["path"].astype(str).str.startswith(path_prefix.strip())

    if min_incidents is not None and "incident_count" in up_df.columns:
        try:
            m &= pd.to_numeric(up_df["incident_count"], errors="coerce").fillna(0) >= int(min_incidents)
        except Exception:
            pass

    if ci_sel and ci_sel != "(any)" and "configuration_items" in up_df.columns:
        m &= up_df["configuration_items"].astype(str).str.contains(re.escape(ci_sel), case=False, na=False)

    if fam_sel and fam_sel != "(any)" and "error_families" in up_df.columns:
        m &= up_df["error_families"].astype(str).str.contains(re.escape(fam_sel), case=False, na=False)

    if file_name_query:
        basename = up_df["path"].astype(str).apply(lambda p: Path(p).name if p else "")
        m &= basename.str.contains(re.escape(file_name_query.strip()), case=False, na=False)

    return up_df[m]

def _drill_through_group(df_group: pd.DataFrame, selected_path: str) -> pd.DataFrame:
    """
    Find incidents in the currently opened group (df_group) that reference the selected path.
    Match on 'paths', then 'Short description', then 'Resolution notes'.
    """
    if df_group is None or len(df_group) == 0 or not selected_path:
        return pd.DataFrame()

    m = pd.Series(False, index=df_group.index)

    if "paths" in df_group.columns:
        m |= df_group["paths"].astype(str).str.contains(re.escape(selected_path), case=False, na=False)
    if "Short description" in df_group.columns:
        m |= df_group["Short description"].astype(str).str.contains(re.escape(selected_path), case=False, na=False)
    if "Resolution notes" in df_group.columns:
        m |= df_group["Resolution notes"].astype(str).str.contains(re.escape(selected_path), case=False, na=False)

    return df_group[m]

# ---------- UI ----------
st.set_page_config(page_title="Incident Partitions Dashboard", layout="wide")
st.title("Incident Partitions Dashboard (Global Filters)")
st.caption("Explore grouped outputs from `partition_incidents.py` — filter globally, visualize, and download.")

# Sidebar: root folder
root_input = st.sidebar.text_input("Output root folder", str(DEFAULT_OUT_ROOT))
out_root = Path(root_input).resolve()
runs = list_runs(out_root)

if not runs:
    st.warning(f"No runs found under: {out_root}\n\nRun the partitioner first.")
    st.stop()

# Select run
run_names = [r.name for r in runs]
sel_run_name = st.sidebar.selectbox("Select run", run_names, index=0)
sel_run = out_root / sel_run_name

# Summary
summary = read_summary(sel_run)
with st.expander("Run summary", expanded=True):
    col1, col2 = st.columns([2, 1])
    with col1:
        if len(summary) > 0:
            fig_sum = px.bar(summary, x="group", y="count", title="Counts by group", text_auto=True)
            st.plotly_chart(fig_sum, use_container_width=True)
        else:
            st.info("No summary.csv found for this run.")
    with col2:
        if "coverage_pct" in summary.columns and len(summary) > 0:
            # Prefer the consolidated coverage in the first row (if present)
            try:
                cov = float(summary["coverage_pct"].iloc[0])
                total = int(summary["total_rows"].iloc[0])
                st.metric("Total rows", value=f"{total:,}")
                st.metric("Coverage (non-misc)", value=f"{cov:.1f}%")
            except Exception:
                pass
        st.write("Run path:", str(sel_run))

st.divider()

# Group selector
avail_groups = [g for g in GROUPS_ORDER if (sel_run / (g.split("/")[0]) ).exists()] or GROUPS_ORDER
group_key = st.selectbox("Open group", avail_groups, index=0)

# Load selected group
df = load_group_df(sel_run, group_key)
if df.empty:
    st.warning("No data file found in this group.")
    st.stop()

# Augment time grains
df = add_time_grains(df)

# ---------- Build global filter options ----------
iface_opt = sorted(pd.unique(df["primary_interface"].dropna())) if "primary_interface" in df.columns else []
errfam_opt = sorted(pd.unique(df["error_family"].dropna())) if "error_family" in df.columns else []
ftp_cmd_opt = sorted(pd.unique(df["ftp_command"].dropna())) if "ftp_command" in df.columns else []
ftp_code_opt = sorted(pd.unique(df["ftp_code"].dropna())) if "ftp_code" in df.columns else []
month_opt = sorted(pd.unique(df["month"].dropna())) if "month" in df.columns else []
dow_opt = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"] if "dow" in df.columns else []

# ---------- Sidebar: Global Filters ----------
st.sidebar.markdown("### Global Filters (apply to incidents & unique paths)")
iface_sel = st.sidebar.selectbox("Primary interface", ["(any)"] + iface_opt) if iface_opt else "(any)"
errfam_sel = st.sidebar.selectbox("Error family", ["(any)"] + errfam_opt) if errfam_opt else "(any)"
ftp_cmd_sel = st.sidebar.selectbox("FTP verb", ["(any)"] + ftp_cmd_opt) if ftp_cmd_opt else "(any)"
ftp_code_sel = st.sidebar.selectbox("FTP code", ["(any)"] + ftp_code_opt) if ftp_code_opt else "(any)"
text_query = st.sidebar.text_input("Search text (Short description / paths / codes / unique paths)")

hour_range = None
if "hour" in df.columns:
    hour_range = st.sidebar.slider("Hour range", 0, 23, (0, 23))
month_sel = st.sidebar.multiselect("Months", month_opt) if month_opt else []
dow_sel = st.sidebar.multiselect("Days of week", dow_opt) if dow_opt else []

# Reset button
if st.sidebar.button("Reset filters"):
    iface_sel = "(any)"; errfam_sel = "(any)"; ftp_cmd_sel = "(any)"; ftp_code_sel = "(any)"
    text_query = ""; month_sel = []; dow_sel = []; hour_range = (0, 23)

# Create filters dict
filters = {
    "interface": None if iface_sel == "(any)" else iface_sel,
    "errfam": None if errfam_sel == "(any)" else errfam_sel,
    "ftp_cmd": None if ftp_cmd_sel == "(any)" else ftp_cmd_sel,
    "ftp_code": None if ftp_code_sel == "(any)" else ftp_code_sel,
    "text_query": text_query,
    "months": month_sel,
    "dows": dow_sel,
    "hour_range": hour_range,
}

# ---------- Apply filters to incidents ----------
df_f = apply_global_filters_df(df, filters)
st.subheader(f"Group: {group_key} — Filtered rows: {len(df_f):,} / {len(df):,}")

# Visuals row 1: Top interfaces & Error family Pareto
c1, c2 = st.columns(2)
with c1:
    if "primary_interface" in df_f.columns:
        s = df_f["primary_interface"].dropna()
        if len(s) > 0:
            vc = s.value_counts().head(20)
            fig = px.bar(vc, title="Top interfaces (count)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No primary_interface values after filters.")
    else:
        st.info("Column 'primary_interface' not present.")

with c2:
    if "error_family" in df_f.columns:
        pareto_df = build_pareto_df(df_f["error_family"])
        if len(pareto_df) > 0:
            fig = px.bar(pareto_df, x="value", y="count", title="Error family Pareto (top 20)")
            fig.update_layout(xaxis_title="error_family")
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Cumulative %: {:.1f}%".format(pareto_df["cum_pct"].iloc[-1] if len(pareto_df) else 0))
        else:
            st.info("No error_family values after filters.")
    else:
        st.info("Column 'error_family' not present.")

# Visuals row 2: Stuck metrics & Hour heatmap
c3, c4 = st.columns(2)
with c3:
    if ("stuck_files_count" in df_f.columns) or ("stuck_for_minutes" in df_f.columns):
        sub = pd.DataFrame({
            "stuck_files_count": df_f.get("stuck_files_count", pd.Series(dtype=float)),
            "stuck_for_minutes": df_f.get("stuck_for_minutes", pd.Series(dtype=float)),
        }).dropna(how="all")
        if len(sub) > 0:
            if "stuck_files_count" in sub.columns and sub["stuck_files_count"].notna().sum() > 0:
                fig = px.histogram(sub, x="stuck_files_count", nbins=30, title="Distribution: stuck_files_count")
                st.plotly_chart(fig, use_container_width=True)
            if "stuck_for_minutes" in sub.columns and sub["stuck_for_minutes"].notna().sum() > 0:
                fig2 = px.histogram(sub, x="stuck_for_minutes", nbins=30, title="Distribution: stuck_for_minutes")
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No stuck metrics present after filters.")
    else:
        st.info("No stuck metrics columns present.")

with c4:
    if all(c in df_f.columns for c in ["dow", "hour"]):
        # Count rows per (dow, hour)
        pivot = (
            df_f.assign(_ones=1)
                .pivot_table(index="dow", columns="hour", values="_ones", aggfunc="sum", fill_value=0)
        )
        dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        pivot = pivot.reindex(dow_order)
        fig = px.imshow(
            pivot.values,
            labels=dict(x="hour", y="day", color="count"),
            x=list(pivot.columns), y=list(pivot.index), aspect="auto",
            title="Hour-of-day heatmap (filtered)"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Time grains unavailable; cannot render hour-of-day heatmap.")

st.divider()

# Table preview & download
st.markdown("### Table preview")
preview = df_f.head(PREVIEW_LIMIT)
st.dataframe(preview, use_container_width=True, height=400)

csv_bytes = df_f.to_csv(index=False).encode("utf-8")
st.download_button("Download filtered CSV", data=csv_bytes, file_name=f"{group_key}_filtered.csv", mime="text/csv")
st.caption(f"Run folder: {sel_run}")

# --------------------------------------------------------------------------------------
# Unique Paths Explorer (Global + Specific Filters)
# --------------------------------------------------------------------------------------
st.divider()
with st.expander("🔎 Unique Paths Explorer (from unique_paths_all.csv)", expanded=False):
    up_df = _load_unique_paths_csv(out_root)

    if up_df.empty:
        st.info(
            "No `unique_paths_all.csv` found.\n\n"
            "Generate it with:\n"
            "  .\\.venv\\Scripts\\python.exe .\\extract_unique_paths.py --input .\\ALL_stuck-files.xlsx --out-dir .\\unique_outputs\n\n"
            "Or point the extractor to a specific group file:\n"
            "  --input .\\outputs_partitioned\\run_YYYY-MM-DD_HH-MM-SS\\stuck-files\\ALL_stuck-files.xlsx --sheet data\n"
        )
    else:
        # ---------- Unique Paths specific filters ----------
        st.markdown("#### Additional filters (Unique Paths only)")
        col_f1, col_f2, col_f3 = st.columns([1.6, 1.2, 1.2])
        with col_f1:
            path_prefix = st.text_input("Path starts with", value="/var/opt/transfers",
                                        help="Filter to a path prefix (e.g., /var/opt/transfers)")
        with col_f2:
            min_inc = st.number_input("Min incident count", min_value=0, value=50, step=5)
        with col_f3:
            file_name_q = st.text_input("Filename contains (basename)")

        # Choices for CI and Family derived from dataset
        ci_choices = sorted(
            set(
                x.strip()
                for s in up_df["configuration_items"].dropna().astype(str).tolist()
                for x in s.split(";")
                if x.strip()
            )
        ) if "configuration_items" in up_df.columns else []
        fam_choices = sorted(
            set(
                x.strip()
                for s in up_df["error_families"].dropna().astype(str).tolist()
                for x in s.split(";")
                if x.strip()
            )
        ) if "error_families" in up_df.columns else []

        col_f4, col_f5 = st.columns(2)
        with col_f4:
            ci_sel = st.selectbox("Configuration item", ["(any)"] + ci_choices) if ci_choices else "(any)"
        with col_f5:
            fam_sel = st.selectbox("Error family", ["(any)"] + fam_choices) if fam_choices else "(any)"

        # ---------- Apply filters: global + specific ----------
        up_f_global = apply_global_filters_up(up_df, filters)
        up_f = apply_unique_paths_specific_filters(
            up_f_global, path_prefix, min_inc, ci_sel, fam_sel, file_name_q
        )

        st.markdown("#### Top Paths by Incident Count")
        if len(up_f) > 0:
            fig_up = px.bar(
                up_f.sort_values("incident_count", ascending=False).head(30),
                x="path",
                y="incident_count",
                title="Top unique paths (filtered)",
                text_auto=True,
            )
            fig_up.update_layout(xaxis_title=None, yaxis_title="Incident count")
            st.plotly_chart(fig_up, use_container_width=True)
        else:
            st.info("No rows after filters.")

        # ---------- Table & download ----------
        st.markdown("#### Unique Paths — Table")
        st.dataframe(up_f.head(PREVIEW_LIMIT), use_container_width=True, height=380)
        buf = io.BytesIO(up_f.to_csv(index=False).encode("utf-8"))
        st.download_button(
            "Download filtered unique paths (CSV)",
            data=buf,
            file_name="unique_paths_filtered.csv",
            mime="text/csv",
            use_container_width=True,
        )

        # ---------- Drill-through to current group ----------
        st.markdown("#### Drill-through: Show matching incidents in the current group")
        path_options = ["(select)"] + up_f["path"].astype(str).tolist()
        path_pick = st.selectbox("Choose a path to drill-through", options=path_options, index=0)
        if path_pick and path_pick != "(select)":
            # Apply global filters to df first, then drill
            df_drill_base = apply_global_filters_df(df, filters)
            df_drill = _drill_through_group(df_drill_base, path_pick)
            cnt = len(df_drill)
            st.metric("Matching incidents in group (after global filters)", value=f"{cnt:,}")

            if cnt > 0:
                st.dataframe(df_drill.head(PREVIEW_LIMIT), use_container_width=True, height=380)
                buf2 = io.BytesIO(df_drill.to_csv(index=False).encode("utf-8"))
                st.download_button(
                    "Download drill-through incidents (CSV)",
                    data=buf2,
                    file_name=f"{group_key}_drill_{Path(path_pick).name}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                st.info("No incidents in this group reference the selected path.")




