
#!/usr/bin/env python3
r"""
Run Analytics – Consolidation over a partitioned run
----------------------------------------------------
Reads a selected outputs_partitioned/run_<timestamp> folder, aggregates counts for each scenario
(stuck-files, ftp-errors, config-file, err-file-remove, callout mfterr03/mfterr04, process-alert, warnings, misc),
and writes a sibling analytics package: master_analytics.xlsx + per-scenario CSVs + summary JSON.

Usage (Windows PowerShell from project root):
  .venv\Scripts\Activate.ps1
  python .\run_analytics.py --run-dir .\outputs_partitioned\run_YYYY-MM-DD_HH-MM-SS

If your run files are "raw" and some helper columns are missing, derive from Short description:
  python .\run_analytics.py --run-dir .\outputs_partitioned\run_YYYY-MM-DD_HH-MM-SS --derive

To emit both Excel and CSV (default Excel only):
  python .\run_analytics.py --run-dir .\outputs_partitioned\run_YYYY-MM-DD_HH-MM-SS --format both
"""

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from datetime import datetime

# ----------------- Regexes for derivation from Short description -----------------
ID_PAT = re.compile(r"\b([EIU][0-9]{3,6}[A-Z]?)\b")
ERR_FAMILY_PAT = re.compile(
    r"\b("
    r"FMS\-FTP\-ERROR|FTP\s*error|"
    r"FMS\-CONFIG\-FILE|"
    r"FMS\-ERR\-FILE\-REMOVE|"
    r"PROCESS\-ALERT|"
    r"MFTERRGEN|"
    r"CALLOUT[_\-]?MFTERR0?3|CALLOUT[_\-]?MFTERR0?4|"
    r"Permission denied|Not connected|Auth fail|"
    r"java\.io\.IOException|Address already in use|"
    r"Stuck files|WARNING"
    r")\b",
    re.IGNORECASE,
)
FAMILY_NORMALIZE = {
    "fms-ftp-error": "FMS-FTP-ERROR",
    "ftp error": "FMS-FTP-ERROR",
    "fms-config-file": "FMS-CONFIG-FILE",
    "fms-err-file-remove": "FMS-ERR-FILE-REMOVE",
    "process-alert": "PROCESS-ALERT",
    "mfterrgen": "MFTERRGEN",
    "callout_mfterr03": "CALLOUT_MFTERR03",
    "callout-mfterr03": "CALLOUT_MFTERR03",
    "callout_mfterr04": "CALLOUT_MFTERR04",
    "callout-mfterr04": "CALLOUT_MFTERR04",
    "permission denied": "Permission denied",
    "not connected": "Not connected",
    "auth fail": "Auth fail",
    "java.io.ioexception": "java.io.IOException",
    "address already in use": "Address already in use",
    "stuck files": "Stuck files",
    "warning": "WARNING",
}
CODE_PAT = re.compile(r"\b([UEI][0-9]{3,6})\b")
PATH_PAT = re.compile(r"(/(?:var|opt|data|mnt|etc|home)[^\s:;,)]*)")
STUCK_CNT_PAT = re.compile(r":\s*-\s*(\d+)\b")
STUCK_MIN_PAT = re.compile(r"from\s+(\d+)\s*min\b", re.IGNORECASE)
FTP_CMD_PAT = re.compile(r"\b(Unsuccessful)\s+(PUT|GET)\b", re.IGNORECASE)
FTP_CODE_550_PAT = re.compile(r"\b550\b")
PRIORITY_PREFIX = ["E", "I", "U"]

# ----------------- Helpers -----------------
def normalize_family(token: Optional[str]) -> str:
    if not token:
        return "other"
    key = token.lower().replace("-", " ").replace("_", " ").strip()
    key = key.replace("  ", " ")
    key = key.replace(" ", "-")
    return FAMILY_NORMALIZE.get(key, token if token else "other")

def pick_primary(ids: List[str]) -> Optional[str]:
    if not ids:
        return np.nan
    for pref in PRIORITY_PREFIX:
        for t in ids:
            if t.startswith(pref):
                return t
    return ids[0]

def parse_short_description(text: str) -> Dict[str, object]:
    """Deterministic derivation for helper fields."""
    s = str(text or "")
    ids = ID_PAT.findall(s)
    primary = pick_primary(ids)
    fams = ERR_FAMILY_PAT.findall(s)
    fam_norm = normalize_family(fams[0]) if fams else "other"
    codes = list(dict.fromkeys(CODE_PAT.findall(s)))
    paths = PATH_PAT.findall(s)
    cnt_m = STUCK_CNT_PAT.search(s)
    mins_m = STUCK_MIN_PAT.search(s)
    cmd_m = FTP_CMD_PAT.search(s)
    ftp_command = cmd_m.group(2).upper() if cmd_m else None
    ftp_code = "550" if FTP_CODE_550_PAT.search(s) else None
    return {
        "interfaces": ",".join(ids) if ids else np.nan,
        "primary_interface": primary,
        "error_family": fam_norm,
        "error_codes": ",".join(codes) if codes else np.nan,
        "paths": ",".join(paths) if paths else np.nan,
        "has_path": bool(paths),
        "stuck_files_count": int(cnt_m.group(1)) if cnt_m else np.nan,
        "stuck_for_minutes": int(mins_m.group(1)) if mins_m else np.nan,
        "ftp_command": ftp_command,
        "ftp_code": ftp_code,
        "permission_denied": bool(re.search(r"Permission denied", s, re.IGNORECASE)),
        "not_connected": bool(re.search(r"Not connected", s, re.IGNORECASE)),
    }

def safe_name(s: str) -> str:
    """Windows-safe filename fragment."""
    s = str(s or "unknown").strip()
    s = re.sub(r'[<>:"/\\|?*]+', "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:80]

def ensure_datetime(series: pd.Series) -> pd.Series:
    """Convert Excel serial or ISO to datetime; return NaT where parsing fails."""
    dt = pd.to_datetime(series, errors="coerce")
    if dt.notna().sum():
        return dt
    # If all NaT, try Excel serial origin approach
    try:
        return pd.to_datetime(series, unit="d", origin="1899-12-30", errors="coerce")
    except Exception:
        return pd.to_datetime(series, errors="coerce")

def explode_paths(df: pd.DataFrame, col: str = "paths") -> pd.DataFrame:
    if col not in df.columns:
        return pd.DataFrame(columns=[col])
    s = df[col].fillna("").astype(str)
    rows = []
    for i, val in s.items():
        for p in [x.strip() for x in val.split(",") if x.strip()]:
            rows.append({"idx": i, "path": p})
    return pd.DataFrame(rows)

# ----------------- IO for groups -----------------
def group_dir(run_dir: Path, group_key: str) -> Path:
    if group_key.startswith("callout/"):
        return run_dir / "callout" / group_key.split("/", 1)[1]
    return run_dir / group_key

def read_group_df(run_dir: Path, group_key: str) -> pd.DataFrame:
    gd = group_dir(run_dir, group_key)
    if not gd.exists():
        return pd.DataFrame()
    # Prefer Excel
    xl_candidates = list(gd.glob("ALL_*.xlsx"))
    if xl_candidates:
        return pd.read_excel(xl_candidates[0], engine="openpyxl")
    csv_candidates = list(gd.glob("ALL_*.csv"))
    if csv_candidates:
        return pd.read_csv(csv_candidates[0])
    return pd.DataFrame()

def ensure_helpers(df: pd.DataFrame, derive: bool, sd_col: Optional[str]) -> pd.DataFrame:
    need_cols = [
        "error_family","stuck_files_count","stuck_for_minutes","paths","has_path",
        "ftp_command","ftp_code","interfaces","primary_interface","permission_denied","not_connected"
    ]
    if derive:
        if not sd_col:
            raise ValueError("Short description column required to derive helper features.")
        parsed = df[sd_col].apply(parse_short_description)
        helpers = pd.DataFrame(parsed.tolist())
        for c in helpers.columns:
            if c not in df.columns:
                df[c] = helpers[c]
            else:
                df[c] = df[c].where(df[c].notna(), helpers[c])
    else:
        for c in need_cols:
            if c not in df.columns:
                if c in ("has_path","permission_denied","not_connected"):
                    df[c] = False
                else:
                    df[c] = np.nan
    return df

# ----------------- Aggregations per scenario -----------------
def agg_stuck(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # Paths counts + stats
    paths_df = explode_paths(df, "paths")
    if not paths_df.empty:
        merged = paths_df.merge(df[["stuck_files_count","stuck_for_minutes"]], left_on="idx", right_index=True, how="left")
        stuck_paths_counts = merged.groupby("path").size().reset_index(name="incidents_count").sort_values("incidents_count", ascending=False)
        stats = merged.groupby("path").agg(
            incidents_count=("path","count"),
            total_stuck_files=("stuck_files_count", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            mean_minutes=("stuck_for_minutes", lambda s: float(pd.to_numeric(s, errors="coerce").mean())),
            median_minutes=("stuck_for_minutes", lambda s: float(pd.to_numeric(s, errors="coerce").median())),
            min_minutes=("stuck_for_minutes", lambda s: float(pd.to_numeric(s, errors="coerce").min())),
            max_minutes=("stuck_for_minutes", lambda s: float(pd.to_numeric(s, errors="coerce").max())),
        ).reset_index().sort_values("incidents_count", ascending=False)
    else:
        stuck_paths_counts = pd.DataFrame(columns=["path","incidents_count"])
        stats = pd.DataFrame(columns=["path","incidents_count","total_stuck_files","mean_minutes","median_minutes","min_minutes","max_minutes"])

    # Top stuck interfaces
    iface = df["primary_interface"].fillna("UNKNOWN").astype(str)
    stuck_top_interfaces = iface.value_counts().reset_index()
    stuck_top_interfaces.columns = ["primary_interface","incidents_count"]
    if "stuck_files_count" in df.columns:
        by_iface = df.groupby(iface).agg(total_stuck_files=("stuck_files_count", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())))
        stuck_top_interfaces = stuck_top_interfaces.merge(by_iface, left_on="primary_interface", right_index=True, how="left").fillna({"total_stuck_files":0})
    return {
        "stuck_paths_counts": stuck_paths_counts,
        "stuck_paths_stats": stats,
        "stuck_top_interfaces": stuck_top_interfaces,
    }

def agg_ftp(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    ftp_by_verb = df["ftp_command"].fillna("UNKNOWN").astype(str).value_counts().reset_index()
    ftp_by_verb.columns = ["ftp_command","incidents_count"]
    ftp_by_code = df["ftp_code"].fillna("UNKNOWN").astype(str).value_counts().reset_index()
    ftp_by_code.columns = ["ftp_code","incidents_count"]
    key = df[["primary_interface","ftp_command","ftp_code"]].copy()
    for c in key.columns:
        key[c] = key[c].fillna("UNKNOWN").astype(str)
    ftp_iface_verb_code = key.value_counts().reset_index(name="incidents_count").sort_values("incidents_count", ascending=False)

    # Permission vs connectivity by interface (booleans derived or text)
    df["permission_denied"] = df.get("permission_denied", False)
    df["not_connected"] = df.get("not_connected", False)
    perm_conn = df.groupby(df["primary_interface"].fillna("UNKNOWN")).agg(
        permission_denied=("permission_denied", "sum"),
        not_connected=("not_connected", "sum"),
        incidents_count=("primary_interface", "count"),
    ).reset_index().sort_values("incidents_count", ascending=False)
    return {
        "ftp_by_verb": ftp_by_verb,
        "ftp_by_code": ftp_by_code,
        "ftp_iface_verb_code": ftp_iface_verb_code,
        "ftp_permission_connectivity": perm_conn,
    }

def agg_config(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    paths_df = explode_paths(df, "paths")
    if not paths_df.empty:
        cfg_paths = paths_df[paths_df["path"].str.startswith("/opt/big/configs", na=False)]
        config_bad_paths = cfg_paths["path"].value_counts().reset_index()
        config_bad_paths.columns = ["path","incidents_count"]
    else:
        config_bad_paths = pd.DataFrame(columns=["path","incidents_count"])
    config_top_interfaces = df["primary_interface"].fillna("UNKNOWN").astype(str).value_counts().reset_index()
    config_top_interfaces.columns = ["primary_interface","incidents_count"]
    return {
        "config_bad_paths": config_bad_paths,
        "config_top_interfaces": config_top_interfaces,
    }

def agg_err_remove(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    err_remove_by_interface = df["primary_interface"].fillna("UNKNOWN").astype(str).value_counts().reset_index()
    err_remove_by_interface.columns = ["primary_interface","incidents_count"]
    paths_df = explode_paths(df, "paths")
    if not paths_df.empty:
        err_remove_paths = paths_df["path"].value_counts().reset_index()
        err_remove_paths.columns = ["path","incidents_count"]
    else:
        err_remove_paths = pd.DataFrame(columns=["path","incidents_count"])
    return {
        "err_remove_by_interface": err_remove_by_interface,
        "err_remove_paths": err_remove_paths,
    }

def add_time_grains(df: pd.DataFrame) -> pd.DataFrame:
    if "Opened" in df.columns:
        dt = ensure_datetime(df["Opened"])
        df["Opened_dt"] = dt
        df["month"] = dt.dt.to_period("M").astype(str)
        df["dow"] = dt.dt.day_name()
        df["hour"] = dt.dt.hour
    return df

def agg_process_alert(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df = add_time_grains(df)
    process_alert_by_interface = df["primary_interface"].fillna("UNKNOWN").astype(str).value_counts().reset_index()
    process_alert_by_interface.columns = ["primary_interface","incidents_count"]
    if "month" in df.columns:
        process_alert_time = df.groupby("month")["primary_interface"].count().reset_index(name="incidents_count").sort_values("month")
    else:
        process_alert_time = pd.DataFrame(columns=["month","incidents_count"])
    return {
        "process_alert_by_interface": process_alert_by_interface,
        "process_alert_time": process_alert_time,
    }

def agg_callout(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    callout_interfaces = df["primary_interface"].fillna("UNKNOWN").astype(str).value_counts().reset_index()
    callout_interfaces.columns = ["primary_interface","incidents_count"]
    # host_tag if present
    host_tag = df.get("host_tag")
    if host_tag is not None:
        callout_hosts = host_tag.fillna("UNKNOWN").astype(str).value_counts().reset_index()
        callout_hosts.columns = ["host_tag","incidents_count"]
    else:
        callout_hosts = pd.DataFrame(columns=["host_tag","incidents_count"])
    return {
        "callout_interfaces": callout_interfaces,
        "callout_hosts": callout_hosts,
    }

def agg_warnings(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # Hosts & filesystem paths
    host_tag = df.get("host_tag")
    if host_tag is not None:
        warnings_hosts = host_tag.fillna("UNKNOWN").astype(str).value_counts().reset_index()
        warnings_hosts.columns = ["host_tag","incidents_count"]
    else:
        warnings_hosts = pd.DataFrame(columns=["host_tag","incidents_count"])
    paths_df = explode_paths(df, "paths")
    if not paths_df.empty:
        warnings_fs_paths = paths_df["path"].value_counts().reset_index()
        warnings_fs_paths.columns = ["path","incidents_count"]
    else:
        warnings_fs_paths = pd.DataFrame(columns=["path","incidents_count"])
    return {
        "warnings_hosts": warnings_hosts,
        "warnings_fs_paths": warnings_fs_paths,
    }

def agg_misc(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # Tokens from error_codes
    if "error_codes" in df.columns:
        codes_s = df["error_codes"].fillna("").astype(str)
        codes_list = []
        for s in codes_s:
            for c in [x.strip() for x in s.split(",") if x.strip()]:
                codes_list.append(c)
        misc_error_codes = pd.Series(codes_list).value_counts().reset_index()
        misc_error_codes.columns = ["error_code","count"]
    else:
        misc_error_codes = pd.DataFrame(columns=["error_code","count"])
    # Top tokens from Short description (very simple split)
    sd = df.get("Short description")
    if sd is not None:
        tokens = []
        for line in sd.fillna("").astype(str).tolist():
            for tok in re.findall(r"[A-Za-z0-9_/.\-]+", line):
                tokens.append(tok.lower())
        misc_top_tokens = pd.Series(tokens).value_counts().head(200).reset_index()
        misc_top_tokens.columns = ["token","count"]
    else:
        misc_top_tokens = pd.DataFrame(columns=["token","count"])
    return {
        "misc_top_tokens": misc_top_tokens,
        "misc_error_codes": misc_error_codes,
    }

# ----------------- Writer -----------------
def write_df(df: pd.DataFrame, path: Path, base: str, fmt: str):
    path.mkdir(parents=True, exist_ok=True)
    base_safe = safe_name(base)
    if fmt in ("xlsx","both"):
        out_xlsx = path / f"{base_safe}.xlsx"
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
            df.to_excel(w, sheet_name="data", index=False)
    if fmt in ("csv","both"):
        out_csv = path / f"{base_safe}.csv"
        df.to_csv(out_csv, index=False, encoding="utf-8")

def write_master_excel(path: Path, sheets: Dict[str, pd.DataFrame]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, sheet_name=name[:31], index=False)  # Excel sheet name limit 31 chars

# ----------------- Main -----------------
GROUPS_ORDER = [
    "stuck-files","ftp-errors","config-file","err-file-remove",
    "callout/mfterr03","callout/mfterr04","process-alert","warnings","misc"
]

def main() -> int:
    ap = argparse.ArgumentParser(description="Aggregate scenario counts for a partitioned run and write analytics outputs.")
    ap.add_argument("--run-dir", required=True, help="Path to outputs_partitioned/run_YYYY-MM-DD_HH-MM-SS")
    ap.add_argument("--derive", action="store_true", help="Derive helper fields from Short description if missing.")
    ap.add_argument("--format", choices=["xlsx","csv","both"], default="xlsx", help="Write per-scenario outputs in this format.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir).resolve()
    if not run_dir.exists():
        print(f"[ERROR] Run directory not found: {run_dir}")
        return 1

    # Prepare output folder
    analytics_dir = run_dir.parent / f"{run_dir.name}_analytics"
    logs_dir = analytics_dir / "_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    summary_counts = {}
    top_offenders_all = pd.Series(dtype=int)

    master_sheets: Dict[str, pd.DataFrame] = {}

    total_rows = 0

    for g in GROUPS_ORDER:
        df = read_group_df(run_dir, g)
        count = len(df)
        summary_counts[g] = int(count)
        total_rows += int(count)

        if count == 0:
            continue

        # Locate Short description column
        sd_col = None
        for c in df.columns:
            if str(c).casefold().replace(" ","") == "shortdescription":
                sd_col = c; break

        # Ensure helpers present
        df = ensure_helpers(df, derive=args.derive, sd_col=sd_col)

        # Add to offenders
        offenders = df["primary_interface"].fillna("UNKNOWN").astype(str).value_counts()
        top_offenders_all = top_offenders_all.add(offenders, fill_value=0)

        # Aggregations
        if g == "stuck-files":
            ag = agg_stuck(df)
            for name, table in ag.items():
                write_df(table, analytics_dir, name, args.format)
            master_sheets["stuck_files"] = pd.concat(ag.values(), axis=1).iloc[:, :].copy() if len(ag) else pd.DataFrame()

        elif g == "ftp-errors":
            ag = agg_ftp(df)
            for name, table in ag.items():
                write_df(table, analytics_dir, name, args.format)
            master_sheets["ftp_errors"] = pd.concat(ag.values(), axis=1) if len(ag) else pd.DataFrame()

        elif g == "config-file":
            ag = agg_config(df)
            for name, table in ag.items():
                write_df(table, analytics_dir, name, args.format)
            master_sheets["config_file"] = pd.concat(ag.values(), axis=1) if len(ag) else pd.DataFrame()

        elif g == "err-file-remove":
            ag = agg_err_remove(df)
            for name, table in ag.items():
                write_df(table, analytics_dir, name, args.format)
            master_sheets["err_file_remove"] = pd.concat(ag.values(), axis=1) if len(ag) else pd.DataFrame()

        elif g in ("callout/mfterr03","callout/mfterr04"):
            ag = agg_callout(df)
            prefix = "callout_mfterr03" if g.endswith("mfterr03") else "callout_mfterr04"
            for suffix, table in ag.items():
                write_df(table, analytics_dir, f"{prefix}_{suffix.split('_')[-1]}", args.format)
            master_sheets[prefix] = pd.concat(ag.values(), axis=1) if len(ag) else pd.DataFrame()

        elif g == "process-alert":
            ag = agg_process_alert(df)
            for name, table in ag.items():
                write_df(table, analytics_dir, name, args.format)
            master_sheets["process_alert"] = pd.concat(ag.values(), axis=1) if len(ag) else pd.DataFrame()

        elif g == "warnings":
            ag = agg_warnings(df)
            for name, table in ag.items():
                write_df(table, analytics_dir, name, args.format)
            master_sheets["warnings"] = pd.concat(ag.values(), axis=1) if len(ag) else pd.DataFrame()

        elif g == "misc":
            ag = agg_misc(df)
            for name, table in ag.items():
                write_df(table, analytics_dir, name, args.format)
            master_sheets["misc_triage"] = pd.concat(ag.values(), axis=1) if len(ag) else pd.DataFrame()

    # Master Excel
    master_path = analytics_dir / "master_analytics.xlsx"
    write_master_excel(master_path, master_sheets)

    # Coverage & quality checks
    coverage = int(sum(v for k, v in summary_counts.items() if k != "misc"))
    pct_missing_primary = 0.0
    pct_err_other = 0.0
    stuck_metrics_rows = 0
    ftp_meta_rows = 0

    # Re-scan for quality stats: combine all loaded dfs quickly
    loaded_dfs = []
    for g in GROUPS_ORDER:
        df = read_group_df(run_dir, g)
        if len(df):
            df = ensure_helpers(df, derive=False, sd_col=None)
            loaded_dfs.append(df)
    if loaded_dfs:
        big = pd.concat(loaded_dfs, ignore_index=True)
        total_rows = int(len(big))
        pct_missing_primary = float(big["primary_interface"].isna().mean()) if total_rows else 0.0
        pct_err_other = float((big["error_family"].astype(str).str.lower() == "other").mean()) if total_rows else 0.0
        stuck_metrics_rows = int(big["stuck_files_count"].notna().sum() + big["stuck_for_minutes"].notna().sum())
        ftp_meta_rows = int(big["ftp_command"].notna().sum() + big["ftp_code"].notna().sum())

    # Top offenders (interfaces)
    top_offenders = top_offenders_all.sort_values(ascending=False).head(10)
    top_offenders = [{"primary_interface": k, "incidents_count": int(v)} for k, v in top_offenders.items()]

    summary_json = {
        "run_dir": str(run_dir),
        "analytics_dir": str(analytics_dir),
        "total_rows": total_rows,
        "coverage_non_misc": coverage,
        "coverage_pct": (coverage / total_rows * 100.0) if total_rows else 0.0,
        "group_counts": summary_counts,
        "quality_checks": {
            "pct_missing_primary_interface": pct_missing_primary,
            "pct_error_family_other": pct_err_other,
            "rows_with_stuck_metrics": stuck_metrics_rows,
            "rows_with_ftp_metadata": ftp_meta_rows,
        },
        "top_offenders_interfaces": top_offenders,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    (logs_dir / "analytics_summary.json").write_text(json.dumps(summary_json, indent=2))

    print(f"[OK] Analytics written to: {analytics_dir}")
    print(f"[OK] Master Excel: {master_path}")
    print(f"[OK] Summary JSON: {logs_dir / 'analytics_summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
