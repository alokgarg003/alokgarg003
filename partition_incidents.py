
#!/usr/bin/env python3
r"""
Incident Partitioning Pipeline (Industry-grade)
-----------------------------------------------
Reads a ServiceNow incident Excel, derives helper fields (optional), and partitions rows
into scenario-specific groups under a timestamped output directory:
- stuck-files/
- ftp-errors/  (optional subgroups: by-verb PUT/GET, by-code 550/...)
- config-file/
- err-file-remove/
- callout/mfterr03/, callout/mfterr04/
- process-alert/
- warnings/
- misc/

Each group gets an "ALL_<group>.xlsx" (and optionally CSV), plus a run-level _logs/summary.csv.

Usage (Windows PowerShell from project root):
  .venv\Scripts\Activate.ps1
  python .\partition_incidents.py --input .\snow_enriched.xlsx --sheet first --out-root .\outputs_partitioned

If your input is raw SNOW (no helper fields yet), derive from 'Short description':
  python .\partition_incidents.py --input .\snow_last_2year_data.xlsx --sheet Sheet1 --out-root .\outputs_partitioned --derive

Emit both Excel and CSV:
  python .\partition_incidents.py --input .\snow_enriched.xlsx --sheet first --out-root .\outputs_partitioned --format both
"""

import argparse
import re
import sys
import os
from pathlib import Path
from typing import List, Optional, Tuple, Union, Dict
from datetime import datetime

import numpy as np
import pandas as pd


# ========= Regexes used for derivation from Short description =========

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


# ========= Helpers =========

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
    """Derive helper features from Short description deterministically."""
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
    }


def find_col(df: pd.DataFrame, name_like: str) -> Optional[str]:
    target = name_like.casefold().replace(" ", "")
    for c in df.columns:
        if str(c).casefold().replace(" ", "") == target:
            return c
    return None


def load_excel_single_sheet(path: Path, sheet: Optional[Union[str, int]]) -> pd.DataFrame:
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheet_names = xl.sheet_names
    if sheet is None or (isinstance(sheet, str) and sheet.strip().lower() in ("", "first", "0")):
        return xl.parse(sheet_name=0)
    if isinstance(sheet, int):
        return xl.parse(sheet_name=sheet)
    name = sheet.strip()
    if name not in sheet_names:
        raise ValueError(f"Worksheet named '{name}' not found. Available: {sheet_names}")
    return xl.parse(sheet_name=name)


def ensure_helpers(df: pd.DataFrame, derive: bool, sd_col: Optional[str]) -> pd.DataFrame:
    """
    Ensure helper columns exist. If derive=True, compute from Short description.
    Otherwise, leave as-is; if missing, they remain NaN.
    """
    need_cols = [
        "error_family", "stuck_files_count", "stuck_for_minutes",
        "paths", "has_path", "ftp_command", "ftp_code",
        "interfaces", "primary_interface"
    ]
    have_all = all(c in df.columns for c in need_cols)

    if derive:
        if not sd_col:
            raise ValueError("Short description column required to derive helper features.")
        parsed = df[sd_col].apply(parse_short_description)
        helpers = pd.DataFrame(parsed.tolist())
        # Do not overwrite existing if already present (but most raw files won't have them)
        for c in helpers.columns:
            if c not in df.columns:
                df[c] = helpers[c]
            else:
                # If existing column is all NaN, fill from derived
                df[c] = df[c].where(df[c].notna(), helpers[c])
    else:
        # Create missing helper columns with NaN/defaults
        for c in need_cols:
            if c not in df.columns:
                if c in ("has_path",):
                    df[c] = False
                else:
                    df[c] = np.nan
    return df


def safe_name(s: str) -> str:
    """Windows-safe filename fragment."""
    s = str(s or "unknown")
    s = s.strip()
    s = re.sub(r'[<>:"/\\|?*]+', "_", s)
    s = re.sub(r"\s+", "_", s)
    return s[:80]  # limit length


def write_df(df: pd.DataFrame, path: Path, base: str, fmt: str):
    """
    Write df to Excel/CSV depending on fmt: 'xlsx' | 'csv' | 'both'.
    """
    path.mkdir(parents=True, exist_ok=True)
    base_safe = safe_name(base)
    if fmt in ("xlsx", "both"):
        out_xlsx = path / f"{base_safe}.xlsx"
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
            df.to_excel(w, sheet_name="data", index=False)
    if fmt in ("csv", "both"):
        out_csv = path / f"{base_safe}.csv"
        df.to_csv(out_csv, index=False, encoding="utf-8")


# ========= Grouping logic (scenario taxonomy) =========

def build_masks(df: pd.DataFrame, sd_col: str) -> Dict[str, pd.Series]:
    """
    Return boolean masks for each top-level group.
    Fallback to text matching when error_family is 'other' or missing.
    """
    ef = df["error_family"].astype(str).str.strip().str.upper()
    sd = df[sd_col].astype(str)

    # Stuck files
    stuck_mask = (
        (ef == "STUCK FILES") |
        (df["stuck_files_count"].fillna(0).astype(float) >= 1) |
        (df["stuck_for_minutes"].fillna(0).astype(float) >= 1) |
        (sd.str.contains(r"Number of stuck files", case=False, regex=True))
    )

    # FTP errors
    ftp_mask = (
        (ef == "FMS-FTP-ERROR") |
        (sd.str.contains(r"FTP\s*error|Unsuccessful\s+PUT|Unsuccessful\s+GET|Not connected", case=False, regex=True)) |
        (df["ftp_code"].astype(str).str.contains(r"\b550\b", regex=True)) |
        (df["ftp_command"].astype(str).str.upper().isin(["PUT", "GET"]))
    )

    # Config file
    cfg_mask = (
        (ef == "FMS-CONFIG-FILE") |
        (sd.str.contains(r"Config exists|contains no config lines", case=False, regex=True))
    )

    # Err file remove
    rm_mask = (
        (ef == "FMS-ERR-FILE-REMOVE") |
        (sd.str.contains(r"Cannot remove file|Cannot move", case=False, regex=True))
    )

    # Callout mfterr03 and mfterr04
    co03_mask = (
        (ef == "CALLOUT_MFTERR03") |
        (sd.str.contains(r"CALLOUT[_\-]?MFTERR0?3", case=False, regex=True))
    )
    co04_mask = (
        (ef == "CALLOUT_MFTERR04") |
        (sd.str.contains(r"CALLOUT[_\-]?MFTERR0?4", case=False, regex=True))
    )

    # Process alert
    proc_mask = (
        (ef == "PROCESS-ALERT") |
        (sd.str.contains(r"PROCESS-ALERT", case=False, regex=True))
    )

    # Warnings
    warn_mask = (
        (ef == "WARNING") |
        (sd.str.contains(r"WARNING:", case=False, regex=True))
    )

    # Misc (anything not in the above)
    any_mask = stuck_mask | ftp_mask | cfg_mask | rm_mask | co03_mask | co04_mask | proc_mask | warn_mask
    misc_mask = ~any_mask

    return {
        "stuck-files": stuck_mask,
        "ftp-errors": ftp_mask,
        "config-file": cfg_mask,
        "err-file-remove": rm_mask,
        "callout_mfterr03": co03_mask,
        "callout_mfterr04": co04_mask,
        "process-alert": proc_mask,
        "warnings": warn_mask,
        "misc": misc_mask,
    }


# ========= Main CLI =========

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Partition incidents by error scenarios and write grouped files with a summary."
    )
    ap.add_argument("--input", required=True, help="Path to input Excel (raw SNOW or enriched).")
    ap.add_argument("--out-root", required=True, help="Output root directory to create partitioned run.")
    ap.add_argument("--sheet", default="first", help="Sheet name or index. Use 'first' to load the first sheet.")
    ap.add_argument("--derive", action="store_true", help="Derive helper fields from 'Short description' if missing.")
    ap.add_argument("--format", choices=["xlsx", "csv", "both"], default="xlsx", help="Output file format(s).")
    ap.add_argument("--misc-samples", type=int, default=200, help="How many misc samples to save for rule refinement.")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"[ERROR] Input file not found: {in_path}")
        return 1

    # Load sheet
    try:
        try:
            sheet_arg = int(args.sheet)
        except ValueError:
            sheet_arg = args.sheet
        df = load_excel_single_sheet(in_path, sheet_arg)
    except Exception as e:
        print(f"[ERROR] Failed reading Excel: {e}")
        return 1

    # Locate Short description
    sd_col = find_col(df, "Short description")
    if not sd_col:
        print("[ERROR] 'Short description' column not found in the selected sheet.")
        return 1

    # Ensure helper columns (derive if asked)
    try:
        df = ensure_helpers(df, derive=args.derive, sd_col=sd_col)
    except Exception as e:
        print(f"[ERROR] Failed deriving helper fields: {e}")
        return 1

    # Build masks
    masks = build_masks(df, sd_col)

    # Prepare run directory
    out_root = Path(args.out_root)
    timestamp = datetime.now().strftime("run_%Y-%m-%d_%H-%M-%S")
    run_dir = out_root / timestamp
    logs_dir = run_dir / "_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Partition and write
    summary_rows = []
    fmt = args.format

    # Top-level groups
    groups_order = [
        "stuck-files", "ftp-errors", "config-file", "err-file-remove",
        "callout_mfterr03", "callout_mfterr04", "process-alert", "warnings", "misc"
    ]

    for g in groups_order:
        mask = masks[g]
        part = df[mask].copy()
        count = len(part)
        summary_rows.append({"group": g, "count": count})

        if count == 0:
            continue

        # Create group directory
        g_dir = run_dir / g if not g.startswith("callout") else (run_dir / "callout" / g.replace("callout_", ""))
        g_dir.mkdir(parents=True, exist_ok=True)

        # Write ALL group
        write_df(part, g_dir, f"ALL_{g}", fmt)

        # Optional subgroups
        if g == "stuck-files":
            # by-interface (primary_interface)
            if "primary_interface" in part.columns:
                by_iface = part.groupby(part["primary_interface"].fillna("UNKNOWN"))
                for iface_val, subdf in by_iface:
                    write_df(subdf, g_dir / "by-interface", f"{iface_val}_stuck", fmt)

        elif g == "ftp-errors":
            # by-verb (PUT/GET)
            if "ftp_command" in part.columns:
                by_verb = part.groupby(part["ftp_command"].fillna("UNKNOWN"))
                for verb, subdf in by_verb:
                    write_df(subdf, g_dir / "by-verb", f"{verb}_ftp-errors", fmt)
            # by-code (e.g., 550)
            if "ftp_code" in part.columns:
                by_code = part.groupby(part["ftp_code"].fillna("UNKNOWN"))
                for code, subdf in by_code:
                    write_df(subdf, g_dir / "by-code", f"{code}_ftp-errors", fmt)

    # Save misc sample for rule refinement
    misc_mask = masks["misc"]
    misc_part = df[misc_mask].copy()
    if len(misc_part) > 0 and args.misc_samples > 0:
        sample = misc_part.head(args.misc_samples)
        write_df(sample, logs_dir, "misc_unmatched_samples", fmt="xlsx")

    # Write summary CSV
    total_rows = len(df)
    coverage = sum(r["count"] for r in summary_rows if r["group"] != "misc")
    summary_df = pd.DataFrame(summary_rows)
    summary_df["total_rows"] = total_rows
    summary_df["coverage_pct"] = (coverage / total_rows * 100.0) if total_rows else 0.0
    summary_df.to_csv(logs_dir / "summary.csv", index=False)
    print(f"[OK] Partitioned {total_rows} rows into '{run_dir}'. See _logs/summary.csv for counts.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
