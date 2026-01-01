
#!/usr/bin/env python3
r"""
Extract structured signals from ServiceNow 'Description' across all sheets

Outputs (depending on flags):
  Combine mode (default or --all-sheets):
    <out-dir>/desc_enriched.csv        # all sheets combined + extracted fields
    <out-dir>/desc_summary.xlsx        # rollups by error family & triplet
    <out-dir>/by_error_family.csv
    <out-dir>/by_triplet.csv

  Per-sheet mode (--per-sheet):
    <out-dir>/<sheet>/desc_enriched.csv
    <out-dir>/<sheet>/desc_summary.xlsx
    <out-dir>/<sheet>/by_error_family.csv
    <out-dir>/<sheet>/by_triplet.csv

Usage (Windows PowerShell from project root):
  python .\extract_description_features.py --input ".\incident (1).xlsx" --out-dir .\unique_outputs -v

Optional flags:
  --all-sheets               # process and combine all sheets (default for Excel)
  --per-sheet                # process all sheets and write outputs per sheet
  --append-only              # write only extracted fields (no original columns)
  --allow-non-absolute       # include non-absolute tokens in fallback path extraction
  -v / -vv                   # verbose logging

Notes:
  - Requires: pandas, numpy, openpyxl.
  - Tolerant to missing columns; focuses on 'Description' but will check
    'Short description' as fallback if needed.
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# -----------------------
# Patterns & Normalizers
# -----------------------

ERR_FAMILY_PAT = re.compile(
    r"\b("
    r"FMS-FTP-ERROR|FTP\s*error|"
    r"FMS-CONFIG-FILE|"
    r"FMS-ERR-FILE-REMOVE|FMS-ERR-FILE-COPY|"
    r"FMS-DIR-CREATE|"
    r"PROCESS-ALERT|FTG(?:-ALERT)?|"
    r"MFTERRGEN|"
    r"CALLOUT[_-]?MFTERR0?2|"
    r"CALLOUT[_-]?MFTERR0?3|"
    r"CALLOUT[_-]?MFTERR0?4|"
    r"ERR-HTTPS-ERROR|"
    r"Missing Files|MISSING-FILES|"
    r"Unhandled\s+Exception\s+Error"
    r")\b",
    re.IGNORECASE,
)

FAMILY_NORMALIZE = {
    "ftp error": "FMS-FTP-ERROR",
    "missing files": "MISSING-FILES",
}
def normalize_family(token: Optional[str]) -> str:
    if not token:
        return ""
    t = token.strip()
    key = re.sub(r"[\s_]+", " ", t.lower()).replace(" ", "-")
    return FAMILY_NORMALIZE.get(key, t.replace("_", "-").upper())

# Interface triplet like E000341-788-U000064, MULTIPLE-693-U000064, INV-...
TRIPLET_PAT = re.compile(
    r"\b(?P<src>(?:[EIU]\d{6,}|MULTIPLE|INV))-(?P<mid>[A-Za-z0-9]+)-(?P<dst>(?:[EIU]\d{6,}|MULTIPLE|INV))\b"
)

# Timestamp patterns
TS_ISO_PAT = re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z)?\b")
TS_SN_PAT = re.compile(
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+GMT\s+\d{4}\b"
)
TS_FIRED_PAT = re.compile(r"Fired\s+Date\s+Time:\s*(?P<ts>\d{4}-\d{2}-\d{2}T\S+)", re.IGNORECASE)

# Filesystem paths (Unix & Windows)
UNIX_PATH_PAT = re.compile(r"(/[-A-Za-z0-9_./]+)")
WIN_PATH_PAT  = re.compile(r"([A-Za-z]:\\[^\s:;,)]+)")

# Windows UNC-like folder references in text (e.g., E:\IPSData\Despatches)
WIN_DRIVE_PAT = WIN_PATH_PAT  # reuse

# File patterns with wildcard (hwdc2edi-predes21_*.xml, Isotrak_*.zip)
FILE_PATTERN_PAT = re.compile(r"\b[^\s/:\\]+?\*[^ \t:/\\]*\b")

# FTP codes + message (e.g., 550 The system cannot find the file specified.)
FTP_CODE_MSG_PAT = re.compile(r"\b(?P<code>[45]\d{2})\b(?:\s+(?P<msg>[^.]+))?")

# Operations / signals
OP_TAGS: List[Tuple[str, re.Pattern]] = [
    ("unsuccessful_put", re.compile(r"Unsuccessful\s+PUT", re.IGNORECASE)),
    ("unsuccessful_get", re.compile(r"Unsuccessful\s+GET", re.IGNORECASE)),
    ("permission_denied", re.compile(r"Permission\s+denied", re.IGNORECASE)),
    ("not_connected", re.compile(r"\bNot\s+connected\b", re.IGNORECASE)),
    ("couldnt_remote_list", re.compile(r"Couldn'?t\s+get\s+remote\s+file\s+list", re.IGNORECASE)),
    ("cant_create", re.compile(r"Can't\s+create\s+file|Cannot\s+create\s+a\s+file", re.IGNORECASE)),
    ("addr_in_use", re.compile(r"Address\s+already\s+in\s+use", re.IGNORECASE)),
    ("connection_closed_foreignhost", re.compile(r"connection\s+is\s+closed\s+by\s+foreignhost", re.IGNORECASE)),
    ("https_curl_error", re.compile(r"ERR-HTTPS-ERROR|curl\s+error", re.IGNORECASE)),
]

# Hostnames / domains
HOST_PAT_APP = re.compile(r"\b[a-z][a-z0-9_-]*app[0-9]{3,4}\b", re.IGNORECASE)  # rmlaweapp0001, rmubbpapp0002
HOST_PAT_FQDN = re.compile(r"\b[A-Za-z0-9.-]+\.(?:royalmailgroup\.net)\b", re.IGNORECASE)  # epcrmg2089.rmgp.royalmailgroup.net

# Stuck files context (Number of stuck files ... : - N)
STUCK_MIN_PAT   = re.compile(r"\bfrom\s+(?P<mins>\d+)\s+min\b", re.IGNORECASE)
STUCK_COUNT_PAT = re.compile(r":-\s*(?P<count>\d+)\b")

# FMS error class line
CLASS_PAT = re.compile(r"\bClass:\s*(?P<class>FMS_[A-Z_]+)")

# -----------------------
# Helpers
# -----------------------

def all_matches(pat: re.Pattern, s: str) -> List[str]:
    return list(dict.fromkeys(pat.findall(s or "")))

def parse_timestamp(text: str) -> Optional[str]:
    s = text or ""
    m = TS_FIRED_PAT.search(s)
    if m:
        return m.group("ts").strip()
    m = TS_ISO_PAT.search(s)
    if m:
        try:
            ts = pd.to_datetime(m.group(0), utc=True, errors="coerce")
            if pd.notna(ts):
                return ts.isoformat()
        except Exception:
            pass
    m = TS_SN_PAT.search(s)
    if m:
        try:
            dt = datetime.strptime(m.group(0), "%a %b %d %H:%M:%S GMT %Y")
            return dt.isoformat() + "Z"
        except Exception:
            pass
    return None

def derive_error_family(text: str) -> str:
    m = ERR_FAMILY_PAT.search(text or "")
    fam = m.group(0) if m else ""
    return normalize_family(fam)

def extract_triplet(text: str) -> Tuple[str, str, str]:
    m = TRIPLET_PAT.search(text or "")
    if not m:
        return ("", "", "")
    return (m.group("src"), m.group("mid"), m.group("dst"))

def extract_ftp_code_message(text: str) -> Tuple[str, str]:
    m = FTP_CODE_MSG_PAT.search(text or "")
    if not m:
        return ("", "")
    code = m.group("code") or ""
    msg = (m.group("msg") or "").strip()
    msg = re.sub(r"[ .;:]+$", "", msg)
    return (code, msg)

def extract_operation_tags(text: str) -> Tuple[str, str, List[str]]:
    ops: List[str] = []
    operation = ""
    success = ""
    for tag, pat in OP_TAGS:
        if pat.search(text or ""):
            ops.append(tag)
    if any(t in ops for t in ("unsuccessful_put", "unsuccessful_get", "permission_denied", "not_connected",
                              "couldnt_remote_list", "cant_create", "connection_closed_foreignhost", "https_curl_error")):
        success = "NO"
    else:
        success = ""
    if "unsuccessful_put" in ops:
        operation = "PUT"
    elif "unsuccessful_get" in ops:
        operation = "GET"
    else:
        if re.search(r"\bPUT\b", text or "", re.IGNORECASE):
            operation = "PUT"
        elif re.search(r"\bGET\b", text or "", re.IGNORECASE):
            operation = "GET"
        else:
            operation = ""
    return (operation, success, ops)

def extract_paths(text: str, allow_non_abs: bool) -> Tuple[List[str], List[str]]:
    s = text or ""
    paths = all_matches(UNIX_PATH_PAT, s) + all_matches(WIN_DRIVE_PAT, s)
    paths = [p if isinstance(p, str) else p[0] for p in paths]
    norm_paths: List[str] = []
    for p in paths:
        x = p.strip().rstrip(" .;:)")
        x = re.sub(r"/{2,}", "/", x)
        if x:
            norm_paths.append(x)
    if not norm_paths and allow_non_abs:
        toks = re.findall(r"[A-Za-z0-9_./\\:-]+", s)
        norm_paths = [t for t in toks if len(t) > 3]
    return (list(dict.fromkeys(norm_paths)), all_matches(FILE_PATTERN_PAT, s))

def extract_hosts(text: str) -> List[str]:
    hosts = [h.lower() for h in all_matches(HOST_PAT_APP, text or "")]
    hosts += [h.lower() for h in all_matches(HOST_PAT_FQDN, text or "")]
    return list(dict.fromkeys(hosts))

def extract_stuck(text: str) -> Tuple[str, str]:
    mins = ""
    cnt = ""
    m1 = STUCK_MIN_PAT.search(text or "")
    if m1:
        mins = m1.group("mins")
    m2 = STUCK_COUNT_PAT.search(text or "")
    if m2:
        cnt = m2.group("count")
    return mins, cnt

def extract_class(text: str) -> str:
    m = CLASS_PAT.search(text or "")
    return m.group("class") if m else ""

def join_nonempty(arr: List[str]) -> str:
    return "; ".join([x for x in arr if str(x).strip()])

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Make sure columns we reference exist
    for c in ["Description", "Short description"]:
        if c not in df.columns:
            df[c] = np.nan
    for c in ["Number", "Status", "Severity", "Assignment group", "Assigned to", "Opened", "Configuration item"]:
        if c not in df.columns:
            df[c] = np.nan
    return df

# -----------------------
# Core extraction per row
# -----------------------

def extract_from_description(row: pd.Series, allow_non_abs: bool) -> Dict[str, str]:
    # Prefer 'Description'; fall back to 'Short description' when blank
    desc = str(row.get("Description", "") or "")
    if not desc.strip():
        desc = str(row.get("Short description", "") or "")

    err_family = derive_error_family(desc)
    src_if, mid_code, dst_if = extract_triplet(desc)
    iso_ts = parse_timestamp(desc)

    ftp_code, ftp_msg = extract_ftp_code_message(desc)
    op, success, ops = extract_operation_tags(desc)

    paths, filepats = extract_paths(desc, allow_non_abs=allow_non_abs)
    hosts = extract_hosts(desc)
    stuck_mins, stuck_cnt = extract_stuck(desc)
    fms_class = extract_class(desc)

    return {
        "error_family_extracted": err_family,
        "src_interface": src_if,
        "route_or_mid": mid_code,
        "dst_interface": dst_if,
        "extracted_timestamp": iso_ts or "",
        "operation": op,
        "operation_success": success,
        "operation_tags": join_nonempty(ops),
        "ftp_code": ftp_code,
        "ftp_message": ftp_msg,
        "hosts": join_nonempty(hosts),
        "paths": join_nonempty(paths),
        "file_patterns": join_nonempty(filepats),
        "stuck_minutes": stuck_mins,
        "stuck_count": stuck_cnt,
        "fms_err_class": fms_class,
    }

# -----------------------
# IO + Driver
# -----------------------

def load_sheet(xf: pd.ExcelFile, sheet: str, verbose: int = 0) -> pd.DataFrame:
    df = pd.read_excel(xf, sheet_name=sheet)
    df = ensure_columns(df)
    if verbose:
        print(f"[INFO] Loaded sheet '{sheet}' rows={len(df):,}")
    return df

def process_dataframe(df: pd.DataFrame, allow_non_abs: bool, append_only: bool, verbose: int = 0) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    ext_rows: List[Dict[str, str]] = []
    for i, r in df.iterrows():
        try:
            ext_rows.append(extract_from_description(r, allow_non_abs=allow_non_abs))
        except Exception as e:
            ext_rows.append({
                "error_family_extracted": "",
                "src_interface": "",
                "route_or_mid": "",
                "dst_interface": "",
                "extracted_timestamp": "",
                "operation": "",
                "operation_success": "",
                "operation_tags": "",
                "ftp_code": "",
                "ftp_message": "",
                "hosts": "",
                "paths": "",
                "file_patterns": "",
                "stuck_minutes": "",
                "stuck_count": "",
                "fms_err_class": "",
            })
            if verbose:
                print(f"[WARN] row {i} parse error: {e}")

    ext = pd.DataFrame(ext_rows)

    enriched = ext if append_only else pd.concat([df.reset_index(drop=True), ext.reset_index(drop=True)], axis=1)

    # Summaries
    fam_counts = (
        ext["error_family_extracted"].fillna("")
        .replace("", np.nan)
        .dropna()
        .value_counts()
        .rename_axis("error_family")
        .reset_index(name="count")
    )

    triplet_series = (
        ext.apply(lambda r: f"{r['src_interface']}-{r['route_or_mid']}-{r['dst_interface']}"
                  if r['src_interface'] or r['dst_interface'] else "", axis=1)
        .replace("", np.nan)
        .dropna()
    )
    triplet_counts = triplet_series.value_counts().rename_axis("triplet").reset_index(name="count")

    summary = {
        "fam_counts": fam_counts,
        "triplet_counts": triplet_counts,
    }
    return enriched, summary

def write_outputs(out_dir: Path, enriched: pd.DataFrame, summary: Dict[str, pd.DataFrame], verbose: int = 0) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    out_enriched = out_dir / "desc_enriched.csv"
    enriched.to_csv(out_enriched, index=False, encoding="utf-8")
    if verbose:
        print(f"[OK] written: {out_enriched} (rows={len(enriched):,})")

    xlsx_path = out_dir / "desc_summary.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
        summary["fam_counts"].to_excel(xw, sheet_name="by_error_family", index=False)
        summary["triplet_counts"].to_excel(xw, sheet_name="by_triplet", index=False)

    summary["fam_counts"].to_csv(out_dir / "by_error_family.csv", index=False, encoding="utf-8")
    summary["triplet_counts"].to_csv(out_dir / "by_triplet.csv", index=False, encoding="utf-8")
    if verbose:
        print(f"[OK] summary written: {xlsx_path}")

def main() -> int:
    ap = argparse.ArgumentParser(description="Extract structured signals from 'Description' across Excel sheets.")
    ap.add_argument("--input", required=True, help="Path to .xlsx/.csv (e.g., incident (1).xlsx)")
    ap.add_argument("--all-sheets", action="store_true", help="Process and combine all sheets")
    ap.add_argument("--per-sheet", action="store_true", help="Process all sheets and write outputs per sheet")
    ap.add_argument("--out-dir", default="unique_outputs", help="Output directory")
    ap.add_argument("--append-only", action="store_true", help="Write only extracted fields")
    ap.add_argument("--allow-non-absolute", action="store_true", help="Looser path fallback tokens")
    ap.add_argument("-v", action="count", default=0, help="Verbose logging (-v, -vv)")
    args = ap.parse_args()

    in_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # CSV path: single-sheet semantics
    if in_path.suffix.lower() == ".csv":
        try:
            df = pd.read_csv(in_path)
        except Exception as e:
            print(f"[ERROR] load '{in_path}': {e}")
            return 1
        df = ensure_columns(df)
        enriched, summary = process_dataframe(df, args.allow_non_absolute, args.append_only, verbose=args.v)
        write_outputs(out_dir, enriched, summary, verbose=args.v)
        return 0

    # Excel path: support multiple sheets
    try:
        xf = pd.ExcelFile(in_path, engine="openpyxl")
    except Exception as e:
        print(f"[ERROR] load '{in_path}': {e}")
        return 1

    sheet_names = xf.sheet_names
    if args.v:
        print(f"[INFO] Sheets found: {sheet_names}")

    # Per-sheet mode
    if args.per_sheet:
        for s in sheet_names:
            df = load_sheet(xf, s, verbose=args.v)
            enriched, summary = process_dataframe(df, args.allow_non_absolute, args.append_only, verbose=args.v)
            write_outputs(out_dir / s, enriched, summary, verbose=args.v)
        return 0

    # All-sheets combine mode (default for Excel if --per-sheet not used)
    enriched_list: List[pd.DataFrame] = []
    fam_all: List[pd.DataFrame] = []
    trip_all: List[pd.DataFrame] = []

    for s in sheet_names:
        df = load_sheet(xf, s, verbose=args.v)
        enriched, summary = process_dataframe(df, args.allow_non_absolute, args.append_only, verbose=args.v)
        enriched.insert(0, "source_sheet", s)  # tag origin
        enriched_list.append(enriched)

        tmp_fam = summary["fam_counts"].copy()
        tmp_fam["source_sheet"] = s
        fam_all.append(tmp_fam)

        tmp_trip = summary["triplet_counts"].copy()
        tmp_trip["source_sheet"] = s
        trip_all.append(tmp_trip)

    combined_enriched = pd.concat(enriched_list, axis=0, ignore_index=True)

    fam_concat = pd.concat(fam_all, axis=0, ignore_index=True)
    fam_global = fam_concat.groupby("error_family", as_index=False)["count"].sum().sort_values("count", ascending=False)

    trip_concat = pd.concat(trip_all, axis=0, ignore_index=True)
    trip_global = trip_concat.groupby("triplet", as_index=False)["count"].sum().sort_values("count", ascending=False)

    summary_global = {
        "fam_counts": fam_global,
        "triplet_counts": trip_global,
    }
    write_outputs(out_dir, combined_enriched, summary_global, verbose=args.v)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
