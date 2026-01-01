
#!/usr/bin/env python3
r"""
Extract structured signals from ServiceNow "Short description" (and related text)

Outputs (depending on flags):
  Combine mode (--all-sheets):
    <out-dir>/shortdesc_enriched.csv        # all sheets combined + extracted fields
    <out-dir>/shortdesc_summary.xlsx        # rollups by error family & triplet
    <out-dir>/by_error_family.csv
    <out-dir>/by_triplet.csv

  Per-sheet mode (--per-sheet):
    <out-dir>/<sheet>/shortdesc_enriched.csv
    <out-dir>/<sheet>/shortdesc_summary.xlsx
    <out-dir>/<sheet>/by_error_family.csv
    <out-dir>/<sheet>/by_triplet.csv

Single-sheet mode (default, or with --sheet):
    same as combine mode but for one chosen sheet

Usage (Windows PowerShell from project root):
  python .\extract_shortdesc_features.py --input .\snow_enriched.xlsx --out-dir .\unique_outputs -v

Optional flags:
  --sheet data               # process only this sheet (fallback: 'data' or first)
  --all-sheets               # process and combine all sheets into one output set
  --per-sheet                # process all sheets and write outputs per sheet
  --append-only              # write only extracted fields (no original columns)
  --allow-non-absolute       # include non-absolute tokens in fallback extraction
  -v / -vv                   # verbose logging

Notes:
  - Requires: pandas, numpy, openpyxl.
  - Tolerant to missing columns; prefers 'Short description' but will also look into
    'Resolution notes', 'Description', 'Comments' when present.
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
    r"FMS-ERR-FILE-REMOVE|"
    r"FMS-DIR-CREATE|"
    r"MFTERRGEN|"
    r"PROCESS-ALERT|"
    r"CALLOUT[_-]?MFTERR0?2|"
    r"CALLOUT[_-]?MFTERR0?3|"
    r"CALLOUT[_-]?MFTERR0?4|"
    r"CALLOUT-FTG|FTG-ALERT|"
    r"MFTERRGEN|WARNING|"
    r"Unhandled\s+Exception\s+Error|"
    r"ERR-HTTPS-ERROR|ERR-FTP-COPY"
    r")\b",
    re.IGNORECASE,
)

FAMILY_NORMALIZE = {
    "ftp error": "FMS-FTP-ERROR",
    "cal lout_mfterr03": "CALLOUT_MFTERR03",
}
def normalize_family(token: Optional[str]) -> str:
    if not token:
        return ""
    t = token.strip()
    key = re.sub(r"[\s_]+", " ", t.lower())
    key = key.replace(" ", "-")
    return FAMILY_NORMALIZE.get(key, t.replace("_", "-").upper())

TRIPLET_PAT = re.compile(
    r"\b(?P<src>(?:[EIU]\d{6,}|MULTIPLE|INV))-(?P<mid>[A-Za-z0-9]+)-(?P<dst>(?:[EIU]\d{6,}|MULTIPLE|INV))\b"
)

TS_ISO_PAT = re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z)?\b")
TS_SN_PAT = re.compile(
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+GMT\s+\d{4}\b"
)

UNIX_PATH_PAT = re.compile(r"(/[-A-Za-z0-9_./]+)")
WIN_PATH_PAT  = re.compile(r"([A-Za-z]:\\[^\s:;,)]+)")

HOST_PAT = re.compile(r"\b[a-z][a-z0-9_-]*app[0-9]{3,4}\b", re.IGNORECASE)
FILE_PATTERN_PAT = re.compile(r"\b[^\s/:]+?\*[^ \t:/\\]*\b")
FTP_CODE_MSG_PAT = re.compile(r"\b(?P<code>[45]\d{2})\b(?:\s+(?P<msg>[^.]+))?")

OP_TAGS: List[Tuple[str, re.Pattern]] = [
    ("unsuccessful_put", re.compile(r"Unsuccessful\s+PUT", re.IGNORECASE)),
    ("unsuccessful_get", re.compile(r"Unsuccessful\s+GET", re.IGNORECASE)),
    ("permission_denied", re.compile(r"Permission\s+denied", re.IGNORECASE)),
    ("not_connected", re.compile(r"\bNot\s+connected\b", re.IGNORECASE)),
    ("couldnt_remote_list", re.compile(r"Couldn'?t\s+get\s+remote\s+file\s+list", re.IGNORECASE)),
    ("cannot_create_exists", re.compile(r"Cannot\s+create\s+a\s+file\s+when\s+that\s+file\s+already\s+exists", re.IGNORECASE)),
    ("rnfr_failed", re.compile(r"\bRNFR\s+command\s+failed\b", re.IGNORECASE)),
    ("addr_in_use", re.compile(r"Address\s+already\s+in\s+use", re.IGNORECASE)),
    ("cannot_remove_file", re.compile(r"Cannot\s+remove\s+file", re.IGNORECASE)),
    ("cannot_move", re.compile(r"\bCannot\s+move\b", re.IGNORECASE)),
    ("dir_create_fail", re.compile(r"\bCannot\s+create\s+[^.]+", re.IGNORECASE)),
    ("config_missing", re.compile(r"Mandatory\s+item\s+missing\s+from\s+config\s+file", re.IGNORECASE)),
    ("config_empty", re.compile(r"Config\s+exists,\s+but\s+contains\s+no\s+config\s+lines", re.IGNORECASE)),
]

STUCK_MIN_PAT   = re.compile(r"\bfrom\s+(?P<mins>\d+)\s+min\b", re.IGNORECASE)
STUCK_COUNT_PAT = re.compile(r":-\s*(?P<count>\d+)\b")

# -----------------------
# Helpers
# -----------------------

def all_matches(pat: re.Pattern, s: str) -> List[str]:
    return list(dict.fromkeys(pat.findall(s)))

def parse_timestamp(text: str) -> Optional[str]:
    s = text or ""
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
    if any(t in ops for t in ("unsuccessful_put", "unsuccessful_get")):
        success = "NO"
    elif any(t in ops for t in ("permission_denied", "not_connected", "couldnt_remote_list", "rnfr_failed", "addr_in_use")):
        success = "NO"
    elif ops:
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
    paths = all_matches(UNIX_PATH_PAT, s) + all_matches(WIN_PATH_PAT, s)
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
    return [h.lower() for h in all_matches(HOST_PAT, text or "")]

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

def join_nonempty(arr: List[str]) -> str:
    return "; ".join([x for x in arr if str(x).strip()])

def ensure_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for required in ["Short description", "Resolution notes", "Description", "Comments"]:
        if required not in df.columns:
            df[required] = np.nan
    return df

# -----------------------
# Core extraction per row
# -----------------------

def extract_fields_from_row(row: pd.Series, allow_non_abs: bool) -> Dict[str, str]:
    texts: List[str] = []
    for col in ["Short description", "Resolution notes", "Description", "Comments"]:
        if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
            texts.append(str(row[col]))
    src_text = " ".join(texts)

    err_family = derive_error_family(src_text)
    src_if, mid_code, dst_if = extract_triplet(src_text)
    iso_ts = parse_timestamp(src_text)

    ftp_code, ftp_msg = extract_ftp_code_message(src_text)
    op, success, ops = extract_operation_tags(src_text)

    paths, filepats = extract_paths(src_text, allow_non_abs=allow_non_abs)
    hosts = extract_hosts(src_text)
    stuck_mins, stuck_cnt = extract_stuck(src_text)

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
    }

# -----------------------
# IO + Driver
# -----------------------

def load_sheet(xf: pd.ExcelFile, sheet: str, verbose: int = 0) -> pd.DataFrame:
    df = pd.read_excel(xf, sheet_name=sheet)
    if verbose:
        print(f"[INFO] Loaded sheet '{sheet}' rows={len(df):,}")
    return df

def process_dataframe(df: pd.DataFrame, allow_non_abs: bool, append_only: bool, verbose: int = 0) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = ensure_text_columns(df)

    if verbose:
        print(f"[INFO] Rows in: {len(df):,}")

    ext_rows: List[Dict[str, str]] = []
    for i, r in df.iterrows():
        try:
            ext_rows.append(extract_fields_from_row(r, allow_non_abs=allow_non_abs))
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
            })
            if verbose:
                print(f"[WARN] row {i} parse error: {e}")

    ext = pd.DataFrame(ext_rows)

    # Enriched table
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
    return enriched, pd.DataFrame(), summary  # second return kept for API symmetry

def write_outputs(out_dir: Path, enriched: pd.DataFrame, summary: Dict[str, pd.DataFrame], verbose: int = 0) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    out_enriched = out_dir / "shortdesc_enriched.csv"
    enriched.to_csv(out_enriched, index=False, encoding="utf-8")
    if verbose:
        print(f"[OK] written: {out_enriched} (rows={len(enriched):,})")

    # Excel summary (multi-sheet)
    xlsx_path = out_dir / "shortdesc_summary.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
        summary["fam_counts"].to_excel(xw, sheet_name="by_error_family", index=False)
        summary["triplet_counts"].to_excel(xw, sheet_name="by_triplet", index=False)

    # CSV variants
    summary["fam_counts"].to_csv(out_dir / "by_error_family.csv", index=False, encoding="utf-8")
    summary["triplet_counts"].to_csv(out_dir / "by_triplet.csv", index=False, encoding="utf-8")
    if verbose:
        print(f"[OK] summary written: {xlsx_path}")

def main() -> int:
    ap = argparse.ArgumentParser(description="Extract structured signals from Short description.")
    ap.add_argument("--input", required=True, help="Path to .xlsx/.csv (e.g., snow_enriched.xlsx)")
    ap.add_argument("--sheet", default=None, help="Excel sheet name (optional)")
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

    # CSV path: treat as single-sheet
    if in_path.suffix.lower() == ".csv":
        try:
            df = pd.read_csv(in_path)
        except Exception as e:
            print(f"[ERROR] load '{in_path}': {e}")
            return 1
        enriched, _, summary = process_dataframe(df, args.allow_non_absolute, args.append_only, verbose=args.v)
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
            enriched, _, summary = process_dataframe(df, args.allow_non_absolute, args.append_only, verbose=args.v)
            write_outputs(out_dir / s, enriched, summary, verbose=args.v)
        return 0

    # All-sheets combine mode
    if args.all_sheets:
        enriched_list: List[pd.DataFrame] = []
        fam_all: List[pd.DataFrame] = []
        trip_all: List[pd.DataFrame] = []

        for s in sheet_names:
            df = load_sheet(xf, s, verbose=args.v)
            enriched, _, summary = process_dataframe(df, args.allow_non_absolute, args.append_only, verbose=args.v)
            # mark origin sheet
            enriched.insert(0, "source_sheet", s)
            enriched_list.append(enriched)

            tmp_fam = summary["fam_counts"].copy()
            tmp_fam["source_sheet"] = s
            fam_all.append(tmp_fam)

            tmp_trip = summary["triplet_counts"].copy()
            tmp_trip["source_sheet"] = s
            trip_all.append(tmp_trip)

        combined_enriched = pd.concat(enriched_list, axis=0, ignore_index=True)

        # global rollups (across all sheets)
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

    # Single-sheet default (explicit sheet or fallback: 'data' / first)
    chosen = args.sheet if args.sheet in sheet_names else ("data" if "data" in sheet_names else sheet_names[0])
    if args.v:
        print(f"[INFO] Using sheet '{chosen}'")
    df = load_sheet(xf, chosen, verbose=args.v)
    enriched, _, summary = process_dataframe(df, args.allow_non_absolute, args.append_only, verbose=args.v)
    write_outputs(out_dir, enriched, summary, verbose=args.v)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
