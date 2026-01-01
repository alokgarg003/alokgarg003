
#!/usr/bin/env python3
r"""
Short Description Extractor (Industry-grade)
-------------------------------------------
Rescans the 'Short description' column from a ServiceNow incident export and produces
an enriched Excel with clean, queryable fields, including interface_chain (clean/raw).

Usage (Windows PowerShell from project root):
  .venv\Scripts\Activate.ps1
  python .\src\snow_ai\shortdesc_extractor.py --input .\snow_last_2year_data.xlsx --sheet first --output .\outputs\shortdesc_extracted.xlsx
  # or, on an already-enriched dataset:
  python .\src\snow_ai\shortdesc_extractor.py --input .\snow_enriched.xlsx --sheet first --output .\outputs\shortdesc_extracted.xlsx

Options:
  --include-raw-chain   Include literal interface_chain_raw (e.g., "U000034-693-U000064")
                        alongside the cleaned chain ("U000034-U000064").
"""

import argparse
import re
from pathlib import Path
from typing import List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from datetime import datetime

# ====== Regexes & Dictionaries ======
# Valid interface IDs: E/I/U + 3-6 digits + optional trailing letter
ID_PAT = re.compile(r"\b([EIU][0-9]{3,6}[A-Z]?)\b")

# Canonical error families (capture, case-insensitive)
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

# Normalization map to canonical labels
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

# Error codes (e.g., U000064, E000095)
CODE_PAT = re.compile(r"\b([UEI][0-9]{3,6})\b")

# POSIX paths: /var..., /opt..., /data..., /mnt..., /etc..., /home...
PATH_PAT = re.compile(r"(/(?:var|opt|data|mnt|etc|home)[^\s:;,)]*)")

# Stuck metrics
STUCK_CNT_PAT = re.compile(r":\s*-\s*(\d+)\b")
STUCK_MIN_PAT = re.compile(r"from\s+(\d+)\s*min\b", re.IGNORECASE)

# FTP command and code
FTP_CMD_PAT = re.compile(r"\b(Unsuccessful)\s+(PUT|GET)\b", re.IGNORECASE)
FTP_CODE_550_PAT = re.compile(r"\b550\b")

# Fired time (ISO-like) e.g., 2025-12-22T17:03 (optional trailing Z)
ISO_TIME_PAT = re.compile(r"\b\d{4}-\d{2}-\d{2}T[0-9:.\-]+Z?\b")

# Natural language timestamps: Mon Dec 22 19:22:11 GMT 2025
NAT_TIME_PAT = re.compile(
    r"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+GMT\s+\d{4}\b"
)

PRIORITY_PREFIX = ["E", "I", "U"]


# ====== Helpers ======
def normalize_family(token: Optional[str]) -> str:
    """Map captured token to a canonical error family."""
    if not token:
        return "other"
    key = token.lower().replace("-", " ").replace("_", " ").strip()
    key = key.replace("  ", " ")
    key = key.replace(" ", "-")  # canonical hyphenated form
    return FAMILY_NORMALIZE.get(key, token if token else "other")


def pick_primary(ids: List[str]) -> Optional[str]:
    """Choose primary interface deterministically (E > I > U > first)."""
    if not ids:
        return np.nan
    for pref in PRIORITY_PREFIX:
        for t in ids:
            if t.startswith(pref):
                return t
    return ids[0]


def extract_interface_chain_clean(s: str, all_ids: List[str]) -> Optional[str]:
    """
    Preserve a contiguous hyphen-joined sequence of valid ID tokens as shown.
    If not found but multiple IDs exist, build the chain by hyphen-joining IDs in source order.
    """
    chain = None
    if "-" in s:
        parts = s.split("-")
        id_parts = [p for p in parts if ID_PAT.fullmatch(p)]
        if len(id_parts) >= 2:
            chain = "-".join(id_parts)
    if not chain and len(all_ids) >= 2:
        chain = "-".join(all_ids)
    return chain


def extract_interface_chain_raw(s: str) -> Optional[str]:
    """
    Extract a literal hyphen segment that appears to be a chain and contains >=2 ID tokens,
    even if a middle segment is not a valid ID (e.g., "U000034-693-U000064").
    """
    if "-" not in s:
        return None
    # Heuristic: find the longest token containing hyphens and >=2 ID-looking segments
    candidates = [tok for tok in s.split() if "-" in tok]
    best = None
    best_len = 0
    for cand in candidates:
        segs = cand.split("-")
        score = sum(1 for seg in segs if ID_PAT.fullmatch(seg))
        if score >= 2 and len(cand) > best_len:
            best = cand
            best_len = len(cand)
    return best


def parse_time_fields(s: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (fired_time_iso, fired_time_source) if parseable, else (None, raw-or-None)."""
    iso_m = ISO_TIME_PAT.search(s)
    if iso_m:
        iso_raw = iso_m.group(0)
        try:
            _ = datetime.fromisoformat(iso_raw.replace("Z", ""))
            return iso_raw, None
        except Exception:
            pass
    nat_m = NAT_TIME_PAT.search(s)
    if nat_m:
        raw = nat_m.group(0)
        try:
            dt = datetime.strptime(raw, "%a %b %d %H:%M:%S GMT %Y")
            return dt.isoformat(), raw
        except Exception:
            return None, raw
    return None, None


def classify_caller(caller: Optional[str]) -> str:
    c = str(caller or "").strip().lower()
    if not c:
        return "Other"
    if "netcool" in c:
        return "NetCool"
    if "int_mft api_prod" in c or "int-mft api_prod" in c or "int mft api_prod" in c:
        return "INT_MFT API_PROD"
    if "atos" in c:
        return "Atos Events"
    return "Other"


def find_col(df: pd.DataFrame, name_like: str) -> Optional[str]:
    """Find a column by case-insensitive, space-insensitive match."""
    target = name_like.casefold().replace(" ", "")
    for c in df.columns:
        if str(c).casefold().replace(" ", "") == target:
            return c
    return None


def parse_short_description(text: str, include_raw_chain: bool = False) -> dict:
    """Core extractor for a single Short description string."""
    s = str(text or "")
    all_ids = ID_PAT.findall(s)
    primary = pick_primary(all_ids)

    fams = ERR_FAMILY_PAT.findall(s)
    fam_norm = normalize_family(fams[0]) if fams else "other"

    codes = list(dict.fromkeys(CODE_PAT.findall(s)))
    paths = PATH_PAT.findall(s)
    cnt_m = STUCK_CNT_PAT.search(s)
    mins_m = STUCK_MIN_PAT.search(s)

    cmd_m = FTP_CMD_PAT.search(s)
    ftp_command = cmd_m.group(2).upper() if cmd_m else None
    ftp_code = "550" if FTP_CODE_550_PAT.search(s) else None

    fired_time_iso, fired_time_src = parse_time_fields(s)

    chain_clean = extract_interface_chain_clean(s, all_ids)
    chain_raw = extract_interface_chain_raw(s) if include_raw_chain else None

    # Script & host tags (heuristics)
    m_script = re.search(r"([A-Za-z0-9_\-]+\.sh)\b", s)
    script_name = m_script.group(1) if m_script else None

    m_host = re.search(r"\brml[a-z]+app[0-9]+\b", s)
    host_tag = m_host.group(0) if m_host else None

    warning_subject = None
    if fam_norm == "WARNING":
        m_warn = re.search(r"WARNING:\s*(.+)", s)
        if m_warn:
            warning_subject = m_warn.group(1)[:200]

    return {
        "interfaces": ",".join(all_ids) if all_ids else np.nan,
        "primary_interface": primary,
        "interface_count": len(all_ids),
        "interface_chain": chain_clean,
        "interface_chain_raw": chain_raw,
        "error_family": fam_norm,
        "error_codes": ",".join(codes) if codes else np.nan,
        "ftp_command": ftp_command,
        "ftp_code": ftp_code,
        "paths": ",".join(paths) if paths else np.nan,
        "has_path": bool(paths),
        "stuck_files_count": int(cnt_m.group(1)) if cnt_m else np.nan,
        "stuck_for_minutes": int(mins_m.group(1)) if mins_m else np.nan,
        "fired_time": fired_time_iso,
        "fired_time_source": fired_time_src,
        "script_name": script_name,
        "host_tag": host_tag,
        "warning_subject": warning_subject,
    }


# ====== Excel loading ======
def load_excel_single_sheet(path: Path, sheet: Optional[Union[str, int]]) -> pd.DataFrame:
    """
    Load a single sheet:
    - If sheet is None/'first'/'0' -> load first sheet by index = 0
    - If sheet is int -> load by index
    - If sheet is str -> validate name exists, then load by name
    """
    xl = pd.ExcelFile(path, engine="openpyxl")
    sheet_names = xl.sheet_names
    if sheet is None or (isinstance(sheet, str) and sheet.strip().lower() in ("", "first", "0")):
        return xl.parse(sheet_name=0)
    if isinstance(sheet, int):
        return xl.parse(sheet_name=sheet)
    # sheet is str
    name = sheet.strip()
    if name not in sheet_names:
        raise ValueError(f"Worksheet named '{name}' not found. Available: {sheet_names}")
    return xl.parse(sheet_name=name)


# ====== Main CLI ======
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Extract fields from 'Short description' and write a new Excel with 'extracted' and 'summary' sheets."
    )
    ap.add_argument("--input", required=True, help="Path to input Excel (raw ServiceNow or enriched).")
    ap.add_argument("--output", required=True, help="Path to output Excel to create.")
    ap.add_argument(
        "--sheet",
        default="first",
        help="Sheet name or index. Use 'first' (default) to load the first sheet."
    )
    ap.add_argument(
        "--include-raw-chain",
        action="store_true",
        help="Include interface_chain_raw (literal hyphen segment)."
    )
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        print(f"[ERROR] Input not found: {in_path}")
        return 1

    # Load one sheet robustly
    try:
        # Allow numeric index: e.g., --sheet 0
        sheet_arg: Optional[Union[str, int]]
        try:
            sheet_arg = int(args.sheet)  # if user passed "0", "1", etc.
        except ValueError:
            sheet_arg = args.sheet
        df = load_excel_single_sheet(in_path, sheet_arg)
    except Exception as e:
        print(f"[ERROR] Failed to read sheet: {e}")
        return 1

    # Locate columns
    sd_col = find_col(df, "Short description")
    if not sd_col:
        print("[ERROR] Could not find 'Short description' column in the selected sheet.")
        return 1

    caller_col = find_col(df, "Caller")

    # Parse each row
    extracted_rows = df[sd_col].apply(lambda x: parse_short_description(x, include_raw_chain=args.include_raw_chain))
    extracted = pd.DataFrame(extracted_rows.tolist())

    # Caller classification (optional)
    if caller_col:
        extracted["caller_class"] = df[caller_col].apply(classify_caller)
    else:
        extracted["caller_class"] = "Other"

    # Keep a few original columns if present
    keep_cols = []
    for want in ["Number", "Short description", "Caller", "Status", "Severity", "Assignment group", "Opened"]:
        col = find_col(df, want)
        if col:
            keep_cols.append(col)

    final_df = pd.concat([df[keep_cols], extracted], axis=1)

    # Build summary tables
    def top_counts(col_name: str, n: int = 30) -> pd.DataFrame:
        s = final_df[col_name].value_counts(dropna=True).head(n)
        return s.rename_axis(col_name).reset_index(name="count")

    top_ifaces = top_counts("primary_interface", n=30)
    top_errors = top_counts("error_family", n=30)

    total = len(final_df)
    stats = pd.DataFrame({
        "metric": [
            "rows_total",
            "pct_rows_with_primary_interface",
            "pct_rows_with_error_codes",
            "pct_rows_with_paths",
            "pct_rows_with_interface_chain",
        ],
        "value": [
            total,
            float(final_df["primary_interface"].notna().mean()) if total else 0.0,
            float(final_df["error_codes"].notna().mean()) if total else 0.0,
            float(final_df["has_path"].mean()) if total else 0.0,
            float(final_df["interface_chain"].notna().mean()) if total else 0.0,
        ],
    })

    # Write output Excel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        final_df.to_excel(w, sheet_name="extracted", index=False)
        top_ifaces.to_excel(w, sheet_name="summary", index=False, startrow=0)
        startrow = len(top_ifaces) + 3
        top_errors.to_excel(w, sheet_name="summary", index=False, startrow=startrow)
        startrow += len(top_errors) + 3
        stats.to_excel(w, sheet_name="summary", index=False, startrow=startrow)

    print(f"[OK] Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
