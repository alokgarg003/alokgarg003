
#!/usr/bin/env python3
r"""
Extract Unique Filesystem Paths with Counts & Context
-----------------------------------------------------
Generates ONE CSV: unique_paths_all.csv
Each row = one unique path with:
  - incident_count               : distinct incident count referencing the path
  - configuration_items          : unique configuration items (joined by '; ')
  - error_families               : unique error families (joined by '; ')
  - top_primary_interfaces       : top 5 interfaces with counts (e.g., "E000123 (12); I200456 (9)")

Input columns (best-effort; some may be missing or named slightly differently):
  Number, paths, Short description, Configuration item, error_family, primary_interface
  Optionally: Resolution notes, Description, Comments

Path extraction strategy:
  1) Prefer 'paths' column (split by comma/semicolon/newline).
  2) Otherwise derive tokens from available text columns using regex:
     - Unix-like absolute paths:  (/[-A-Za-z0-9_./]+)
     - Windows absolute paths:    ([A-Za-z]:\\[^\\s:;,)]+)

De-duplication rule:
  - Each (incident Number, path) contributes at most once to incident_count.

Usage (Windows PowerShell from project root):
  .\.venv\Scripts\Activate.ps1
  pip install pandas openpyxl
  python .\extract_unique_paths.py --input .\ALL_stuck-files.xlsx --out-dir .\unique_outputs

Common case for partitioned files (sheet 'data'):
  python .\extract_unique_paths.py --input .\outputs_partitioned\run_YYYY-MM-DD_HH-MM-SS\stuck-files\ALL_stuck-files.xlsx --sheet data --out-dir .\unique_outputs

Optional flags:
  --sheet <name>           # explicit Excel sheet name
  --derive-family          # derive error_family from text tokens (best-effort)
  --allow-non-absolute     # include tokens that don't start with '/', useful if sources have relative paths
  -v / -vv                 # verbose logging

"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
from typing import Optional, List, Dict, Iterable

import numpy as np
import pandas as pd

# ---------- Regexes ----------
# Unix-like absolute paths
UNIX_PATH_PAT = re.compile(r"(/[-A-Za-z0-9_./]+)")
# Windows absolute paths (e.g., C:\dir\file.ext)
WIN_PATH_PAT = re.compile(r"([A-Za-z]:\\[^\s:;,)]+)")
# Delimiters for 'paths' column splitting
SEPS = re.compile(r"[,;\n]+")

# Optional error_family derivation
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

# ---------- Utilities ----------
def normalize_family(token: Optional[str]) -> str:
    if not token:
        return ""
    key = token.lower().replace("-", " ").replace("_", " ").strip()
    key = re.sub(r"\s+", " ", key).replace(" ", "-")
    return FAMILY_NORMALIZE.get(key, token)

def derive_error_family(text: str) -> str:
    s = str(text or "")
    m = ERR_FAMILY_PAT.search(s)
    fam = m.group(1) if m else ""
    return normalize_family(fam)

def normalize_path(p: str) -> str:
    s = str(p or "").strip()
    s = s.rstrip(" .;:\t)")
    s = re.sub(r"/{2,}", "/", s)  # collapse repeated slashes
    return s

def split_paths_cell(val: str) -> List[str]:
    if not val:
        return []
    return [normalize_path(x) for x in SEPS.split(val) if normalize_path(x)]

def safe_get(df: pd.DataFrame, name_candidates: Iterable[str]) -> Optional[str]:
    """Return the actual column name in df that matches any candidate ignoring case/space."""
    norm = {c: re.sub(r"\s+", "", str(c)).casefold() for c in df.columns}
    targets = [re.sub(r"\s+", "", n).casefold() for n in name_candidates]
    for c, nc in norm.items():
        if nc in targets:
            return c
    return None

# ---------- IO ----------
def load_table(path: Path, sheet: Optional[str], verbose: int = 0) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xls"):
        # Prefer explicit sheet; otherwise pick 'data' or first sheet
        xf = pd.ExcelFile(path, engine="openpyxl")
        chosen = sheet
        if chosen is None or chosen not in xf.sheet_names:
            if "data" in xf.sheet_names:
                chosen = "data"
                if verbose:
                    print(f"[INFO] Using sheet 'data'.")
            else:
                chosen = xf.sheet_names[0]
                if verbose:
                    print(f"[INFO] Using first sheet '{chosen}'.")
        df = pd.read_excel(xf, sheet_name=chosen)
        return df
    # CSV
    return pd.read_csv(path)

# ---------- Core ----------
def build_unique_paths(df: pd.DataFrame, derive_family_flag: bool, allow_non_abs: bool, verbose: int) -> pd.DataFrame:
    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    # Map commonly used columns
    col_number = safe_get(df, ["Number"])
    col_paths = safe_get(df, ["paths", "path"])
    col_short = safe_get(df, ["Short description", "Short Description", "shortdescription"])
    col_res_notes = safe_get(df, ["Resolution notes", "resolutionnotes"])
    col_desc = safe_get(df, ["Description", "description"])
    col_comments = safe_get(df, ["Comments", "comments"])
    col_ci = safe_get(df, ["Configuration item", "Configuration Item", "configurationitem"])
    col_family = safe_get(df, ["error_family", "Error family", "errorfamily"])
    col_iface = safe_get(df, ["primary_interface", "Primary interface", "primaryinterface"])

    # Ensure missing columns exist
    for c in (col_number, col_paths, col_short, col_res_notes, col_desc, col_comments, col_ci, col_family, col_iface):
        if c is None:
            # create a placeholder column if needed
            placeholder = {
                col_number: "Number",
                col_paths: "paths",
                col_short: "Short description",
                col_res_notes: "Resolution notes",
                col_desc: "Description",
                col_comments: "Comments",
                col_ci: "Configuration item",
                col_family: "error_family",
                col_iface: "primary_interface",
            }
            # choose first missing key name from dict where value matches None
    # Manually ensure the required columns exist
    for required in ["Number", "paths", "Short description", "Resolution notes", "Description", "Comments",
                     "Configuration item", "error_family", "primary_interface"]:
        if required not in df.columns:
            df[required] = np.nan

    # Optional: derive error_family
    if derive_family_flag:
        fam_series = df["error_family"].astype(str).str.strip()
        need = fam_series.eq("").fillna(True)
        if need.any():
            # Build source text by concatenating available text columns
            text_cols = [c for c in ["Short description", "Resolution notes", "Description", "Comments"] if c in df.columns]
            if text_cols:
                joined = df[text_cols].astype(str).apply(lambda r: " ".join(r), axis=1)
                derived = joined.apply(derive_error_family)
                df.loc[need, "error_family"] = derived

    # Explode into (incident, path)
    rows = []
    total_rows = len(df)
    incidents_with_paths = 0

    for idx, r in df.iterrows():
        # Incident ID key
        inc_id = r["Number"] if "Number" in df.columns else idx

        # Prefer explicit 'paths'
        parts: List[str] = []
        if col_paths and pd.notna(r[col_paths]) and str(r[col_paths]).strip():
            parts = split_paths_cell(str(r[col_paths]))
        else:
            # Build source text: Short description + Resolution notes + Description + Comments
            texts = []
            for c in ["Short description", "Resolution notes", "Description", "Comments"]:
                val = r.get(c)
                if pd.notna(val) and str(val).strip():
                    texts.append(str(val))
            source = " ".join(texts)

            # Extract Unix + Windows paths
            parts = [normalize_path(m) for m in UNIX_PATH_PAT.findall(source)]
            parts += [normalize_path(m) for m in WIN_PATH_PAT.findall(source)]

            # As a fallback, if allow_non_abs is set and nothing found, split words/tokens and keep plausible tokens
            if allow_non_abs and not parts:
                tokens = re.findall(r"[A-Za-z0-9_./\\-]+", source)
                parts = [normalize_path(t) for t in tokens if t]

        # Filter to absolute if required
        if not allow_non_abs:
            parts = [p for p in parts if p.startswith("/") or re.match(r"^[A-Za-z]:\\", p)]

        # Normalize + de-duplicate within the incident
        parts = [p for p in parts if p]
        parts = list(dict.fromkeys(parts))

        if parts:
            incidents_with_paths += 1

        for p in parts:
            rows.append({
                "incident": inc_id,
                "path": p,
                "Configuration item": r.get("Configuration item"),
                "error_family": r.get("error_family"),
                "primary_interface": r.get("primary_interface"),
            })

    if verbose:
        print(f"[INFO] Scanned rows: {total_rows}")
        print(f"[INFO] Incidents with at least one path: {incidents_with_paths}")
        print(f"[INFO] Raw exploded rows: {len(rows)}")

    exp = pd.DataFrame(rows)
    if exp.empty:
        raise ValueError("No paths could be extracted. Try --sheet data (for partitioned outputs) or --allow-non-absolute.")

    # De-duplicate across incidents per path
    exp = exp.drop_duplicates(subset=["incident", "path"])

    # Aggregations
    counts = exp.groupby("path")["incident"].nunique().reset_index(name="incident_count")
    cfg = (exp.groupby("path")["Configuration item"]
           .apply(lambda s: "; ".join(sorted({str(x) for x in s.dropna().astype(str) if str(x).strip()})))
           .reset_index(name="configuration_items"))
    fam = (exp.groupby("path")["error_family"]
           .apply(lambda s: "; ".join(sorted({str(x) for x in s.dropna().astype(str) if str(x).strip()})))
           .reset_index(name="error_families"))
    iface = (exp.groupby("path")["primary_interface"]
             .apply(lambda s: "; ".join([f"{k} ({v})" for k, v in s.dropna().astype(str).value_counts().head(5).items()]))
             .reset_index(name="top_primary_interfaces"))

    out = (counts.merge(cfg, on="path", how="left")
                 .merge(fam, on="path", how="left")
                 .merge(iface, on="path", how="left")
                 .sort_values(["incident_count", "path"], ascending=[False, True]))

    if verbose:
        print(f"[INFO] Unique paths: {len(out)}")
        if len(out):
            print("[INFO] Top sample:")
            print(out.head(10).to_string(index=False))

    return out

def main() -> int:
    ap = argparse.ArgumentParser(description="Generate unique_paths_all.csv from incidents data.")
    ap.add_argument("--input", required=True, help="Path to .xlsx/.csv file (e.g., ALL_stuck-files.xlsx)")
    ap.add_argument("--sheet", default=None, help="Excel sheet name (e.g., 'data'); auto-picks if None")
    ap.add_argument("--out-dir", default="unique_outputs", help="Folder to write unique_paths_all.csv")
    ap.add_argument("--derive-family", action="store_true", help="Derive error_family from available text fields where missing")
    ap.add_argument("--allow-non-absolute", action="store_true", help="Include non-absolute tokens if no absolute paths found")
    ap.add_argument("-v", action="count", default=0, help="Verbose logging (-v, -vv)")
    args = ap.parse_args()

    in_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "unique_paths_all.csv"

    try:
        df = load_table(in_path, args.sheet, verbose=args.v)
    except Exception as e:
        print(f"[ERROR] Failed to load '{in_path}': {e}")
        return 1

    try:
        result = build_unique_paths(
            df,
            derive_family_flag=args.derive_family,
            allow_non_abs=args.allow_non_absolute,
            verbose=args.v,
        )
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1

    try:
        result.to_csv(out_csv, index=False, encoding="utf-8")
    except Exception as e:
        print(f"[ERROR] Failed to write '{out_csv}': {e}")
        return 1

    print(f"[OK] Written: {out_csv} (rows={len(result):,})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
