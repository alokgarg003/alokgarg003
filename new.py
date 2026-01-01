
#!/usr/bin/env python3
r"""
Extract Unique Filesystem Paths with Counts & Context (derives error_family from Description)
-------------------------------------------------------------------------------------------
Generates unique_paths_all.csv (combined) or per-sheet variants.

Each row = one unique path with:
  - incident_count               : distinct incident count referencing the path
  - configuration_items          : unique configuration items (joined by '; ')
  - error_families               : unique error families (joined by '; ')
  - top_primary_interfaces       : top 5 interfaces with counts (e.g., "E000123 (12); I200456 (9)")

Input columns (best-effort; some may be missing or named slightly differently):
  Number, paths, Short description, Description, Resolution notes, Comments,
  Configuration item, error_family, primary_interface

Key changes vs older script:
  - Reads error families from --error-family-file and derives error_family from 'Description' only by default.
  - Matching is flexible: hyphens/underscores/spaces are ignored for detection (e.g., "CALLOUT MFTERR 03" -> CALLOUT-MFTERR03).
  - Supports Unix, Windows drive (C:\...), and Windows UNC (\\server\share\...) paths.

Optional flags:
  --sheet <name>           # process only this sheet (fallback: 'data' or first)
  --all-sheets             # combine all sheets
  --per-sheet              # write outputs per sheet
  --error-family-file <path>  # text file: one canonical error family per line
  --derive-family             # derive error_family when missing (default True)
  --force-rederive            # overwrite any existing error_family with derived (use with care)
  --description-only          # derive using ONLY 'Description' (default True)
  --allow-non-absolute        # include tokens that don't start with '/', 'C:\', or '\\server\' if nothing absolute found
  --min-count <N>             # include only paths with incident_count >= N (default: 1)
  -v / -vv                    # verbose logging

"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
from typing import Optional, List, Dict, Iterable, Tuple, Set

import numpy as np
import pandas as pd

# ---------- Regexes for path extraction ----------
UNIX_PATH_PAT = re.compile(r"(/[-A-Za-z0-9_./]+)")
WIN_PATH_PAT  = re.compile(r"([A-Za-z]:\\[^\s:;,)]+)")
UNC_PATH_PAT  = re.compile(r"(\\\\[A-Za-z0-9._-]+\\[^\s:;,)]+)")
SEPS = re.compile(r"[,;\n]+")

# ---------- Utilities ----------
def normalize_path(p: str) -> str:
    s = str(p or "").strip()
    s = s.rstrip(" .;:\t)")
    s = re.sub(r"/{2,}", "/", s)  # collapse repeated slashes for Unix
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

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure canonical columns exist so downstream never crashes."""
    for required in [
        "Number", "paths", "Short description", "Resolution notes",
        "Description", "Comments", "Configuration item", "error_family",
        "primary_interface"
    ]:
        if required not in df.columns:
            df[required] = np.nan
    return df

# ---------- Error family lexicon & matching ----------
def load_error_family_lexicon(path: Optional[Path], verbose: int = 0) -> List[str]:
    """Load canonical error families from a text file (one per line)."""
    families: List[str] = []
    if path and path.exists():
        try:
            data = path.read_text(encoding="utf-8", errors="ignore")
            for line in data.splitlines():
                fam = line.strip()
                if fam:
                    families.append(fam)
            # de-duplicate preserving order
            seen: Set[str] = set()
            families = [f for f in families if not (f in seen or seen.add(f))]
            if verbose:
                print(f"[INFO] Loaded {len(families)} error families from '{path}'.")
        except Exception as e:
            print(f"[WARN] Failed to read error family file '{path}': {e}")
    return families

def _flex_key(s: str) -> str:
    """Make a flexible key by removing non-alphanumerics and lowercasing."""
    return re.sub(r"[^A-Za-z0-9]", "", str(s or "").lower())

def build_family_matchers(families: List[str]) -> List[Tuple[str, re.Pattern]]:
    """
    Build regex matchers for each canonical family.
    We match after normalizing the source text by removing non-alphanumerics.
    For canonical pattern, we allow variable _, -, or spaces between tokens.
    """
    matchers: List[Tuple[str, re.Pattern]] = []
    for fam in families:
        # Split canonical fam into alphanumeric chunks and allow flexible separators in regex
        tokens = re.findall(r"[A-Za-z0-9]+", fam)
        if not tokens:
            continue
        # e.g., ["CALLOUT", "MFTERR", "03"] -> r"(?:cal lout)[-_ ]*(?:mfterr)[-_ ]*(?:03)"
        pattern = r"".join([re.escape(t) + r"[^A-Za-z0-9]*" for t in tokens])
        # compile as case-insensitive; we'll apply on a normalized text (non-alnum removed) OR direct
        # To keep things robust, we match on the original text (case-insensitive) with flexible separators.
        regex = re.compile(pattern, re.IGNORECASE)
        matchers.append((fam, regex))
    return matchers

def derive_family_from_description(text: str, matchers: List[Tuple[str, re.Pattern]]) -> str:
    """
    Derive the first matching canonical family from Description.
    Priority = order in lexicon file (top-most wins); if multiple, earliest match wins.
    """
    s = str(text or "")
    if not s.strip():
        return ""
    best_fam = ""
    best_pos = None
    for fam, rx in matchers:
        m = rx.search(s)
        if m:
            pos = m.start()
            if best_pos is None or pos < best_pos:
                best_pos = pos
                best_fam = fam
    return best_fam

# ---------- IO ----------
def load_table(path: Path, sheet: Optional[str], verbose: int = 0) -> pd.DataFrame:
    """Single-sheet loader (kept for backward compatibility)."""
    if path.suffix.lower() in (".xlsx", ".xls"):
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
        return ensure_columns(df)
    # CSV
    return ensure_columns(pd.read_csv(path))

def load_excel_all_sheets(path: Path, verbose: int = 0) -> Dict[str, pd.DataFrame]:
    xf = pd.ExcelFile(path, engine="openpyxl")
    sheets = {}
    for s in xf.sheet_names:
        df = pd.read_excel(xf, sheet_name=s)
        sheets[s] = ensure_columns(df)
        if verbose:
            print(f"[INFO] Loaded sheet '{s}' rows={len(df):,}")
    return sheets

# ---------- Core ----------
def extract_paths_from_text(source: str, allow_non_abs: bool) -> List[str]:
    """Return normalized list of paths from mixed text."""
    parts: List[str] = []
    s = str(source or "")
    parts += [normalize_path(m) for m in UNIX_PATH_PAT.findall(s)]
    parts += [normalize_path(m) for m in WIN_PATH_PAT.findall(s)]
    parts += [normalize_path(m) for m in UNC_PATH_PAT.findall(s)]

    parts = [p for p in parts if p]
    parts = list(dict.fromkeys(parts))  # de-dupe

    if not parts and allow_non_abs:
        tokens = re.findall(r"[A-Za-z0-9_./\\-]+", s)
        parts = [normalize_path(t) for t in tokens if t]
    return parts

def build_unique_paths(
    df: pd.DataFrame,
    families: List[str],
    description_only: bool,
    derive_family_flag: bool,
    force_rederive: bool,
    allow_non_abs: bool,
    min_count: int,
    verbose: int
) -> pd.DataFrame:
    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    df = ensure_columns(df)

    # Column mappings
    col_number = safe_get(df, ["Number"])
    col_paths  = safe_get(df, ["paths", "path"])
    col_desc   = safe_get(df, ["Description", "description"])

    # Prepare matchers
    matchers = build_family_matchers(families) if families else []

    # Derive/overwrite error_family if requested
    if derive_family_flag:
        # Determine which rows need derivation
        if force_rederive:
            need = pd.Series([True] * len(df))
        else:
            fam_series = df["error_family"].astype(str).str.strip()
            need = fam_series.eq("").fillna(True)
        if need.any() and matchers:
            if description_only:
                to_use = ["Description"] if col_desc else []
            else:
                # fallback to all text cols
                to_use = [c for c in ["Short description", "Description", "Resolution notes", "Comments"] if c in df.columns]
            if to_use:
                joined = df[to_use].astype(str).apply(lambda r: " ".join(r), axis=1)
                derived = joined.apply(lambda s: derive_family_from_description(s, matchers))
                df.loc[need, "error_family"] = derived
                if verbose:
                    filled = int(derived.astype(bool).sum())
                    print(f"[INFO] Derived error_family for {filled} rows (description_only={description_only}).")

    # Explode into (incident, path)
    rows = []
    total_rows = len(df)
    incidents_with_paths = 0

    for idx, r in df.iterrows():
        inc_id = r[col_number] if col_number else idx

        # Prefer explicit 'paths' column
        parts: List[str] = []
        if col_paths and pd.notna(r[col_paths]) and str(r[col_paths]).strip():
            parts = split_paths_cell(str(r[col_paths]))
        else:
            # Build source text: Description only (or all text fields if not description_only)
            if description_only:
                source = str(r.get("Description") or "")
            else:
                texts = []
                for c in ["Short description", "Description", "Resolution notes", "Comments"]:
                    val = r.get(c)
                    if pd.notna(val) and str(val).strip():
                        texts.append(str(val))
                source = " ".join(texts)

            parts = extract_paths_from_text(source, allow_non_abs=allow_non_abs)

            # Filter to absolute if required
            if not allow_non_abs:
                parts = [
                    p for p in parts
                    if p.startswith("/")
                    or re.match(r"^[A-Za-z]:\\", p)
                    or p.startswith("\\\\")
                ]

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

    # Optional filter by min_count
    if min_count and min_count > 1:
        out = out[out["incident_count"] >= min_count]

    if verbose:
        print(f"[INFO] Unique paths (post-filter): {len(out)}")
        if len(out):
            print("[INFO] Top sample:")
            print(out.head(10).to_string(index=False))

    return out

def write_output_csv(out_csv: Path, df_out: pd.DataFrame) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[OK] Written: {out_csv} (rows={len(df_out):,})")

def main() -> int:
    ap = argparse.ArgumentParser(description="Generate unique_paths_all.csv from incidents data (Description-based error_family).")
    ap.add_argument("--input", required=True, help="Path to .xlsx/.csv file (e.g., ALL_stuck-files.xlsx)")
    ap.add_argument("--sheet", default=None, help="Excel sheet name (e.g., 'data'); auto-picks if None")
    ap.add_argument("--all-sheets", action="store_true", help="Process and combine all sheets")
    ap.add_argument("--per-sheet", action="store_true", help="Process all sheets and write outputs per sheet")
    ap.add_argument("--out-dir", default="unique_outputs", help="Folder to write outputs")
    ap.add_argument("--error-family-file", default=None, help="Text file containing canonical error families (one per line)")
    ap.add_argument("--derive-family", action="store_true", help="Derive error_family from Description where missing")
    ap.add_argument("--force-rederive", action="store_true", help="Overwrite existing error_family values with derived ones")
    ap.add_argument("--description-only", action="store_true", help="Use ONLY 'Description' for path & family derivation (default)")
    ap.add_argument("--allow-non-absolute", action="store_true", help="Include non-absolute tokens if no absolute paths found")
    ap.add_argument("--min-count", type=int, default=1, help="Minimum incident_count to include (default 1)")
    ap.add_argument("-v", action="count", default=0, help="Verbose logging (-v, -vv)")
    args = ap.parse_args()

    in_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load family lexicon (optional but recommended)
    fam_path = Path(args.error_family_file).resolve() if args.error_family_file else None
    families = load_error_family_lexicon(fam_path, verbose=args.v)

    # Default description-only mode ON unless user disables by not passing the flag
    description_only = True if args.description_only else True  # default ON

    try:
        if in_path.suffix.lower() == ".csv":
            df = load_table(in_path, args.sheet, verbose=args.v)
            result = build_unique_paths(
                df,
                families=families,
                description_only=description_only,
                derive_family_flag=args.derive_family or True,  # default derive on
                force_rederive=args.force_rederive,
                allow_non_abs=args.allow_non_absolute,
                min_count=args.min_count,
                verbose=args.v,
            )
            write_output_csv(out_dir / "unique_paths_all.csv", result)
            return 0

        # Excel input
        if args.per_sheet:
            sheets = load_excel_all_sheets(in_path, verbose=args.v)
            for sname, sdf in sheets.items():
                try:
                    res = build_unique_paths(
                        sdf,
                        families=families,
                        description_only=description_only,
                        derive_family_flag=args.derive_family or True,
                        force_rederive=args.force_rederive,
                        allow_non_abs=args.allow_non_absolute,
                        min_count=args.min_count,
                        verbose=args.v,
                    )
                except ValueError as e:
                    print(f"[WARN] Sheet '{sname}' skipped: {e}")
                    continue
                write_output_csv(out_dir / sname / "unique_paths_all.csv", res)
            return 0

        if args.all_sheets or (not args.sheet):
            sheets = load_excel_all_sheets(in_path, verbose=args.v)
            combined_df = pd.concat(list(sheets.values()), axis=0, ignore_index=True) if sheets else pd.DataFrame()
            if combined_df.empty:
                print(f"[ERROR] No sheets found or empty workbook: {in_path}")
                return 1
            result = build_unique_paths(
                combined_df,
                families=families,
                description_only=description_only,
                derive_family_flag=args.derive_family or True,
                force_rederive=args.force_rederive,
                allow_non_abs=args.allow_non_absolute,
                min_count=args.min_count,
                verbose=args.v,
            )
            write_output_csv(out_dir / "unique_paths_all.csv", result)
            return 0

        # Single specific sheet
        df = load_table(in_path, args.sheet, verbose=args.v)
        result = build_unique_paths(
            df,
            families=families,
            description_only=description_only,
            derive_family_flag=args.derive_family or True,
            force_rederive=args.force_rederive,
            allow_non_abs=args.allow_non_absolute,
            min_count=args.min_count,
            verbose=args.v,
        )
        write_output_csv(out_dir / "unique_paths_all.csv", result)
        return 0

    except Exception as e:
        print(f"[ERROR] Failed to process '{in_path}': {e}")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())

