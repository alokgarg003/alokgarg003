
from __future__ import annotations
import numpy as np
import pandas as pd
from .parsers import parse_short_description, canonical_status
from .config import DAYPART_BINS, DAYPART_LABELS

REQUIRED_COLS = [
    'Number','Caller','Description','Category','Severity','Status',
    'Assignment group','Assigned to','Opened','Resolution notes','Configuration item'
]


def load_ci_lookup(path: str | None) -> dict:
    if not path:
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    req = ['CI','CI_role','CI_protocol','CI_env','CI_techstack','CI_criticality']
    for c in req:
        if c not in df.columns:
            raise ValueError(f"ci_lookup.csv missing column: {c}")
    m = {}
    for r in df.itertuples(index=False):
        m[getattr(r,'CI')] = {
            'CI_role': getattr(r,'CI_role'),
            'CI_protocol': getattr(r,'CI_protocol'),
            'CI_env': getattr(r,'CI_env'),
            'CI_techstack': getattr(r,'CI_techstack'),
            'CI_criticality': getattr(r,'CI_criticality')
        }
    return m


def enrich(input_path: str, ci_lookup_path: str | None) -> pd.DataFrame:
    df = pd.read_excel(input_path, engine='openpyxl')

    # ensure required columns exist (allow missing CI column and create if absent)
    for c in REQUIRED_COLS:
        if c not in df.columns:
            if c == 'Configuration item':
                df[c] = np.nan
            else:
                raise ValueError(f"Missing required column: {c}")

    # normalize strings
    for c in ['Short description','Resolution notes','Assignment group','Caller','Status','Category','Configuration item','Assigned to']:
        df[c] = df[c].astype(str).str.strip()

    # time
    opened = df['Opened']
    if np.issubdtype(opened.dtype, np.number):
        df['Opened_dt'] = pd.to_datetime(opened, unit='d', origin='1899-12-30', errors='coerce')
    else:
        df['Opened_dt'] = pd.to_datetime(opened, errors='coerce')
    df = df[df['Opened_dt'].notna()].copy()
    df['month']   = df['Opened_dt'].dt.to_period('M').astype(str)
    df['week']    = df['Opened_dt'].dt.to_period('W').astype(str)
    df['dow']     = df['Opened_dt'].dt.day_name()
    df['hour']    = df['Opened_dt'].dt.hour
    df['daypart'] = pd.cut(df['hour'], bins=DAYPART_BINS, labels=DAYPART_LABELS)

    # status canonicalization
    df['Status_canonical'] = df['Status'].apply(canonical_status)

    # parsing
    parsed = df['Description'].apply(parse_short_description)
    df = df.join(parsed)

    # intermittent + closure quality
    notes = df['Resolution notes'].str.lower().fillna('')
    df['is_intermittent'] = notes.str.contains('intermittent')
    df['closure_quality_score'] = (
        notes.str.contains('rca').astype(int) +
        notes.str.contains('resolution|fix|reprocessed|triggered|housekeep|housekeeping').astype(int) +
        notes.str.contains('confirmation|verified|hence closing|hence we are resolving').astype(int)
    )

    # CI taxonomy join
    ci_map = load_ci_lookup(ci_lookup_path)
    def map_ci(ci):
        meta = ci_map.get(ci, {})
        return pd.Series({
            'CI': ci,
            'CI_role': meta.get('CI_role'),
            'CI_protocol': meta.get('CI_protocol'),
            'CI_env': meta.get('CI_env'),
            'CI_techstack': meta.get('CI_techstack'),
            'CI_criticality': meta.get('CI_criticality')
        })

    df = df.join(df['Configuration item'].apply(map_ci))

    # recurrence: same (primary_interface, error_family) within 24h
    df = df.sort_values('Opened_dt')
    df['prev_time'] = df.groupby(['primary_interface','error_family'])['Opened_dt'].shift(1)
    df['recurs_24h'] = (df['Opened_dt'] - df['prev_time']).dt.total_seconds().le(24*3600)

    return df
