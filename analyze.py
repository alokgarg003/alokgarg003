
import pandas as pd
import numpy as np


def compute_kpis(df: pd.DataFrame) -> dict:
    total = len(df)
    closed = int(df['Status_canonical'].isin(['Resolved','Closed']).sum())
    open_  = int(total - closed)
    intermittent_ratio = float(df['is_intermittent'].mean()) if total else 0.0

    top_ifaces = (df['primary_interface'].value_counts().head(15).to_dict())
    top_errors = (df['error_family'].value_counts().head(10).to_dict())

    return {
        'total_incidents': total,
        'closed_incidents': closed,
        'open_incidents': open_,
        'intermittent_ratio': intermittent_ratio,
        'top_interfaces': top_ifaces,
        'top_error_families': top_errors,
    }


def ci_error_pairs(df: pd.DataFrame) -> pd.DataFrame:
    t = (df.groupby(['CI','error_family'])['Number']
            .count().reset_index(name='count')
            .sort_values('count', ascending=False))
    return t


def recurrence_pairs(df: pd.DataFrame) -> pd.DataFrame:
    t = (df.groupby(['primary_interface','error_family'])['recurs_24h']
            .sum().reset_index(name='recurs_24h_count')
            .sort_values('recurs_24h_count', ascending=False))
    return t


def quality_checks(df: pd.DataFrame) -> dict:
    no_iface = float(df['primary_interface'].isna().mean())
    other_err = float((df['error_family'] == 'other').mean())
    stuck_cnt = int(df['stuck_files_count'].notna().sum())
    stuck_min = int(df['stuck_for_minutes'].notna().sum())

    # intermittent with recurrence
    inter = df[df['is_intermittent']]
    inter_recur_ratio = float(inter['recurs_24h'].mean()) if len(inter) else 0.0

    return {
        'pct_no_interface': no_iface,
        'pct_other_error_family': other_err,
        'rows_with_stuck_files_count': stuck_cnt,
        'rows_with_stuck_for_minutes': stuck_min,
        'intermittent_with_recurrence_ratio': inter_recur_ratio
    }
