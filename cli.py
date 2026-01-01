
import argparse
from pathlib import Path
import pandas as pd

from .config import RunConfig
from .enrich import enrich
from .visualize import monthly_trend, error_family_pareto, hourly_heatmap, dow_bar
from .analyze import compute_kpis, ci_error_pairs, recurrence_pairs, quality_checks
from .report import write_summary


def run_pipeline(cfg: RunConfig):
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    # Enrich
    df = enrich(str(cfg.input_path), str(cfg.ci_lookup) if cfg.ci_lookup else None)

    # Save enriched Excel
    enriched_path = cfg.out_dir / 'incident (1).xlsx'
    # enriched_path = cfg.out_dir / 'snow_enriched.xlsx'
    with pd.ExcelWriter(enriched_path, engine='openpyxl') as w:
        df.to_excel(w, sheet_name='enriched', index=False)

    # Analytics
    kpis = compute_kpis(df)
    ci_pairs = ci_error_pairs(df)
    recur = recurrence_pairs(df)
    qcheck = quality_checks(df)

    # Save CSVs
    ci_pairs.to_csv(cfg.out_dir / 'ci_error_pairs.csv', index=False)
    recur.to_csv(cfg.out_dir / 'recurrence_pairs.csv', index=False)

    # Visuals
    monthly_trend(df, cfg.out_dir)
    error_family_pareto(df, cfg.out_dir)
    hourly_heatmap(df, cfg.out_dir)
    dow_bar(df, cfg.out_dir)

    # Report
    ci_top_list = [((f"{r.CI} × {r.error_family}"), int(getattr(r, 'count'))) for r in ci_pairs.head(10).itertuples(index=False)]
    actions = [
        'Automate housekeeping for stuck-file dirs with age/size gates and a sidelined queue.',
        'Add FTP/SFTP pre-flight checks (host key, credentials, dir existence, passive/active).',
        'Introduce config linter for FMS-CONFIG-FILE before deploy; block missing/disabled configs.',
        'Tighten monitor cadence and suppress known benign CALLOUT patterns to reduce noise.',
        'Standardize closure notes: require RCA evidence + action + explicit confirmation.'
    ]
    write_summary(cfg.out_dir, kpis, qcheck, ci_top_list, actions)


def main():
    ap = argparse.ArgumentParser(description='Snow Incident AI Pipeline')
    ap.add_argument('--input', required=True, help='Path to ServiceNow Excel (e.g., snow_last_2year_data.xlsx)')
    ap.add_argument('--out', required=True, help='Output directory')
    ap.add_argument('--ci-lookup', default=None, help='Optional CI taxonomy CSV (CI,CI_role,CI_protocol,CI_env,CI_techstack,CI_criticality)')
    args = ap.parse_args()

    cfg = RunConfig(input_path=Path(args.input), out_dir=Path(args.out), ci_lookup=Path(args.ci_lookup) if args.ci_lookup else None)
    run_pipeline(cfg)

if __name__ == '__main__':
    main()
