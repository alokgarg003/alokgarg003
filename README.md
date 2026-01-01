
# Snow Incident AI – End-to-End Implementation

This project parses ServiceNow incident logs (especially MFT/BIG incidents), enriches them with
interface & error-family intelligence, groups by **Configuration Item (CI)**, and produces
ready-to-use analytics outputs (Excel + PNG charts + CSVs + Markdown summary).

## Quick start

```bash
# 1) Create a Python 3.10+ virtualenv (recommended) and install deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2) Put your input file in the project root
#    e.g., snow_last_2year_data.xlsx

# 3) Run the pipeline
python -m snow_ai run   --input snow_last_2year_data.xlsx   --out outputs   --ci-lookup ci_lookup.csv

# 4) Open the results in outputs/
```

### What you get
- `snow_enriched.xlsx` (sheet: `enriched`) — normalized, parsed, CI-augmented dataset
- `recurrence_pairs.csv` — repeating (interface, error_family) pairs within 24h
- `ci_error_pairs.csv` — matrix-like pairs for CI × error_family counts
- PNG charts — monthly trend, error-family Pareto, hour-of-day heatmap, DOW bar
- `summary.md` — executive summary + KPIs + quality checks + top 5 actions

## Configuration Item (CI) taxonomy
Provide an optional CSV file `ci_lookup.csv` with headers:

```
CI,CI_role,CI_protocol,CI_env,CI_techstack,CI_criticality
E000005,Broker,SFTP,PROD,BIG,P1
...
```

If `ci_lookup.csv` is omitted, the pipeline still runs (CI meta will be empty). You can iteratively
fill this file to improve CI-centric insights.

## Re-running and reproducibility
The pipeline is deterministic and re-runnable. It never overwrites your original columns; it writes
all new fields into the `enriched` sheet.

## AI prompt
A comprehensive AI prompt that encodes rules for future deep dives is in `prompts/ai_prompt.md`.
You can paste it into Microsoft 365 Copilot or any LLM tool to regenerate an analysis on newer files.

## Notes
- The scripts expect column names: Number, Caller, Short description, Category, Severity, Status,
  Assignment group, Assigned to, Opened, Resolution notes, Configuration item.
- Timestamps are derived from the Excel serial in `Opened`.
- Charts are generated via Matplotlib (no internet required).



python -m streamlit run app.py
