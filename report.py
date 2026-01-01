from pathlib import Path
import json

def write_summary(out_dir: Path, kpis: dict, qcheck: dict, ci_top: list[tuple[str,int]], actions: list[str]):
    parts = []
    parts.append('# Incident Analytics Summary')
    parts.append('')
    parts.append('## KPIs')
    parts.append('```')
    parts.append(json.dumps(kpis, indent=2))
    parts.append('```')
    parts.append('')
    parts.append('## Quality checks')
    parts.append('```')
    parts.append(json.dumps(qcheck, indent=2))
    parts.append('```')
    parts.append('')
    if ci_top:
        parts.append('## Top CI x Error Families (sample)')
        for (pair, cnt) in ci_top[:10]:
            parts.append(f'- {pair}: {cnt}')
        parts.append('')
    parts.append('## Recommended actions (Top 5)')
    for a in actions:
        parts.append(f'- {a}')
    content = ''
    for p in parts:
        content += p + chr(10)
    (out_dir / 'summary.md').write_text(content)