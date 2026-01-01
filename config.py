
from dataclasses import dataclass
from pathlib import Path

@dataclass
class RunConfig:
    input_path: Path
    out_dir: Path
    ci_lookup: Path | None = None

DAYPART_BINS = [-1, 5, 11, 17, 23]
DAYPART_LABELS = ['Night(00-05)', 'Morning(06-11)', 'Afternoon(12-17)', 'Evening(18-23)']
