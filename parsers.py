'''
import re
import numpy as np
import pandas as pd

ID_PAT       = re.compile(r"\b([EIU][0-9]{3,6}[A-Z]?)\b")
ERR_FAMILY   = re.compile(r"\b(FMS\-[A-Z\-]+|PROCESS\-ALERT|MFTERRGEN|MFTERR0[1-9]|FTP\s*error|Permission denied|Stuck files|FMS\-CONFIG\-FILE|FMS\-ERR\-FILE\-REMOVE|Not connected|Auth fail|java\.io\.IOException|Address already in use)\b", re.I)
CODE_PAT     = re.compile(r"\b([UEI][0-9]{3,6})\b")
PATH_PAT     = re.compile(r"(/(?:var|opt|data)[^\s]*)")
STUCK_CNT    = re.compile(r":\s*-\s*(\d+)\b")
STUCK_MIN    = re.compile(r"from\s+(\d+)\s*min\b", re.I)

STATUS_MAP = {
    'accepted / in progress': 'In Progress',
    'in progress': 'In Progress',
    'accepted': 'In Progress',
    'resolved': 'Resolved',
    'closed': 'Closed'
}

FAMILY_NORMALIZE = {
    'fms-ftp-error': 'FMS-FTP-ERROR',
    'ftp error': 'FMS-FTP-ERROR',
    'fms-config-file': 'FMS-CONFIG-FILE',
    'fms-err-file-remove': 'FMS-ERR-FILE-REMOVE',
    'process-alert': 'PROCESS-ALERT',
    'mfterrgen': 'MFTERRGEN',
    'permission denied': 'Permission denied',
    'not connected': 'Not connected',
    'auth fail': 'Auth fail',
    'java.io.ioexception': 'java.io.IOException',
    'address already in use': 'Address already in use',
    'stuck files': 'Stuck files'
}

PRIORITY_PREFIX = ['E','I','U']

def canonical_status(s: str) -> str:
    s = (s or '').strip().lower()
    return STATUS_MAP.get(s, s.title())


def parse_short_description(text: str) -> pd.Series:
    s = str(text or '')
    ifaces = ID_PAT.findall(s)
    efam   = (ERR_FAMILY.findall(s) or ['other'])[0].lower()
    efam   = FAMILY_NORMALIZE.get(efam, efam.title() if efam != 'other' else 'other')
    codes  = list(dict.fromkeys(CODE_PAT.findall(s)))  # preserve order, unique
    paths  = PATH_PAT.findall(s)
    cnt    = STUCK_CNT.search(s)
    mins   = STUCK_MIN.search(s)

    # primary interface: prefer order by PRIORITY_PREFIX then first occurrence
    def pick_primary(tokens):
        if not tokens:
            return np.nan
        for pref in PRIORITY_PREFIX:
            for t in tokens:
                if t.startswith(pref):
                    return t
        return tokens[0]

    return pd.Series({
        'interfaces': ','.join(ifaces) if ifaces else np.nan,
        'primary_interface': pick_primary(ifaces),
        'error_family': efam,
        'error_codes': ','.join(codes) if codes else np.nan,
        'paths': ','.join(paths) if paths else np.nan,
        'stuck_files_count': int(cnt.group(1)) if cnt else np.nan,
        'stuck_for_minutes': int(mins.group(1)) if mins else np.nan,
        'has_path': bool(paths)
    })
'''


import re
import numpy as np
import pandas as pd

# ---------- Canonical families we want to catch ----------
# (expandable: add more strings here if you see new anomalies)
CANONICAL_FAMILIES = [
    "FMS-FTP-ERROR",
    "FMS-CONFIG-FILE",
    "FMS-ERR-FILE-REMOVE",
    "FMS-ERR-FILE-COPY",
    "FMS-DIR-CREATE",
    "PROCESS-ALERT",
    "MFTERRGEN",
    "CALLOUT-MFTERR02",
    "CALLOUT-MFTERR03",
    "CALLOUT-MFTERR04",
    "ERR-HTTPS-ERROR",
    "UNHANDLED EXCEPTION ERROR",
    "MISSING FILES",
    # Keep legacy textual anomalies too:
    "FTP error",
    "Permission denied",
    "Not connected",
    "Auth fail",
    "java.io.IOException",
    "Address already in use",
    "Stuck files",
]

# ---------- Flexible family matching ----------
# Build regex that tolerates hyphens/underscores/spaces between tokens
def _flex_family_regex(fam: str) -> re.Pattern:
    tokens = re.findall(r"[A-Za-z0-9]+", fam)
    if not tokens:
        return re.compile(r"(?!x)x")  # never matches
    pattern = r"".join(re.escape(t) + r"[^A-Za-z0-9]*" for t in tokens)
    return re.compile(pattern, re.IGNORECASE)

FLEX_FAMILY_MATCHERS = [(fam, _flex_family_regex(fam)) for fam in CANONICAL_FAMILIES]

# ---------- Existing patterns (slightly refined) ----------
ID_PAT       = re.compile(r"\b([EIU][0-9]{3,6}[A-Z]?)\b")
CODE_PAT     = re.compile(r"\b([UEI][0-9]{3,6})\b")

# Keep your scoped Linux dirs, but consider broader later if needed
PATH_PAT     = re.compile(r"(/(?:var|opt|data)[^\s]*)")

STUCK_CNT    = re.compile(r":\s*-\s*(\d+)\b")
STUCK_MIN    = re.compile(r"\bfrom\s+(\d+)\s*min\b", re.I)

STATUS_MAP = {
    'accepted / in progress': 'In Progress',
    'in progress': 'In Progress',
    'accepted': 'In Progress',
    'resolved': 'Resolved',
    'closed': 'Closed'
}

FAMILY_NORMALIZE = {
    # Canonical mappings (flex lowercased keys)
    'fms-ftp-error': 'FMS-FTP-ERROR',
    'ftp error': 'FMS-FTP-ERROR',
    'fms-config-file': 'FMS-CONFIG-FILE',
    'fms-err-file-remove': 'FMS-ERR-FILE-REMOVE',
    'fms-err-file-copy': 'FMS-ERR-FILE-COPY',
    'fms-dir-create': 'FMS-DIR-CREATE',
    'process-alert': 'PROCESS-ALERT',
    'mfterrgen': 'MFTERRGEN',
    'callout-mfterr02': 'CALLOUT-MFTERR02',
    'callout-mfterr03': 'CALLOUT-MFTERR03',
    'callout-mfterr04': 'CALLOUT-MFTERR04',
    'err-https-error': 'ERR-HTTPS-ERROR',
    'unhandled exception error': 'UNHANDLED EXCEPTION ERROR',
    'missing files': 'MISSING FILES',
    'permission denied': 'Permission denied',
    'not connected': 'Not connected',
    'auth fail': 'Auth fail',
    'java.io.ioexception': 'java.io.IOException',
    'address already in use': 'Address already in use',
    'stuck files': 'Stuck files',
}

# Optional extra anomaly tokens that often indicate errors but aren't canonical families
# These help us surface "unmatched anomalies" for review.
ANOMALY_TOKENS = [
    r"\btime(?:out)?\b",
    r"\bconnection\s+refused\b",
    r"\bconnection\s+reset\b",
    r"\bno\s+such\s+file\b",
    r"\bpermission\b",
    r"\bdenied\b",
    r"\bauth(?:entication)?\s+fail(?:ure)?\b",
    r"\bssl\s+error\b",
    r"\bh(?:ttps|ttp)\b",
    r"\bcertificate\b",
    r"\bhandshake\b",
    r"\bproxy\b",
    r"\brefused\b",
    r"\breset\b",
    r"\bnot\s+found\b",
]
ANOMALY_RX = re.compile("|".join(ANOMALY_TOKENS), re.IGNORECASE)

PRIORITY_PREFIX = ['E','I','U']

def canonical_status(s: str) -> str:
    s = (s or '').strip().lower()
    return STATUS_MAP.get(s, s.title())

def _normalize_family_label(token: str) -> str:
    k = (token or '').strip().lower().replace('_', '-')
    k = re.sub(r"\s+", " ", k)
    return FAMILY_NORMALIZE.get(k, token)

def _derive_error_family(text: str) -> str:
    """
    Choose the first canonical family that flex-matches in text.
    If multiple match, earliest occurrence wins.
    """
    s = str(text or "")
    best = ""
    best_pos = None
    for fam, rx in FLEX_FAMILY_MATCHERS:
        m = rx.search(s)
        if m:
            pos = m.start()
            if best_pos is None or pos < best_pos:
                best_pos = pos
                best = fam
    return best or "other"

def _collect_unmatched_anomalies(text: str, matched_family: str) -> list:
    """
    Return a list of anomaly substrings that matched ANOMALY_RX but are not the canonical family.
    This helps you log/inspect additional error hints not covered by families.
    """
    s = str(text or "")
    hits = ANOMALY_RX.findall(s)
    # light de-dupe & ignore if these are already part of the matched family wording
    base = re.sub(r"[^A-Za-z0-9]", "", matched_family.lower())
    out = []
    seen = set()
    for h in hits:
        h_norm = h.strip().lower()
        if not h_norm:
            continue
        # Skip if family text already contains the anomaly token (very rough check)
        if base and re.sub(r"[^A-Za-z0-9]", "", h_norm) in base:
            continue
        if h_norm not in seen:
            seen.add(h_norm)
            out.append(h_norm)
    return out

def parse_short_description(text: str) -> pd.Series:
    s = str(text or '')
    ifaces = ID_PAT.findall(s)
    codes  = list(dict.fromkeys(CODE_PAT.findall(s)))  # preserve order, unique
    paths  = PATH_PAT.findall(s)
    cnt    = STUCK_CNT.search(s)
    mins   = STUCK_MIN.search(s)

    # Derive canonical error family by flexible matching
    derived_family = _derive_error_family(s)
    efam = _normalize_family_label(derived_family)

    # Primary interface: prefer order by PRIORITY_PREFIX then first occurrence
    def pick_primary(tokens):
        if not tokens:
            return np.nan
        for pref in PRIORITY_PREFIX:
            for t in tokens:
                if t.startswith(pref):
                    return t
        return tokens[0]

    # Collect anomalies not mentioned in families (for inspection/reporting)
    extra_anomalies = _collect_unmatched_anomalies(s, efam)
    extra_anomalies_str = "; ".join(extra_anomalies) if extra_anomalies else np.nan

    return pd.Series({
        'interfaces': ','.join(ifaces) if ifaces else np.nan,
        'primary_interface': pick_primary(ifaces),
        'error_family': efam,                             # canonical family
        'error_codes': ','.join(codes) if codes else np.nan,
        'paths': ','.join(paths) if paths else np.nan,
        'stuck_files_count': int(cnt.group(1)) if cnt else np.nan,
        'stuck_for_minutes': int(mins.group(1)) if mins else np.nan,
        'has_path': bool(paths),
        'error_anomalies_unmatched': extra_anomalies_str # anomalies not covered by canonical families
    })
