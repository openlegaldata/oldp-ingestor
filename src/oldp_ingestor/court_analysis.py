"""Analyse missing courts from ingestor logs against OLDP API data.

Pure-Python module — no Django dependency.  Uses only data fetched from
the OLDP REST API (courts, cities, states).
"""

import re
from collections import Counter

# ---------------------------------------------------------------------------
# German court types — mirrors CourtTypesDE from oldp-de without Django import
# ---------------------------------------------------------------------------

COURT_TYPES: dict[str, dict] = {
    "AG": {"name": "Amtsgericht"},
    "ARBG": {"name": "Arbeitsgericht"},
    "BAG": {"name": "Bundesarbeitsgericht"},
    "BGH": {"name": "Bundesgerichtshof"},
    "BFH": {"name": "Bundesfinanzhof"},
    "BSG": {"name": "Bundessozialgericht"},
    "BVerfG": {"name": "Bundesverfassungsgericht"},
    "BVerwG": {"name": "Bundesverwaltungsgericht"},
    "BPatG": {"name": "Bundespatentgericht"},
    "FG": {"name": "Finanzgericht"},
    "LAG": {"name": "Landesarbeitsgericht"},
    "LSG": {"name": "Landessozialgericht"},
    "LVG": {"name": "Landesverfassungsgericht"},
    "LBGH": {"name": "Landesberufsgericht"},
    "LG": {"name": "Landgericht"},
    "OLG": {"name": "Oberlandesgericht"},
    "OBLG": {"name": "Oberstes Landesgericht"},
    "OVG": {"name": "Oberverwaltungsgericht"},
    "SG": {"name": "Sozialgericht"},
    "STGH": {"name": "Staatsgerichtshof"},
    "SCHG": {"name": "Schifffahrtsgericht"},
    "SCHOG": {"name": "Schifffahrtsobergericht"},
    "VERFG": {"name": "Verfassungsgerichtshof", "aliases": ["Verfassungsgericht"]},
    "VG": {"name": "Verwaltungsgericht"},
    "VGH": {"name": "Verwaltungsgerichtshof"},
    "KG": {"name": "Kammergericht"},
    "EuGH": {"name": "Europäischer Gerichtshof"},
    "AWG": {"name": "Anwaltsgericht", "aliases": ["Anwaltsgerichtshof"]},
    "MSCHOG": {"name": "Moselschifffahrtsobergericht"},
    "RSCHGD": {"name": "Rheinschifffahrtsgericht"},
    "RSCHOG": {"name": "Rheinschifffahrtsobergericht"},
}

# Build lookup: full name / alias -> type code  (longest names first so greedy
# matching picks the most specific name).
_NAME_TO_CODE: list[tuple[str, str]] = []
for _code, _info in COURT_TYPES.items():
    _NAME_TO_CODE.append((_info["name"], _code))
    for _alias in _info.get("aliases", []):
        _NAME_TO_CODE.append((_alias, _code))
_NAME_TO_CODE.sort(key=lambda t: len(t[0]), reverse=True)

# Filler words that appear between type name and location
_FILLER_WORDS = [
    "für das Land",
    "des Landes",
    "des Freistaates",
    "des Saarlandes",
    "der Freien Hansestadt",
    "der Freien und Hansestadt",
]

# Regex for log lines produced by OLDP's court resolver
_MISSING_RE = re.compile(r"Could not resolve court from name:\s*(.+?)(?:['\"\}]|\s*$)")


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------


def extract_type_code(name: str) -> str | None:
    """Return the court type code (e.g. ``'OLG'``) for a court *name*.

    Tries abbreviation match first, then full-name / alias match.
    Returns ``None`` if no type can be determined.
    """
    # Try abbreviation at start: "OLG Hamm" -> "OLG"
    for code in COURT_TYPES:
        if re.match(rf"\b{re.escape(code)}\b", name):
            return code

    # Try full name / alias match (longest first)
    for type_name, code in _NAME_TO_CODE:
        if type_name in name:
            return code

    return None


def extract_location(name: str, type_code: str | None) -> str:
    """Strip the court-type portion and filler words, returning the location."""
    location = name

    if type_code:
        info = COURT_TYPES.get(type_code, {})
        # Remove abbreviation
        location = re.sub(rf"\b{re.escape(type_code)}\b\s*", "", location)
        # Remove full type name and aliases
        for label in [info.get("name", "")] + info.get("aliases", []):
            if label:
                location = location.replace(label, "")

    # Remove filler words
    for filler in _FILLER_WORDS:
        location = location.replace(filler, "")

    return location.strip(" -,")


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------


def parse_missing_courts(lines: list[str]) -> Counter:
    """Scan log *lines* and return a ``Counter`` of missing court names."""
    counts: Counter = Counter()
    for line in lines:
        m = _MISSING_RE.search(line)
        if m:
            court_name = m.group(1).strip()
            if court_name:
                counts[court_name] += 1
    return counts


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------


def _normalize_for_match(text: str) -> str:
    """Lower-case and strip common German adjective suffixes for fuzzy matching."""
    t = text.lower().strip()
    # Strip trailing adjective suffixes so "Schleswig-Holsteinisches" matches
    # state name "Schleswig-Holstein".
    for suffix in ("isches", "ische", "ischer", "isch", "es", "er"):
        if t.endswith(suffix) and len(t) > len(suffix) + 2:
            t = t[: -len(suffix)]
            break
    return t


def analyze_missing_courts(
    missing_names: Counter,
    courts: list[dict],
    cities: list[dict],
    states: list[dict],
) -> list[dict]:
    """Cross-reference missing court names against existing API data.

    Returns a list of analysis dicts, one per missing court name.
    """
    # Index: city name (lower) -> list of courts in that city
    city_courts: dict[str, list[dict]] = {}
    for court in courts:
        city_name = (court.get("city_name") or "").lower()
        if city_name:
            city_courts.setdefault(city_name, []).append(court)

    # Index: state id -> state name, state name (lower) -> list of courts
    state_by_id: dict[int, str] = {}
    for s in states:
        state_by_id[s["id"]] = s["name"]

    state_courts: dict[str, list[dict]] = {}
    for court in courts:
        state_id = court.get("state")
        if state_id and state_id in state_by_id:
            sname = state_by_id[state_id].lower()
            state_courts.setdefault(sname, []).append(court)

    # State names for fuzzy matching
    state_names_lower = [s["name"].lower() for s in states]

    results = []
    for name, count in missing_names.most_common():
        type_code = extract_type_code(name)
        location = extract_location(name, type_code)
        type_label = ""
        if type_code:
            type_label = COURT_TYPES[type_code]["name"]

        # Find matching courts by city
        matching_city_courts: list[dict] = []
        loc_lower = location.lower()
        for city_name, cts in city_courts.items():
            if city_name and (city_name in loc_lower or loc_lower in city_name):
                matching_city_courts.extend(cts)

        # Find matching courts by state
        matching_state_courts: list[dict] = []
        matched_state = ""
        loc_norm = _normalize_for_match(location)
        for sname in state_names_lower:
            snorm = _normalize_for_match(sname)
            if snorm and (snorm in loc_norm or loc_norm in snorm):
                matched_state = sname
                matching_state_courts = state_courts.get(sname, [])
                break

        results.append(
            {
                "name": name,
                "count": count,
                "type_code": type_code,
                "type_label": type_label,
                "location": location,
                "city_courts": matching_city_courts,
                "state_courts": matching_state_courts,
                "matched_state": matched_state,
            }
        )

    return results


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def _group_by_location(analyses: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for a in analyses:
        loc = a["location"] or "(unknown)"
        groups.setdefault(loc, []).append(a)
    return groups


def format_table(analyses: list[dict]) -> str:
    """Human-readable grouped report."""
    total_errors = sum(a["count"] for a in analyses)
    lines = [
        f"Missing Courts Analysis: {len(analyses)} unique court name(s) "
        f"from {total_errors} error(s)",
        "=" * 80,
    ]

    for loc, entries in _group_by_location(analyses).items():
        lines.append("")
        lines.append(f"--- Location: {loc} ---")

        for a in entries:
            lines.append("")
            lines.append(f"  MISSING: {a['name']}  (x{a['count']})")

            type_info = ""
            if a["type_code"]:
                type_info = f"Type: {a['type_code']} ({a['type_label']})"
            loc_info = f"Location: {a['location']}" if a["location"] else ""
            parts = [p for p in (type_info, loc_info) if p]
            if parts:
                lines.append(f"    {' | '.join(parts)}")

            if a["city_courts"]:
                lines.append("    Existing courts at this location:")
                for c in a["city_courts"]:
                    code = c.get("code", "?")
                    ctype = c.get("court_type", "?")
                    lines.append(f"      - {c.get('name', '?')} [{code}] type={ctype}")
            elif a["state_courts"]:
                lines.append("    Existing courts in this state:")
                for c in a["state_courts"]:
                    code = c.get("code", "?")
                    ctype = c.get("court_type", "?")
                    lines.append(f"      - {c.get('name', '?')} [{code}] type={ctype}")
            else:
                lines.append("    No matching existing courts found.")

    return "\n".join(lines)


def format_tsv(analyses: list[dict]) -> str:
    """Tab-separated output for scripting."""
    header = [
        "name",
        "count",
        "type_code",
        "type_label",
        "location",
        "city_matches",
        "state_matches",
    ]
    rows = ["\t".join(header)]
    for a in analyses:
        city_names = "; ".join(c.get("name", "") for c in a["city_courts"])
        state_names = "; ".join(c.get("name", "") for c in a["state_courts"])
        rows.append(
            "\t".join(
                [
                    a["name"],
                    str(a["count"]),
                    a["type_code"] or "",
                    a["type_label"],
                    a["location"],
                    city_names,
                    state_names,
                ]
            )
        )
    return "\n".join(rows)
