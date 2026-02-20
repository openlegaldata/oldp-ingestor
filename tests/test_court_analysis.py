"""Tests for oldp_ingestor.court_analysis and the analyze-courts CLI command."""

import subprocess
import sys

from oldp_ingestor.court_analysis import (
    analyze_missing_courts,
    extract_location,
    extract_type_code,
    format_table,
    format_tsv,
    parse_missing_courts,
)


# ---------------------------------------------------------------------------
# extract_type_code
# ---------------------------------------------------------------------------


class TestExtractTypeCode:
    def test_abbreviation_start(self):
        assert extract_type_code("OLG Hamm") == "OLG"

    def test_abbreviation_single_word(self):
        assert extract_type_code("BGH") == "BGH"

    def test_full_name(self):
        assert extract_type_code("Oberlandesgericht Hamm") == "OLG"

    def test_full_name_with_filler(self):
        assert (
            extract_type_code("Oberverwaltungsgericht für das Land Schleswig-Holstein")
            == "OVG"
        )

    def test_alias(self):
        assert extract_type_code("Verfassungsgericht Berlin") == "VERFG"

    def test_kammergericht(self):
        assert extract_type_code("KG Berlin") == "KG"
        assert extract_type_code("Kammergericht Berlin") == "KG"

    def test_unknown(self):
        assert extract_type_code("Foo Bar Baz") is None

    def test_amtsgericht(self):
        assert extract_type_code("AG Charlottenburg") == "AG"
        assert extract_type_code("Amtsgericht Charlottenburg") == "AG"

    def test_anwaltsgericht_alias(self):
        assert extract_type_code("Anwaltsgerichtshof Berlin") == "AWG"

    def test_bundesverfassungsgericht(self):
        assert extract_type_code("BVerfG") == "BVerfG"

    def test_verwaltungsgericht(self):
        assert extract_type_code("VG Berlin") == "VG"
        assert extract_type_code("Verwaltungsgericht Berlin") == "VG"


# ---------------------------------------------------------------------------
# extract_location
# ---------------------------------------------------------------------------


class TestExtractLocation:
    def test_abbreviation_style(self):
        assert extract_location("OLG Hamm", "OLG") == "Hamm"

    def test_full_name_style(self):
        assert extract_location("Oberlandesgericht Hamm", "OLG") == "Hamm"

    def test_with_filler(self):
        loc = extract_location(
            "Oberverwaltungsgericht für das Land Schleswig-Holstein", "OVG"
        )
        assert loc == "Schleswig-Holstein"

    def test_des_landes_filler(self):
        loc = extract_location("Landessozialgericht des Landes Berlin", "LSG")
        assert loc == "Berlin"

    def test_kammergericht(self):
        assert extract_location("KG Berlin", "KG") == "Berlin"

    def test_no_type_code(self):
        assert extract_location("Some Court Name", None) == "Some Court Name"

    def test_bundesgericht_no_location(self):
        loc = extract_location("BGH", "BGH")
        assert loc == ""

    def test_alias_stripped(self):
        loc = extract_location("Verfassungsgericht Berlin", "VERFG")
        assert loc == "Berlin"


# ---------------------------------------------------------------------------
# parse_missing_courts
# ---------------------------------------------------------------------------


class TestParseMissingCourts:
    def test_basic_log_line(self):
        lines = [
            "2026-01-15 10:00:00 ERROR [oldp] Could not resolve court from name: OLG Hamm",
        ]
        result = parse_missing_courts(lines)
        assert result["OLG Hamm"] == 1

    def test_multiple_occurrences(self):
        lines = [
            "ERROR Could not resolve court from name: KG Berlin",
            "INFO something else",
            "ERROR Could not resolve court from name: KG Berlin",
            "ERROR Could not resolve court from name: OLG Hamm",
        ]
        result = parse_missing_courts(lines)
        assert result["KG Berlin"] == 2
        assert result["OLG Hamm"] == 1

    def test_quoted_name(self):
        lines = [
            "Could not resolve court from name: OLG Hamm'",
        ]
        result = parse_missing_courts(lines)
        assert result["OLG Hamm"] == 1

    def test_double_quoted(self):
        lines = [
            'Could not resolve court from name: OLG Hamm"',
        ]
        result = parse_missing_courts(lines)
        assert result["OLG Hamm"] == 1

    def test_json_brace(self):
        lines = [
            "Could not resolve court from name: OLG Hamm}",
        ]
        result = parse_missing_courts(lines)
        assert result["OLG Hamm"] == 1

    def test_no_matches(self):
        lines = [
            "INFO everything is fine",
            "DEBUG no court errors here",
        ]
        result = parse_missing_courts(lines)
        assert len(result) == 0

    def test_empty_input(self):
        assert len(parse_missing_courts([])) == 0

    def test_long_court_name(self):
        lines = [
            "ERROR Could not resolve court from name: Oberverwaltungsgericht für das Land Schleswig-Holstein",
        ]
        result = parse_missing_courts(lines)
        assert result["Oberverwaltungsgericht für das Land Schleswig-Holstein"] == 1


# ---------------------------------------------------------------------------
# analyze_missing_courts
# ---------------------------------------------------------------------------


_MOCK_COURTS = [
    {
        "id": 1,
        "name": "Amtsgericht Charlottenburg",
        "code": "AG-Charlottenburg",
        "court_type": "AG",
        "city_name": "Berlin",
        "state": 1,
    },
    {
        "id": 2,
        "name": "Landgericht Berlin",
        "code": "LG-Berlin",
        "court_type": "LG",
        "city_name": "Berlin",
        "state": 1,
    },
    {
        "id": 3,
        "name": "Verwaltungsgericht Berlin",
        "code": "VG-Berlin",
        "court_type": "VG",
        "city_name": "Berlin",
        "state": 1,
    },
    {
        "id": 4,
        "name": "Schleswig-Holsteinisches OLG",
        "code": "OLG-SH",
        "court_type": "OLG",
        "city_name": "Schleswig",
        "state": 2,
    },
]

_MOCK_CITIES = [
    {"id": 1, "name": "Berlin", "state": 1},
    {"id": 2, "name": "Schleswig", "state": 2},
]

_MOCK_STATES = [
    {"id": 1, "name": "Berlin"},
    {"id": 2, "name": "Schleswig-Holstein"},
]


class TestAnalyzeMissingCourts:
    def test_city_match(self):
        from collections import Counter

        missing = Counter({"KG Berlin": 4})
        results = analyze_missing_courts(
            missing, _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        assert len(results) == 1
        r = results[0]
        assert r["name"] == "KG Berlin"
        assert r["count"] == 4
        assert r["type_code"] == "KG"
        assert r["location"] == "Berlin"
        assert len(r["city_courts"]) == 3  # AG, LG, VG in Berlin

    def test_state_match(self):
        from collections import Counter

        missing = Counter(
            {
                "Oberverwaltungsgericht für das Land Schleswig-Holstein": 7,
            }
        )
        results = analyze_missing_courts(
            missing, _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        assert len(results) == 1
        r = results[0]
        assert r["type_code"] == "OVG"
        assert r["location"] == "Schleswig-Holstein"
        assert len(r["state_courts"]) == 1  # OLG-SH

    def test_no_match(self):
        from collections import Counter

        missing = Counter({"AG Nowhereville": 1})
        results = analyze_missing_courts(
            missing, _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        assert len(results) == 1
        r = results[0]
        assert r["city_courts"] == []
        assert r["state_courts"] == []

    def test_empty_missing(self):
        from collections import Counter

        results = analyze_missing_courts(
            Counter(), _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        assert results == []


# ---------------------------------------------------------------------------
# format_table
# ---------------------------------------------------------------------------


class TestFormatTable:
    def test_header(self):
        from collections import Counter

        missing = Counter({"KG Berlin": 4})
        analyses = analyze_missing_courts(
            missing, _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        output = format_table(analyses)
        assert "Missing Courts Analysis: 1 unique court name(s)" in output
        assert "from 4 error(s)" in output

    def test_contains_missing_name(self):
        from collections import Counter

        missing = Counter({"KG Berlin": 2})
        analyses = analyze_missing_courts(
            missing, _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        output = format_table(analyses)
        assert "MISSING: KG Berlin  (x2)" in output

    def test_contains_existing_courts(self):
        from collections import Counter

        missing = Counter({"KG Berlin": 1})
        analyses = analyze_missing_courts(
            missing, _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        output = format_table(analyses)
        assert "Amtsgericht Charlottenburg" in output
        assert "Existing courts at this location:" in output

    def test_no_match_message(self):
        from collections import Counter

        missing = Counter({"AG Nowhereville": 1})
        analyses = analyze_missing_courts(
            missing, _MOCK_COURTS, _MOCK_CITIES, _MOCK_STATES
        )
        output = format_table(analyses)
        assert "No matching existing courts found." in output

    def test_state_match_section(self):
        """When only state courts match (no city match), show 'in this state'."""
        from collections import Counter

        # Use courts/cities that won't trigger a city match for "Sachsen"
        courts = [
            {
                "id": 10,
                "name": "OLG Dresden",
                "code": "OLG-DD",
                "court_type": "OLG",
                "city_name": "Dresden",
                "state": 3,
            },
        ]
        states = [{"id": 3, "name": "Sachsen"}]
        missing = Counter({"Oberverwaltungsgericht des Freistaates Sachsen": 3})
        analyses = analyze_missing_courts(missing, courts, [], states)
        output = format_table(analyses)
        assert "Existing courts in this state:" in output


# ---------------------------------------------------------------------------
# format_tsv
# ---------------------------------------------------------------------------


class TestFormatTsv:
    def test_header_row(self):
        output = format_tsv([])
        assert output.startswith("name\tcount\ttype_code")

    def test_data_row(self):
        from collections import Counter

        missing = Counter({"OLG Hamm": 5})
        analyses = analyze_missing_courts(missing, [], [], [])
        output = format_tsv(analyses)
        lines = output.splitlines()
        assert len(lines) == 2  # header + 1 row
        fields = lines[1].split("\t")
        assert fields[0] == "OLG Hamm"
        assert fields[1] == "5"
        assert fields[2] == "OLG"


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------


class TestCLIAnalyzeCourts:
    def test_help_includes_analyze_courts(self):
        result = subprocess.run(
            [sys.executable, "-m", "oldp_ingestor.cli", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "analyze-courts" in result.stdout

    def test_analyze_courts_subcommand_help(self):
        result = subprocess.run(
            [sys.executable, "-m", "oldp_ingestor.cli", "analyze-courts", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--input" in result.stdout
        assert "--format" in result.stdout

    def test_analyze_courts_no_errors_in_file(self, tmp_path, monkeypatch):
        log_file = tmp_path / "test.log"
        log_file.write_text("INFO everything is fine\n")

        monkeypatch.setenv("OLDP_API_URL", "http://localhost:8000")

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "oldp_ingestor.cli",
                "analyze-courts",
                "--input",
                str(log_file),
            ],
            capture_output=True,
            text=True,
            env={
                **dict(__import__("os").environ),
                "OLDP_API_URL": "http://localhost:8000",
            },
        )
        assert result.returncode == 0
        assert "No 'court_not_found' errors found" in result.stdout
