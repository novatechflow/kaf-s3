import importlib.util
import pathlib

import pytest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / ".github" / "scripts" / "release_notes.py"
spec = importlib.util.spec_from_file_location("release_notes", SCRIPT)
release_notes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release_notes)

CHANGELOG = """# Changelog

## v1.4.0
- Newest entry.
- Another line.

## v1.3.0
- Older entry.
"""


def test_extracts_the_top_section():
    notes = release_notes.extract(CHANGELOG, "v1.4.0")
    assert notes.startswith("## v1.4.0")
    assert "Newest entry." in notes
    assert "Older entry." not in notes


def test_notes_are_not_empty():
    """The previous implementation split on a leading delimiter and returned ''."""
    assert release_notes.extract(CHANGELOG, "v1.4.0").strip() != "## v1.4.0"
    assert len(release_notes.extract(CHANGELOG, "v1.4.0")) > len("## v1.4.0")


def test_tag_without_the_v_prefix_matches():
    assert release_notes.extract(CHANGELOG, "1.4.0").startswith("## v1.4.0")


def test_mismatched_tag_is_rejected():
    with pytest.raises(ValueError, match="does not match"):
        release_notes.extract(CHANGELOG, "v1.4.1")


def test_changelog_without_sections_is_rejected():
    with pytest.raises(ValueError, match="No version sections"):
        release_notes.extract("# Changelog\n\nnothing here\n", "v1.4.0")


def test_real_changelog_matches_the_chart_version():
    changelog = (pathlib.Path(__file__).resolve().parents[1] / "CHANGELOG.md").read_text()
    chart = (
        pathlib.Path(__file__).resolve().parents[1]
        / "charts" / "kaf-s3-connector" / "Chart.yaml"
    ).read_text()
    app_version = next(
        line.split(":", 1)[1].strip().strip('"')
        for line in chart.splitlines()
        if line.startswith("appVersion:")
    )
    notes = release_notes.extract(changelog, app_version)
    assert notes.strip()
