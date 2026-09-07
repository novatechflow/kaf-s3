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


def test_stops_at_the_previous_released_tag():
    notes = release_notes.extract(CHANGELOG, "v1.4.0", {"v1.3.0"})
    assert notes.startswith("## v1.4.0")
    assert "Newest entry." in notes
    assert "Older entry." not in notes


def test_untagged_sections_below_are_included():
    """Batching several unreleased versions into one tag is normal here, and
    those sections would otherwise never be announced."""
    notes = release_notes.extract(CHANGELOG, "v1.4.0", set())
    assert "Newest entry." in notes
    assert "Older entry." in notes
    assert [line for line in notes.splitlines() if line.startswith("## ")] == [
        "## v1.4.0", "## v1.3.0",
    ]


def test_notes_are_not_empty():
    """The original implementation split on a leading delimiter and returned ''."""
    notes = release_notes.extract(CHANGELOG, "v1.4.0", {"v1.3.0"})
    assert notes.strip() != "## v1.4.0"
    assert len(notes) > len("## v1.4.0")


def test_tag_without_the_v_prefix_matches():
    assert release_notes.extract(CHANGELOG, "1.4.0", {"1.3.0"}).startswith("## v1.4.0")


def test_mismatched_tag_is_rejected():
    with pytest.raises(ValueError, match="does not match"):
        release_notes.extract(CHANGELOG, "v1.4.1", set())


def test_changelog_without_sections_is_rejected():
    with pytest.raises(ValueError, match="No version sections"):
        release_notes.extract("# Changelog\n\nnothing here\n", "v1.4.0", set())


def test_chart_app_version_is_a_documented_release():
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
    # The chart's default image tag derives from appVersion, so it has to name a
    # release the changelog documents. It may trail the top section when the chart
    # alone changes.
    assert f"## v{app_version}" in changelog


def test_real_changelog_release_notes_reach_the_last_tag():
    changelog = (pathlib.Path(__file__).resolve().parents[1] / "CHANGELOG.md").read_text()
    top = next(line[3:].strip() for line in changelog.splitlines() if line.startswith("## "))
    notes = release_notes.extract(changelog, top, {"v1.2.5"})
    assert "## v1.3.0" in notes
