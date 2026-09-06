"""
Extracts the release notes for a tag from CHANGELOG.md.

Run as: release_notes.py <tag> <changelog> <output>
"""
import pathlib
import re
import sys

SECTION = re.compile(r"(?m)^## ")


def extract(changelog: str, tag: str) -> str:
    """
    Returns the top CHANGELOG section, verifying it belongs to the given tag.
    """
    sections = SECTION.split(changelog)
    if len(sections) < 2:
        raise ValueError("No version sections found in CHANGELOG.md")

    latest = sections[1].rstrip()
    heading = latest.splitlines()[0].strip()
    if heading.lstrip("v") != tag.lstrip("v"):
        raise ValueError(
            f"Tag {tag} does not match the top CHANGELOG section '{heading}'. "
            "Add a section for this release before tagging."
        )
    return "## " + latest


def main(argv):
    tag, changelog_path, output_path = argv[1], argv[2], argv[3]
    notes = extract(pathlib.Path(changelog_path).read_text(), tag)
    pathlib.Path(output_path).write_text(notes + "\n")
    print(f"Release notes for {tag}: {len(notes)} chars")


if __name__ == "__main__":
    try:
        main(sys.argv)
    except (ValueError, IndexError) as exc:
        sys.exit(str(exc))
