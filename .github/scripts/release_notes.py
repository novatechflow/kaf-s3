"""
Extracts the release notes for a tag from CHANGELOG.md.

Sections below the tag that were never released are included too: batching
several unreleased versions into one tag is normal here, and a release that
mentioned only its own section would leave the rest unannounced.

Run as: release_notes.py <tag> <changelog> <output> [existing-tags...]
"""
import pathlib
import re
import subprocess
import sys

SECTION = re.compile(r"(?m)^## ")


def released_tags():
    try:
        out = subprocess.run(
            ["git", "tag", "--list"], capture_output=True, text=True, check=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return set()
    return {line.strip() for line in out.splitlines() if line.strip()}


def normalise(version):
    return version.lstrip("v")


def extract(changelog: str, tag: str, existing_tags=frozenset()) -> str:
    """
    Returns the top CHANGELOG section for the tag, plus any sections beneath it
    that have never been tagged.
    """
    sections = SECTION.split(changelog)
    if len(sections) < 2:
        raise ValueError("No version sections found in CHANGELOG.md")

    heading = sections[1].splitlines()[0].strip()
    if normalise(heading) != normalise(tag):
        raise ValueError(
            f"Tag {tag} does not match the top CHANGELOG section '{heading}'. "
            "Add a section for this release before tagging."
        )

    already_released = {normalise(t) for t in existing_tags}
    collected = []
    for section in sections[1:]:
        version = section.splitlines()[0].strip()
        if collected and normalise(version) in already_released:
            break
        collected.append("## " + section.rstrip())
    return "\n\n".join(collected)


def main(argv):
    tag, changelog_path, output_path = argv[1], argv[2], argv[3]
    existing = set(argv[4:]) or released_tags()
    notes = extract(pathlib.Path(changelog_path).read_text(), tag, existing)
    pathlib.Path(output_path).write_text(notes + "\n")
    versions = [line[3:].strip() for line in notes.splitlines() if line.startswith("## ")]
    print(f"Release notes for {tag}: {len(notes)} chars covering {', '.join(versions)}")


if __name__ == "__main__":
    try:
        main(sys.argv)
    except (ValueError, IndexError) as exc:
        sys.exit(str(exc))
