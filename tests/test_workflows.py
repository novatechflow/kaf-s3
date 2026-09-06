"""
The release workflows resolve a version from the git ref. Getting that wrong
publishes the wrong version, or fails the build with an opaque error, so the
shell logic is pinned here.
"""
import pathlib
import re
import subprocess

import pytest

WORKFLOWS = pathlib.Path(__file__).resolve().parents[1] / ".github" / "workflows"

RESOLVE_VERSION = """
TAG_NAME="$RELEASE_TAG"
if [ -z "$TAG_NAME" ] && [ "$GITHUB_REF_TYPE" = "tag" ]; then
  TAG_NAME="${GITHUB_REF#refs/tags/}"
fi
if [ -z "$TAG_NAME" ]; then
  exit 1
fi
echo "${TAG_NAME#v}"
"""


def resolve(ref, ref_type, release_tag=""):
    return subprocess.run(
        ["bash", "-c", RESOLVE_VERSION],
        env={"GITHUB_REF": ref, "GITHUB_REF_TYPE": ref_type, "RELEASE_TAG": release_tag,
             "PATH": "/usr/bin:/bin"},
        capture_output=True, text=True,
    )


@pytest.mark.parametrize("ref,ref_type,release_tag,expected", [
    ("refs/tags/v1.9.0", "tag", "", "1.9.0"),
    ("refs/tags/1.9.0", "tag", "", "1.9.0"),
    ("refs/tags/v1.9.0", "tag", "v1.9.0", "1.9.0"),
])
def test_tagged_runs_resolve_a_pep440_version(ref, ref_type, release_tag, expected):
    result = resolve(ref, ref_type, release_tag)
    assert result.returncode == 0
    assert result.stdout.strip() == expected


def test_dispatch_from_a_branch_fails_instead_of_building_a_junk_version():
    """It previously yielded SETUPTOOLS_SCM_PRETEND_VERSION=refs/heads/main."""
    assert resolve("refs/heads/main", "branch").returncode == 1


def test_pypi_workflow_uses_the_guarded_resolution():
    body = (WORKFLOWS / "pypi.yml").read_text()
    assert 'GITHUB_REF_TYPE" = "tag"' in body
    assert re.search(r"exit 1", body)


def test_docker_workflow_uses_the_guarded_resolution():
    body = (WORKFLOWS / "docker.yml").read_text()
    assert 'GITHUB_REF_TYPE" = "tag"' in body
