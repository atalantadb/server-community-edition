#!/usr/bin/env python3
"""Read the project release version or prepare sources for a requested version."""
import argparse
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET

NS = {"m": "http://maven.apache.org/POM/4.0.0"}
PROJECT = Path(__file__).resolve().parents[1]


def validate_version(value):
    if not re.fullmatch(r"[0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValueError("Invalid version: use up to 128 letters, digits, dots, underscores or hyphens, starting with a digit")
    return value


def release_version(project=PROJECT):
    value = ET.parse(Path(project) / "pom.xml").getroot().findtext("m:version", namespaces=NS)
    validate_version(value or "")
    # Historical Maven versions used zero-padded components (0.09.09).
    value = value.removesuffix("-ce")
    return re.sub(r"^[0-9]+(?:\.[0-9]+)*", lambda m: ".".join(str(int(n)) for n in m[0].split(".")), value)


def docker_versions(text, version):
    version = version.removesuffix("-ce")
    text = re.sub(
        r"((?:ghcr\.io/atalantadb/atalanta-ce-(?:master|rs|web-ui)|atalanta-ee-demo(?:-kdc|-ranger)?):)[A-Za-z0-9._-]+",
        lambda m: m[1] + version, text)
    return re.sub(
        r"(?m)^(\s*image:\s*['\"]?atalanta-(?:master|rs|web-ui))(?::[A-Za-z0-9._-]+)?(['\"]?[^\S\n]*)$",
        lambda m: m[1] + ":" + version + m[2], text)


def set_version(project, version, core_only=False):
    """Preserve formatting and dependency versions; validate all edits before writing."""
    project = Path(project)
    validate_version(version)
    changes = {}
    if not core_only:
        root = ET.parse(project / "pom.xml").getroot()
        old = root.findtext("m:version", namespaces=NS)
        validate_version(old or "")
        artifact_version = version
        if old.endswith("-ce") and not version.endswith("-ce"):
            artifact_version += "-ce"
        poms = [project / "pom.xml"]
        poms += sorted({project / module.text / "pom.xml" for module in root.findall(".//m:modules/m:module", NS)})
        for path in poms:
            text = path.read_text()
            parsed = ET.fromstring(text)
            element = parsed.find("m:parent/m:version", NS) if path != poms[0] else parsed.find("m:version", NS)
            if element is None or element.text != old:
                raise ValueError("Unexpected project/parent version in " + str(path))
            text, count = re.subn(r"(<version>\s*)" + re.escape(old) + r"(\s*</version>)",
                                  lambda m: m[1] + artifact_version + m[2], text, count=1)
            if count != 1:
                raise ValueError("Cannot update project version in " + str(path))
            changes[path] = text
        for relative in ("atalanta-docker-compose-ce/atalanta/docker-compose.yml",
                         "atalanta-docker-compose/atalanta/docker-compose.yml",
                         "atalanta-docker-compose/atalanta/docker-compose-5rs.yml",
                         "atalanta-docker-compose-ee-demo/docker-compose.yml",
                         "atalanta-docker-compose-ee-demo/.env.example"):
            path = project / relative
            if path.is_file():
                changes[path] = docker_versions(path.read_text(), version)
    for path in (project / "main.c", project / "adb-core/main.c"):
        if path.is_file():
            text, count = re.subn(r'(#define\s+VERSION\s+"ADB_)[^"\n]+(_(?:ENTERPRISE|COMMUNITY)_EDITION")',
                                  lambda m: m[1] + version.removesuffix("-ce") + m[2], path.read_text())
            if count != 1:
                raise ValueError("Cannot find the core version in " + str(path))
            changes[path] = text
    if core_only and not changes:
        raise ValueError("Core main.c was not found in " + str(project))
    for path, text in changes.items():
        path.write_text(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=PROJECT)
    parser.add_argument("--version", help="Update project sources to this version; otherwise print the current release version")
    parser.add_argument("--core-only", action="store_true", help="Update only native core sources in an isolated build copy")
    args = parser.parse_args()
    if args.version is not None:
        set_version(args.project, args.version, args.core_only)
    else:
        print(release_version(args.project))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, ET.ParseError) as error:
        print("ERROR: " + str(error), file=sys.stderr)
        sys.exit(1)
