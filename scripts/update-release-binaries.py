#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
Automates updating LICENSE-binary and NOTICE-binary for Apache Pinot releases.

Instead of the manual wiki process (temporarily hacking pinot-distribution/pom.xml,
generating HTML reports, copy-pasting from a browser, hacking POMs for shade plugin
NOTICE aggregation, etc.), this script:

  LICENSE-binary:
    1. Parses pinot-assembly.xml to find ALL modules shipped in the binary
    2. Runs `mvn dependency:list` on each to get the real transitive dependency set
    3. Parses the current LICENSE-binary
    4. Diffs old vs new dependencies
    5. Auto-detects licenses for new deps from Maven POM metadata
    6. Generates an updated LICENSE-binary and a human-readable report

  NOTICE-binary:
    7. Opens each dependency JAR in ~/.m2/repository
    8. Extracts META-INF/NOTICE files (replacing the shade plugin POM hack)
    9. Deduplicates and merges into a new NOTICE-binary

Prerequisites:
  - Build the full project first:
      mvn clean install -DskipTests -T1C
  - Python 3.7+

Usage:
  cd /path/to/pinot
  python3 scripts/update-release-binaries.py                  # both files (default)
  python3 scripts/update-release-binaries.py --license-only   # LICENSE-binary only
  python3 scripts/update-release-binaries.py --notice-only    # NOTICE-binary only
  python3 scripts/update-release-binaries.py --report-only    # just show diff
"""

import argparse
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


def _version_key(version: str):
    """
    Convert a version string into a tuple suitable for comparison,
    so that "10.0" > "9.0" works correctly (unlike lexicographic comparison).
    Non-numeric segments are compared as strings.
    """
    parts = re.split(r"[.\-]", version)
    key = []
    for p in parts:
        if p.isdigit():
            key.append((0, int(p)))
        else:
            key.append((1, p))
    return tuple(key)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

class Dep:
    """A Maven dependency identified by group:artifact:version."""

    def __init__(self, group_id: str, artifact_id: str, version: str):
        self.group_id = group_id
        self.artifact_id = artifact_id
        self.version = version

    def key(self) -> Tuple[str, str]:
        return (self.group_id, self.artifact_id)

    def __str__(self):
        return f"{self.group_id}:{self.artifact_id}:{self.version}"

    def __eq__(self, other):
        return isinstance(other, Dep) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))


class LicenseSection:
    """A section in LICENSE-binary: a license name + list of deps under it."""

    def __init__(self, name: str, preamble_lines: List[str], deps: List[str]):
        # name: e.g. "Apache License Version 2.0", "MIT License", "BSD 3-Clause"
        self.name = name
        # preamble_lines: header/decoration lines before the dep list
        self.preamble_lines = preamble_lines
        # deps: list of "group:artifact:version" strings
        self.deps = deps

    def __repr__(self):
        return f"LicenseSection({self.name!r}, {len(self.deps)} deps)"


# ---------------------------------------------------------------------------
# Known license classification
# ---------------------------------------------------------------------------

# Maps normalized license names/URLs from Maven POMs to LICENSE-binary section names.
# Order matters for matching: first match wins.
LICENSE_CLASSIFIERS = [
    # Apache 2.0
    (r"apache.*2", "Apache License Version 2.0"),
    (r"asf\s*2", "Apache License Version 2.0"),
    (r"apache\.org/licenses/LICENSE-2\.0", "Apache License Version 2.0"),

    # MIT
    (r"\bmit\b(?!-0)", "MIT License"),
    (r"opensource\.org/licenses/MIT", "MIT License"),

    # MIT-0
    (r"mit-0|mit.0.license", "MIT-0 License"),

    # BSD 2-Clause (must come before generic BSD)
    (r"bsd[\s._-]*2|2[\s._-]*clause.*bsd|simplified.*bsd|freebsd", "BSD 2-Clause"),

    # BSD 3-Clause (must come before generic BSD)
    (r"bsd[\s._-]*3|3[\s._-]*clause.*bsd|new.*bsd|revised.*bsd|modified.*bsd", "BSD 3-Clause"),

    # BSD (plain/unspecified) — fallback after specific variants
    (r"\bbsd\b", "BSD"),

    # CDDL 1.0
    (r"cddl.*1\.0|common.*development.*distribution.*1\.0", "Common Development and Distribution License (CDDL) 1.0"),

    # CDDL 1.1
    (r"cddl.*1\.1|common.*development.*distribution.*1\.1", "Common Development and Distribution License (CDDL) 1.1"),

    # EPL 1.0
    (r"eclipse.*public.*1\.0|epl.*1\.0", "Eclipse Public License (EPL) 1.0"),

    # EPL 2.0
    (r"eclipse.*public.*2\.0|epl.*2\.0", "Eclipse Public License (EPL) 2.0"),

    # EDL 1.0
    (r"eclipse.*distribution|edl", "Eclipse Distribution License (EDL) 1.0"),

    # LGPL
    (r"lgpl|lesser.*general.*public", "LGPL"),

    # Bouncy Castle
    (r"bouncy\s*castle", "Bouncy Castle License"),

    # ISC
    (r"\bisc\b", "ISC License"),

    # Go License
    (r"\bgo\b.*license|golang", "The Go License"),

    # WTFPL
    (r"wtfpl|do what the fuck", "WTFPL License"),

    # Public Domain
    (r"public\s*domain", "Public Domain"),
]


def classify_license(license_name: str, license_url: str = "") -> Optional[str]:
    """Map a POM license name/URL to a LICENSE-binary section name."""
    combined = f"{license_name} {license_url}".lower()
    for pattern, section_name in LICENSE_CLASSIFIERS:
        if re.search(pattern, combined):
            return section_name
    return None


# ---------------------------------------------------------------------------
# Step 1: Parse assembly descriptor for shipped modules
# ---------------------------------------------------------------------------

def parse_assembly_modules(pinot_root: Path) -> List[Path]:
    """Parse pinot-assembly.xml to find all module directories shipped in the binary."""
    assembly_file = pinot_root / "pinot-distribution" / "pinot-assembly.xml"
    if not assembly_file.exists():
        print(f"ERROR: Assembly file not found: {assembly_file}", file=sys.stderr)
        sys.exit(1)

    tree = ET.parse(assembly_file)
    root = tree.getroot()

    modules = set()

    # Walk all elements looking for <source> tags with ${pinot.root}/.../target/ paths
    for elem in root.iter():
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if tag == "source" and elem.text:
            source = elem.text.strip()
            match = re.search(r"\$\{pinot\.root\}/(.+?)/target/", source)
            if match:
                module_path = match.group(1)
                full_path = pinot_root / module_path
                if full_path.exists() and (full_path / "pom.xml").exists():
                    modules.add(full_path)

    # Always include pinot-distribution itself (its deps: pinot-tools, pinot-jdbc-client)
    dist_path = pinot_root / "pinot-distribution"
    if dist_path.exists():
        modules.add(dist_path)

    sorted_modules = sorted(modules)
    print(f"Found {len(sorted_modules)} modules in assembly descriptor:")
    for m in sorted_modules:
        print(f"  {m.relative_to(pinot_root)}")
    print()
    return sorted_modules


# ---------------------------------------------------------------------------
# Step 2: Get Maven artifact IDs for modules
# ---------------------------------------------------------------------------

def get_maven_artifact_id(module_dir: Path) -> Optional[str]:
    """Read pom.xml to extract artifactId."""
    pom_file = module_dir / "pom.xml"
    if not pom_file.exists():
        return None

    tree = ET.parse(pom_file)
    root = tree.getroot()

    # Handle Maven namespace
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    elem = root.find("m:artifactId", ns)
    if elem is None:
        elem = root.find("artifactId")
    return elem.text.strip() if elem is not None and elem.text else None


# ---------------------------------------------------------------------------
# Step 3: Run mvn dependency:list
# ---------------------------------------------------------------------------

def run_dependency_list(pinot_root: Path, module_dirs: List[Path]) -> Set[Dep]:
    """Run mvn dependency:list on all shipped modules; return merged dep set."""
    artifact_ids = []
    for mod_dir in module_dirs:
        aid = get_maven_artifact_id(mod_dir)
        if aid:
            artifact_ids.append(aid)
        else:
            print(f"  WARNING: Could not read artifactId from {mod_dir}/pom.xml",
                  file=sys.stderr)

    if not artifact_ids:
        print("ERROR: No module artifact IDs found", file=sys.stderr)
        sys.exit(1)

    pl_arg = ",".join(f":{aid}" for aid in artifact_ids)

    output_file = pinot_root / "target" / "license-check-deps.txt"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists():
        output_file.unlink()

    cmd = [
        "mvn", "dependency:list",
        f"-pl", pl_arg,
        "-DincludeScope=runtime",
        "-DexcludeGroupIds=org.apache.pinot",
        f"-DoutputFile={output_file}",
        "-DappendOutput=true",
        "-DoutputAbsoluteArtifactFilename=false",
    ]

    print(f"Running mvn dependency:list for {len(artifact_ids)} modules...")
    print(f"  This may take a few minutes...\n")

    result = subprocess.run(
        cmd, cwd=pinot_root, capture_output=True, text=True, timeout=600
    )

    if result.returncode != 0:
        print("ERROR: Maven command failed.", file=sys.stderr)
        # Show last part of output for debugging
        stderr_tail = result.stderr[-3000:] if len(result.stderr) > 3000 else result.stderr
        stdout_tail = result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout
        if stderr_tail.strip():
            print(f"STDERR:\n{stderr_tail}", file=sys.stderr)
        if stdout_tail.strip():
            print(f"STDOUT:\n{stdout_tail}", file=sys.stderr)
        sys.exit(1)

    # Parse the output file
    deps = set()
    if not output_file.exists():
        print("ERROR: Maven did not produce output file", file=sys.stderr)
        sys.exit(1)

    with open(output_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) == 5:
                group_id, artifact_id, _pkg_type, version, scope = parts
            elif len(parts) == 6:
                group_id, artifact_id, _pkg_type, _classifier, version, scope = parts
            else:
                continue

            scope = scope.strip().lower()
            if scope in ("test", "provided", "system"):
                continue

            deps.add(Dep(group_id.strip(), artifact_id.strip(), version.strip()))

    print(f"Found {len(deps)} unique non-Pinot dependencies from Maven\n")
    return deps


# ---------------------------------------------------------------------------
# Step 4: Parse current LICENSE-binary
# ---------------------------------------------------------------------------

# Regex matching a dependency line: group.id:artifact-id:version
DEP_LINE_RE = re.compile(
    r"^[a-zA-Z][a-zA-Z0-9._-]*:[a-zA-Z][a-zA-Z0-9._-]*:[a-zA-Z0-9._+-]+$"
)


def parse_license_binary(license_file: Path) -> Tuple[List[str], List[LicenseSection], Dict[str, str]]:
    """
    Parse LICENSE-binary into:
      - file_lines: all original lines (for reconstruction)
      - sections: list of LicenseSection objects
      - dep_to_section: maps "group:artifact:version" -> section name

    Returns (file_lines, sections, dep_to_section)
    """
    with open(license_file) as f:
        lines = f.read().split("\n")

    sections: List[LicenseSection] = []
    dep_to_section: Dict[str, str] = {}

    # --- Identify the Apache 2.0 section ---
    # Find the end of the Apache License text, then the first separator after it
    license_text_end = None
    for i, line in enumerate(lines):
        if "END OF TERMS AND CONDITIONS" in line:
            license_text_end = i
            break

    if license_text_end is None:
        print("ERROR: Cannot find 'END OF TERMS AND CONDITIONS' in LICENSE-binary",
              file=sys.stderr)
        sys.exit(1)

    first_sep = None
    for i in range(license_text_end, len(lines)):
        if re.match(r"^-{20,}$", lines[i]):
            first_sep = i
            break

    if first_sep is None:
        print("ERROR: Cannot find first separator in LICENSE-binary", file=sys.stderr)
        sys.exit(1)

    # Find the second separator (ends Apache 2.0 section)
    second_sep = None
    for i in range(first_sep + 1, len(lines)):
        if re.match(r"^-{20,}$", lines[i]):
            second_sep = i
            break

    # Apache 2.0 section: between first_sep and second_sep
    apache_preamble = []
    apache_deps = []
    for i in range(first_sep + 1, second_sep if second_sep else len(lines)):
        line = lines[i].strip()
        if DEP_LINE_RE.match(line):
            apache_deps.append(line)
        elif line:
            apache_preamble.append(lines[i])

    sections.append(LicenseSection(
        name="Apache License Version 2.0",
        preamble_lines=apache_preamble,
        deps=sorted(apache_deps),
    ))
    for d in apache_deps:
        dep_to_section[d] = "Apache License Version 2.0"

    # --- Parse remaining sections ---
    if second_sep is None:
        return lines, sections, dep_to_section

    # Scan from second_sep onward for license section headers
    # Strategy: a section header is a non-empty, non-dep line followed by a line of dashes
    # or it's the section intro text. We detect sections by their dash-underlines.
    i = second_sep + 1
    # Skip the intro paragraph
    current_section_name = None
    current_preamble: List[str] = []
    current_deps: List[str] = []

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Check if this line is a dash-underline (indicating previous line was a header)
        if re.match(r"^-{3,}$", stripped) and i > 0:
            # The previous non-blank line was the section header name
            # Save current section if we have one
            if current_section_name is not None:
                sections.append(LicenseSection(
                    name=current_section_name,
                    preamble_lines=current_preamble,
                    deps=sorted(current_deps),
                ))
                for d in current_deps:
                    dep_to_section[d] = current_section_name

            # Find the header: walk backward from dash line to find the header text
            header_line = ""
            for j in range(i - 1, second_sep, -1):
                if lines[j].strip():
                    header_line = lines[j].strip()
                    break

            current_section_name = header_line
            current_preamble = []
            current_deps = []
            i += 1
            continue

        # If we're in a section, classify the line
        if current_section_name is not None:
            if DEP_LINE_RE.match(stripped):
                current_deps.append(stripped)
            elif stripped:
                current_preamble.append(line)
        i += 1

    # Don't forget the last section
    if current_section_name is not None and (current_deps or current_preamble):
        sections.append(LicenseSection(
            name=current_section_name,
            preamble_lines=current_preamble,
            deps=sorted(current_deps),
        ))
        for d in current_deps:
            dep_to_section[d] = current_section_name

    return lines, sections, dep_to_section


# ---------------------------------------------------------------------------
# Step 5: Compute diff
# ---------------------------------------------------------------------------

def compute_diff(
    old_sections: List[LicenseSection],
    new_deps: Set[Dep],
) -> Tuple[List[Dep], List[str], List[Tuple[str, str, str]]]:
    """
    Compare old LICENSE-binary deps with new Maven deps.

    Returns:
      - added: list of Dep objects not in old
      - removed: list of "group:artifact:version" strings not in new
      - changed: list of (group:artifact, old_version, new_version) tuples
    """
    # Build maps from old sections
    old_by_key: Dict[Tuple[str, str], str] = {}  # (group, artifact) -> version
    old_dep_strings: Set[str] = set()
    for section in old_sections:
        for dep_str in section.deps:
            parts = dep_str.split(":")
            if len(parts) == 3:
                g, a, v = parts
                old_by_key[(g, a)] = v
                old_dep_strings.add(dep_str)

    # Build map from new deps
    new_by_key: Dict[Tuple[str, str], str] = {}
    for dep in new_deps:
        key = dep.key()
        # If multiple versions exist (shouldn't happen but just in case), pick the latest
        if key not in new_by_key or _version_key(dep.version) > _version_key(new_by_key[key]):
            new_by_key[key] = dep.version

    old_keys = set(old_by_key.keys())
    new_keys = set(new_by_key.keys())

    # Added: in new but not in old
    added = []
    for key in sorted(new_keys - old_keys):
        added.append(Dep(key[0], key[1], new_by_key[key]))

    # Removed: in old but not in new
    removed = []
    for key in sorted(old_keys - new_keys):
        removed.append(f"{key[0]}:{key[1]}:{old_by_key[key]}")

    # Version changed: in both but different version
    changed = []
    for key in sorted(old_keys & new_keys):
        old_ver = old_by_key[key]
        new_ver = new_by_key[key]
        if old_ver != new_ver:
            changed.append((f"{key[0]}:{key[1]}", old_ver, new_ver))

    return added, removed, changed


# ---------------------------------------------------------------------------
# Step 6: Detect licenses for deps (new and version-bumped)
# ---------------------------------------------------------------------------

def detect_license_from_pom(dep: Dep, m2_repo: Path) -> Optional[str]:
    """Look up the license from the dependency's POM in the local Maven repo."""
    group_path = dep.group_id.replace(".", "/")
    pom_path = m2_repo / group_path / dep.artifact_id / dep.version / f"{dep.artifact_id}-{dep.version}.pom"

    if not pom_path.exists():
        return None

    try:
        tree = ET.parse(pom_path)
        root = tree.getroot()
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}

        licenses_elem = root.find("m:licenses", ns)
        if licenses_elem is None:
            licenses_elem = root.find("licenses")
        if licenses_elem is None:
            return None

        # Collect all license names and URLs
        names = []
        urls = []
        for lic in licenses_elem:
            tag_name = lic.tag.split("}")[-1] if "}" in lic.tag else lic.tag
            if tag_name == "license":
                name_elem = lic.find("{http://maven.apache.org/POM/4.0.0}name")
                if name_elem is None:
                    name_elem = lic.find("name")
                url_elem = lic.find("{http://maven.apache.org/POM/4.0.0}url")
                if url_elem is None:
                    url_elem = lic.find("url")

                if name_elem is not None and name_elem.text:
                    names.append(name_elem.text.strip())
                if url_elem is not None and url_elem.text:
                    urls.append(url_elem.text.strip())

        # Try to classify using each name/url pair
        for name in names:
            result = classify_license(name, " ".join(urls))
            if result:
                return result

        for url in urls:
            result = classify_license("", url)
            if result:
                return result

        # Return raw name if we couldn't classify
        if names:
            return f"UNKNOWN ({names[0]})"

    except ET.ParseError:
        pass

    return None


def check_version_bump_license_changes(
    changed: List[Tuple[str, str, str]],
    dep_to_section: Dict[str, str],
    m2_repo: Path,
) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str, str, str, str]]]:
    """
    For version-bumped deps, re-check the POM license of the NEW version
    and compare to the section the OLD version is currently in.

    Returns:
      - safe_changed: version bumps where the license stayed the same
      - license_changed: tuples of (group:artifact, old_ver, new_ver, old_section, new_license)
    """
    safe_changed = []
    license_changed = []

    for ga, old_ver, new_ver in changed:
        parts = ga.split(":")
        if len(parts) != 2:
            safe_changed.append((ga, old_ver, new_ver))
            continue

        g, a = parts
        old_dep_str = f"{ga}:{old_ver}"
        old_section = dep_to_section.get(old_dep_str)

        new_dep = Dep(g, a, new_ver)
        new_license = detect_license_from_pom(new_dep, m2_repo)

        if (new_license and old_section
                and not new_license.startswith("UNKNOWN")
                and new_license != old_section):
            license_changed.append((ga, old_ver, new_ver, old_section, new_license))
        else:
            safe_changed.append((ga, old_ver, new_ver))

    return safe_changed, license_changed


# ---------------------------------------------------------------------------
# Step 6b: Detect orphaned license files in licenses-binary/
# ---------------------------------------------------------------------------

def find_orphaned_license_files(
    pinot_root: Path,
    sections: List[LicenseSection],
    removed: List[str],
    added: List[Dep],
    changed: List[Tuple[str, str, str]],
    license_changed: List[Tuple[str, str, str, str, str]],
    license_map: Dict[str, str],
) -> Tuple[List[str], List[str]]:
    """
    After applying all changes, check which license sections will be empty
    and which license files in licenses-binary/ are no longer referenced.

    Returns:
      - empty_sections: section names that will have 0 deps after updates
      - orphaned_files: filenames in licenses-binary/ not referenced by LICENSE-binary
    """
    licenses_dir = pinot_root / "licenses-binary"

    # Compute the final dep count per section after applying all changes
    removed_set = set(removed)
    # Also remove deps whose license changed (they'll move to a different section)
    for ga, old_ver, _new_ver, _old_section, _new_license in license_changed:
        removed_set.add(f"{ga}:{old_ver}")

    section_dep_counts: Dict[str, int] = {}
    for section in sections:
        remaining = [d for d in section.deps if d not in removed_set]
        section_dep_counts[section.name] = len(remaining)

    # Account for new deps being added to sections
    for dep in added:
        lic = license_map.get(str(dep))
        if lic and not lic.startswith("UNKNOWN"):
            section_dep_counts[lic] = section_dep_counts.get(lic, 0) + 1

    # Account for license-changed deps moving to new sections
    for ga, _old_ver, new_ver, _old_section, new_license in license_changed:
        section_dep_counts[new_license] = section_dep_counts.get(new_license, 0) + 1

    # Sections that have preamble content (e.g., "(see licenses/...)") are kept even
    # with zero deps — they serve as license-text placeholders for their dep category.
    sections_with_preamble = {
        s.name for s in sections if s.preamble_lines
    }
    empty_sections = [
        name for name, count in section_dep_counts.items()
        if count == 0
        and name != "Apache License Version 2.0"  # never remove the main license
        and name not in sections_with_preamble  # keep sections that reference license files
    ]

    # Find license files referenced in LICENSE-binary via "(see licenses/...)" lines
    # and by implicit association with non-empty sections
    referenced_files: Set[str] = set()
    license_file_path = pinot_root / "LICENSE-binary"
    if license_file_path.exists():
        with open(license_file_path) as f:
            content = f.read()
        # Find explicit references like (see licenses/LICENSE-xxx.txt)
        for match in re.finditer(r"licenses/([A-Za-z0-9._-]+)", content):
            referenced_files.add(match.group(1))

    # Check which files in licenses-binary/ are not referenced
    orphaned_files = []
    if licenses_dir.exists():
        for f in sorted(licenses_dir.iterdir()):
            if f.is_file() and f.name not in referenced_files:
                orphaned_files.append(f.name)

    return empty_sections, orphaned_files


# ---------------------------------------------------------------------------
# Step 7: Generate report
# ---------------------------------------------------------------------------

def print_report(
    added: List[Dep],
    removed: List[str],
    changed: List[Tuple[str, str, str]],
    license_changed: List[Tuple[str, str, str, str, str]],
    license_map: Dict[str, str],
    empty_sections: List[str],
    orphaned_files: List[str],
):
    """Print a human-readable diff report."""
    print("=" * 78)
    print("LICENSE-BINARY UPDATE REPORT")
    print("=" * 78)

    if (not added and not removed and not changed and not license_changed
            and not empty_sections and not orphaned_files):
        print("\nNo changes detected. LICENSE-binary is up to date.")
        return

    # Summary
    print(f"\n  Added:               {len(added)}")
    print(f"  Removed:             {len(removed)}")
    print(f"  Version changed:     {len(changed)}")
    print(f"  License changed:     {len(license_changed)}")
    print(f"  Empty sections:      {len(empty_sections)}")
    print(f"  Orphaned files:      {len(orphaned_files)}")
    print()

    # Removed
    if removed:
        print("-" * 78)
        print("REMOVED (delete these lines from LICENSE-binary):")
        print("-" * 78)
        for dep_str in removed:
            print(f"  - {dep_str}")
        print()

    # Version changes (license unchanged — safe to update in place)
    if changed:
        print("-" * 78)
        print("VERSION CHANGED (update version numbers — license unchanged):")
        print("-" * 78)
        for ga, old_v, new_v in changed:
            print(f"  {ga}")
            print(f"    {old_v}  -->  {new_v}")
        print()

    # License changed on version bump — needs manual attention
    if license_changed:
        print("-" * 78)
        print("LICENSE CHANGED ON VERSION BUMP (must move to different section):")
        print("-" * 78)
        for ga, old_v, new_v, old_section, new_license in license_changed:
            print(f"  {ga}")
            print(f"    {old_v}  -->  {new_v}")
            print(f"    MOVE: [{old_section}] --> [{new_license}]")
        print()

    # Added, grouped by detected license
    if added:
        print("-" * 78)
        print("ADDED (new deps to add to LICENSE-binary):")
        print("-" * 78)

        by_license: Dict[str, List[Dep]] = OrderedDict()
        unknown = []
        for dep in added:
            dep_str = str(dep)
            lic = license_map.get(dep_str)
            if lic and not lic.startswith("UNKNOWN"):
                by_license.setdefault(lic, []).append(dep)
            else:
                unknown.append((dep, lic))

        # Flag which sections are NEW (don't exist in LICENSE-binary yet)
        existing_sections = {s for s in license_map.values()
                            if s and not s.startswith("UNKNOWN")}

        for lic_name, deps in sorted(by_license.items()):
            new_marker = " ** NEW SECTION NEEDED **" if lic_name not in existing_sections else ""
            print(f"\n  [{lic_name}]{new_marker}")
            for dep in sorted(deps, key=str):
                print(f"    + {dep}")

        if unknown:
            print(f"\n  [REQUIRES MANUAL LICENSE LOOKUP]")
            print(f"  (Visit the project's website to determine the license,")
            print(f"   then add to the appropriate section. If it's a new license")
            print(f"   type, create a new file in licenses-binary/ and a new section.)")
            for dep, raw_lic in sorted(unknown, key=lambda x: str(x[0])):
                hint = f"  (POM says: {raw_lic})" if raw_lic else ""
                print(f"    ? {dep}{hint}")
        print()

    # Empty sections — may want to remove from LICENSE-binary
    if empty_sections:
        print("-" * 78)
        print("EMPTY SECTIONS (consider removing from LICENSE-binary):")
        print("-" * 78)
        for section_name in sorted(empty_sections):
            print(f"  - {section_name}")
        print()

    # Orphaned license files
    if orphaned_files:
        print("-" * 78)
        print("ORPHANED LICENSE FILES (not referenced in LICENSE-binary — consider deleting):")
        print("-" * 78)
        for filename in orphaned_files:
            print(f"  - licenses-binary/{filename}")
        print()


# ---------------------------------------------------------------------------
# Step 8: Generate updated LICENSE-binary
# ---------------------------------------------------------------------------

def generate_updated_license_binary(
    license_file: Path,
    sections: List[LicenseSection],
    added: List[Dep],
    removed: List[str],
    changed: List[Tuple[str, str, str]],
    license_changed: List[Tuple[str, str, str, str, str]],
    license_map: Dict[str, str],
    output_file: Path,
):
    """Generate an updated LICENSE-binary file."""
    # Read original file
    with open(license_file) as f:
        original = f.read()

    # Build removed set and change map for quick lookup
    removed_set = set(removed)
    change_map = {}  # "group:artifact" -> new_version
    for ga, _old_v, new_v in changed:
        change_map[ga] = new_v

    # License-changed deps: remove old version from old section, add new version to new section
    for ga, old_ver, new_ver, _old_section, new_license in license_changed:
        removed_set.add(f"{ga}:{old_ver}")

    # Build a map from section name -> deps to add
    added_by_section: Dict[str, List[str]] = {}
    unclassified: List[Dep] = []
    for dep in added:
        dep_str = str(dep)
        lic = license_map.get(dep_str)
        if lic and not lic.startswith("UNKNOWN"):
            added_by_section.setdefault(lic, []).append(dep_str)
        else:
            unclassified.append(dep)

    # Also add license-changed deps to their new sections
    for ga, _old_ver, new_ver, _old_section, new_license in license_changed:
        added_by_section.setdefault(new_license, []).append(f"{ga}:{new_ver}")

    # Process the file line by line
    lines = original.split("\n")
    output_lines = []
    current_section_name = None

    # Track section boundaries so we can insert new deps at end of section
    # We'll buffer dep lines per section and flush them sorted
    in_dep_block = False
    dep_buffer: List[str] = []
    section_for_buffer: Optional[str] = None

    def flush_dep_buffer():
        """Sort and write buffered deps, including any additions for this section."""
        nonlocal dep_buffer, section_for_buffer
        if section_for_buffer and section_for_buffer in added_by_section:
            dep_buffer.extend(added_by_section.pop(section_for_buffer))
        dep_buffer.sort()
        for d in dep_buffer:
            output_lines.append(d)
        dep_buffer = []
        section_for_buffer = None

    i = 0
    # We need to track which section we're in based on the parsed sections
    section_names = {s.name for s in sections}

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Detect section header (line of dashes following a non-empty line)
        if re.match(r"^-{3,}$", stripped) and i > 0:
            # Check if previous non-blank line is a section name
            prev_text = ""
            for j in range(i - 1, max(i - 5, -1), -1):
                if lines[j].strip():
                    prev_text = lines[j].strip()
                    break

            if prev_text in section_names:
                # We're entering a new section; flush previous section's deps
                if in_dep_block:
                    flush_dep_buffer()
                    in_dep_block = False

                current_section_name = prev_text
                output_lines.append(line)
                i += 1
                in_dep_block = True
                section_for_buffer = current_section_name
                continue

        # Special case: Apache 2.0 section starts after first separator
        # Detect by "This project bundles" text
        if "This project bundles some components" in stripped:
            if in_dep_block:
                flush_dep_buffer()
            current_section_name = "Apache License Version 2.0"
            in_dep_block = True
            section_for_buffer = current_section_name
            output_lines.append(line)
            i += 1
            continue

        # If we hit a separator line while in a dep block, flush and exit block
        if re.match(r"^-{20,}$", stripped) and in_dep_block:
            flush_dep_buffer()
            in_dep_block = False
            current_section_name = None
            output_lines.append(line)
            i += 1
            continue

        # Process dep lines
        if in_dep_block and DEP_LINE_RE.match(stripped):
            # Check if removed
            if stripped in removed_set:
                i += 1
                continue  # skip this line

            # Check if version changed
            parts = stripped.split(":")
            if len(parts) == 3:
                ga = f"{parts[0]}:{parts[1]}"
                if ga in change_map:
                    new_dep_str = f"{ga}:{change_map[ga]}"
                    dep_buffer.append(new_dep_str)
                    i += 1
                    continue

            dep_buffer.append(stripped)
            i += 1
            continue

        # If we were in a dep block and hit a non-dep, non-blank line
        # that's not a section header, this might be a note line (e.g., "(see licenses/...)")
        # or we've exited the dep area
        if in_dep_block and stripped and not DEP_LINE_RE.match(stripped):
            # Check if this could be a section header (next line might be dashes)
            if i + 1 < len(lines) and re.match(r"^-{3,}$", lines[i + 1].strip()):
                # It's a new section header; flush current deps
                flush_dep_buffer()
                in_dep_block = False
                current_section_name = None
                output_lines.append(line)
                i += 1
                continue
            else:
                # It's a note/preamble line within the section; flush deps first
                # then add this line
                if dep_buffer:
                    flush_dep_buffer()
                    in_dep_block = False
                output_lines.append(line)
                i += 1
                continue

        # Skip blank lines that trail deps in a section — the post-processor
        # re-inserts proper spacing between sections.  Without this, blank
        # lines from the original file (between the last dep and the next
        # section header) end up before the flushed deps, creating
        # double-spacing.  We only skip when the buffer already has deps;
        # blank lines before any deps (e.g., after a preamble like
        # "(see licenses/...)") are preserved.
        if in_dep_block and not stripped and dep_buffer:
            i += 1
            continue

        # Default: pass through
        output_lines.append(line)
        i += 1

    # Final flush
    if in_dep_block:
        flush_dep_buffer()

    # Handle unclassified deps: add a TODO section at the end
    if unclassified or added_by_section:
        output_lines.append("")
        if added_by_section:
            # Sections that weren't matched to existing sections in the file
            for section_name, deps in sorted(added_by_section.items()):
                output_lines.append("")
                output_lines.append(f"TODO: Add to [{section_name}] section:")
                for d in sorted(deps):
                    output_lines.append(d)

        if unclassified:
            output_lines.append("")
            output_lines.append("TODO: The following new deps need manual license lookup:")
            for dep in sorted(unclassified, key=str):
                output_lines.append(f"  {dep}")

    # Post-process: ensure blank line separators between sections.
    # A section boundary is a non-blank, non-dep line followed by a line of dashes.
    # We need at least 2 blank lines before each section header.
    final_lines: List[str] = []
    for idx, line in enumerate(output_lines):
        is_section_header = (
            line.strip()
            and not DEP_LINE_RE.match(line.strip())
            and idx + 1 < len(output_lines)
            and re.match(r"^-{3,}$", output_lines[idx + 1].strip())
        )
        if is_section_header and final_lines:
            # Count existing trailing blank lines
            trailing_blanks = 0
            for prev in reversed(final_lines):
                if prev.strip() == "":
                    trailing_blanks += 1
                else:
                    break
            # Ensure at least 2 blank lines before the section header
            while trailing_blanks < 2:
                final_lines.append("")
                trailing_blanks += 1
        final_lines.append(line)
    output_lines = final_lines

    content = "\n".join(output_lines)
    # Clean up excessive blank lines (more than 2 consecutive)
    content = re.sub(r"\n{4,}", "\n\n\n", content)
    # Ensure trailing newline
    if not content.endswith("\n"):
        content += "\n"

    with open(output_file, "w") as f:
        f.write(content)

    print(f"Updated LICENSE-binary written to: {output_file}")


# ---------------------------------------------------------------------------
# Step 9: Generate NOTICE-binary
# ---------------------------------------------------------------------------

_DEFAULT_NOTICE_HEADER = """\
This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).

// ------------------------------------------------------------------
// NOTICE file corresponding to the section 4d of The Apache License,
// Version 2.0, in this case for
// ------------------------------------------------------------------
"""


def _read_notice_header(notice_file: Path, num_lines: int = 7) -> str:
    """Read the header from an existing NOTICE-binary file.

    Falls back to a built-in default if the file doesn't exist.
    """
    if not notice_file.exists():
        return _DEFAULT_NOTICE_HEADER
    with open(notice_file) as f:
        lines = [f.readline() for _ in range(num_lines)]
    return "".join(lines)


def extract_notices_from_jars(deps: Set[Dep], m2_repo: Path) -> List[Tuple[str, str]]:
    """
    For each dependency, open its JAR in ~/.m2/repository and extract
    META-INF/NOTICE (or META-INF/NOTICE.txt) if present.

    Returns list of (dep_str, notice_text) tuples.
    """
    notices = []
    checked = 0
    found = 0

    for dep in sorted(deps, key=str):
        group_path = dep.group_id.replace(".", "/")
        jar_path = (
            m2_repo / group_path / dep.artifact_id / dep.version
            / f"{dep.artifact_id}-{dep.version}.jar"
        )

        if not jar_path.exists():
            continue

        checked += 1
        try:
            with zipfile.ZipFile(jar_path, "r") as zf:
                notice_text = None
                # Look for NOTICE files (case-insensitive, with or without .txt)
                for name in zf.namelist():
                    basename = name.lower()
                    if basename in (
                        "meta-inf/notice",
                        "meta-inf/notice.txt",
                        "meta-inf/notice.md",
                    ):
                        notice_text = zf.read(name).decode("utf-8", errors="replace")
                        break

                if notice_text and notice_text.strip():
                    # Skip notices that are just the generic ASF boilerplate
                    # (many Apache projects have identical trivial notices)
                    notices.append((str(dep), notice_text.strip()))
                    found += 1
        except (zipfile.BadZipFile, KeyError, OSError):
            pass

    print(f"  Scanned {checked} JARs, found {found} with NOTICE files")
    return notices


def strip_asf_boilerplate(text: str) -> str:
    """
    Strip the common Apache Software Foundation boilerplate that appears in
    most Apache project NOTICE files. The ApacheNoticeResourceTransformer
    puts this once at the top; we do the same.

    Strips lines matching patterns like:
      - "This product includes software developed at"
      - "This product includes software developed by"
      - "The Apache Software Foundation (http://www.apache.org/)."
    """
    lines = text.split("\n")
    filtered = []
    skip_next_blank = False
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Check for the 2-line ASF boilerplate pattern
        if re.match(r"^This product includes software developed (at|by)$", line):
            # Check if next line is the ASF URL line
            if (i + 1 < len(lines)
                    and re.match(r"^The Apache Software Foundation", lines[i + 1].strip())):
                i += 2
                # Skip trailing blank lines after the boilerplate
                while i < len(lines) and not lines[i].strip():
                    i += 1
                continue

        # Check for the single-line variant
        if re.match(
            r"^This product includes software developed (at|by)\s+"
            r"The Apache Software Foundation",
            line,
        ):
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        filtered.append(lines[i])
        i += 1

    # Strip leading/trailing blank lines
    result = "\n".join(filtered).strip()
    return result


def deduplicate_notices(notices: List[Tuple[str, str]]) -> str:
    """
    Deduplicate and concatenate NOTICE texts.

    Uses two strategies:
    1. Exact dedup after whitespace normalization (catches identical copies)
    2. Title-based dedup: if two notices share the same "title" (first
       non-blank line, e.g. "# Jackson JSON processor"), keep only the
       longest version (catches Jackson-style near-duplicates where each
       module ships a slightly different variant)
    """
    seen_normalized: Set[str] = set()
    by_title: Dict[str, Tuple[str, str]] = OrderedDict()  # title -> (dep, longest_text)

    for dep_str, raw_text in notices:
        text = strip_asf_boilerplate(raw_text)
        if not text:
            continue

        # Exact dedup
        normalized = re.sub(r"\s+", " ", text)
        if normalized in seen_normalized:
            continue
        seen_normalized.add(normalized)

        # Title-based dedup: extract first non-blank line as "title"
        title = ""
        for line in text.split("\n"):
            stripped = line.strip().lstrip("#").strip()
            if stripped:
                title = stripped
                break

        if title and title in by_title:
            # Keep the longer version (more complete notice)
            existing_text = by_title[title][1]
            if len(text) > len(existing_text):
                by_title[title] = (dep_str, text)
        else:
            key = title if title else dep_str
            by_title[key] = (dep_str, text)

    return "\n\n".join(text for _, text in by_title.values())


def generate_notice_binary(
    deps: Set[Dep],
    m2_repo: Path,
    output_file: Path,
    existing_notice_file: Optional[Path] = None,
):
    """Generate NOTICE-binary by extracting and merging NOTICE files from dependency JARs."""
    print("\nGenerating NOTICE-binary...")
    notices = extract_notices_from_jars(deps, m2_repo)

    if not notices:
        print("  WARNING: No NOTICE files found in any dependency JARs.")
        print("  Make sure the project has been built (mvn install) first.")
        return

    header = _read_notice_header(existing_notice_file) if existing_notice_file else _DEFAULT_NOTICE_HEADER
    merged = deduplicate_notices(notices)
    content = header + "\n" + merged + "\n"

    with open(output_file, "w") as f:
        f.write(content)

    print(f"  NOTICE-binary written to: {output_file}")
    print(f"  Contains notices from {len(notices)} dependencies "
          f"({len(set(t for _, t in notices))} unique)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Update LICENSE-binary and NOTICE-binary for Apache Pinot releases"
    )
    parser.add_argument(
        "--pinot-root",
        type=Path,
        default=None,
        help="Path to pinot repository root (auto-detected if not set)",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only print the diff report; don't generate updated files",
    )

    scope_group = parser.add_mutually_exclusive_group()
    scope_group.add_argument(
        "--license-only",
        action="store_true",
        help="Only update LICENSE-binary (skip NOTICE-binary)",
    )
    scope_group.add_argument(
        "--notice-only",
        action="store_true",
        help="Only update NOTICE-binary (skip LICENSE-binary)",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path for updated LICENSE-binary (default: LICENSE-binary.updated)",
    )
    parser.add_argument(
        "--notice-output",
        type=Path,
        default=None,
        help="Output path for updated NOTICE-binary (default: NOTICE-binary.updated)",
    )
    parser.add_argument(
        "--m2-repo",
        type=Path,
        default=Path.home() / ".m2" / "repository",
        help="Path to local Maven repository (default: ~/.m2/repository)",
    )
    parser.add_argument(
        "--skip-maven",
        type=Path,
        default=None,
        help="Skip Maven invocation; read deps from this file instead "
             "(format: one group:artifact:type:version:scope per line)",
    )
    args = parser.parse_args()

    update_license = not args.notice_only
    update_notice = not args.license_only

    # Determine pinot root
    if args.pinot_root:
        pinot_root = args.pinot_root.resolve()
    else:
        # Try to detect from script location or cwd
        script_dir = Path(__file__).resolve().parent
        candidate = script_dir.parent
        if (candidate / "LICENSE-binary").exists():
            pinot_root = candidate
        elif (Path.cwd() / "LICENSE-binary").exists():
            pinot_root = Path.cwd()
        else:
            print("ERROR: Cannot detect pinot root. Use --pinot-root.", file=sys.stderr)
            sys.exit(1)

    license_file = pinot_root / "LICENSE-binary"
    if not license_file.exists():
        print(f"ERROR: {license_file} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Pinot root: {pinot_root}")
    print(f"Updating: ", end="")
    if update_license and update_notice:
        print("LICENSE-binary + NOTICE-binary")
    elif update_license:
        print("LICENSE-binary only")
    else:
        print("NOTICE-binary only")
    print()

    # Step 1: Parse assembly to find shipped modules
    modules = parse_assembly_modules(pinot_root)

    # Step 2-3: Get new deps from Maven
    if args.skip_maven:
        print(f"Reading deps from {args.skip_maven}...")
        new_deps = set()
        with open(args.skip_maven) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(":")
                if len(parts) == 5:
                    # group:artifact:type:version:scope
                    g, a, v = parts[0], parts[1], parts[3]
                    new_deps.add(Dep(g.strip(), a.strip(), v.strip()))
                elif len(parts) == 6:
                    # group:artifact:type:classifier:version:scope
                    g, a, v = parts[0], parts[1], parts[4]
                    new_deps.add(Dep(g.strip(), a.strip(), v.strip()))
        print(f"Read {len(new_deps)} deps from file\n")
    else:
        new_deps = run_dependency_list(pinot_root, modules)

    # LICENSE-binary update
    if update_license:
        # Step 4: Parse current LICENSE-binary
        print("Parsing current LICENSE-binary...")
        file_lines, sections, dep_to_section = parse_license_binary(license_file)
        total_old = sum(len(s.deps) for s in sections)
        print(f"  Found {len(sections)} license sections with {total_old} total deps\n")
        for s in sections:
            print(f"    {s.name}: {len(s.deps)} deps")
        print()

        # Step 5: Compute diff
        added, removed, changed = compute_diff(sections, new_deps)

        # Step 6: Detect licenses for new deps
        print("Detecting licenses for new dependencies...")
        license_map: Dict[str, str] = {}
        detected = 0
        for dep in added:
            lic = detect_license_from_pom(dep, args.m2_repo)
            if lic:
                license_map[str(dep)] = lic
                detected += 1
        print(f"  Auto-detected licenses for {detected}/{len(added)} new deps")

        # Step 6a: Check if version-bumped deps changed their license
        print("Checking version-bumped deps for license changes...")
        changed, license_changed = check_version_bump_license_changes(
            changed, dep_to_section, args.m2_repo
        )
        if license_changed:
            print(f"  WARNING: {len(license_changed)} deps changed license on version bump!")
        else:
            print(f"  All {len(changed)} version bumps have unchanged licenses")

        # Step 6b: Detect orphaned license files
        print("Checking for orphaned license files...")
        empty_sections, orphaned_files = find_orphaned_license_files(
            pinot_root, sections, removed, added, changed, license_changed, license_map,
        )
        if empty_sections:
            print(f"  {len(empty_sections)} sections will be empty after update")
        if orphaned_files:
            print(f"  {len(orphaned_files)} license files not referenced in LICENSE-binary")
        print()

        # Step 7: Print report
        print_report(added, removed, changed, license_changed, license_map,
                     empty_sections, orphaned_files)

        # Step 8: Generate updated file
        if not args.report_only:
            output_path = args.output or (pinot_root / "LICENSE-binary.updated")
            generate_updated_license_binary(
                license_file, sections, added, removed, changed,
                license_changed, license_map, output_path,
            )
            print(f"\nReview the updated file, then:")
            print(f"  diff {license_file} {output_path}")
            print(f"  cp {output_path} {license_file}")
        else:
            print("(--report-only mode: no LICENSE-binary file written)")

    # NOTICE-binary update
    if update_notice:
        if args.report_only:
            print("(--report-only mode: no NOTICE-binary file written)")
        else:
            notice_output = args.notice_output or (pinot_root / "NOTICE-binary.updated")
            generate_notice_binary(
                new_deps, args.m2_repo, notice_output,
                existing_notice_file=pinot_root / "NOTICE-binary",
            )
            print(f"\n  Review the updated NOTICE file, then:")
            print(f"    diff {pinot_root / 'NOTICE-binary'} {notice_output}")
            print(f"    cp {notice_output} {pinot_root / 'NOTICE-binary'}")


if __name__ == "__main__":
    main()
