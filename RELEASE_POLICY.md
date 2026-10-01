<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Apache Pinot release policy

This page is the in-repo summary of how Apache Pinot is versioned, published,
and upgraded. Detailed notes live in the project documentation; this file is
meant as a stable pointer for operators and for release trackers.

## Version numbering

Pinot releases use `major.minor.patch` tags of the form `release-X.Y.Z`
(for example `release-1.5.0`, `release-1.5.1`).

- A **minor** release (`X.Y.0`) is the usual feature line.
- A **patch** release (`X.Y.Z`) is used for security and dependency fixes
  without functional, API, configuration, or wire-format changes. Apache Pinot
  1.5.1 is an example: a security patch on 1.5.0.

GitHub tags and the [download page](https://pinot.apache.org/download/) are
the canonical list of published versions.

## Cadence

Since Apache Pinot 1.0.0 (September 2023) the project has published on the
order of two minor releases per year, with occasional patch releases between
them:

| Version | Date |
| ------- | ---- |
| 1.0.0 | September 2023 |
| 1.1.0 | March 2024 |
| 1.2.0 | August 2024 |
| 1.3.0 | February 2025 |
| 1.4.0 | September 2025 |
| 1.5.0 | April 2026 |
| 1.5.1 | June 2026 |

This table is historical, not a commitment to a fixed calendar.

Releases are cut through the Apache Software Foundation process (discussion
and vote on the Pinot dev mailing list). Subscribe at
`dev-subscribe@pinot.apache.org`.

## Current stable and older builds

The current stable release is listed on the
[download page](https://pinot.apache.org/download/) and in the
[version reference](https://docs.pinot.apache.org/start-here/pinot-versions).
As of this writing that is **1.5.1**.

- Production deployments should pin a specific release tag, not the `latest`
  Docker tag. `latest` tracks nightly builds from the main branch.
- Older binaries remain on <https://archive.apache.org/dist/pinot/>.

## Compatibility and upgrades

Pinot aims to keep releases backward-compatible and to introduce features in
a compatible way, but a given cluster may still hit a combination of
config/schema/data that was not covered in review.

Before upgrading:

1. Read the [release notes](https://docs.pinot.apache.org/reference/release-notes/releases)
   for every version you will cross.
2. Read the [upgrade notes](https://docs.pinot.apache.org/operate-pinot/upgrades)
   for behavior changes, deprecations, and operator actions.
3. Run the compatibility test suite (shipped since 0.8.0) against your
   tables, schemas, and queries. See
   [Upgrading Pinot](https://docs.pinot.apache.org/operate-pinot/upgrades/upgrading-pinot-cluster).

Rolling upgrades of a cluster are the recommended path. If you have skipped
several releases, review the incompatibility notes for the whole range and
plan downtime if required.

## Runtime support

Published documentation currently states:

| Requirement | Detail |
| ----------- | ------ |
| Pinot services (build and runtime) | JDK 25+ |
| SPI and Java/JDBC client artifacts | JDK 11+ |
| Last release with JDK 8 | 0.12.1 |

See the [version reference](https://docs.pinot.apache.org/start-here/pinot-versions)
for the live matrix. Client connectors that run inside Spark or Flink keep a
lower bytecode baseline than the Pinot servers.

## Support window

Apache Pinot does **not** currently publish a calendar end-of-life date per
minor line. The project recommends running a current stable release and
reading the upgrade notes before moving between versions. Security patch
releases (such as 1.5.1) are issued against the latest minor when needed.

If you need a statement for compliance tooling, treat "current stable on
the download page" as the supported line, and treat archive builds as
available but not actively patched unless a patch release is published for
that line.

## Links

- Downloads: https://pinot.apache.org/download/
- All releases: https://docs.pinot.apache.org/reference/release-notes/releases
- Version reference: https://docs.pinot.apache.org/start-here/pinot-versions
- Upgrade guides: https://docs.pinot.apache.org/operate-pinot/upgrades
- Compatibility tester: https://docs.pinot.apache.org/operate-pinot/upgrades/upgrading-pinot-cluster
- Source tags: https://github.com/apache/pinot/tags
