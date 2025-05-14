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

# Pinot Dependency Verifier

This module implements a custom Maven Enforcer plugin rule that validates dependency declarations in the Apache Pinot
project. It enforces internal
[Dependency Management Guidelines](https://docs.pinot.apache.org/developers/developers-and-contributors/dependency-management)
by checking for hardcoded versions and misplaced dependencies.

## Skipped Modules

To avoid circular resolution and redundant checks, the enforcer rule is skipped in these submodules:
- pinot-plugins
- pinot-connectors
- pinot-integration-tests
- pinot-tools
- contrib
- pinot-dependency-verifier

That means when you run the full project build, those modules will be excluded from dependency‐verifier validation.

## Two-Phase Build Workflow for Changes in `pinot-dependency-verifier`

Maven resolves plugin dependencies before building reactor modules. This means it cannot build the verifier JAR and
simultaneously use it in the same build. Therefore, any changes to the `pinot-dependency-verifier` module must follow a
two-phase (two-PR) process:
### PR #1 – Build & Install the Verifier Module

1. In the root POM, locate the `<plugin>` entry for `maven-enforcer-plugin` under `<build><plugins>`.
2. Temporarily comment out the `<dependency>` block that pulls in `org.apache.pinot:pinot-dependency-verifier`.
3. Commit and open the first PR.

To test locally:
```
mvn clean install \
    -pl pinot-dependency-verifier \
    -DskipTests \
    -Denforcer.skip=true
```

This installs the verifier module into the local Maven repository without triggering enforcement.

### PR #2 – Re-enable the Enforcer Dependency & Full Build

1. Uncomment the `<dependency>` block from PR #1.
2. Commit and open PR #2.

To test locally:
```
mvn clean install
```
The Enforcer plugin can now resolve and load freshly installed verifier JAR and execute the custom rule.

## Running the Plugin

To manually run the enforcer plugin across the entire Pinot codebase:

```bash
mvn enforcer:enforce
```
