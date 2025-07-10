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

## Two-Phase Build Workflow

Maven resolves plugin dependencies before building reactor modules. This means it cannot build the verifier JAR and 
use it in the same build cycle. Therefore, any changes to the `pinot-dependency-verifier` module must follow a
two-phase process:

### Phase 1 - Build & Install the Verifier Module

From the repo root, build and install only `pinot-dependency-verifier` without triggering verification.
This ensures the artifact is available in the local Maven repository:

```bash
mvn clean install \
  -pl pinot-dependency-verifier \
  -am \
  -DskipTests
   ```

### Phase 2 – Full Reactor Build + Dependency Verifier

Run the full Pinot build with the Enforcer Plugin enabled to execute the custom rule:

```bash
   mvn clean verify \
      -Pbin-dist,dependency-verifier
      -DskipTests
   ``````

## Running the Plugin

To manually run the enforcer plugin without the customized rule:
```bash
mvn enforcer:enforce
```

To manually run it with the custom rule activated:
```bash
mvn enforcer:enforce -Pdependency-verifier
```