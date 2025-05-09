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

## Building the Module
From the repo root, build and install only `pinot-dependency-verifier` without triggering the verification, 
so the artifact is installed into the local Maven repository:

   ```bash
   mvn clean install \
      -pl pinot-dependency-verifier \
      -am \
      -DskipTests \
      -Drun.dependency.verifier=false
   ```

Then, to build the full Pinot project with verification enabled:

   ```bash
   mvn clean verify \
      -Pbin-dist,dependency-verifier \
      -Drun.dependency.verifier=true \
      -DskipTests
   ```

## Running the Plugin

To manually run the enforcer plugin across the entire Pinot codebase:

```bash
mvn enforcer:enforce -Pdependency-verifier -Drun.dependency.verifier=true
```

## Developing & Testing
To run the unit tests:

   ```bash
   mvn test
   ```