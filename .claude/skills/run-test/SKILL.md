---
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
name: run-test
description: Run a single Pinot JUnit/TestNG test class by name. Auto-detects the owning Maven module and builds the correct ./mvnw invocation, including the integration-test flags when needed.
---

# /run-test

Purpose: resolve a test class name to its Maven module and run only that test, without the user having to remember the exact `-pl`, `-am`, `-Dtest`, and `-Dsurefire.failIfNoSpecifiedTests` flags.

Usage:
- `/run-test RangeIndexTest` — single class.
- `/run-test RangeIndexTest#testSpecificMethod` — single method.
- `/run-test OfflineClusterIntegrationTest` — integration test (auto-detected, adds the required flag).

## Procedure

1. **Parse the argument.** Split on `#` into `<className>` and optional `<methodName>`. If the class name contains a dot, treat it as FQN.

2. **Locate the source file.**
   - Glob for `**/<className>.java` under the repo.
   - Prefer matches under `src/test/java/`.
   - If multiple matches, list them (with module prefixes) and ask the user which one. Do not guess.
   - If zero matches, report and stop.

3. **Find the owning module.** Walk up from the test file until you find a `pom.xml` that is not the repo root. That's the module.

4. **Detect integration tests.** A test is an integration test if *any* of these hold:
   - The file path contains `pinot-integration-tests`.
   - The file is named `*IntegrationTest.java`, `*IT.java`, `*ClusterTest.java`, or `*EndToEndTest.java`.
   - The module is `pinot-integration-tests` or `pinot-compatibility-verifier`.
   Integration tests need `-Dsurefire.failIfNoSpecifiedTests=false` to avoid "no tests matching" failures when the module has other test classes the filter won't match.

5. **Build the command.**
   - Unit test:
     ```
     ./mvnw -pl <module> -am -Dtest=<className>[#<methodName>] test
     ```
   - Integration test:
     ```
     ./mvnw -pl <module> -am -Dtest=<className>[#<methodName>] -Dsurefire.failIfNoSpecifiedTests=false test
     ```
   - `-am` is intentional: the test needs upstream module JARs built.

6. **Run and report.** Print the exact command before running so the user can copy/tweak it. On failure, show the last ~60 lines of the Maven output (or the Surefire report path under `<module>/target/surefire-reports/`) so the user can jump straight to the stack trace.

## Notes

- These runs can take 2–15 minutes depending on the module and whether deps are already built. Consider `run_in_background` only if the user says so — default is foreground so they see progress.
- Never strip `-am`. The first run after a clean checkout will fail without it.
- If the user wants to run without rebuilding upstream (faster iteration), suggest they add `-o` (offline) or drop `-am` after the first successful build — but don't do it automatically.
- For repeat runs of the same test, suggest `-DfailIfNoTests=false` if the first run reported "No tests were executed" — usually a typo in the class name.
- If the class is `abstract` or has no `@Test` methods (it's a base class), warn the user and suggest concrete subclasses found via grep.
