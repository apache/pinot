# run-test

Purpose: resolve a test class name to its Maven module and run only that test, without the user having to remember the exact `-pl`, `-am`, `-Dtest`, and `-Dsurefire.failIfNoSpecifiedTests` flags.

Usage:
- `/run-test RangeIndexTest` — single class.
- `/run-test RangeIndexTest#testSpecificMethod` — single method.
- `/run-test OfflineClusterIntegrationTest` — integration test (auto-detected for runtime expectations).

## Procedure

1. **Parse the argument.** Split on `#` into `<className>` and optional `<methodName>`. If the class name contains a dot, retain it as the FQN selector and extract its simple class name for file lookup.

2. **Locate the source file.**
   - Glob for `**/<simpleClassName>.java` under the repo; verify the package declaration when the user supplied an FQN.
   - Prefer matches under `src/test/java/`.
   - Use the supplied module or established task scope to disambiguate matches. Ask with module-qualified candidates only if multiple matches remain.
   - If zero matches, report and stop.

3. **Find the owning module.** Walk up from the test file until you find a `pom.xml` that is not the repo root. That's the module.

4. **Note on the `failIfNoSpecifiedTests` flag.** Always pass `-Dsurefire.failIfNoSpecifiedTests=false`, regardless of whether the target is a unit or integration test. With `-am`, Maven runs the full Surefire goal on every upstream module (e.g. `pinot-spi` → `pinot-common` → … → target module). Each of those modules invokes Surefire with the same `-Dtest=<className>` filter, and Surefire's default behaviour is to **fail the whole build** when the pattern doesn't match any test in a given module. Without this flag, the build dies at the first upstream module that doesn't happen to contain `<className>`. This applies equally to unit tests (upstream modules don't have the test) and integration tests (the `pinot-integration-tests` module has tests the filter doesn't match).

   Optional: detect integration tests for reporting/warnings only. A test is an integration test if *any* of these hold:
   - The file path contains `pinot-integration-tests`.
   - The file is named `*IntegrationTest.java`, `*IT.java`, `*ClusterTest.java`, or `*EndToEndTest.java`.
   - The module is `pinot-integration-tests` or `pinot-compatibility-verifier`.

   Use this only to warn the user about expected runtime ("integration tests typically take 10–20 min"), not to alter the command.

5. **Build the command.**
   ```
   ./mvnw -pl <module> -am '-Dtest=<selector>' -Dsurefire.failIfNoSpecifiedTests=false test
   ```
   - Use the class name or FQN as `<selector>`; append `#<methodName>` when a method is requested.
   - Keep `-am` when current upstream dependencies are unverified. For module-only iteration, omit it only when the required artifacts are available to Maven and verified against the current source, dependency versions, JDK, and build configuration. A prior reactor `test` alone does not establish that dependency artifacts are installed for a module-only invocation.
   - `-Dsurefire.failIfNoSpecifiedTests=false` is always required when `-am` is set (see step 4).

6. **Run and report.** Print the exact command before running so the user can copy/tweak it. Long runs may execute asynchronously; retain the log and Maven exit code, report meaningful progress, and do independent work while they run. Await completion and verify that the requested test actually ran before reporting success. On failure, show the relevant Maven output or the Surefire report path under `<module>/target/surefire-reports/`.

## Notes

- These runs can take 2–15 minutes depending on the module and dependency state. Asynchronous execution does not require separate user authorization.
- `-o` disables remote artifact resolution; it does not skip upstream reactor modules or replace `-am`. Use it only when all required artifacts are cached and offline execution is appropriate.
- Reuse a passing test result if the tested sources, dependencies, JDK, and relevant configuration have not changed. Rerun only when changes or unresolved failures invalidate that evidence.
- If the requested test did not execute, check the selector, source class, and Surefire reports. Do not suppress the missing-test failure to report success.
- If the class is abstract, suggest concrete subclasses. Before treating a class with no declared `@Test` methods as a base class, check inherited tests and class-level test annotations.
- Integration tests can start embedded Helix/ZK/Kafka and bind fixed localhost ports. Do not overlap runs that share ports or other exclusive resources.
