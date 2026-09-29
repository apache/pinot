# quickstart

Purpose: get a local Pinot cluster up for manual testing or debugging, without the user having to remember where the scripts live or which build profile produces them.

Usage:
- `/quickstart` — default, runs `quick-start-batch.sh`.
- `/quickstart batch` — batch mode (offline table with sample data).
- `/quickstart hybrid` — hybrid table (offline + realtime).
- `/quickstart streaming` — realtime consumption from an embedded Kafka.
- `/quickstart auth` / `auth-zk` — auth-enabled variants.

Batch includes the offline JSON-index and complex-type examples. Streaming includes JSON-index, complex-type, full-upsert, and partial-upsert examples. These no longer have separate generated scripts in this checkout.

## Procedure

1. **Resolve the mode before building.** Check the generated program names in [`pinot-tools/pom.xml`](../../pinot-tools/pom.xml). For a legacy or feature-specific request, consult the quickstart implementation's `types()` and examples under [`org/apache/pinot/tools`](../../pinot-tools/src/main/java/org/apache/pinot/tools) and map it to the supported batch or streaming script. Deprecated type aliases are not separate script names. For an unsupported mode, report the available modes without building.

2. **Find the selected script and its matching artifacts.** Look in order, reusing a build only when it matches the requested checkout and scope:
   - `build/bin/quick-start-<mode>.sh` (produced by `-Pbin-dist`)
   - `pinot-tools/target/pinot-tools-pkg/bin/quick-start-<mode>.sh` (produced by a plain `./mvnw package` of `pinot-tools`)

3. **Build missing or outdated artifacts within the requested scope.** A request to launch quickstart authorizes its routine local prerequisites. Run `./mvnw -pl pinot-tools -am package -DskipTests` asynchronously, retaining logs and the Maven exit code. After success, use the script from `pinot-tools/target/pinot-tools-pkg/bin/`. Use the full binary distribution only when the requested deliverable requires it. If the expected script is still missing, inspect the packaging output instead of repeating the build.

4. **Run the selected script in the background.** Quickstart processes run indefinitely. Use the execution tool's background/session support:
   ```
   "<resolved-script-path>"
   ```
   Record the session handle, process IDs, command, start time, and log path. Track any Java child process so later cleanup targets only this launch. Check for occupied ports before launching; reuse an existing cluster only when it matches the requested mode and scope. Do not kill an unrelated process to free a port.

5. **Verify readiness within a deadline.** Use an existing startup budget, or allow up to 120 seconds after launch. Check process state, startup logs, and `GET http://localhost:9000/tables` with a short request timeout. Once the expected table is available, run one small query from the selected mode's example and check for query exceptions and an expected result. Use the mode's credentials when authentication is enabled. A fixed delay or controller log line alone does not prove the cluster can serve queries. Stop checking on success, process failure, or the deadline; report any remaining initialization without an open-ended wait.

6. **Report how to use it.** Include readiness and query results, then:
   - Controller UI: http://localhost:9000
   - Query console: http://localhost:9000/#/query
   - To stop: use the recorded session/process IDs after verifying they still belong to this launch.
   - Logs: provide the retained log path or session handle.

## Notes

- Multiple default quickstarts use the same ports (9000, 8000, 7050, 8098, 8099). If a port is occupied and the existing cluster cannot satisfy the request, report the conflict. Ask only if stopping that other process is necessary and not already authorized. Never use a broad process-name kill command.
- The auth quickstart uses a default admin/verysecret credential; mention this if the user picks `auth` or `auth-zk`.
- The generated quickstart launchers request an initial 4 GB heap; account for that when checking local resources.
- Use the JDK required by `AGENTS.md` and the shipped launcher's JVM flags. Add module-access flags only when an observed launcher or runtime error shows they are needed.
