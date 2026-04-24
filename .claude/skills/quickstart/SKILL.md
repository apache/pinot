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
name: quickstart
description: Launch a local Pinot quickstart cluster (batch, hybrid, streaming, upsert, etc.) with the right script, building the binary distribution first if needed.
---

# /quickstart

Purpose: get a local Pinot cluster up for manual testing or debugging, without the user having to remember where the scripts live or which build profile produces them.

Usage:
- `/quickstart` — default, runs `quick-start-batch.sh`.
- `/quickstart batch` — batch mode (offline table with sample data).
- `/quickstart hybrid` — hybrid table (offline + realtime).
- `/quickstart streaming` — realtime consumption from an embedded Kafka.
- `/quickstart upsert-streaming` — upsert table on Kafka.
- `/quickstart partial-upsert-streaming` — partial-upsert table on Kafka.
- `/quickstart json-index-batch` / `json-index-streaming` — JSON index demos.
- `/quickstart complex-type-handling-offline` / `complex-type-handling-streaming` — complex type demos.
- `/quickstart auth` / `auth-zk` — auth-enabled variants.

## Procedure

1. **Find the quickstart script.** Look in order:
   - `build/bin/quick-start-<mode>.sh` (produced by `-Pbin-dist`)
   - `pinot-tools/target/pinot-tools-pkg/bin/quick-start-<mode>.sh` (produced by a plain `./mvnw package` of `pinot-tools`)

   If neither exists, the binary distribution hasn't been built.

2. **If the script is missing, offer to build.** Ask the user:
   > Quickstart scripts not found. Build them now?
   > - Full bin-dist (recommended for first time): `./mvnw clean install -DskipTests -Pbin-dist -Pbuild-shaded-jar` (~10 min)
   > - Just pinot-tools: `./mvnw -pl pinot-tools -am package -DskipTests` (~3 min)

   Run the one they pick in the foreground. If they're re-running after a prior build, `build/bin/` should already exist.

3. **Validate the mode.** If the user passed an unrecognized mode, list the available scripts:
   ```
   ls build/bin/quick-start-*.sh 2>/dev/null || ls pinot-tools/target/pinot-tools-pkg/bin/quick-start-*.sh
   ```

4. **Run the script in the background.** Quickstart processes run indefinitely; they're servers. Use `run_in_background` so the user can keep working:
   ```
   build/bin/quick-start-<mode>.sh
   ```
   Capture the shell id so the user can check output later.

5. **Report how to use it.** Once started (wait ~30s or until logs show "Pinot Controller started"), tell the user:
   - Controller UI: http://localhost:9000
   - Query console: http://localhost:9000/#/query
   - To stop: kill the background shell (provide the id).
   - Logs: printed to the background shell's stdout.

## Notes

- Quickstart processes are long-running. Do not poll with `sleep` loops. The user can check status via the HTTP UI.
- Multiple quickstarts can't run simultaneously — they all bind to the same ports (9000, 8000, 7050, 8098, 8099). If a run fails with "Address already in use", check for an existing quickstart and ask the user before killing it.
- The auth quickstart uses a default admin/verysecret credential; mention this if the user picks `auth` or `auth-zk`.
- On macOS with JDK 21+, quickstarts may need extra `--add-opens` flags; the shipped scripts handle this already, so don't add flags unless the user reports a module-access error.
