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
## Pinot CLI

An interactive and batch command-line client for Apache Pinot. It supports a rich interactive REPL, multiple output formats, history, pagination, configuration files, and batch execution.

## Requirements

- Java 21+ on PATH

## Build

From the repository root:

```bash
./mvnw -DskipTests -pl pinot-clients/pinot-cli -am package
```

Artifacts:

- `pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar` (executable, recommended)
- `pinot-clients/pinot-cli/target/pinot-cli-1.5.0-SNAPSHOT.jar` (thin)

## Running

### Interactive mode

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://<controller-host>:<port>
```

- Multi-line SQL is supported; end statements with `;` to execute.
- Built-in commands: `help`, `clear`, `exit`, `quit`.
- Default history file: `~/.pinot_history` (customize with `--history-file`).
- Enable paging with a pager (e.g., `less`) via `--pager` or environment variables below.

### Batch mode

Execute a single statement:

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://<controller-host>:<port> \
  --output-format=CSV_HEADER \
  --execute "SELECT * FROM myTable LIMIT 3;"
```

Execute statements from a file:

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://<controller-host>:<port> \
  --output-format=JSON \
  -f queries.sql
```

## Options

- `-u, --url` JDBC URL. Example: `jdbc:pinot://controller:9000` or `jdbc:pinotgrpc://controller:9000` (required)
- `-n, --user` Username
- `-p, --password` Password
- `--header` Extra request header `key=value` (repeatable), e.g., `--header Authorization=Bearer <token>`
- `--set` Query/session option `key=value` (repeatable). Forwarded as connection properties
- `-e, --execute` Execute SQL and exit
- `-f, --file` Execute SQL from file and exit
- `-o, --output` Legacy: `table|csv|json` (backward compatibility). Prefer the formats below
- `--output-format` Batch output format
- `--output-format-interactive` Interactive output format (default: `ALIGNED`)
- `--pager` Pager program used in interactive mode (e.g., `less -SRFXK`). Empty disables pagination
- `--history-file` Path to history file for interactive mode (default: `~/.pinot_history`)
- `--config` Path to a properties file with defaults (see Configuration below)
- `--debug` Print stack traces and timing diagnostics to stderr
- `--progress-interval-ms` Query progress polling interval in milliseconds. Use `0` to disable progress polling (default: `1000`)

### Output formats

Available values for `--output-format` and `--output-format-interactive` (case-insensitive):

- `CSV`, `CSV_HEADER`, `CSV_UNQUOTED`, `CSV_HEADER_UNQUOTED`
- `TSV`, `TSV_HEADER`
- `JSON` (one JSON object per line)
- `ALIGNED` (ASCII table)
- `VERTICAL` (record-oriented)
- `AUTO` (chooses `ALIGNED` if it fits terminal width, otherwise `VERTICAL`)
- `MARKDOWN` (Markdown table)
- `NULL` (suppress normal results; useful for timing/error checks)

## Configuration

You can load defaults from a properties file using `--config` or via environment variables:

- `PINOT_CONFIG` (preferred)

Supported keys in the properties file (CLI flags take precedence):

- `server` (maps to `--url`)
- `user`, `password`
- `output-format`, `output-format-interactive`, `output`
- `pager`, `history-file`, `debug`, `progress-interval-ms`
- `headers.*` (e.g., `headers.Authorization=Bearer <token>`) -> becomes extra headers
- Any other key is forwarded as a session option (equivalent to `--set key=value`)

Example `pinot-cli.properties`:

```properties
server=jdbc:pinot://localhost:9000
user=alice
output-format-interactive=AUTO
pager=less -SRFXK
history-file=/Users/alice/.pinot_history
headers.Authorization=Bearer abc123
debug=true
progress-interval-ms=1000
timeoutMs=60000
```

Run with:

```bash
PINOT_CONFIG=/path/to/pinot-cli.properties \
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar
```

## Environment variables

- `PINOT_CONFIG`: path to a config properties file
- `PINOT_PAGER`: pager command for interactive mode (e.g., `less -SRFXK`)

## Examples

Interactive with AUTO format and pager:

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://localhost:9000 \
  --output-format-interactive=AUTO \
  --pager "less -SRFXK"
```

Batch to JSON:

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://localhost:9000 \
  --output-format=JSON \
  --execute "SELECT col1, col2 FROM myTable LIMIT 3;"
```

## Query progress

Pinot exposes query progress for long-running queries through the controller, and
both the query console and CLI can show the progress while the query is in the
`RUNNING` state.

The query console automatically attaches a client query id, polls query progress,
and shows a numeric progress bar while the query is running. The percentage is
derived from processed work units divided by total work units. For V1 queries,
work units are based on server segment processing. For V2 queries, work units are
estimated from the multi-stage operators and stage progress.

The CLI enables progress polling by default for interactive terminals. It updates
the status at the bottom of the terminal and clears it before printing query
results. Single-stage queries render one compact progress line. Multi-stage
queries render the aggregate query progress plus component progress rows when
the progress response includes them. Redirected output does not print progress
updates.

Tune the refresh interval:

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://localhost:9000 \
  --progress-interval-ms=2000 \
  --execute "SELECT COUNT(*) FROM baseballStats"
```

Disable CLI progress polling:

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://localhost:9000 \
  --progress-interval-ms=0 \
  --execute "SELECT COUNT(*) FROM baseballStats"
```

Sample V1 query using the quickstart `baseballStats` table:

```sql
SELECT playerName, SUM(runs), SUM(hits), SUM(homeRuns)
FROM baseballStats
GROUP BY playerName
ORDER BY SUM(hits) DESC
LIMIT 1000;
```

Sample V2 query using the quickstart `baseballStats` table:

```bash
pinot-clients/pinot-cli/target/pinot-cli-*-executable.jar \
  -u jdbc:pinot://localhost:9000 \
  --set useMultistageEngine=true \
  --set maxRowsInJoin=10000000 \
  --progress-interval-ms=1000 \
  --execute "SELECT COUNT(*) FROM baseballStats a JOIN baseballStats b ON a.yearID = b.yearID"
```

## Notes

- CLI arguments take precedence over config file values.
- Pager is only used in interactive mode. Batch mode prints directly to stdout.
