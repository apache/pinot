/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.requesthandler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;


/// Settings for the broker startup warmup performed before readiness is granted.
///
/// The exit condition is a **depth floor OR a budget ceiling**, never a latency guess:
///   - [#budgetMs] is a hard ceiling. Warmup releases the readiness gate when it expires whatever the
///     probe progress, so a slow or unreachable server cannot stall a rolling restart.
///   - [#minIterations] is the floor for the default probe: it declares the broker warm once this many
///     probe queries have completed successfully -- enough invocations to drive the query path's JIT to
///     its top tier. (A latency target was deliberately avoided: a trivial probe reaches low latency
///     after a handful of iterations while the code is still only partially compiled, so latency is a
///     false early-exit signal.)
///
/// Probe selection has two optional overrides, each falling back to auto-selection when unset:
///   - [#queries]: one or more complete probe queries (each names its own table), separated by `;` in
///     the config value when more than one. When set they are used instead of the default
///     `SELECT * FROM "<t>" LIMIT 1`, so representative production query shapes are warmed -- and several
///     shapes together warm several serve-path branches (scan, aggregation, group-by, join), which a
///     single shape does not. Every configured query is run each round, exiting on the same
///     [#minIterations] probe-count depth floor as the default probe (a cost-independent warm signal),
///     or the budget. [#tables] is ignored
///     when this is set. Note: the config layer treats a bare comma as its own list delimiter, so a comma
///     inside a query loses any following space (`a, b` becomes `a,b`); this is harmless for column lists
///     but a string literal containing `, ` (or `;`) would be altered.
///   - [#tables]: an explicit list of tables to probe with the default query. Empty means auto-select
///     via greedy set-cover over routable servers.
///
/// [#concurrency] is how many probes are fired at once per round: serial (1, the default) warms the
/// serve path; a higher value additionally warms the concurrency step the first real burst hits.
///
/// Immutable; safe to share across threads.
public record BrokerWarmupConfig(boolean enabled, long budgetMs, int minIterations, int maxTables,
                                 List<String> queries, List<String> tables, int concurrency) {

  /// Separator for multiple probe queries inside the `warmup.query` config value. A semicolon, the SQL
  /// statement separator: a trailing one is dropped (blank), and the probe compiler does not accept the
  /// `SET x=y;` prefix anyway, so the only value a `;` would wrongly split is one inside a string literal,
  /// which the config layer's comma handling already makes unsafe. Comma cannot be used: the config layer
  /// claims it as its own list delimiter.
  private static final String QUERY_DELIMITER = ";";

  /// Upper bound on the probe concurrency. Warmup runs before any real traffic, so a modest cap is plenty;
  /// this only exists so a fat-fingered config value cannot ask for a pathological thread pool (an
  /// OutOfMemoryError creating native threads), which would defeat the whole point of warming up.
  private static final int MAX_CONCURRENCY = 64;

  public static BrokerWarmupConfig from(PinotConfiguration config) {
    String queryProperty = config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_QUERY,
        Broker.DEFAULT_BROKER_STARTUP_WARMUP_QUERY);
    List<String> queries = splitAndTrim(queryProperty, QUERY_DELIMITER);
    String tablesCsv = config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_TABLES,
        Broker.DEFAULT_BROKER_STARTUP_WARMUP_TABLES);
    List<String> tables = splitAndTrim(tablesCsv, ",");
    return new BrokerWarmupConfig(
        config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_ENABLED,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_ENABLED),
        config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_BUDGET_MS,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_BUDGET_MS),
        Math.max(1, config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MIN_ITERATIONS,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_MIN_ITERATIONS)),
        Math.max(1, config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_MAX_TABLES,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_MAX_TABLES)),
        queries,
        tables,
        Math.min(MAX_CONCURRENCY, Math.max(1, config.getProperty(Broker.CONFIG_OF_BROKER_STARTUP_WARMUP_CONCURRENCY,
            Broker.DEFAULT_BROKER_STARTUP_WARMUP_CONCURRENCY))));
  }

  /// Splits on `delimiter`, trims each element, and drops blanks. Empty or null input yields an empty list.
  private static List<String> splitAndTrim(String value, String delimiter) {
    return value == null || value.isBlank() ? List.of()
        : Arrays.stream(value.split(delimiter)).map(String::trim).filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
  }

  /// Whether one or more explicit probe queries were configured (each a complete query naming its table).
  public boolean hasCustomQuery() {
    return queries != null && !queries.isEmpty();
  }

  /// Whether an explicit table list was configured for the default probe.
  public boolean hasCustomTables() {
    return tables != null && !tables.isEmpty();
  }
}
