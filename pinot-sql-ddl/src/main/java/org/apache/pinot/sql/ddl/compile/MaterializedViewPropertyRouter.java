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
package org.apache.pinot.sql.ddl.compile;

import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/// Routes Pinot DDL `PROPERTIES (...)` entries onto a [TableConfigBuilder] for
/// `CREATE MATERIALIZED VIEW`.
///
/// Routing rules (applied in order, case-insensitive on the key):
/// 1. **Synthetic keys reserved by DDL clauses are rejected.** `schedule` (from
///    `REFRESH EVERY`), `definedSQL` (from `AS <query>`), and the
///    `streamType` / `stream.*` / `realtime.*` family (MV is OFFLINE) all return 400
///    with a message naming the offending key and pointing at the correct clause.
/// 1. **Promoted scalar.** Lower-cased key matches a known table-level field
///    (`timeColumnName`, `timeType`, `replication`, `brokerTenant`, `serverTenant`) —
///    call the corresponding [TableConfigBuilder] setter directly.
/// 1. **MaterializedViewTask-specific scalar.** Lower-cased key matches one of the
///    well-known MV task knobs (`bucketTimePeriod`, `bufferTimePeriod`,
///    `maxNumRecordsPerSegment`, `maxTasksPerBatch`, `taskMode`, `stalenessThresholdMs`) —
///    route into `task.MaterializedViewTask.<originalKey>` with the user-provided value.
///    **Key names are preserved verbatim**, never aliased.
/// 1. **Task prefix.** Key starts with `task.<taskType>.` — route the remainder into
///    `TableTaskConfig.taskTypeConfigsMap[taskType]`. Mirrors
///    [PropertyMapping#apply]'s rule 4 so any current or future minion task type composes
///    with MV without grammar changes.
/// 1. **Custom (fallback).** Anything else — store verbatim in
///    [TableCustomConfig#getCustomConfigs()].
///
/// After routing the user-provided properties, one or two synthetic entries are merged
/// into `task.MaterializedViewTask.*`:
///   * `definedSQL` — always written; the raw user-typed query substring extracted by
///     the compiler from the `AS <query>` clause.
///   * `schedule` — written iff the DDL supplied a `REFRESH EVERY <period>` clause. The
///     value is the Quartz cron expression produced by [#periodToCron(String)]. When the
///     entire REFRESH clause is omitted, the key is absent and `PinotTaskManager` falls
///     back to the cluster-wide MV task cron.
///
/// The router is stateless; all entry points are `public static`.
public final class MaterializedViewPropertyRouter {

  /// PinotTaskManager.SCHEDULE_KEY lives in `pinot-controller`. We duplicate the literal
  /// here (rather than introducing a back-dependency) and pin the contract with a unit test.
  /// Exposed for the reverse-DDL extractor in the sibling `reverse` package so it can identify
  /// (and exclude) the schedule entry that the forward router writes — the schedule round-trips
  /// via the `REFRESH EVERY` clause, never via `PROPERTIES`.
  public static final String SCHEDULE_KEY = "schedule";

  private static final String TASK_PREFIX = "task.";
  private static final String STREAM_PREFIX = "stream.";
  private static final String STREAM_TYPE_KEY = "streamtype";
  private static final String REALTIME_PREFIX = "realtime.";

  /// MV-promoted scalar property names (lower-cased).
  private static final Set<String> PROMOTED_KEYS = ImmutableSet.of(
      "timecolumnname",
      "timecolumn",
      "timetype",
      "replication",
      "brokertenant",
      "servertenant");

  /// MV task-config scalar property names. Lower-cased lookup key → canonical-cased on-wire
  /// key from [MaterializedViewTask] constants. Used both by [#apply] (forward routing) and
  /// by [#canonicalKnobName] (reverse DDL emission).
  private static final Map<String, String> TASK_CONFIG_KEYS;
  static {
    Map<String, String> keys = new LinkedHashMap<>();
    keys.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY.toLowerCase(Locale.ROOT),
        MaterializedViewTask.BUCKET_TIME_PERIOD_KEY);
    keys.put(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY.toLowerCase(Locale.ROOT),
        MaterializedViewTask.BUFFER_TIME_PERIOD_KEY);
    keys.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY.toLowerCase(Locale.ROOT),
        MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    keys.put(MaterializedViewTask.MAX_TASKS_PER_BATCH_KEY.toLowerCase(Locale.ROOT),
        MaterializedViewTask.MAX_TASKS_PER_BATCH_KEY);
    keys.put(MaterializedViewTask.TASK_MODE_KEY.toLowerCase(Locale.ROOT),
        MaterializedViewTask.TASK_MODE_KEY);
    keys.put(MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY.toLowerCase(Locale.ROOT),
        MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY);
    TASK_CONFIG_KEYS = keys;
  }

  /// Returns the canonical (CommonConstants.MaterializedViewTask) casing for a knob name
  /// that the DDL grammar lets users write bare in `PROPERTIES (...)` — or null when the
  /// name is not a recognized MV task knob. The reverse-DDL emitter uses this to flatten
  /// `task.MaterializedViewTask.<knob>` back to `<knob>` while normalizing legacy off-case
  /// keys to canonical casing on the way out.
  @Nullable
  public static String canonicalKnobName(String lowerCasedKnob) {
    return TASK_CONFIG_KEYS.get(lowerCasedKnob);
  }

  /// Keys reserved by DDL clauses. Setting these in PROPERTIES is a user error because
  /// the same value already arrives through a first-class clause (`REFRESH EVERY`,
  /// `AS <query>`).
  private static final Set<String> RESERVED_BY_CLAUSE_KEYS = ImmutableSet.of(
      SCHEDULE_KEY,
      MaterializedViewTask.DEFINED_SQL_KEY.toLowerCase(Locale.ROOT));

  private MaterializedViewPropertyRouter() {
  }

  /// Applies `properties` plus the synthetic `definedSql` / `schedule` onto `builder`.
  ///
  /// Pre-conditions:
  ///   * `definedSql` is non-null and non-empty (the compiler guarantees this).
  ///   * `schedule` is non-null iff the DDL had a `REFRESH EVERY` clause; when `null`, no
  ///     `task.MaterializedViewTask.schedule` entry is written and the MV minion task runs
  ///     under the cluster-wide cron registered in `PinotTaskManager`.
  ///   * The caller has already validated `properties` keys do not duplicate (case-insensitive).
  public static void apply(Map<String, String> properties, String definedSql,
      @Nullable String schedule, TableConfigBuilder builder) {
    Map<String, Map<String, String>> taskConfigs = new LinkedHashMap<>();
    Map<String, String> customConfigs = new LinkedHashMap<>();
    Map<String, String> mvTaskConfig = new LinkedHashMap<>();

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String rawKey = entry.getKey();
      String value = entry.getValue();
      String lower = rawKey.toLowerCase(Locale.ROOT);

      if (RESERVED_BY_CLAUSE_KEYS.contains(lower)) {
        throw new DdlCompilationException(
            "Property '" + rawKey + "' is reserved for the corresponding DDL clause "
                + "(REFRESH EVERY for 'schedule', AS <query> for 'definedSQL'); "
                + "remove it from PROPERTIES.");
      }

      // Reserved by the table-DDL clause family. MV doesn't expose TABLE_TYPE / TABLE_NAME
      // clauses (always OFFLINE, name from the statement head) but rejecting these keys
      // here keeps the user-visible error message consistent with CREATE TABLE.
      if ("tabletype".equals(lower) || "tablename".equals(lower) || "ifnotexists".equals(lower)) {
        throw new DdlCompilationException(
            "Property '" + rawKey + "' is reserved and not valid on CREATE MATERIALIZED VIEW.");
      }

      // The materialized-view flag is identity, not configuration: the compiler stamps it
      // automatically on this path (see DdlCompiler#compileCreateMaterializedView and PR #18564).
      // Accepting it here as a user-typed PROPERTY would silently land in TableCustomConfig
      // (the fallback below treats unknown keys as custom) — harmless for identity but a
      // confusing knob to leave dangling. Reject explicitly so the operator does not believe
      // the value affected anything.
      if ("ismaterializedview".equals(lower)) {
        throw new DdlCompilationException(
            "Property 'isMaterializedView' is reserved: CREATE MATERIALIZED VIEW already stamps "
                + "the canonical TableConfig#isMaterializedView flag on the resulting config. "
                + "Remove the PROPERTY entry.");
      }

      if (STREAM_TYPE_KEY.equals(lower) || lower.startsWith(STREAM_PREFIX)
          || lower.startsWith(REALTIME_PREFIX)) {
        throw new DdlCompilationException(
            "Property '" + rawKey + "' is REALTIME-only and not valid on CREATE MATERIALIZED VIEW "
                + "(materialized views are always OFFLINE).");
      }

      if (PROMOTED_KEYS.contains(lower)) {
        applyPromoted(lower, value, builder);
        continue;
      }

      if (TASK_CONFIG_KEYS.containsKey(lower)) {
        // Re-canonicalize the on-wire key from the CommonConstants definition. This avoids
        // case drift when users write 'BUCKETTIMEPERIOD' but downstream consumers
        // (MaterializedViewTaskScheduler / Analyzer) read the exact constant casing.
        mvTaskConfig.put(TASK_CONFIG_KEYS.get(lower), value);
        continue;
      }

      if (lower.startsWith(TASK_PREFIX)) {
        // task.<taskType>.<key> = value
        // Pass-through for arbitrary task types (forward-compat: a future task type
        // composed onto an MV table just works). For MaterializedViewTask itself we accept
        // both the prefixed form ('task.MaterializedViewTask.bucketTimePeriod') and the
        // bare form ('bucketTimePeriod'); the bare form is preferred and documented.
        String afterPrefix = rawKey.substring(TASK_PREFIX.length());
        int dot = afterPrefix.indexOf('.');
        if (dot <= 0 || dot == afterPrefix.length() - 1) {
          throw new DdlCompilationException(
              "Task property '" + rawKey + "' must follow the form task.<taskType>.<key>");
        }
        String taskType = afterPrefix.substring(0, dot);
        String taskKey = afterPrefix.substring(dot + 1);
        if (MaterializedViewTask.TASK_TYPE.equals(taskType)
            && (SCHEDULE_KEY.equals(taskKey.toLowerCase(Locale.ROOT))
                || MaterializedViewTask.DEFINED_SQL_KEY.equalsIgnoreCase(taskKey))) {
          throw new DdlCompilationException(
              "Property '" + rawKey + "' is reserved for the corresponding DDL clause "
                  + "(REFRESH EVERY for 'schedule', AS <query> for 'definedSQL'); "
                  + "remove it from PROPERTIES.");
        }
        if (MaterializedViewTask.TASK_TYPE.equals(taskType)) {
          // Canonicalize the knob casing the same way the bare-form branch does, so
          // `task.MaterializedViewTask.BUCKETTIMEPERIOD` and bare `BUCKETTIMEPERIOD` end up
          // under the same on-wire key (the constant casing in CommonConstants).
          String canonical = TASK_CONFIG_KEYS.getOrDefault(taskKey.toLowerCase(Locale.ROOT), taskKey);
          mvTaskConfig.put(canonical, value);
        } else {
          taskConfigs.computeIfAbsent(taskType, k -> new LinkedHashMap<>()).put(taskKey, value);
        }
        continue;
      }

      customConfigs.put(rawKey, value);
    }

    // Synthetic keys from AS <query> (always) and REFRESH EVERY (when supplied). Always
    // last so they overwrite any user-supplied collision detected above (we already rejected
    // those, so this is purely belt-and-braces for future contributors). Omitting `schedule`
    // entirely is a documented contract — `PinotTaskManager#getTaskToCronExpressionMap`
    // skips per-table cron registration when the key is absent, leaving the task to run on
    // the cluster-wide schedule.
    mvTaskConfig.put(MaterializedViewTask.DEFINED_SQL_KEY, definedSql);
    if (schedule != null) {
      mvTaskConfig.put(SCHEDULE_KEY, schedule);
    }

    taskConfigs.put(MaterializedViewTask.TASK_TYPE, mvTaskConfig);
    builder.setTaskConfig(new TableTaskConfig(taskConfigs));

    if (!customConfigs.isEmpty()) {
      builder.setCustomConfig(new TableCustomConfig(customConfigs));
    }
  }

  private static void applyPromoted(String lowerKey, String value, TableConfigBuilder builder) {
    switch (lowerKey) {
      case "timecolumnname":
      case "timecolumn":
        builder.setTimeColumnName(value);
        return;
      case "timetype":
        builder.setTimeType(value);
        return;
      case "replication":
        builder.setNumReplicas(parseInt("replication", value));
        return;
      case "brokertenant":
        builder.setBrokerTenant(value);
        return;
      case "servertenant":
        builder.setServerTenant(value);
        return;
      default:
        throw new IllegalStateException("Unreachable: applyPromoted called with non-promoted key " + lowerKey);
    }
  }

  // ------------------------------------------------------------------------------------------
  // Period → Quartz cron translation
  // ------------------------------------------------------------------------------------------

  /// Translates a Pinot period string (`'Nd'`, `'Nh'`, `'Nm'`) into a Quartz cron expression
  /// for the `task.MaterializedViewTask.schedule` field. The grammar in pinot-common already
  /// normalizes `EVERY 2 DAYS` → `'2d'`; this method is the only consumer of the normalized
  /// form on the compile side.
  ///
  /// Mapping (all in cluster-local time, i.e. whatever the controller JVM's default zone is):
  /// | Input    | Output cron        | Semantics                                  |
  /// | -------- | ------------------ | ------------------------------------------ |
  /// | `1m`     | `0 * * * * ?`      | every minute at second 0                   |
  /// | `Nm`     | `0 0/N * * * ?`    | every N minutes starting at minute 0       |
  /// | `1h`     | `0 0 * * * ?`      | every hour at minute 0                     |
  /// | `Nh`     | `0 0 0/N * * ?`    | every N hours starting at hour 0           |
  /// | `1d`     | `0 0 0 * * ?`      | every day at 00:00:00                      |
  /// | `Nd`     | `0 0 0 1/N * ?`    | every N days starting day 1 of each month  |
  ///
  /// Caveat for `Nd` with `N > 1`: the day-of-month cycle resets at the start of each month
  /// (Quartz semantics). With `N ≤ 28` this is well-defined; larger values are rejected.
  /// Operators needing irregular schedules can use the raw `task.MaterializedViewTask.schedule`
  /// escape hatch via PROPERTIES — currently disallowed for safety; tracked as a follow-up.
  public static String periodToCron(String period) {
    if (period == null) {
      throw new DdlCompilationException(
          "REFRESH EVERY <period> is required (e.g. '1d', '15m', or '6 HOURS').");
    }
    String trimmed = period.trim().toLowerCase(Locale.ROOT);
    if (trimmed.isEmpty()) {
      throw new DdlCompilationException("REFRESH EVERY <period> must not be empty.");
    }
    char unit = trimmed.charAt(trimmed.length() - 1);
    if (unit != 'd' && unit != 'h' && unit != 'm') {
      throw new DdlCompilationException(
          "REFRESH EVERY '" + period + "' must end with 'd' (days), 'h' (hours), or 'm' (minutes).");
    }
    String numericPart = trimmed.substring(0, trimmed.length() - 1);
    int n;
    try {
      n = Integer.parseInt(numericPart);
    } catch (NumberFormatException e) {
      throw new DdlCompilationException(
          "REFRESH EVERY '" + period + "' has a non-integer count; expected e.g. '15m', '6h', or '1d'.");
    }
    if (n < 1) {
      throw new DdlCompilationException(
          "REFRESH EVERY period must be ≥ 1; got '" + period + "'.");
    }
    switch (unit) {
      case 'm':
        if (n > 59) {
          throw new DdlCompilationException(
              "REFRESH EVERY '" + period + "' exceeds 59 minutes; use '1h' or larger.");
        }
        return n == 1 ? "0 * * * * ?" : "0 0/" + n + " * * * ?";
      case 'h':
        if (n > 23) {
          throw new DdlCompilationException(
              "REFRESH EVERY '" + period + "' exceeds 23 hours; use '1d' or larger.");
        }
        return n == 1 ? "0 0 * * * ?" : "0 0 0/" + n + " * * ?";
      case 'd':
        if (n > 28) {
          throw new DdlCompilationException(
              "REFRESH EVERY '" + period + "' exceeds 28 days; daily cycles must be at most 28 "
                  + "days to remain well-defined under the monthly day-of-month wrap.");
        }
        return n == 1 ? "0 0 0 * * ?" : "0 0 0 1/" + n + " * ?";
      default:
        throw new IllegalStateException("unreachable");
    }
  }

  private static int parseInt(String key, String value) {
    try {
      return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      throw new DdlCompilationException(
          "Property '" + key + "' must be an integer; got '" + value + "'.");
    }
  }

  // ------------------------------------------------------------------------------------------
  // Quartz cron -> period (inverse of periodToCron, used by reverse DDL emission)
  // ------------------------------------------------------------------------------------------

  /// Exact patterns produced by [#periodToCron]. Reverse DDL emission matches the persisted
  /// `task.MaterializedViewTask.schedule` value against these patterns; anything not produced
  /// by a `REFRESH EVERY '<period>'` round-trip is treated as a non-standard cron the user
  /// must have written by hand via the JSON API, and emission rejects it (the DDL grammar has
  /// no syntax to express arbitrary cron). Pinning the inverse mapping in the same file as
  /// the forward mapping makes it a single edit to add or correct a period unit.
  private static final Pattern EVERY_ONE_MINUTE = Pattern.compile("0 \\* \\* \\* \\* \\?");
  private static final Pattern EVERY_N_MINUTES = Pattern.compile("0 0/(\\d+) \\* \\* \\* \\?");
  private static final Pattern EVERY_ONE_HOUR = Pattern.compile("0 0 \\* \\* \\* \\?");
  private static final Pattern EVERY_N_HOURS = Pattern.compile("0 0 0/(\\d+) \\* \\* \\?");
  private static final Pattern EVERY_ONE_DAY = Pattern.compile("0 0 0 \\* \\* \\?");
  private static final Pattern EVERY_N_DAYS = Pattern.compile("0 0 0 1/(\\d+) \\* \\?");

  /// Inverse of [#periodToCron]: turns a persisted Quartz cron expression back into the
  /// `'Nd'` / `'Nh'` / `'Nm'` period string the DDL `REFRESH EVERY` clause produces.
  /// Returns `null` when `cron` is null/blank or when the expression is not one of the six
  /// patterns this router itself emits. The reverse DDL emitter consults this method and
  /// rejects MVs whose schedule it cannot express — silently dropping a schedule on
  /// SHOW CREATE would let `emit -> parse -> emit` produce a config that re-runs against the
  /// cluster-wide cron instead of the operator-chosen one (an invisible behavioural change).
  @Nullable
  public static String cronToPeriod(@Nullable String cron) {
    if (cron == null) {
      return null;
    }
    String trimmed = cron.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (EVERY_ONE_MINUTE.matcher(trimmed).matches()) {
      return "1m";
    }
    Matcher m = EVERY_N_MINUTES.matcher(trimmed);
    if (m.matches()) {
      // Round-trip safety: only return the inverse if the forward direction would re-produce
      // this exact cron. Guards against an operator-typed `0 0/60 * * * ?` (legal Quartz but
      // rejected by periodToCron as >59 minutes) silently round-tripping through the DDL form.
      int n = Integer.parseInt(m.group(1));
      if (n >= 2 && n <= 59) {
        return n + "m";
      }
      return null;
    }
    if (EVERY_ONE_HOUR.matcher(trimmed).matches()) {
      return "1h";
    }
    m = EVERY_N_HOURS.matcher(trimmed);
    if (m.matches()) {
      int n = Integer.parseInt(m.group(1));
      if (n >= 2 && n <= 23) {
        return n + "h";
      }
      return null;
    }
    if (EVERY_ONE_DAY.matcher(trimmed).matches()) {
      return "1d";
    }
    m = EVERY_N_DAYS.matcher(trimmed);
    if (m.matches()) {
      int n = Integer.parseInt(m.group(1));
      if (n >= 2 && n <= 28) {
        return n + "d";
      }
      return null;
    }
    return null;
  }
}
