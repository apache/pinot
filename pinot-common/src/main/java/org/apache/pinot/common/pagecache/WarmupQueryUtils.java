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
package org.apache.pinot.common.pagecache;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Source-agnostic logic for generating page-cache warm-up queries: it ranks observed queries into a
 * warm-up set ({@link #select}), rewrites a query's table reference to the table-name-with-type the
 * server expects ({@link #rewriteTableName}), and parses broker query-log lines into candidates
 * ({@link #parseLogLine}).
 *
 * <p>This is the OSS port of the long-standing offline warm-up selection job, kept in one place so the
 * admin CLI today and a future on-broker collector can reuse it. The nested {@link Candidate},
 * {@link Policy} and {@link Config} types model the inputs; all methods are stateless and thread-safe.</p>
 *
 * <p>The warm-up <i>execution</i> and <i>storage</i> machinery (the controller {@code /pagecache/queries}
 * endpoint and the server replay) is generic and lives elsewhere; this class only fills the gap of
 * deciding <i>which</i> queries to store.</p>
 */
public class WarmupQueryUtils {
  private static final long MILLIS_PER_HOUR = 3600_000L;

  // --- Query-log parsing patterns ---------------------------------------------------------------
  private static final String QUERY_MARKER = ",query=";
  private static final Pattern TABLE_PATTERN = Pattern.compile("[,\\s]table=([^,]+)");
  private static final Pattern TIME_MS_PATTERN = Pattern.compile("[,\\s]timeMs=(\\d+)");
  private static final Pattern DOCS_PATTERN = Pattern.compile("[,\\s]docs=(\\d+)/");
  private static final Pattern EXCEPTIONS_PATTERN = Pattern.compile("[,\\s]exceptions=(\\d+)");
  private static final Pattern QUERY_HASH_PATTERN = Pattern.compile("[,\\s]queryHash=([^,]+)");

  private WarmupQueryUtils() {
  }

  // ===============================================================================================
  // Selection
  // ===============================================================================================

  /**
   * Selects warm-up queries from {@code candidates} using {@code policy}.
   *
   * <p>All statistics are best-effort: when a source cannot provide timestamps or latency/scan
   * statistics, the percentile/time-bucket policies degrade to taking candidates in supplied order, so
   * a plain file of queries still yields a usable warm-up set. The returned queries still reference the
   * raw table name; callers rewrite them via {@link #rewriteTableName} before storing.</p>
   *
   * @return the de-duplicated, capped list of selected query strings, in selection order; never null
   */
  public static List<String> select(List<Candidate> candidates, Policy policy, Config config) {
    List<Candidate> filtered = prefilter(candidates, config);
    if (filtered.isEmpty()) {
      return List.of();
    }
    List<Candidate> selected;
    switch (policy) {
      case LATENCY:
        selected = byPercentile(filtered, config, Candidate::getLatencyMs);
        break;
      case NUM_DOCS_SCANNED:
        selected = byPercentile(filtered, config, Candidate::getNumDocsScanned);
        break;
      case HYBRID:
        selected = hybrid(filtered, config);
        break;
      case UNIFORM:
      default:
        selected = uniform(filtered, config);
        break;
    }
    return dedupAndCap(selected, config.getMaxQueries());
  }

  /** Drops error responses, blank queries and queries longer than the configured maximum. */
  private static List<Candidate> prefilter(List<Candidate> candidates, Config config) {
    if (candidates == null) {
      return List.of();
    }
    return candidates.stream()
        .filter(c -> c != null && c.getErrorCode() == 0)
        .filter(c -> StringUtils.isNotBlank(c.getQuery()))
        .filter(c -> c.getQuery().length() <= config.getMaxQueryLength())
        .collect(Collectors.toList());
  }

  /**
   * Spreads selection across the look-back window: divides it into {@code frequencyHours} buckets and
   * draws an even share from each, newest first. Candidates without a usable timestamp fall outside
   * every bucket; if that leaves the result empty, falls back to insertion order.
   */
  private static List<Candidate> uniform(List<Candidate> filtered, Config config) {
    long now = config.getNowMs();
    int lookbackHours = config.getLookbackHours();
    int frequencyHours = config.getFrequencyHours();
    int numBuckets = (lookbackHours + frequencyHours - 1) / frequencyHours;
    int perBucket = Math.max(1, config.getMaxQueries() / numBuckets);

    List<Candidate> result = new ArrayList<>();
    for (int lowerHours = 0; lowerHours < lookbackHours && result.size() < config.getMaxQueries();
        lowerHours += frequencyHours) {
      int upperHours = Math.min(lowerHours + frequencyHours, lookbackHours);
      long olderBoundMs = now - upperHours * MILLIS_PER_HOUR;
      long newerBoundMs = now - lowerHours * MILLIS_PER_HOUR;
      int taken = 0;
      for (Candidate candidate : filtered) {
        if (taken >= perBucket) {
          break;
        }
        long requestTimeMs = candidate.getRequestTimeMs();
        if (requestTimeMs > olderBoundMs && requestTimeMs <= newerBoundMs) {
          result.add(candidate);
          taken++;
        }
      }
    }
    return result.isEmpty() ? new ArrayList<>(filtered) : result;
  }

  /**
   * Keeps candidates whose metric exceeds the configured percentile cut-off, ordered by the metric
   * descending. Falls back to insertion order when the metric is unavailable (all zero).
   */
  private static List<Candidate> byPercentile(List<Candidate> filtered, Config config,
      ToLongFunction<Candidate> metric) {
    long[] values = filtered.stream().mapToLong(metric).toArray();
    long cutoff = percentile(values, config.getPercentile());
    List<Candidate> selected = filtered.stream()
        .filter(c -> metric.applyAsLong(c) > cutoff)
        .sorted(Comparator.comparingLong(metric).reversed())
        .collect(Collectors.toList());
    return selected.isEmpty() ? new ArrayList<>(filtered) : selected;
  }

  /**
   * Round-robin merge of the uniform, latency and num-docs-scanned selections, so the final set blends
   * recency, slow queries and heavy scans. De-duplication and capping are applied by {@link #select}.
   */
  private static List<Candidate> hybrid(List<Candidate> filtered, Config config) {
    List<List<Candidate>> lists = List.of(
        uniform(filtered, config),
        byPercentile(filtered, config, Candidate::getLatencyMs),
        byPercentile(filtered, config, Candidate::getNumDocsScanned));
    List<Candidate> merged = new ArrayList<>();
    int maxSize = lists.stream().mapToInt(List::size).max().orElse(0);
    for (int i = 0; i < maxSize; i++) {
      for (List<Candidate> list : lists) {
        if (i < list.size()) {
          merged.add(list.get(i));
        }
      }
    }
    return merged;
  }

  /**
   * De-duplicates by {@link Candidate#getDedupKey()} preserving order, returns the query strings, and
   * caps the result at {@code maxQueries}.
   */
  private static List<String> dedupAndCap(List<Candidate> candidates, int maxQueries) {
    Set<String> seen = new HashSet<>();
    List<String> result = new ArrayList<>();
    for (Candidate candidate : candidates) {
      if (result.size() >= maxQueries) {
        break;
      }
      if (seen.add(candidate.getDedupKey())) {
        result.add(candidate.getQuery());
      }
    }
    return result;
  }

  /** Nearest-rank percentile over {@code values}. Returns {@code 0} for an empty array. */
  static long percentile(long[] values, int percentile) {
    if (values.length == 0) {
      return 0;
    }
    long[] sorted = values.clone();
    Arrays.sort(sorted);
    int rank = (int) Math.ceil((percentile / 100.0) * sorted.length);
    int index = Math.min(Math.max(rank, 1), sorted.length) - 1;
    return sorted[index];
  }

  // ===============================================================================================
  // Table-name rewrite
  // ===============================================================================================

  /**
   * Returns {@code query} with every {@code FROM}/{@code JOIN} reference to {@code rawTableName}
   * replaced by {@code tableNameWithType} (e.g. {@code myTable} &rarr; {@code myTable_OFFLINE}).
   *
   * <p>This is required because the server warm-up executor compiles each stored query and resolves
   * the table strictly by the name in its {@code FROM} clause. Unlike a naive {@code String.replace},
   * the table identifier is only substituted when it is a {@code FROM}/{@code JOIN} target, guarded by
   * word boundaries and an optional pair of double quotes, so columns, aliases or string literals that
   * merely contain the table name are left untouched. The keyword is matched case-insensitively; the
   * identifier case-sensitively (Pinot table names are case-sensitive).</p>
   */
  public static String rewriteTableName(String query, String rawTableName, String tableNameWithType) {
    if (query == null || query.isEmpty() || rawTableName == null || rawTableName.isEmpty()) {
      return query;
    }
    // \b((?i:from|join)) keyword (group 1); (\s+) whitespace (group 2); ("?) optional quote (group 3);
    // the literal raw table name; \3 the matching closing quote; a trailing token boundary so we don't
    // match a prefix of a longer identifier such as "myTable_OFFLINE" or a column like "myTable_id".
    Pattern pattern = Pattern.compile(
        "\\b((?i:from|join))(\\s+)(\"?)" + Pattern.quote(rawTableName) + "\\3(?=\\s|,|\\)|;|$)");
    Matcher matcher = pattern.matcher(query);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      String quote = matcher.group(3);
      matcher.appendReplacement(sb,
          Matcher.quoteReplacement(matcher.group(1) + matcher.group(2) + quote + tableNameWithType + quote));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  // ===============================================================================================
  // Broker query-log parsing
  // ===============================================================================================

  /**
   * Parses a single broker query-log line (as emitted by {@code QueryLogger}) into a candidate.
   *
   * <p>A line is a comma-separated list of {@code key=value} entries with the SQL appended last as
   * {@code query=<SQL>}; because the SQL may contain commas, everything after the final {@code ,query=}
   * marker is the query text. The {@code table=} field is normalized to the raw table name (a
   * {@code _OFFLINE}/{@code _REALTIME} suffix is stripped); multi-stage queries that touch several
   * tables are logged as {@code table=[t1, t2]} and skipped, since warm-up replays each query against a
   * single table. Truncated lines (query length reaches {@code maxQueryLengthToLog}) and lines without a
   * parseable query/table are skipped (return {@code null}); query exceptions are recorded as a non-zero
   * error code so {@link #select} filters them.</p>
   *
   * @param maxQueryLengthToLog the broker's {@code pinot.broker.query.log.length}; pass {@code 0} to
   *                            disable the truncation check (the broker default does not truncate).
   */
  @Nullable
  public static ParsedLine parseLogLine(String line, int maxQueryLengthToLog) {
    if (StringUtils.isBlank(line)) {
      return null;
    }
    int markerIndex = line.indexOf(QUERY_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String query = line.substring(markerIndex + QUERY_MARKER.length());
    if (StringUtils.isBlank(query)) {
      return null;
    }
    if (maxQueryLengthToLog > 0 && query.length() >= maxQueryLengthToLog) {
      return null;
    }
    String structured = line.substring(0, markerIndex);
    String tableField = firstGroup(TABLE_PATTERN, structured);
    if (StringUtils.isBlank(tableField)) {
      return null;
    }
    tableField = tableField.trim();
    if (tableField.startsWith("[")) {
      return null;
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableField);
    int exceptions = (int) longGroup(EXCEPTIONS_PATTERN, structured, 0);
    long latencyMs = longGroup(TIME_MS_PATTERN, structured, 0);
    long numDocsScanned = longGroup(DOCS_PATTERN, structured, 0);
    String queryHash = firstGroup(QUERY_HASH_PATTERN, structured);
    return new ParsedLine(rawTableName, new Candidate(query, 0L, latencyMs, numDocsScanned, exceptions, queryHash));
  }

  @Nullable
  private static String firstGroup(Pattern pattern, String text) {
    Matcher matcher = pattern.matcher(text);
    return matcher.find() ? matcher.group(1) : null;
  }

  private static long longGroup(Pattern pattern, String text, long defaultValue) {
    Matcher matcher = pattern.matcher(text);
    if (matcher.find()) {
      try {
        return Long.parseLong(matcher.group(1));
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  // ===============================================================================================
  // Input types
  // ===============================================================================================

  /** Strategy for ranking {@link Candidate}s into a warm-up set; names line up with the table config. */
  public enum Policy {
    /** Sample uniformly from time buckets across the look-back window. */
    UNIFORM,
    /** Prefer the slowest queries (above the latency percentile cut-off). */
    LATENCY,
    /** Prefer the heaviest scans (above the documents-scanned percentile cut-off). */
    NUM_DOCS_SCANNED,
    /** Round-robin merge of {@link #UNIFORM}, {@link #LATENCY} and {@link #NUM_DOCS_SCANNED}. */
    HYBRID;

    /** Parses a policy name case-insensitively, defaulting to {@link #UNIFORM} for null/blank input. */
    public static Policy fromString(String value) {
      if (value == null || value.trim().isEmpty()) {
        return UNIFORM;
      }
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
  }

  /**
   * A single observed query plus the lightweight statistics used to rank it. Immutable. Any statistic
   * unknown for a source is left at {@code 0}; the {@link #getQuery() query} references the raw table
   * name.
   */
  public static class Candidate {
    private final String _query;
    private final long _requestTimeMs;
    private final long _latencyMs;
    private final long _numDocsScanned;
    private final int _errorCode;
    @Nullable
    private final String _queryHash;

    public Candidate(String query, long requestTimeMs, long latencyMs, long numDocsScanned, int errorCode,
        @Nullable String queryHash) {
      _query = query;
      _requestTimeMs = requestTimeMs;
      _latencyMs = latencyMs;
      _numDocsScanned = numDocsScanned;
      _errorCode = errorCode;
      _queryHash = queryHash;
    }

    public String getQuery() {
      return _query;
    }

    public long getRequestTimeMs() {
      return _requestTimeMs;
    }

    public long getLatencyMs() {
      return _latencyMs;
    }

    public long getNumDocsScanned() {
      return _numDocsScanned;
    }

    public int getErrorCode() {
      return _errorCode;
    }

    /** The de-duplication key: the query hash when present, otherwise the trimmed query text. */
    public String getDedupKey() {
      return StringUtils.isNotBlank(_queryHash) ? _queryHash : StringUtils.trimToEmpty(_query);
    }
  }

  /** The result of parsing one query-log line: the raw target table and its warm-up candidate. */
  public static class ParsedLine {
    private final String _tableName;
    private final Candidate _candidate;

    public ParsedLine(String tableName, Candidate candidate) {
      _tableName = tableName;
      _candidate = candidate;
    }

    public String getTableName() {
      return _tableName;
    }

    public Candidate getCandidate() {
      return _candidate;
    }
  }

  /** Selection tunables; build with {@link Builder}. Defaults mirror the offline warm-up job. */
  public static class Config {
    public static final int DEFAULT_LOOKBACK_HOURS = 48;
    public static final int DEFAULT_FREQUENCY_HOURS = 4;
    public static final int DEFAULT_MAX_QUERIES = 20000;
    public static final int DEFAULT_MAX_QUERY_LENGTH = 5000;
    public static final int DEFAULT_PERCENTILE = 90;

    private final int _lookbackHours;
    private final int _frequencyHours;
    private final int _maxQueries;
    private final int _maxQueryLength;
    private final int _percentile;
    // Reference "now" used to bucket candidates by age; injectable so selection is deterministic in tests.
    private final long _nowMs;

    private Config(Builder builder) {
      _lookbackHours = builder._lookbackHours;
      _frequencyHours = builder._frequencyHours;
      _maxQueries = builder._maxQueries;
      _maxQueryLength = builder._maxQueryLength;
      _percentile = builder._percentile;
      _nowMs = builder._nowMs;
    }

    public int getLookbackHours() {
      return _lookbackHours;
    }

    public int getFrequencyHours() {
      return _frequencyHours;
    }

    public int getMaxQueries() {
      return _maxQueries;
    }

    public int getMaxQueryLength() {
      return _maxQueryLength;
    }

    public int getPercentile() {
      return _percentile;
    }

    public long getNowMs() {
      return _nowMs;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private int _lookbackHours = DEFAULT_LOOKBACK_HOURS;
      private int _frequencyHours = DEFAULT_FREQUENCY_HOURS;
      private int _maxQueries = DEFAULT_MAX_QUERIES;
      private int _maxQueryLength = DEFAULT_MAX_QUERY_LENGTH;
      private int _percentile = DEFAULT_PERCENTILE;
      private long _nowMs = System.currentTimeMillis();

      public Builder setLookbackHours(int lookbackHours) {
        _lookbackHours = lookbackHours;
        return this;
      }

      public Builder setFrequencyHours(int frequencyHours) {
        _frequencyHours = frequencyHours;
        return this;
      }

      public Builder setMaxQueries(int maxQueries) {
        _maxQueries = maxQueries;
        return this;
      }

      public Builder setMaxQueryLength(int maxQueryLength) {
        _maxQueryLength = maxQueryLength;
        return this;
      }

      public Builder setPercentile(int percentile) {
        _percentile = percentile;
        return this;
      }

      public Builder setNowMs(long nowMs) {
        _nowMs = nowMs;
        return this;
      }

      public Config build() {
        Preconditions.checkArgument(_lookbackHours > 0, "lookbackHours must be positive");
        Preconditions.checkArgument(_frequencyHours > 0, "frequencyHours must be positive");
        Preconditions.checkArgument(_frequencyHours <= _lookbackHours, "frequencyHours must be <= lookbackHours");
        Preconditions.checkArgument(_maxQueries > 0, "maxQueries must be positive");
        Preconditions.checkArgument(_maxQueryLength > 0, "maxQueryLength must be positive");
        Preconditions.checkArgument(_percentile >= 0 && _percentile <= 100, "percentile must be in [0, 100]");
        return new Config(this);
      }
    }
  }
}
