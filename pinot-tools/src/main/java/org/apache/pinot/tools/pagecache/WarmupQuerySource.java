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
package org.apache.pinot.tools.pagecache;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.pagecache.WarmupQueryUtils;
import org.apache.pinot.common.pagecache.WarmupQueryUtils.Candidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Supplies page-cache warm-up query candidates for a set of tables. Implementations read from a
 * particular origin (broker query logs, a query-statistics Pinot table, a curated file) and emit
 * source-agnostic {@link Candidate}s that {@link WarmupQueryUtils#select} then ranks.
 */
public interface WarmupQuerySource {

  /**
   * Fetches candidate queries for the requested raw table names.
   *
   * @return a map from raw table name to its candidate queries; tables with no candidates may be omitted
   */
  Map<String, List<Candidate>> fetchCandidatesByTable(Set<String> rawTableNames)
      throws Exception;

  /**
   * Collects candidates by parsing broker query-log files — the zero-extra-infrastructure source, since
   * every deployment already produces broker query logs. All files are scanned once and lines grouped by
   * the (raw) table they targeted; truncated, errored, multi-stage or unparseable lines are skipped.
   */
  class QueryLog implements WarmupQuerySource {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryLog.class);
    private final List<File> _logFiles;
    private final int _maxQueryLengthToLog;

    /**
     * @param maxQueryLengthToLog the broker's {@code pinot.broker.query.log.length}; queries at this
     *                            length are treated as truncated. Pass {@code 0} to disable the check.
     */
    public QueryLog(List<File> logFiles, int maxQueryLengthToLog) {
      _logFiles = logFiles;
      _maxQueryLengthToLog = maxQueryLengthToLog;
    }

    @Override
    public Map<String, List<Candidate>> fetchCandidatesByTable(Set<String> rawTableNames)
        throws IOException {
      Map<String, List<Candidate>> candidatesByTable = new HashMap<>();
      long matchedLines = 0;
      for (File logFile : _logFiles) {
        try (BufferedReader reader = Files.newBufferedReader(logFile.toPath(), StandardCharsets.UTF_8)) {
          String line;
          while ((line = reader.readLine()) != null) {
            WarmupQueryUtils.ParsedLine parsed = WarmupQueryUtils.parseLogLine(line, _maxQueryLengthToLog);
            if (parsed == null || !rawTableNames.contains(parsed.getTableName())) {
              continue;
            }
            candidatesByTable.computeIfAbsent(parsed.getTableName(), k -> new ArrayList<>()).add(parsed.getCandidate());
            matchedLines++;
          }
        }
      }
      LOGGER.info("Collected {} candidate(s) for {} table(s) from {} query-log file(s)", matchedLines,
          candidatesByTable.size(), _logFiles.size());
      return candidatesByTable;
    }
  }

  /**
   * Collects candidates by querying a user-supplied query-statistics Pinot table (the OSS generalization
   * of an internal "query stats" table). For each target table it pulls a bounded, recent, error-free
   * sample of observed queries with their latency, scan and timestamp statistics. Column names are
   * configurable so any stats-table layout can be used.
   */
  class PinotTable implements WarmupQuerySource {
    private static final Logger LOGGER = LoggerFactory.getLogger(PinotTable.class);
    private final Connection _connection;
    private final String _statsTable;
    private final String _queryColumn;
    private final String _tableColumn;
    private final String _requestTimeColumn;
    private final String _latencyColumn;
    private final String _numDocsScannedColumn;
    private final String _errorCodeColumn;
    private final int _lookbackHours;
    private final int _maxCandidatesPerTable;

    public PinotTable(Connection connection, String statsTable, String queryColumn, String tableColumn,
        String requestTimeColumn, String latencyColumn, String numDocsScannedColumn, String errorCodeColumn,
        int lookbackHours, int maxCandidatesPerTable) {
      _connection = connection;
      // Table/column names are inlined into the SQL below; reject identifiers that could break it.
      _statsTable = requireSafeIdentifier(statsTable, "statsTable");
      _queryColumn = requireSafeIdentifier(queryColumn, "queryColumn");
      _tableColumn = requireSafeIdentifier(tableColumn, "tableColumn");
      _requestTimeColumn = requireSafeIdentifier(requestTimeColumn, "requestTimeColumn");
      _latencyColumn = requireSafeIdentifier(latencyColumn, "latencyColumn");
      _numDocsScannedColumn = requireSafeIdentifier(numDocsScannedColumn, "numDocsScannedColumn");
      _errorCodeColumn = requireSafeIdentifier(errorCodeColumn, "errorCodeColumn");
      _lookbackHours = lookbackHours;
      _maxCandidatesPerTable = maxCandidatesPerTable;
    }

    @Override
    public Map<String, List<Candidate>> fetchCandidatesByTable(Set<String> rawTableNames) {
      Map<String, List<Candidate>> candidatesByTable = new HashMap<>();
      for (String rawTableName : rawTableNames) {
        try {
          List<Candidate> candidates = fetchForTable(rawTableName);
          if (!candidates.isEmpty()) {
            candidatesByTable.put(rawTableName, candidates);
          }
          LOGGER.info("Fetched {} candidate(s) from stats table {} for table {}", candidates.size(), _statsTable,
              rawTableName);
        } catch (Exception e) {
          LOGGER.error("Failed to fetch candidates from stats table {} for table {}", _statsTable, rawTableName, e);
        }
      }
      return candidatesByTable;
    }

    private List<Candidate> fetchForTable(String rawTableName) {
      requireSafeIdentifier(rawTableName, "table name");
      // Column order is significant: query, requestTime, latency, numDocsScanned, errorCode.
      String sql = String.format(
          "SELECT %s, %s, %s, %s, %s FROM %s WHERE \"%s\" = '%s' AND %s > ago('PT%dH') AND %s = 0 LIMIT %d",
          _queryColumn, _requestTimeColumn, _latencyColumn, _numDocsScannedColumn, _errorCodeColumn, _statsTable,
          _tableColumn, rawTableName, _requestTimeColumn, _lookbackHours, _errorCodeColumn, _maxCandidatesPerTable);
      ResultSetGroup resultSetGroup = _connection.execute(_statsTable, sql);
      List<Candidate> candidates = new ArrayList<>();
      if (resultSetGroup.getResultSetCount() == 0) {
        return candidates;
      }
      ResultSet resultSet = resultSetGroup.getResultSet(0);
      for (int row = 0; row < resultSet.getRowCount(); row++) {
        String query = resultSet.getString(row, 0);
        if (StringUtils.isBlank(query)) {
          continue;
        }
        long requestTimeMs = parseLong(resultSet.getString(row, 1));
        long latencyMs = parseLong(resultSet.getString(row, 2));
        long numDocsScanned = parseLong(resultSet.getString(row, 3));
        int errorCode = (int) parseLong(resultSet.getString(row, 4));
        candidates.add(new Candidate(query, requestTimeMs, latencyMs, numDocsScanned, errorCode, null));
      }
      return candidates;
    }

    private static long parseLong(String value) {
      if (StringUtils.isBlank(value)) {
        return 0L;
      }
      try {
        return (long) Double.parseDouble(value.trim());
      } catch (NumberFormatException e) {
        return 0L;
      }
    }

    private static String requireSafeIdentifier(String identifier, String label) {
      if (StringUtils.isBlank(identifier) || StringUtils.containsAny(identifier, '\'', '"', ';', '`')) {
        throw new IllegalArgumentException("Invalid " + label + " for the PINOT_TABLE source: " + identifier);
      }
      return identifier;
    }
  }

  /**
   * Reads warm-up queries verbatim from a file, one SQL statement per line, all attributed to a single
   * target table — the manual-override source. Blank lines and {@code --} comments are ignored;
   * candidates carry no statistics, so selection effectively de-duplicates and caps them in file order.
   */
  class QueryFile implements WarmupQuerySource {
    private final File _queryFile;
    private final String _rawTableName;

    public QueryFile(File queryFile, String rawTableName) {
      _queryFile = queryFile;
      _rawTableName = rawTableName;
    }

    @Override
    public Map<String, List<Candidate>> fetchCandidatesByTable(Set<String> rawTableNames)
        throws IOException {
      List<Candidate> candidates = new ArrayList<>();
      for (String line : Files.readAllLines(_queryFile.toPath(), StandardCharsets.UTF_8)) {
        String query = line.trim();
        if (query.isEmpty() || query.startsWith("--")) {
          continue;
        }
        candidates.add(new Candidate(query, 0L, 0L, 0L, 0, null));
      }
      return candidates.isEmpty() ? Map.of() : Map.of(_rawTableName, candidates);
    }
  }
}
