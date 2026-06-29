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
package org.apache.pinot.tools.admin.command;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.TableAdminClient;
import org.apache.pinot.common.pagecache.WarmupQueryUtils;
import org.apache.pinot.spi.config.table.PageCacheWarmupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.pagecache.WarmupQuerySource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Generates and stores page-cache warm-up queries for one or more tables.
 *
 * <p>This is the OSS, infrastructure-free way to populate the warm-up query files that Pinot servers
 * replay on restart and segment refresh. It reads candidate queries from a configurable source
 * (broker query logs, a query-statistics Pinot table, or a curated file), ranks them with the chosen
 * {@link WarmupQueryUtils.Policy}, rewrites the table reference to the table-name-with-type the
 * server expects, and stores them via the controller {@code /pagecache/queries} endpoint. Run it
 * periodically (e.g. from cron) to keep the warm-up sets fresh.</p>
 *
 * <p>Sample usage:</p>
 * <pre>{@code
 * pinot-admin.sh GeneratePageCacheWarmupQueries \
 *     -controllerHost localhost -controllerPort 9000 \
 *     -tables myTable -tableType OFFLINE \
 *     -source QUERY_LOG -queryLogFile /var/log/pinot/broker.log \
 *     -policy HYBRID
 * }</pre>
 */
@CommandLine.Command(name = "GeneratePageCacheWarmupQueries", mixinStandardHelpOptions = true)
public class GeneratePageCacheWarmupQueriesCommand extends AbstractDatabaseBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeneratePageCacheWarmupQueriesCommand.class);

  /** Where warm-up query candidates are read from. */
  public enum Source {
    QUERY_LOG, PINOT_TABLE, FILE
  }

  @CommandLine.Option(names = {"-tables"}, description = "Comma-separated raw table names to generate "
      + "warm-up queries for. If omitted, all tables with page-cache warmup enabled are auto-discovered.")
  private String _tables;

  @CommandLine.Option(names = {"-tableType"}, description = "Table type for -tables: OFFLINE, REALTIME or "
      + "HYBRID (writes both). Default: OFFLINE. Ignored when tables are auto-discovered.")
  private String _tableType = "OFFLINE";

  @CommandLine.Option(names = {"-source"}, description = "Candidate query source: QUERY_LOG, PINOT_TABLE or "
      + "FILE. Default: QUERY_LOG.")
  private Source _source = Source.QUERY_LOG;

  @CommandLine.Option(names = {"-policy"}, description = "Selection policy: UNIFORM, LATENCY, "
      + "NUM_DOCS_SCANNED or HYBRID. Default: UNIFORM.")
  private String _policy = WarmupQueryUtils.Policy.UNIFORM.name();

  // QUERY_LOG source options
  @CommandLine.Option(names = {"-queryLogFile"}, description = "QUERY_LOG source: comma-separated broker "
      + "query-log file path(s) to parse.")
  private String _queryLogFile;

  @CommandLine.Option(names = {"-queryLogMaxLength"}, description = "QUERY_LOG source: the broker's "
      + "pinot.broker.query.log.length; queries logged at this length are treated as truncated and dropped. "
      + "Default: 0 (disabled, matching the broker default of no truncation).")
  private int _queryLogMaxLength = 0;

  // PINOT_TABLE source options
  @CommandLine.Option(names = {"-statsTable"}, description = "PINOT_TABLE source: name of the query-statistics "
      + "Pinot table to read candidates from.")
  private String _statsTable;

  @CommandLine.Option(names = {"-statsQueryColumn"}, description = "PINOT_TABLE source: query text column. "
      + "Default: query.")
  private String _statsQueryColumn = "query";

  @CommandLine.Option(names = {"-statsTableColumn"}, description = "PINOT_TABLE source: issuing-table column. "
      + "Default: table.")
  private String _statsTableColumn = "table";

  @CommandLine.Option(names = {"-statsRequestTimeColumn"}, description = "PINOT_TABLE source: request-time "
      + "(epoch millis) column. Default: requestTime.")
  private String _statsRequestTimeColumn = "requestTime";

  @CommandLine.Option(names = {"-statsLatencyColumn"}, description = "PINOT_TABLE source: latency column. "
      + "Default: queryProcessingDuration.")
  private String _statsLatencyColumn = "queryProcessingDuration";

  @CommandLine.Option(names = {"-statsNumDocsScannedColumn"}, description = "PINOT_TABLE source: documents-scanned "
      + "column. Default: scannedDocumentCount.")
  private String _statsNumDocsScannedColumn = "scannedDocumentCount";

  @CommandLine.Option(names = {"-statsErrorCodeColumn"}, description = "PINOT_TABLE source: error-code column. "
      + "Default: errorCode.")
  private String _statsErrorCodeColumn = "errorCode";

  @CommandLine.Option(names = {"-statsMaxCandidates"}, description = "PINOT_TABLE source: max candidate rows to "
      + "pull per table. Default: 50000.")
  private int _statsMaxCandidates = 50000;

  // FILE source options
  @CommandLine.Option(names = {"-queryFile"}, description = "FILE source: file containing one SQL query per line.")
  private String _queryFile;

  // Selection tuning
  @CommandLine.Option(names = {"-lookbackHours"}, description = "Look-back window in hours. Default: 48.")
  private int _lookbackHours = WarmupQueryUtils.Config.DEFAULT_LOOKBACK_HOURS;

  @CommandLine.Option(names = {"-frequencyHours"}, description = "UNIFORM bucket width in hours. Default: 4.")
  private int _frequencyHours = WarmupQueryUtils.Config.DEFAULT_FREQUENCY_HOURS;

  @CommandLine.Option(names = {"-minQueries"}, description = "Warn (but still store) if fewer than this many "
      + "queries are selected for a table. Default: 0.")
  private int _minQueries = 0;

  @CommandLine.Option(names = {"-maxQueries"}, description = "Max queries to store per table. Default: 20000.")
  private int _maxQueries = WarmupQueryUtils.Config.DEFAULT_MAX_QUERIES;

  @CommandLine.Option(names = {"-maxQueryLength"}, description = "Drop candidate queries longer than this. "
      + "Default: 5000.")
  private int _maxQueryLength = WarmupQueryUtils.Config.DEFAULT_MAX_QUERY_LENGTH;

  @CommandLine.Option(names = {"-dryRun"}, description = "Log the selected queries instead of storing them.")
  private boolean _dryRun = false;

  @Override
  public String getName() {
    return "GeneratePageCacheWarmupQueries";
  }

  @Override
  public String description() {
    return "Generate and store page-cache warm-up queries for tables, selected from broker query logs, a "
        + "query-statistics table, or a file.";
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }
    WarmupQueryUtils.Policy policy = WarmupQueryUtils.Policy.fromString(_policy);
    WarmupQueryUtils.Config selectorConfig = WarmupQueryUtils.Config.builder()
        .setLookbackHours(_lookbackHours)
        .setFrequencyHours(_frequencyHours)
        .setMaxQueries(_maxQueries)
        .setMaxQueryLength(_maxQueryLength)
        .build();

    try (PinotAdminClient adminClient = getPinotAdminClient()) {
      // Map of raw table name -> the table types to write for it.
      Map<String, Set<TableType>> tableToTypes = resolveTargetTables(adminClient);
      if (tableToTypes.isEmpty()) {
        LOGGER.warn("No target tables resolved. Provide -tables, or enable page-cache warmup on some table.");
        return false;
      }
      LOGGER.info("Generating warm-up queries for {} table(s) using source={}, policy={}", tableToTypes.size(),
          _source, policy);

      // Only the PINOT_TABLE source needs a query connection; own it here so it is always closed.
      Connection connection = _source == Source.PINOT_TABLE ? createControllerConnection() : null;
      try {
        WarmupQuerySource querySource = buildSource(tableToTypes.keySet(), connection);
        Map<String, List<WarmupQueryUtils.Candidate>> candidatesByTable =
            querySource.fetchCandidatesByTable(tableToTypes.keySet());

        boolean allOk = true;
        for (Map.Entry<String, Set<TableType>> entry : tableToTypes.entrySet()) {
          String rawTable = entry.getKey();
          List<WarmupQueryUtils.Candidate> candidates = candidatesByTable.getOrDefault(rawTable, List.of());
          List<String> selected = WarmupQueryUtils.select(candidates, policy, selectorConfig);
          if (selected.isEmpty()) {
            LOGGER.warn("No warm-up queries selected for table {} (candidates={})", rawTable, candidates.size());
            allOk = false;
            continue;
          }
          if (selected.size() < _minQueries) {
            LOGGER.warn("Selected only {} warm-up queries for table {} (below -minQueries={})", selected.size(),
                rawTable, _minQueries);
          }
          for (TableType tableType : entry.getValue()) {
            storeForType(adminClient, rawTable, tableType, selected);
          }
        }
        return allOk;
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    }
  }

  /**
   * Rewrites the selected queries to reference {@code rawTable_TYPE} and stores them (or logs them in
   * dry-run mode).
   */
  private void storeForType(PinotAdminClient adminClient, String rawTable, TableType tableType,
      List<String> selectedQueries)
      throws Exception {
    String tableNameWithType = rawTable + "_" + tableType.name();
    List<String> rewritten = selectedQueries.stream()
        .map(q -> WarmupQueryUtils.rewriteTableName(q, rawTable, tableNameWithType))
        .collect(Collectors.toList());
    if (_dryRun) {
      LOGGER.info("[dry-run] Would store {} warm-up queries for {}:", rewritten.size(), tableNameWithType);
      for (String query : rewritten) {
        LOGGER.info("[dry-run]   {}", query);
      }
      return;
    }
    adminClient.getPageCacheWarmupClient().storeWarmupQueries(rawTable, tableType.name(), rewritten);
    LOGGER.info("Stored {} warm-up queries for {}", rewritten.size(), tableNameWithType);
  }

  /**
   * Resolves the set of (raw table, table types) to generate for: either the explicit {@code -tables}
   * with {@code -tableType}, or, when none is given, every table that has page-cache warmup enabled.
   */
  @VisibleForTesting
  Map<String, Set<TableType>> resolveTargetTables(PinotAdminClient adminClient)
      throws Exception {
    Map<String, Set<TableType>> tableToTypes = new LinkedHashMap<>();
    if (StringUtils.isNotBlank(_tables)) {
      Set<TableType> types = parseTableTypes(_tableType);
      for (String rawTable : splitCsv(_tables)) {
        tableToTypes.put(rawTable, types);
      }
      return tableToTypes;
    }
    // Auto-discover: keep tables whose config enables warmup on restart or refresh.
    TableAdminClient tableClient = adminClient.getTableClient();
    // Result order is normalized via the TreeSet below, so no server-side sort is requested.
    for (String rawTable : new TreeSet<>(tableClient.listTables(null, null, null, false))) {
      for (TableType tableType : TableType.values()) {
        TableConfig tableConfig = tableClient.getTableConfigObject(rawTable, tableType.name());
        if (tableConfig != null && isWarmupEnabled(tableConfig.getPageCacheWarmupConfig())) {
          tableToTypes.computeIfAbsent(rawTable, k -> new HashSet<>()).add(tableType);
        }
      }
    }
    return tableToTypes;
  }

  private static boolean isWarmupEnabled(PageCacheWarmupConfig warmupConfig) {
    if (warmupConfig == null) {
      return false;
    }
    PageCacheWarmupConfig.Spec onRestart = warmupConfig.getOnRestart();
    PageCacheWarmupConfig.Spec onRefresh = warmupConfig.getOnRefresh();
    return (onRestart != null && onRestart.isEnabled()) || (onRefresh != null && onRefresh.isEnabled());
  }

  private WarmupQuerySource buildSource(Set<String> rawTableNames, Connection connection) {
    switch (_source) {
      case PINOT_TABLE:
        if (StringUtils.isBlank(_statsTable)) {
          throw new IllegalArgumentException("-statsTable is required for the PINOT_TABLE source");
        }
        return new WarmupQuerySource.PinotTable(connection, _statsTable, _statsQueryColumn, _statsTableColumn,
            _statsRequestTimeColumn, _statsLatencyColumn, _statsNumDocsScannedColumn, _statsErrorCodeColumn,
            _lookbackHours, _statsMaxCandidates);
      case FILE:
        if (StringUtils.isBlank(_queryFile)) {
          throw new IllegalArgumentException("-queryFile is required for the FILE source");
        }
        if (rawTableNames.size() != 1) {
          throw new IllegalArgumentException("The FILE source requires exactly one table via -tables");
        }
        return new WarmupQuerySource.QueryFile(new File(_queryFile), rawTableNames.iterator().next());
      case QUERY_LOG:
      default:
        if (StringUtils.isBlank(_queryLogFile)) {
          throw new IllegalArgumentException("-queryLogFile is required for the QUERY_LOG source");
        }
        List<File> logFiles = splitCsv(_queryLogFile).stream().map(File::new).collect(Collectors.toList());
        return new WarmupQuerySource.QueryLog(logFiles, _queryLogMaxLength);
    }
  }

  private Connection createControllerConnection() {
    String controllerUrl = _controllerProtocol + "://" + _controllerHost + ":" + _controllerPort;
    return ConnectionFactory.fromController(controllerUrl);
  }

  private static Set<TableType> parseTableTypes(String tableType) {
    String normalized = tableType == null ? "OFFLINE" : tableType.trim().toUpperCase(Locale.ROOT);
    if ("HYBRID".equals(normalized)) {
      return new HashSet<>(Arrays.asList(TableType.OFFLINE, TableType.REALTIME));
    }
    return new HashSet<>(List.of(TableType.valueOf(normalized)));
  }

  private static List<String> splitCsv(String value) {
    List<String> result = new ArrayList<>();
    for (String part : value.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        result.add(trimmed);
      }
    }
    return result;
  }
}
