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
package org.apache.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class BrokerReduceService extends BaseReduceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceService.class);

  public BrokerReduceService(PinotConfiguration config) {
    super(config);
  }

  /// [org.apache.pinot.spi.query.QueryThreadContext] must already be set up before calling this method.
  public BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest, BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, long reduceTimeOutMs, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    boolean enableTrace =
        queryOptions != null && Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));

    ExecutionStatsAggregator aggregator = new ExecutionStatsAggregator(enableTrace);
    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();

    // Process server response metadata, drop empty/null-schema/conflicting-schema data tables, and pick
    // a data schema (preferring one backed by data rows).
    List<ServerRoutingInstance> serversWithConflictingDataSchema = new ArrayList<>();
    DataSchema cachedDataSchema =
        filterDataTablesAndPickSchema(dataTableMap, aggregator, serversWithConflictingDataSchema);

    String tableName = serverBrokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    // Set execution statistics and Update broker metrics.
    aggregator.setStats(rawTableName, brokerResponseNative, brokerMetrics);

    // If configured, filter out SERVER_SEGMENT_MISSING exceptions emitted by servers. This must happen after
    // aggregator.setStats(), because the aggregator appends server exceptions during setStats.
    Map<String, String> brokerQueryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    if (brokerQueryOptions != null && QueryOptionsUtils.isIgnoreMissingSegments(brokerQueryOptions)) {
      brokerResponseNative.getExceptions().removeIf(
          ex -> ex.getErrorCode() == QueryErrorCode.SERVER_SEGMENT_MISSING.getId());
    }

    // Report the servers with conflicting data schema.
    if (!serversWithConflictingDataSchema.isEmpty()) {
      QueryErrorCode errorCode = QueryErrorCode.MERGE_RESPONSE;
      String errorMessage = errorCode.getDefaultMessage() + ": responses for table: " + tableName
          + " from servers: " + serversWithConflictingDataSchema + " got dropped due to data schema inconsistency.";
      LOGGER.warn(errorMessage);
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1);
      brokerResponseNative.addException(new QueryProcessingException(errorCode, errorMessage));
    }

    // NOTE: When there is no cached data schema, that means all servers encountered exception. In such case, return the
    //       response with metadata only.
    if (cachedDataSchema == null) {
      return brokerResponseNative;
    }

    QueryContext serverQueryContext = QueryContextConverterUtils.getQueryContext(serverBrokerRequest.getPinotQuery());
    DataTableReducer dataTableReducer = ResultReducerFactory.getResultReducer(serverQueryContext);
    DataTableReducerContext reducerContext = createReducerContext(queryOptions, reduceTimeOutMs);

    try {
      dataTableReducer.reduceAndSetResults(rawTableName, cachedDataSchema, dataTableMap, brokerResponseNative,
          reducerContext, brokerMetrics);
    } catch (RuntimeException e) {
      // First check terminate exception and use it as the results block if exists. We want to return the termination
      // reason when query is explicitly terminated.
      QueryException queryException = QueryThreadContext.getTerminateException();
      if (queryException == null && e instanceof QueryException) {
        queryException = (QueryException) e;
      }
      if (queryException != null) {
        brokerResponseNative.addException(QueryProcessingException.fromQueryException(queryException));
      } else {
        LOGGER.error("Caught exception while reducing data tables (query: {})", serverQueryContext, e);
        brokerResponseNative.addException(new QueryProcessingException(QueryErrorCode.MERGE_RESPONSE,
            "Caught exception while reducing data tables: " + e.getMessage()));
      }
    }
    QueryContext queryContext;
    if (brokerRequest == serverBrokerRequest) {
      queryContext = serverQueryContext;
    } else {
      /// `brokerRequest` and `serverBrokerRequest` differ when the query is either (a) a gapfill
      /// (parser wrapped the user's SELECT into an outer SELECT) or (b) an MV-rewritten query
      /// (broker rewrote `serverBrokerRequest` to target the MV table while the user's
      /// `brokerRequest` stays at the base table for result-shape derivations).  Only (a) needs
      /// the gapfill post-processor; (b) just uses the user's `queryContext` directly.  Any other
      /// mismatched `brokerRequest != serverBrokerRequest` caller is rejected to preserve the
      /// original `BadQueryRequestException` invariant ("Nested query is not supported without
      /// gapfill") that pre-dated MV rewrite.
      queryContext = QueryContextConverterUtils.getQueryContext(brokerRequest.getPinotQuery());
      GapfillUtils.GapfillType gapfillType = GapfillUtils.getGapfillType(queryContext);
      if (gapfillType != null) {
        BaseGapfillProcessor gapfillProcessor = GapfillProcessorFactory.getGapfillProcessor(queryContext, gapfillType);
        gapfillProcessor.process(brokerResponseNative);
      } else if (!isMaterializedViewRewrite(serverBrokerRequest)) {
        throw new BadQueryRequestException("Nested query is not supported without gapfill");
      }
    }

    if (!serverQueryContext.isExplain()) {
      updateAlias(queryContext, brokerResponseNative);
    }
    return brokerResponseNative;
  }

  /// MV rewrite signal: when the broker applies a FULL_REWRITE, it stamps the rewritten
  /// server-side query with the [QueryOptionKey#MATERIALIZED_VIEW_REWRITE] marker.  Reading
  /// the marker here keeps the "Nested query is not supported without gapfill" safety net
  /// active for any other path that produces `brokerRequest != serverBrokerRequest` (federated
  /// query, JOIN rewrite, logical-table swap) — only the MV rewrite path explicitly opts out.
  private static boolean isMaterializedViewRewrite(BrokerRequest serverBrokerRequest) {
    if (serverBrokerRequest.getPinotQuery() == null
        || serverBrokerRequest.getPinotQuery().getQueryOptions() == null) {
      return false;
    }
    return Boolean.parseBoolean(
        serverBrokerRequest.getPinotQuery().getQueryOptions().get(QueryOptionKey.MATERIALIZED_VIEW_REWRITE));
  }

  /**
   * Merge-only counterpart of {@link #reduceOnDataTable}: merges the per-server DataTables into a single
   * intermediate {@link DataTable} WITHOUT finalizing (no {@code extractFinalResult}). Returns
   * {@code null} when there is nothing to merge (empty map, or all servers returned no data / errored).
   * Reuses the same schema-filtering preamble and reducer/trim resolution as the regular reduce, but
   * does NOT aggregate per-server execution stats (the merged intermediate does not carry them) and
   * skips the nested-query / gapfill / alias handling (those query shapes are out of scope for
   * merge-only).
   *
   * <p>The returned DataTable carries intermediate, non-finalized state (byte-shape identical to a
   * single server's partial response), so a downstream consumer can intercept it and custom handle
   * the intermediate results.
   *
   * <p>If one or more input server DataTables are dropped during merge (e.g., due to a schema
   * conflict with the first non-empty table), the returned DataTable's metadata carries the
   * {@link DataTable.MetadataKey#PARTIAL_INTERMEDIATE_RESULT} flag set to {@code "true"} and the
   * {@link BrokerMeter#RESPONSE_MERGE_EXCEPTIONS} meter is incremented. This is symmetric with how
   * the regular reduce path surfaces conflicting-schema servers via a response exception and the
   * same meter; downstream consumers can read the flag and decide policy (skip, retry, accept).
   *
   * <p>Execution stats from the input DataTables are aggregated via {@link ExecutionStatsAggregator}
   * and written back onto the merged DataTable: additive longs (e.g. {@code numDocsScanned},
   * {@code numSegments*}, {@code threadCpuTimeNs}), {@code minConsumingFreshnessTimeMs} (MIN-reduced),
   * boolean flags ({@code groupsTrimmed}, {@code numGroupsLimitReached}, etc., OR-reduced),
   * per-server exceptions, and trace info (JSON-encoded if {@code trace=true}). Unlike the regular
   * reduce path, this method does NOT bump broker meters/timers for the input stats — those will
   * fire when the result is eventually re-reduced through {@link #reduceOnDataTable}, so a consumer
   * that uses both APIs sees one set of increments per logical query, not two.
   *
   * <p>Limitations of the round-trip:
   * <ul>
   *   <li>CPU and memory stats round-trip as a single combined value per key (the wire format has
   *       no per-tableType variants). On a re-reduce, the downstream aggregator dumps the whole
   *       value into one bucket — whichever tableType the caller assigned to the synthetic
   *       server response — so the offline-vs-realtime split surfaced on {@link
   *       BrokerResponseNative} is lost across the round-trip, even though the total is preserved.
   *   <li>Exception attribution to original servers is lost; the wire format is {@code Map<Integer,
   *       String>} so collisions on the same error code are resolved last-write-wins.
   *   <li>Per-server trace info is JSON-encoded into a single {@code TRACE_INFO} entry; the
   *       downstream aggregator reads it back as one trace blob under the synthetic server's name.
   * </ul>
   *
   * <p>WARNING: this performs a full cross-server merge and re-serializes the result — heavyweight work
   * that must be run asynchronously, decoupled from request serving. Invoking it inline while a query is
   * being served can severely degrade its latency. Intended for downstream consumers that want to
   * intercept the merged intermediate result instead of the finalized one.
   *
   * <p>[org.apache.pinot.spi.query.QueryThreadContext] must already be set up before calling this method.
   */
  @Nullable
  public DataTable mergeOnDataTable(BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, long reduceTimeOutMs, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      return null;
    }
    Map<String, String> queryOptions = serverBrokerRequest.getPinotQuery().getQueryOptions();
    boolean enableTrace =
        queryOptions != null && Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));
    // Aggregate stats while filtering so we can write them back onto the merged DataTable's metadata.
    // Aggregator.aggregate() runs BEFORE filterDataTablesAndPickSchema removes any entry, matching
    // reduceOnDataTable: empty / conflicting-schema servers' stats are still counted.
    ExecutionStatsAggregator aggregator = new ExecutionStatsAggregator(enableTrace);
    List<ServerRoutingInstance> conflictingServers = new ArrayList<>();
    DataSchema cachedDataSchema = filterDataTablesAndPickSchema(dataTableMap, aggregator, conflictingServers);
    if (cachedDataSchema == null) {
      // All servers returned no data or encountered exceptions; nothing to merge.
      return null;
    }

    String rawTableName = TableNameBuilder.extractRawTableName(serverBrokerRequest.getQuerySource().getTableName());
    QueryContext serverQueryContext = QueryContextConverterUtils.getQueryContext(serverBrokerRequest.getPinotQuery());
    DataTableReducer dataTableReducer = ResultReducerFactory.getResultReducer(serverQueryContext);
    DataTableReducerContext reducerContext =
        createReducerContext(serverBrokerRequest.getPinotQuery().getQueryOptions(), reduceTimeOutMs);
    DataTable merged = dataTableReducer.mergeDataTablesOnly(rawTableName, cachedDataSchema, dataTableMap,
        reducerContext, brokerMetrics);

    if (merged != null) {
      // Write accumulated stats (additive longs, booleans, MIN freshness, exceptions, trace) onto the
      // merged DataTable so it round-trips through reduceOnDataTable. Unlike setStats() this does NOT
      // bump broker meters — those will fire when the result is eventually re-reduced.
      aggregator.setStatsOnMergedDataTable(merged);

      // Symmetric with reduceOnDataTable: surface conflicting-schema drops via meter + warn + a metadata
      // flag on the merged DataTable so downstream consumers can detect a partial merge.
      if (!conflictingServers.isEmpty()) {
        LOGGER.warn("Merge-only reduce dropped {} server response(s) for table {} due to data schema "
            + "inconsistency: {}", conflictingServers.size(), rawTableName, conflictingServers);
        brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1);
        merged.getMetadata().put(DataTable.MetadataKey.PARTIAL_INTERMEDIATE_RESULT.getName(), "true");
      }
    }
    return merged;
  }

  /**
   * Processes per-server response metadata and filters {@code dataTableMap} in place: drops tables with a
   * null schema, drops empty tables (remembering their schema as a fallback), and drops tables whose
   * column data types conflict with the first non-empty table (collected into {@code conflictingServers}).
   * When an {@code aggregator} is provided, per-table execution stats are aggregated before a table is
   * dropped. Returns the remembered data schema (non-empty preferred, else empty-table schema, else
   * {@code null}).
   */
  private static DataSchema filterDataTablesAndPickSchema(Map<ServerRoutingInstance, DataTable> dataTableMap,
      @Nullable ExecutionStatsAggregator aggregator, List<ServerRoutingInstance> conflictingServers) {
    DataSchema dataSchemaFromEmptyDataTable = null;
    DataSchema dataSchemaFromNonEmptyDataTable = null;
    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataTable dataTable = entry.getValue();

      // aggregate metrics
      if (aggregator != null) {
        aggregator.aggregate(entry.getKey(), dataTable);
      }

      // After processing the metadata, remove data tables without data rows inside.
      DataSchema dataSchema = dataTable.getDataSchema();
      if (dataSchema == null) {
        iterator.remove();
      } else {
        // Try to cache a data table with data rows inside, or cache one with data schema inside.
        if (dataTable.getNumberOfRows() == 0) {
          if (dataSchemaFromEmptyDataTable == null) {
            dataSchemaFromEmptyDataTable = dataSchema;
          }
          iterator.remove();
        } else {
          if (dataSchemaFromNonEmptyDataTable == null) {
            dataSchemaFromNonEmptyDataTable = dataSchema;
          } else {
            // Remove data tables with conflicting data schema.
            // NOTE: Only compare the column data types, since the column names (string representation of expression)
            //       can change across different versions.
            if (!Arrays.equals(dataSchema.getColumnDataTypes(), dataSchemaFromNonEmptyDataTable.getColumnDataTypes())) {
              conflictingServers.add(entry.getKey());
              iterator.remove();
            }
          }
        }
      }
    }
    return dataSchemaFromNonEmptyDataTable != null ? dataSchemaFromNonEmptyDataTable : dataSchemaFromEmptyDataTable;
  }

  /**
   * Resolves the group-by trim parameters (query option overrides, else broker defaults) and builds the
   * {@link DataTableReducerContext}. Shared by the regular reduce and the merge-only path.
   */
  private DataTableReducerContext createReducerContext(@Nullable Map<String, String> queryOptions,
      long reduceTimeOutMs) {
    Integer minGroupTrimSizeQueryOption = null;
    Integer groupTrimThresholdQueryOption = null;
    Integer minInitialIndexedTableCapacityQueryOption = null;
    if (queryOptions != null) {
      minGroupTrimSizeQueryOption = QueryOptionsUtils.getMinBrokerGroupTrimSize(queryOptions);
      groupTrimThresholdQueryOption = QueryOptionsUtils.getGroupTrimThreshold(queryOptions);
      minInitialIndexedTableCapacityQueryOption = QueryOptionsUtils.getMinInitialIndexedTableCapacity(queryOptions);
    }
    int minGroupTrimSize = minGroupTrimSizeQueryOption != null ? minGroupTrimSizeQueryOption : _minGroupTrimSize;
    int groupTrimThreshold =
        groupTrimThresholdQueryOption != null ? groupTrimThresholdQueryOption : _groupByTrimThreshold;
    int minInitialIndexedTableCapacity =
        minInitialIndexedTableCapacityQueryOption != null ? minInitialIndexedTableCapacityQueryOption
            : _minInitialIndexedTableCapacity;
    return new DataTableReducerContext(_reduceExecutorService, _maxReduceThreadsPerQuery, reduceTimeOutMs,
        groupTrimThreshold, minGroupTrimSize, minInitialIndexedTableCapacity);
  }

  public void shutDown() {
    _reduceExecutorService.shutdownNow();
  }
}
