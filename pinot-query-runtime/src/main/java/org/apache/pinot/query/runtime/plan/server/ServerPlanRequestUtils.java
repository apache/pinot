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
package org.apache.pinot.query.runtime.plan.server;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.TableSegmentsInfo;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.LogicalTableContext;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.RuntimeFilterNode;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.plan.OpChainConverterDispatcher;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.rewriter.NonAggregationGroupByToDistinctQueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.apache.pinot.sql.parsers.rewriter.RlsFiltersRewriter;


public class ServerPlanRequestUtils {
  private ServerPlanRequestUtils() {
  }

  private static final int DEFAULT_LEAF_NODE_LIMIT = Integer.MAX_VALUE;
  private static final List<String> QUERY_REWRITERS_CLASS_NAMES =
      List.of(PredicateComparisonRewriter.class.getName(),
          NonAggregationGroupByToDistinctQueryRewriter.class.getName(), RlsFiltersRewriter.class.getName());
  private static final List<QueryRewriter> QUERY_REWRITERS =
      new ArrayList<>(QueryRewriterFactory.getQueryRewriters(QUERY_REWRITERS_CLASS_NAMES));
  private static final QueryOptimizer QUERY_OPTIMIZER = new QueryOptimizer();

  public static OpChain compileLeafStage(OpChainExecutionContext executionContext, StagePlan stagePlan,
      QueryExecutor leafQueryExecutor, ExecutorService executorService, Map<String, String> rowFilters) {
    return compileLeafStage(executionContext, stagePlan, leafQueryExecutor, executorService,
        (planNode, multiStageOperator) -> {
        }, false, rowFilters);
  }

  /**
   * main entry point for compiling leaf-stage {@link StagePlan}.
   *
   * @param executionContext the execution context used by the leaf-stage execution engine.
   * @param stagePlan the distribute stage plan on the leaf.
   * @return an opChain that executes the leaf-stage, with the leaf-stage execution encapsulated within.
   */
  public static OpChain compileLeafStage(
      OpChainExecutionContext executionContext,
      StagePlan stagePlan,
      QueryExecutor leafQueryExecutor,
      ExecutorService executorService,
      BiConsumer<PlanNode, MultiStageOperator> relationConsumer,
      boolean explain, @Nullable Map<String, String> rowFilters) {
    long queryArrivalTimeMs = System.currentTimeMillis();

    ServerPlanRequestContext serverContext = new ServerPlanRequestContext(stagePlan, leafQueryExecutor, executorService,
        executionContext.getPipelineBreakerResult());
    // 1. Compile the PinotQuery
    constructPinotQueryPlan(serverContext, executionContext.getOpChainMetadata());
    // 2. Convert PinotQuery into InstanceRequest list (one for each physical table)
    PinotQuery pinotQuery = serverContext.getPinotQuery();
    pinotQuery.setExplain(explain);

    if (MapUtils.isNotEmpty(rowFilters)) {
      pinotQuery.setQueryOptions(rowFilters);
    }

    List<InstanceRequest> instanceRequests;
    if (executionContext.getWorkerMetadata().getLogicalTableSegmentsMap() != null) {
      instanceRequests = constructLogicalTableServerQueryRequests(executionContext, pinotQuery,
          leafQueryExecutor.getInstanceDataManager());
    } else {
      instanceRequests =
          constructServerQueryRequests(executionContext, pinotQuery, leafQueryExecutor.getInstanceDataManager());
    }
    int numRequests = instanceRequests.size();
    List<ServerQueryRequest> serverQueryRequests = new ArrayList<>(numRequests);
    for (InstanceRequest instanceRequest : instanceRequests) {
      serverQueryRequests.add(new ServerQueryRequest(instanceRequest, ServerMetrics.get(), queryArrivalTimeMs, true));
    }
    serverContext.setServerQueryRequests(serverQueryRequests);
    // 3. Compile the OpChain
    executionContext.setLeafStageContext(serverContext);
    return OpChainConverterDispatcher.convert(stagePlan.getRootNode(), executionContext, relationConsumer);
  }

  /**
   * First step of Server physical plan - construct {@link PinotQuery} and determine the leaf-stage boundary
   * {@link PlanNode}.
   *
   * It constructs the content for {@link ServerPlanRequestContext#getPinotQuery()} and set the boundary via:
   *   {@link ServerPlanRequestContext#setLeafStageBoundaryNode(PlanNode)}.
   */
  private static void constructPinotQueryPlan(ServerPlanRequestContext serverContext,
      Map<String, String> requestMetadata) {
    StagePlan stagePlan = serverContext.getStagePlan();
    PinotQuery pinotQuery = serverContext.getPinotQuery();
    // attach leaf node limit it not set
    Integer leafNodeLimit = QueryOptionsUtils.getMultiStageLeafLimit(requestMetadata);
    pinotQuery.setLimit(leafNodeLimit != null ? leafNodeLimit : DEFAULT_LEAF_NODE_LIMIT);
    // visit the plan and create PinotQuery and determine the leaf stage boundary PlanNode.
    ServerPlanRequestVisitor.walkPlanNode(stagePlan.getRootNode(), serverContext);
  }

  /**
   * Entry point to construct a list of {@link InstanceRequest}s for executing leaf-stage v1 runner.
   */
  public static List<InstanceRequest> constructServerQueryRequests(OpChainExecutionContext executionContext,
      PinotQuery pinotQuery, InstanceDataManager instanceDataManager) {
    StageMetadata stageMetadata = executionContext.getStageMetadata();
    String rawTableName = TableNameBuilder.extractRawTableName(stageMetadata.getTableName());
    Map<String, List<String>> tableSegmentsMap = executionContext.getWorkerMetadata().getTableSegmentsMap();
    assert tableSegmentsMap != null;
    TimeBoundaryInfo timeBoundary = stageMetadata.getTimeBoundary();
    int numRequests = tableSegmentsMap.size();
    if (numRequests == 1) {
      Map.Entry<String, List<String>> entry = tableSegmentsMap.entrySet().iterator().next();
      String tableType = entry.getKey();
      List<String> segments = entry.getValue();
      if (tableType.equals(TableType.OFFLINE.name())) {
        String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName);
        TableDataManager tableDataManager = instanceDataManager.getTableDataManager(offlineTableName);
        Preconditions.checkState(tableDataManager != null, "Failed to find data manager for table: %s",
            offlineTableName);
        Pair<TableConfig, Schema> tableConfigAndSchema = tableDataManager.getCachedTableConfigAndSchema();
        return List.of(compileInstanceRequest(executionContext, pinotQuery, timeBoundary, TableType.OFFLINE,
            tableDataManager.getTableName(), tableConfigAndSchema.getLeft(), tableConfigAndSchema.getRight(), segments,
            null));
      } else {
        assert tableType.equals(TableType.REALTIME.name());
        String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName);
        TableDataManager tableDataManager = instanceDataManager.getTableDataManager(realtimeTableName);
        Preconditions.checkState(tableDataManager != null, "Failed to find data manager for table: %s",
            realtimeTableName);
        Pair<TableConfig, Schema> tableConfigAndSchema = tableDataManager.getCachedTableConfigAndSchema();
        return List.of(compileInstanceRequest(executionContext, pinotQuery, timeBoundary, TableType.REALTIME,
            tableDataManager.getTableName(), tableConfigAndSchema.getLeft(), tableConfigAndSchema.getRight(), segments,
            null));
      }
    } else {
      assert numRequests == 2;
      List<String> offlineSegments = tableSegmentsMap.get(TableType.OFFLINE.name());
      List<String> realtimeSegments = tableSegmentsMap.get(TableType.REALTIME.name());
      assert offlineSegments != null && realtimeSegments != null;
      String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName);
      TableDataManager offlineTableDataManager = instanceDataManager.getTableDataManager(offlineTableName);
      Preconditions.checkState(offlineTableDataManager != null, "Failed to find data manager for table: %s",
          offlineTableName);
      Pair<TableConfig, Schema> offlineTableConfigAndSchema = offlineTableDataManager.getCachedTableConfigAndSchema();
      String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName);
      TableDataManager realtimeTableDataManager = instanceDataManager.getTableDataManager(realtimeTableName);
      Preconditions.checkState(realtimeTableDataManager != null, "Failed to find data manager for table: %s",
          realtimeTableName);
      Pair<TableConfig, Schema> realtimeTableConfigAndSchema =
          realtimeTableDataManager.getCachedTableConfigAndSchema();
      // NOTE: Make a deep copy of PinotQuery for OFFLINE request.
      return List.of(
          compileInstanceRequest(executionContext, new PinotQuery(pinotQuery), timeBoundary, TableType.OFFLINE,
              offlineTableDataManager.getTableName(), offlineTableConfigAndSchema.getLeft(),
              offlineTableConfigAndSchema.getRight(), offlineSegments, null),
          compileInstanceRequest(executionContext, pinotQuery, timeBoundary, TableType.REALTIME,
              realtimeTableDataManager.getTableName(), realtimeTableConfigAndSchema.getLeft(),
              realtimeTableConfigAndSchema.getRight(), realtimeSegments, null));
    }
  }

  /**
   * Convert {@link PinotQuery} into an {@link InstanceRequest}.
   */
  private static InstanceRequest compileInstanceRequest(OpChainExecutionContext executionContext, PinotQuery pinotQuery,
      @Nullable TimeBoundaryInfo timeBoundaryInfo, TableType tableType,
      String tableNameWithType, TableConfig tableConfig, Schema schema, @Nullable List<String> segmentList,
      @Nullable List<TableSegmentsInfo> tableRouteInfoList) {
    Preconditions.checkArgument(segmentList == null || tableRouteInfoList == null,
        "Either segmentList OR tableRouteInfoList should be set");

    // 1. Modify the PinotQuery
    pinotQuery.getDataSource().setTableName(tableNameWithType);
    if (timeBoundaryInfo != null) {
      attachTimeBoundary(pinotQuery, timeBoundaryInfo, tableType == TableType.OFFLINE);
    }
    for (QueryRewriter queryRewriter : QUERY_REWRITERS) {
      pinotQuery = queryRewriter.rewrite(pinotQuery);
    }
    QUERY_OPTIMIZER.optimize(pinotQuery, schema);

    // 2. Update query options according to requestMetadataMap
    updateQueryOptions(pinotQuery, executionContext);

    // 3. Wrap PinotQuery into BrokerRequest
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableNameWithType);
    brokerRequest.setQuerySource(querySource);

    // 4. Create InstanceRequest with segmentList
    InstanceRequest instanceRequest = new InstanceRequest();
    // Making a unique requestId for leaf stages otherwise it causes problem on stats/metrics/tracing.
    // TODO: Revisit if this is still necessary
    long requestId = (executionContext.getRequestId() << 16) + ((long) executionContext.getStageId() << 8) + (
        tableType == TableType.REALTIME ? 1 : 0);
    instanceRequest.setRequestId(requestId);
    instanceRequest.setCid(executionContext.getCid());
    instanceRequest.setBrokerId(executionContext.getBrokerId());
    instanceRequest.setEnableTrace(executionContext.isTraceEnabled());
    /*
     * If segmentList is not null, it means that the query is for a single table and we can directly set the segments.
     * If segmentList is null, it means that the query is for a logical table and we need to set TableSegmentInfoList
     *
     * Either one of segmentList or tableRouteInfoList has to be set, but not both.
     */
    if (segmentList != null) {
      instanceRequest.setSearchSegments(segmentList);
    } else {
      instanceRequest.setTableSegmentsInfoList(tableRouteInfoList);
    }
    instanceRequest.setQuery(brokerRequest);

    return instanceRequest;
  }

  /**
   * Helper method to update query options.
   */
  private static void updateQueryOptions(PinotQuery pinotQuery, OpChainExecutionContext executionContext) {
    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    if (queryOptions != null) {
      queryOptions.putAll(executionContext.getOpChainMetadata());
    } else {
      queryOptions = new HashMap<>(executionContext.getOpChainMetadata());
      pinotQuery.setQueryOptions(queryOptions);
    }
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS,
        Long.toString(executionContext.getActiveDeadlineMs() - System.currentTimeMillis()));
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.EXTRA_PASSIVE_TIMEOUT_MS,
        Long.toString(executionContext.getPassiveDeadlineMs() - executionContext.getActiveDeadlineMs()));
  }

  /**
   * Helper method to attach the time boundary to the given PinotQuery.
   */
  private static void attachTimeBoundary(PinotQuery pinotQuery, TimeBoundaryInfo timeBoundaryInfo,
      boolean isOfflineRequest) {
    String timeColumn = timeBoundaryInfo.getTimeColumn();
    String timeValue = timeBoundaryInfo.getTimeValue();
    Expression timeFilterExpression = RequestUtils.getFunctionExpression(
        isOfflineRequest ? FilterKind.LESS_THAN_OR_EQUAL.name() : FilterKind.GREATER_THAN.name(),
        RequestUtils.getIdentifierExpression(timeColumn), RequestUtils.getLiteralExpression(timeValue));

    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      Expression andFilterExpression =
          RequestUtils.getFunctionExpression(FilterKind.AND.name(), filterExpression, timeFilterExpression);
      pinotQuery.setFilterExpression(andFilterExpression);
    } else {
      pinotQuery.setFilterExpression(timeFilterExpression);
    }
  }

  /**
   * Attach an additive INNER-join probe-side runtime filter to the given leaf {@link PinotQuery}.
   *
   * <p>{@code probeKeys} index the leaf select list (which equals the probe pipeline's output row type),
   * {@code buildKeys} index {@code dataSchema} (the build-side join keys). The reducer never
   * introduces false negatives, so the real hash join in the intermediate stage remains the source of
   * truth and the filter can be omitted at any time without affecting correctness.
   *
   * <p>Tiering (the bloom tier is single-key only):
   * <ul>
   *   <li>{@code IN} — always an exact {@code IN} list (index-accelerated, drives segment pruning).</li>
   *   <li>{@code BLOOM} — a serialized bloom ({@code IN_ID_SET}) plus a {@code BETWEEN(min, max)}
   *       range predicate (for numeric keys) to enable cheap range-based segment pruning.</li>
   *   <li>{@code AUTO} — exact {@code IN} at/below {@code maxInSize} build-key rows, else bloom.</li>
   * </ul>
   * Build sides larger than {@code maxBuildRows}, or any predicate (bloom or exact {@code IN}) whose
   * estimated serialized size exceeds {@code maxBytes}, are abandoned (no filter), which is always correct.
   * An exact {@code IN} over a FLOAT/DOUBLE key that contains {@code NaN} is likewise abandoned: its
   * literals canonicalize NaN, which the leaf's raw-bit membership test cannot match faithfully, whereas
   * the bloom tier reduces NaN keys correctly.
   */
  static void attachRuntimeFilter(PinotQuery pinotQuery, List<Integer> probeKeys, List<Integer> buildKeys,
      List<Object[]> dataContainer, DataSchema dataSchema, RuntimeFilterNode.Type type) {
    attachRuntimeFilter(pinotQuery, probeKeys, buildKeys, dataContainer, dataSchema, type,
        CommonConstants.Broker.DEFAULT_RUNTIME_FILTER_MAX_IN_SIZE, CommonConstants.Broker.DEFAULT_RUNTIME_FILTER_FPP,
        CommonConstants.Broker.DEFAULT_RUNTIME_FILTER_MAX_BYTES,
        CommonConstants.Broker.DEFAULT_RUNTIME_FILTER_MAX_BUILD_ROWS);
  }

  /**
   * Sizing-parameterized variant (package-private for testing). {@code maxBuildRows} MUST equal the value
   * the planner used for its leaf fetch cap (both use
   * {@link CommonConstants.Broker#DEFAULT_RUNTIME_FILTER_MAX_BUILD_ROWS}); see the truncation note below.
   */
  static void attachRuntimeFilter(PinotQuery pinotQuery, List<Integer> probeKeys, List<Integer> buildKeys,
      List<Object[]> dataContainer, DataSchema dataSchema, RuntimeFilterNode.Type type, int maxInSize, double fpp,
      int maxBytes, int maxBuildRows) {
    // CORRECTNESS: the planner caps the build-key stage at maxBuildRows + 1, which TRUNCATES the key set.
    // If the cap was hit (rawCount > maxBuildRows) the set is incomplete, so a reducer built from it could
    // drop probe rows that should join (false negative). Abandon the filter — the join is still correct.
    // This threshold MUST match the planner's fetch cap.
    if (dataContainer.size() > maxBuildRows) {
      return;
    }
    // Drop build rows with any null join key: a null key never matches an INNER equi-join, so excluding
    // it is sound (no false negatives), and it keeps the leaf builders (which cast unconditionally)
    // null-safe regardless of how the planner's IS NOT NULL filter behaved under null handling.
    List<Object[]> rows = retainNonNullKeyRows(dataContainer, buildKeys);
    int rowCount = rows.size();

    // Empty (or all-null) build side: nothing can match -> a constant-false predicate prunes the probe.
    if (rowCount == 0) {
      attachDynamicFilter(pinotQuery, probeKeys, buildKeys, rows, dataSchema);
      return;
    }

    // Decide the tier. The bloom tier is single-key only; BIG_DECIMAL is not supported by IdSet.
    boolean useBloom;
    switch (type) {
      case IN:
        useBloom = false;
        break;
      case BLOOM:
        useBloom = buildKeys.size() == 1;
        break;
      case AUTO:
      default:
        useBloom = buildKeys.size() == 1 && rowCount > maxInSize;
        break;
    }
    if (useBloom) {
      FieldSpec.DataType storedType = dataSchema.getColumnDataType(buildKeys.get(0)).getStoredType().toDataType();
      if (storedType == FieldSpec.DataType.BIG_DECIMAL) {
        useBloom = false;
      }
    }

    if (!useBloom) {
      // A FLOAT/DOUBLE exact IN cannot faithfully reduce a NaN key. The IN literals canonicalize NaN (the
      // value round-trips through "NaN" as a string), while the leaf set compares probe values by raw
      // bits; and the hash join's key equality matches neither (multi-key canonicalizes NaN, single-key
      // compares raw bits). A NaN build key could therefore drop a joinable probe row (false negative), so
      // abandon exact IN when one is present. The bloom tier (single-key) keeps NaN faithfully -> only the
      // exact-IN path needs this guard.
      if (hasNaNFloatOrDoubleKey(rows, buildKeys, dataSchema)) {
        return;
      }
      // Apply the same footprint ceiling the bloom tier honors. The exact-IN path (IN mode, or any
      // multi-key, or AUTO below the threshold) emits up to maxBuildRows literals per key, AND'd across
      // keys, which can be multi-MB. Abandon if the estimated serialized size exceeds maxBytes so there is
      // one consistent ceiling regardless of tier; dropping the filter keeps the join correct.
      if (estimateExactInBytes(rows, buildKeys, dataSchema) > maxBytes) {
        return;
      }
      attachDynamicFilter(pinotQuery, probeKeys, buildKeys, rows, dataSchema);
      return;
    }

    Expression bloomPredicate =
        buildBloomPredicate(pinotQuery.getSelectList().get(probeKeys.get(0)), buildKeys.get(0), rows, dataSchema,
            rowCount, fpp, maxBytes);
    if (bloomPredicate != null) {
      andIntoFilter(pinotQuery, bloomPredicate);
    }
    // else: bloom exceeded maxBytes -> abandon (no filter); the join remains the source of truth.
  }

  /**
   * Returns the build rows whose every join-key column is non-null. A null key cannot match an INNER
   * equi-join, so dropping such rows is sound and keeps the leaf filter builders null-safe.
   */
  private static List<Object[]> retainNonNullKeyRows(List<Object[]> dataContainer, List<Integer> buildKeys) {
    List<Object[]> result = new ArrayList<>(dataContainer.size());
    for (Object[] row : dataContainer) {
      boolean allNonNull = true;
      for (int buildKey : buildKeys) {
        if (row[buildKey] == null) {
          allNonNull = false;
          break;
        }
      }
      if (allNonNull) {
        result.add(row);
      }
    }
    return result;
  }

  /**
   * Returns true if any FLOAT or DOUBLE join-key column holds a {@code NaN} value. Such a key cannot be
   * reduced by an exact {@code IN} without risking a false negative against the hash join (see the caller),
   * so the exact-IN tier is abandoned when this is true.
   */
  private static boolean hasNaNFloatOrDoubleKey(List<Object[]> rows, List<Integer> buildKeys,
      DataSchema dataSchema) {
    for (int buildKey : buildKeys) {
      FieldSpec.DataType storedType = dataSchema.getColumnDataType(buildKey).getStoredType().toDataType();
      if (storedType == FieldSpec.DataType.FLOAT) {
        for (Object[] row : rows) {
          if (Float.isNaN((float) row[buildKey])) {
            return true;
          }
        }
      } else if (storedType == FieldSpec.DataType.DOUBLE) {
        for (Object[] row : rows) {
          if (Double.isNaN((double) row[buildKey])) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /** Rough per-literal overhead (proto Expression + Literal wrapping) used by {@link #estimateExactInBytes}. */
  private static final int IN_LITERAL_OVERHEAD_BYTES = 8;

  /**
   * Estimates the serialized footprint (in bytes) of the exact-IN predicate this build set would produce:
   * one IN list per key over all rows. Used to abandon the exact-IN tier when it would exceed the same
   * {@code maxBytes} ceiling the bloom tier honors. Fixed-width types are O(1); STRING/BYTES sum lengths.
   */
  private static long estimateExactInBytes(List<Object[]> rows, List<Integer> buildKeys, DataSchema dataSchema) {
    int numRows = rows.size();
    long bytes = 0;
    for (int buildKey : buildKeys) {
      FieldSpec.DataType storedType = dataSchema.getColumnDataType(buildKey).getStoredType().toDataType();
      switch (storedType) {
        case INT:
        case FLOAT:
          bytes += (long) numRows * (IN_LITERAL_OVERHEAD_BYTES + 4);
          break;
        case LONG:
        case DOUBLE:
          bytes += (long) numRows * (IN_LITERAL_OVERHEAD_BYTES + 8);
          break;
        case STRING:
          for (Object[] row : rows) {
            bytes += IN_LITERAL_OVERHEAD_BYTES + ((String) row[buildKey]).length();
          }
          break;
        case BYTES:
          for (Object[] row : rows) {
            bytes += IN_LITERAL_OVERHEAD_BYTES + ((ByteArray) row[buildKey]).length();
          }
          break;
        case BIG_DECIMAL:
          // Variable-length: approximate the serialized form as the unscaled magnitude bytes plus scale.
          for (Object[] row : rows) {
            bytes += IN_LITERAL_OVERHEAD_BYTES + 4 + (((BigDecimal) row[buildKey]).unscaledValue().bitLength() / 8 + 1);
          }
          break;
        default:
          bytes += (long) numRows * (IN_LITERAL_OVERHEAD_BYTES + 16);
          break;
      }
    }
    return bytes;
  }

  /**
   * Builds a single-key bloom predicate: {@code IN_ID_SET(probeCol, '<base64>') = 1}, AND'd (for numeric
   * keys) with {@code BETWEEN(probeCol, min, max)} to enable range-based segment pruning. Returns
   * {@code null} if the serialized bloom would exceed {@code maxBytes} (caller abandons the filter).
   */
  @Nullable
  private static Expression buildBloomPredicate(Expression probeColExpr, int buildKey, List<Object[]> dataContainer,
      DataSchema dataSchema, int rowCount, double fpp, int maxBytes) {
    FieldSpec.DataType storedType = dataSchema.getColumnDataType(buildKey).getStoredType().toDataType();
    int expectedInsertions = Math.max(1, rowCount);
    IdSet idSet = IdSets.create(storedType, -1, expectedInsertions, fpp);
    Expression rangePredicate = null;
    switch (storedType) {
      case INT: {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (Object[] row : dataContainer) {
          int value = (int) row[buildKey];
          idSet.add(value);
          min = Math.min(min, value);
          max = Math.max(max, value);
        }
        rangePredicate = betweenPredicate(probeColExpr, RequestUtils.getLiteralExpression(min),
            RequestUtils.getLiteralExpression(max));
        break;
      }
      case LONG: {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (Object[] row : dataContainer) {
          long value = (long) row[buildKey];
          idSet.add(value);
          min = Math.min(min, value);
          max = Math.max(max, value);
        }
        rangePredicate = betweenPredicate(probeColExpr, RequestUtils.getLiteralExpression(min),
            RequestUtils.getLiteralExpression(max));
        break;
      }
      case FLOAT: {
        float min = Float.POSITIVE_INFINITY;
        float max = Float.NEGATIVE_INFINITY;
        boolean hasNaN = false;
        for (Object[] row : dataContainer) {
          float value = (float) row[buildKey];
          idSet.add(value);
          // NaN must NOT poison the range bounds: a finite BETWEEN(min, max) would drop probe NaN rows
          // that should match a build NaN (false negative), and BETWEEN(NaN, NaN) drops everything. Keep
          // NaN in the bloom (membership), but skip the range predicate entirely if any NaN is present.
          if (Float.isNaN(value)) {
            hasNaN = true;
          } else {
            min = Math.min(min, value);
            max = Math.max(max, value);
          }
        }
        if (!hasNaN) {
          rangePredicate = betweenPredicate(probeColExpr, RequestUtils.getLiteralExpression(min),
              RequestUtils.getLiteralExpression(max));
        }
        break;
      }
      case DOUBLE: {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        boolean hasNaN = false;
        for (Object[] row : dataContainer) {
          double value = (double) row[buildKey];
          idSet.add(value);
          if (Double.isNaN(value)) {
            hasNaN = true;
          } else {
            min = Math.min(min, value);
            max = Math.max(max, value);
          }
        }
        if (!hasNaN) {
          rangePredicate = betweenPredicate(probeColExpr, RequestUtils.getLiteralExpression(min),
              RequestUtils.getLiteralExpression(max));
        }
        break;
      }
      case STRING:
        for (Object[] row : dataContainer) {
          idSet.add((String) row[buildKey]);
        }
        break;
      case BYTES:
        for (Object[] row : dataContainer) {
          idSet.add(((ByteArray) row[buildKey]).getBytes());
        }
        break;
      default:
        // Unsupported stored type for bloom (e.g. BIG_DECIMAL is filtered earlier) — abandon.
        return null;
    }
    if (idSet.getSerializedSizeInBytes() > maxBytes) {
      return null;
    }
    String base64IdSet;
    try {
      base64IdSet = idSet.toBase64String();
    } catch (IOException e) {
      // Serialization failed — abandon the filter (the join remains the source of truth).
      return null;
    }
    Expression inIdSet = RequestUtils.getFunctionExpression(TransformFunctionType.IN_ID_SET.getName(), probeColExpr,
        RequestUtils.getLiteralExpression(base64IdSet));
    Expression bloomEq =
        RequestUtils.getFunctionExpression(FilterKind.EQUALS.name(), inIdSet, RequestUtils.getLiteralExpression(1));
    if (rangePredicate == null) {
      return bloomEq;
    }
    return RequestUtils.getFunctionExpression(FilterKind.AND.name(), bloomEq, rangePredicate);
  }

  private static Expression betweenPredicate(Expression column, Expression min, Expression max) {
    return RequestUtils.getFunctionExpression(FilterKind.BETWEEN.name(), column, min, max);
  }

  /**
   * ANDs the given predicate into the query's existing filter (or sets it if there is none).
   */
  private static void andIntoFilter(PinotQuery pinotQuery, Expression predicate) {
    Expression existing = pinotQuery.getFilterExpression();
    if (existing != null) {
      pinotQuery.setFilterExpression(RequestUtils.getFunctionExpression(FilterKind.AND.name(), existing, predicate));
    } else {
      pinotQuery.setFilterExpression(predicate);
    }
  }

  /**
   * attach the dynamic filter to the given PinotQuery.
   */
  static void attachDynamicFilter(PinotQuery pinotQuery, List<Integer> leftKeys, List<Integer> rightKeys,
      List<Object[]> dataContainer, DataSchema dataSchema) {
    List<Expression> expressions = new ArrayList<>();
    for (int i = 0; i < leftKeys.size(); i++) {
      Expression leftExpr = pinotQuery.getSelectList().get(leftKeys.get(i));
      if (dataContainer.isEmpty()) {
        // put a constant false expression
        expressions.add(RequestUtils.getLiteralExpression(false));
      } else {
        int rightIdx = rightKeys.get(i);
        List<Expression> operands = new ArrayList<>(dataContainer.size() + 1);
        operands.add(leftExpr);
        operands.addAll(computeInOperands(dataContainer, dataSchema, rightIdx));
        expressions.add(RequestUtils.getFunctionExpression(FilterKind.IN.name(), operands));
      }
    }
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      expressions.add(filterExpression);
    }
    if (expressions.size() > 1) {
      pinotQuery.setFilterExpression(RequestUtils.getFunctionExpression(FilterKind.AND.name(), expressions));
    } else {
      pinotQuery.setFilterExpression(expressions.get(0));
    }
  }

  private static List<Expression> computeInOperands(List<Object[]> dataContainer, DataSchema dataSchema, int colIdx) {
    final DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(colIdx);
    final FieldSpec.DataType storedType = columnDataType.getStoredType().toDataType();
    final int numRows = dataContainer.size();
    List<Expression> expressions = new ArrayList<>();
    switch (storedType) {
      case INT:
        int[] arrInt = new int[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrInt[rowIdx] = (int) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrInt);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrInt[rowIdx]));
        }
        break;
      case LONG:
        long[] arrLong = new long[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrLong[rowIdx] = (long) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrLong);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrLong[rowIdx]));
        }
        break;
      case FLOAT:
        float[] arrFloat = new float[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrFloat[rowIdx] = (float) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrFloat);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrFloat[rowIdx]));
        }
        break;
      case DOUBLE:
        double[] arrDouble = new double[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrDouble[rowIdx] = (double) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrDouble);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrDouble[rowIdx]));
        }
        break;
      case STRING:
        String[] arrString = new String[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrString[rowIdx] = (String) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrString);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrString[rowIdx]));
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] arrBigDecimal = new BigDecimal[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrBigDecimal[rowIdx] = (BigDecimal) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrBigDecimal);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrBigDecimal[rowIdx]));
        }
        break;
      case BYTES:
        ByteArray[] arrBytes = new ByteArray[numRows];
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          arrBytes[rowIdx] = (ByteArray) dataContainer.get(rowIdx)[colIdx];
        }
        Arrays.sort(arrBytes);
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
          expressions.add(RequestUtils.getLiteralExpression(arrBytes[rowIdx].getBytes()));
        }
        break;
      default:
        throw new IllegalStateException("Illegal SV data type for IN filter: " + storedType);
    }
    return expressions;
  }

  private static List<InstanceRequest> constructLogicalTableServerQueryRequests(
      OpChainExecutionContext executionContext, PinotQuery pinotQuery, InstanceDataManager instanceDataManager) {
    StageMetadata stageMetadata = executionContext.getStageMetadata();
    String logicalTableName = stageMetadata.getTableName();
    LogicalTableContext logicalTableContext = instanceDataManager.getLogicalTableContext(logicalTableName);
    Preconditions.checkNotNull(logicalTableContext,
        String.format("LogicalTableContext not found for logical table name: %s, query context id: %s",
            logicalTableName, executionContext.getCid()));

    Map<String, List<String>> logicalTableSegmentsMap =
        executionContext.getWorkerMetadata().getLogicalTableSegmentsMap();
    List<TableSegmentsInfo> offlineTableRouteInfoList = new ArrayList<>();
    List<TableSegmentsInfo> realtimeTableRouteInfoList = new ArrayList<>();

    Preconditions.checkNotNull(logicalTableSegmentsMap);
    for (Map.Entry<String, List<String>> entry : logicalTableSegmentsMap.entrySet()) {
      String physicalTableName = entry.getKey();
      TableType tableType = TableNameBuilder.getTableTypeFromTableName(physicalTableName);
      TableSegmentsInfo tableSegmentsInfo = new TableSegmentsInfo();
      tableSegmentsInfo.setTableName(physicalTableName);
      tableSegmentsInfo.setSegments(entry.getValue());
      if (tableType == TableType.REALTIME) {
        realtimeTableRouteInfoList.add(tableSegmentsInfo);
      } else {
        offlineTableRouteInfoList.add(tableSegmentsInfo);
      }
    }

    TimeBoundaryInfo timeBoundaryInfo = stageMetadata.getTimeBoundary();

    if (offlineTableRouteInfoList.isEmpty() || realtimeTableRouteInfoList.isEmpty()) {
      List<TableSegmentsInfo> routeInfoList =
          offlineTableRouteInfoList.isEmpty() ? realtimeTableRouteInfoList : offlineTableRouteInfoList;
      String tableType = offlineTableRouteInfoList.isEmpty() ? TableType.REALTIME.name() : TableType.OFFLINE.name();
      if (tableType.equals(TableType.OFFLINE.name())) {
        Preconditions.checkNotNull(logicalTableContext.getRefOfflineTableConfig());
        String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(logicalTableName);
        return List.of(
            compileInstanceRequest(executionContext, pinotQuery, timeBoundaryInfo, TableType.OFFLINE, offlineTableName,
                logicalTableContext.getRefOfflineTableConfig(), logicalTableContext.getLogicalTableSchema(), null,
                routeInfoList));
      } else {
        Preconditions.checkNotNull(logicalTableContext.getRefRealtimeTableConfig());
        String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(logicalTableName);
        return List.of(
            compileInstanceRequest(executionContext, pinotQuery, timeBoundaryInfo, TableType.REALTIME,
                realtimeTableName, logicalTableContext.getRefRealtimeTableConfig(),
                logicalTableContext.getLogicalTableSchema(), null, routeInfoList));
      }
    } else {
      Preconditions.checkNotNull(logicalTableContext.getRefOfflineTableConfig());
      Preconditions.checkNotNull(logicalTableContext.getRefRealtimeTableConfig());
      String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(logicalTableName);
      String realtimeTableName = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(logicalTableName);
      PinotQuery offlinePinotQuery = pinotQuery.deepCopy();
      PinotQuery realtimePinotQuery = pinotQuery.deepCopy();
      return List.of(
          compileInstanceRequest(executionContext, offlinePinotQuery, timeBoundaryInfo, TableType.OFFLINE,
              offlineTableName, logicalTableContext.getRefOfflineTableConfig(),
              logicalTableContext.getLogicalTableSchema(), null, offlineTableRouteInfoList),
          compileInstanceRequest(executionContext, realtimePinotQuery, timeBoundaryInfo, TableType.REALTIME,
              realtimeTableName, logicalTableContext.getRefRealtimeTableConfig(),
              logicalTableContext.getLogicalTableSchema(), null, realtimeTableRouteInfoList));
    }
  }
}
