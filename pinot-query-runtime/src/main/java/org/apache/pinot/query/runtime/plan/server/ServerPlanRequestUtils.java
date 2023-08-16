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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.PhysicalPlanContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.rewriter.NonAggregationGroupByToDistinctQueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerPlanRequestUtils {
  private static final int DEFAULT_LEAF_NODE_LIMIT = Integer.MAX_VALUE;
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerPlanRequestUtils.class);
  private static final List<String> QUERY_REWRITERS_CLASS_NAMES =
      ImmutableList.of(PredicateComparisonRewriter.class.getName(),
          NonAggregationGroupByToDistinctQueryRewriter.class.getName());
  private static final List<QueryRewriter> QUERY_REWRITERS =
      new ArrayList<>(QueryRewriterFactory.getQueryRewriters(QUERY_REWRITERS_CLASS_NAMES));
  private static final QueryOptimizer QUERY_OPTIMIZER = new QueryOptimizer();

  private ServerPlanRequestUtils() {
    // do not instantiate.
  }

  /**
   * Entry point to construct a {@link ServerPlanRequestContext} for executing leaf-stage runner.
   *
   * @param planContext physical plan context of the stage.
   * @param distributedStagePlan distributed stage plan of the stage.
   * @param requestMetadataMap metadata map
   * @param helixPropertyStore helix property store used to fetch table config and schema for leaf-stage execution.
   * @return a list of server plan request context to be run
   */
  public static List<ServerPlanRequestContext> constructServerQueryRequests(PhysicalPlanContext planContext,
      DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadataMap,
      ZkHelixPropertyStore<ZNRecord> helixPropertyStore) {
    StageMetadata stageMetadata = distributedStagePlan.getStageMetadata();
    WorkerMetadata workerMetadata = distributedStagePlan.getCurrentWorkerMetadata();
    String rawTableName = StageMetadata.getTableName(stageMetadata);
    Map<String, List<String>> tableToSegmentListMap = WorkerMetadata.getTableSegmentsMap(workerMetadata);
    List<ServerPlanRequestContext> requests = new ArrayList<>();
    for (Map.Entry<String, List<String>> tableEntry : tableToSegmentListMap.entrySet()) {
      String tableType = tableEntry.getKey();
      // ZkHelixPropertyStore extends from ZkCacheBaseDataAccessor so it should not cause too much out-of-the-box
      // network traffic. but there's chance to improve this:
      // TODO: use TableDataManager: it is already getting tableConfig and Schema when processing segments.
      if (TableType.OFFLINE.name().equals(tableType)) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(helixPropertyStore,
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName));
        Schema schema = ZKMetadataProvider.getTableSchema(helixPropertyStore,
            TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(rawTableName));
        requests.add(ServerPlanRequestUtils.build(planContext, distributedStagePlan, requestMetadataMap, tableConfig,
            schema, StageMetadata.getTimeBoundary(stageMetadata), TableType.OFFLINE, tableEntry.getValue()));
      } else if (TableType.REALTIME.name().equals(tableType)) {
        TableConfig tableConfig = ZKMetadataProvider.getTableConfig(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        Schema schema = ZKMetadataProvider.getTableSchema(helixPropertyStore,
            TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(rawTableName));
        requests.add(ServerPlanRequestUtils.build(planContext, distributedStagePlan, requestMetadataMap, tableConfig,
            schema, StageMetadata.getTimeBoundary(stageMetadata), TableType.REALTIME, tableEntry.getValue()));
      } else {
        throw new IllegalArgumentException("Unsupported table type key: " + tableType);
      }
    }
    return requests;
  }

  private static ServerPlanRequestContext build(PhysicalPlanContext planContext, DistributedStagePlan stagePlan,
      Map<String, String> requestMetadataMap, TableConfig tableConfig, Schema schema, TimeBoundaryInfo timeBoundaryInfo,
      TableType tableType, List<String> segmentList) {
    // Before-visit: construct the ServerPlanRequestContext baseline
    // Making a unique requestId for leaf stages otherwise it causes problem on stats/metrics/tracing.
    long requestId = (Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_ID)) << 16) + (
        (long) stagePlan.getStageId() << 8) + (tableType == TableType.REALTIME ? 1 : 0);
    long timeoutMs = Long.parseLong(requestMetadataMap.get(QueryConfig.KEY_OF_BROKER_REQUEST_TIMEOUT_MS));
    boolean traceEnabled = Boolean.parseBoolean(requestMetadataMap.get(CommonConstants.Broker.Request.TRACE));
    PinotQuery pinotQuery = new PinotQuery();
    Integer leafNodeLimit = QueryOptionsUtils.getMultiStageLeafLimit(requestMetadataMap);
    if (leafNodeLimit != null) {
      pinotQuery.setLimit(leafNodeLimit);
    } else {
      pinotQuery.setLimit(DEFAULT_LEAF_NODE_LIMIT);
    }
    LOGGER.debug("QueryID" + requestId + " leafNodeLimit:" + leafNodeLimit);
    pinotQuery.setExplain(false);
    ServerPlanRequestContext serverContext = new ServerPlanRequestContext(planContext, pinotQuery, tableType);

    // visit the plan and create query physical plan.
    ServerPlanRequestVisitor.walkStageNode(stagePlan.getStageRoot(), serverContext);

    // Post-visit: finalize context.
    // 1. global rewrite/optimize
    if (timeBoundaryInfo != null) {
      attachTimeBoundary(pinotQuery, timeBoundaryInfo, tableType == TableType.OFFLINE);
    }
    for (QueryRewriter queryRewriter : QUERY_REWRITERS) {
      pinotQuery = queryRewriter.rewrite(pinotQuery);
    }
    QUERY_OPTIMIZER.optimize(pinotQuery, tableConfig, schema);

    // 2. set pinot query options according to requestMetadataMap
    updateQueryOptions(pinotQuery, requestMetadataMap, timeoutMs, traceEnabled);

    // 3. wrapped around in broker request
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    DataSource dataSource = pinotQuery.getDataSource();
    if (dataSource != null) {
      QuerySource querySource = new QuerySource();
      querySource.setTableName(dataSource.getTableName());
      brokerRequest.setQuerySource(querySource);
    }

    // 3. create instance request with segmentList
    InstanceRequest instanceRequest = new InstanceRequest();
    instanceRequest.setRequestId(requestId);
    instanceRequest.setBrokerId("unknown");
    instanceRequest.setEnableTrace(Boolean.parseBoolean(requestMetadataMap.get(CommonConstants.Broker.Request.TRACE)));
    instanceRequest.setSearchSegments(segmentList);
    instanceRequest.setQuery(brokerRequest);

    serverContext.setInstanceRequest(instanceRequest);
    return serverContext;
  }

  /**
   * Helper method to update query options.
   */
  private static void updateQueryOptions(PinotQuery pinotQuery, Map<String, String> requestMetadataMap, long timeoutMs,
      boolean traceEnabled) {
    Map<String, String> queryOptions = new HashMap<>();
    // put default timeout and trace options
    queryOptions.put(CommonConstants.Broker.Request.QueryOptionKey.TIMEOUT_MS, String.valueOf(timeoutMs));
    if (traceEnabled) {
      queryOptions.put(CommonConstants.Broker.Request.TRACE, "true");
    }
    // overwrite with requestMetadataMap to carry query options from request:
    queryOptions.putAll(requestMetadataMap);
    pinotQuery.setQueryOptions(queryOptions);
  }

  /**
   * Helper method to attach the time boundary to the given PinotQuery.
   */
  private static void attachTimeBoundary(PinotQuery pinotQuery, TimeBoundaryInfo timeBoundaryInfo,
      boolean isOfflineRequest) {
    String timeColumn = timeBoundaryInfo.getTimeColumn();
    String timeValue = timeBoundaryInfo.getTimeValue();
    Expression timeFilterExpression = RequestUtils.getFunctionExpression(
        isOfflineRequest ? FilterKind.LESS_THAN_OR_EQUAL.name() : FilterKind.GREATER_THAN.name());
    timeFilterExpression.getFunctionCall().setOperands(
        Arrays.asList(RequestUtils.getIdentifierExpression(timeColumn), RequestUtils.getLiteralExpression(timeValue)));

    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      Expression andFilterExpression = RequestUtils.getFunctionExpression(FilterKind.AND.name());
      andFilterExpression.getFunctionCall().setOperands(Arrays.asList(filterExpression, timeFilterExpression));
      pinotQuery.setFilterExpression(andFilterExpression);
    } else {
      pinotQuery.setFilterExpression(timeFilterExpression);
    }
  }

  /**
   * attach the dynamic filter to the given PinotQuery.
   */
  static void attachDynamicFilter(PinotQuery pinotQuery, JoinNode.JoinKeys joinKeys, List<Object[]> dataContainer,
      DataSchema dataSchema) {
    FieldSelectionKeySelector leftSelector = (FieldSelectionKeySelector) joinKeys.getLeftJoinKeySelector();
    FieldSelectionKeySelector rightSelector = (FieldSelectionKeySelector) joinKeys.getRightJoinKeySelector();
    List<Expression> expressions = new ArrayList<>();
    for (int i = 0; i < leftSelector.getColumnIndices().size(); i++) {
      Expression leftExpr = pinotQuery.getSelectList().get(leftSelector.getColumnIndices().get(i));
      int rightIdx = rightSelector.getColumnIndices().get(i);
      Expression inFilterExpr = RequestUtils.getFunctionExpression(FilterKind.IN.name());
      List<Expression> operands = new ArrayList<>(dataContainer.size() + 1);
      operands.add(leftExpr);
      operands.addAll(computeInOperands(dataContainer, dataSchema, rightIdx));
      inFilterExpr.getFunctionCall().setOperands(operands);
      expressions.add(inFilterExpr);
    }
    attachFilterExpression(pinotQuery, FilterKind.AND, expressions);
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
      default:
        throw new IllegalStateException("Illegal SV data type for ID_SET aggregation function: " + storedType);
    }
    return expressions;
  }

  /**
   * Attach Filter Expression to existing PinotQuery.
   */
  private static void attachFilterExpression(PinotQuery pinotQuery, FilterKind attachKind, List<Expression> exprs) {
    Preconditions.checkState(attachKind == FilterKind.AND || attachKind == FilterKind.OR);
    Expression filterExpression = pinotQuery.getFilterExpression();
    List<Expression> arrayList = new ArrayList<>(exprs);
    if (filterExpression != null) {
      arrayList.add(filterExpression);
    }
    if (arrayList.size() > 1) {
      Expression attachFilterExpression = RequestUtils.getFunctionExpression(attachKind.name());
      attachFilterExpression.getFunctionCall().setOperands(arrayList);
      pinotQuery.setFilterExpression(attachFilterExpression);
    } else {
      pinotQuery.setFilterExpression(arrayList.get(0));
    }
  }
}
