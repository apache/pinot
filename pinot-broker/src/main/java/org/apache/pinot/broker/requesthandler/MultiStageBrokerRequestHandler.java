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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListener;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);

  private final WorkerManager _workerManager;
  private final MailboxService _mailboxService;
  private final QueryDispatcher _queryDispatcher;

  public MultiStageBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics, BrokerQueryEventListener brokerQueryEventListener) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache, brokerMetrics,
        brokerQueryEventListener);
    LOGGER.info("Using Multi-stage BrokerRequestHandler.");
    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    int port = Integer.parseInt(config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT));
    _workerManager = new WorkerManager(hostname, port, _routingManager);
    _mailboxService = new MailboxService(hostname, port, config);
    _queryDispatcher = new QueryDispatcher(_mailboxService);

    // TODO: move this to a startUp() function.
    _mailboxService.start();
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      HttpHeaders httpHeaders) {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    long compilationStartTimeNs;
    long queryTimeoutMs;
    QueryEnvironment.QueryPlannerResult queryPlanResult;
    try {
      // Parse the request
      sqlNodeAndOptions = sqlNodeAndOptions != null ? sqlNodeAndOptions : RequestUtils.parseQuery(query, request);
    } catch (RuntimeException e) {
      String consolidatedMessage = ExceptionUtils.consolidateExceptionMessages(e);
      LOGGER.info("Caught exception parsing request {}: {}, {}", requestId, query, consolidatedMessage);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
      return new BrokerResponseNative(
          QueryException.getException(QueryException.SQL_PARSING_ERROR, consolidatedMessage));
    }
    try {
      Long timeoutMsFromQueryOption = QueryOptionsUtils.getTimeoutMs(sqlNodeAndOptions.getOptions());
      queryTimeoutMs = timeoutMsFromQueryOption == null ? _brokerTimeoutMs : timeoutMsFromQueryOption;
      String database = DatabaseUtils.extractDatabaseFromQueryRequest(sqlNodeAndOptions.getOptions(), httpHeaders);
      // Compile the request
      compilationStartTimeNs = System.nanoTime();
      QueryEnvironment queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
          CalciteSchemaBuilder.asRootSchema(new PinotCatalog(database, _tableCache), database), _workerManager,
          _tableCache);
      switch (sqlNodeAndOptions.getSqlNode().getKind()) {
        case EXPLAIN:
          queryPlanResult = queryEnvironment.explainQuery(query, sqlNodeAndOptions, requestId);
          String plan = queryPlanResult.getExplainPlan();
          Set<String> tableNames = queryPlanResult.getTableNames();
          if (!hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders)) {
            throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
          }

          return constructMultistageExplainPlan(query, plan);
        case SELECT:
        default:
          queryPlanResult = queryEnvironment.planQuery(query, sqlNodeAndOptions, requestId);
          break;
      }
    } catch (DatabaseConflictException e) {
      LOGGER.info("{}. Request {}: {}", e.getMessage(), requestId, query);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      requestContext.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e));
    } catch (WebApplicationException e) {
      throw e;
    } catch (RuntimeException e) {
      String consolidatedMessage = ExceptionUtils.consolidateExceptionMessages(e);
      LOGGER.warn("Caught exception planning request {}: {}, {}", requestId, query, consolidatedMessage);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      if (e.getMessage().matches(".* Column .* not found in any table'")) {
        requestContext.setErrorCode(QueryException.UNKNOWN_COLUMN_ERROR_CODE);
        return new BrokerResponseNative(
            QueryException.getException(QueryException.UNKNOWN_COLUMN_ERROR, consolidatedMessage));
      }
      requestContext.setErrorCode(QueryException.QUERY_PLANNING_ERROR_CODE);
      return new BrokerResponseNative(
          QueryException.getException(QueryException.QUERY_PLANNING_ERROR, consolidatedMessage));
    }

    DispatchableSubPlan dispatchableSubPlan = queryPlanResult.getQueryPlan();
    Set<String> tableNames = queryPlanResult.getTableNames();

    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_STAGE_QUERIES_GLOBAL, 1);
    for (String tableName : tableNames) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.MULTI_STAGE_QUERIES, 1);
    }

    requestContext.setTableNames(List.copyOf(tableNames));

    // Compilation Time. This includes the time taken for parsing, compiling, create stage plans and assigning workers.
    long compilationEndTimeNs = System.nanoTime();
    long compilationTimeNs = (compilationEndTimeNs - compilationStartTimeNs) + sqlNodeAndOptions.getParseTimeNs();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.REQUEST_COMPILATION, compilationTimeNs);

    // Validate table access.
    if (!hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders)) {
      throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
    }

    // Validate QPS quota
    if (hasExceededQPSQuota(tableNames, requestContext)) {
      String errorMessage = String.format("Request %d: %s exceeds query quota.", requestId, query);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
    }

    Map<String, String> queryOptions = sqlNodeAndOptions.getOptions();

    long executionStartTimeNs = System.nanoTime();
    QueryDispatcher.QueryResult queryResults;
    try {
      queryResults =
          _queryDispatcher.submitAndReduce(requestContext, dispatchableSubPlan, queryTimeoutMs, queryOptions);
    } catch (TimeoutException e) {
      for (String table : tableNames) {
        _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
      }
      LOGGER.warn("Timed out executing request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryException.EXECUTION_TIMEOUT_ERROR_CODE);
      return new BrokerResponseNative(QueryException.EXECUTION_TIMEOUT_ERROR);
    } catch (Throwable t) {
      String consolidatedMessage = ExceptionUtils.consolidateExceptionMessages(t);
      LOGGER.error("Caught exception executing request {}: {}, {}", requestId, query, consolidatedMessage);
      requestContext.setErrorCode(QueryException.QUERY_EXECUTION_ERROR_CODE);
      return new BrokerResponseNative(
          QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, consolidatedMessage));
    }
    long executionEndTimeNs = System.nanoTime();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.QUERY_EXECUTION, executionEndTimeNs - executionStartTimeNs);

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    brokerResponse.setResultTable(queryResults.getResultTable());

    // Attach unavailable segments
    int numUnavailableSegments = 0;
    for (Map.Entry<String, Set<String>> entry : dispatchableSubPlan.getTableToUnavailableSegmentsMap().entrySet()) {
      String tableName = entry.getKey();
      Set<String> unavailableSegments = entry.getValue();
      numUnavailableSegments += unavailableSegments.size();
      brokerResponse.addToExceptions(new QueryProcessingException(QueryException.SERVER_SEGMENT_MISSING_ERROR_CODE,
          String.format("Find unavailable segments: %s for table: %s", unavailableSegments, tableName)));
    }

    fillOldBrokerResponseStats(brokerResponse, queryResults.getQueryStats(), dispatchableSubPlan);

    // Set total query processing time
    // TODO: Currently we don't emit metric for QUERY_TOTAL_TIME_MS
    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(
        sqlNodeAndOptions.getParseTimeNs() + (executionEndTimeNs - compilationStartTimeNs));
    brokerResponse.setTimeUsedMs(totalTimeMs);
    requestContext.setQueryProcessingTime(totalTimeMs);
    augmentStatistics(requestContext, brokerResponse);

    // Log query and stats
    _queryLogger.log(
        new QueryLogger.QueryLogParams(requestId, query, requestContext, tableNames.toString(), numUnavailableSegments,
            null, brokerResponse, totalTimeMs, requesterIdentity));

    return brokerResponse;
  }

  private void fillOldBrokerResponseStats(BrokerResponseNativeV2 brokerResponse,
      List<MultiStageQueryStats.StageStats.Closed> queryStats, DispatchableSubPlan dispatchableSubPlan) {
    List<PlanNode> planNodes = dispatchableSubPlan.getQueryStageList().stream()
            .map(DispatchablePlanFragment::getPlanFragment)
            .map(PlanFragment::getFragmentRoot)
            .collect(Collectors.toList());
    MultiStageStatsTreeBuilder treeBuilder = new MultiStageStatsTreeBuilder(planNodes, queryStats);
    brokerResponse.setStageStats(treeBuilder.jsonStatsByStage(0));

    for (MultiStageQueryStats.StageStats.Closed stageStats : queryStats) {
      stageStats.forEach((type, stats) -> type.mergeInto(brokerResponse, stats));
    }
  }

  /**
   * Validates whether the requester has access to all the tables.
   */
  private boolean hasTableAccess(RequesterIdentity requesterIdentity, Set<String> tableNames,
      RequestContext requestContext, HttpHeaders httpHeaders) {
    final long startTimeNs = System.nanoTime();
    AccessControl accessControl = _accessControlFactory.create();
    boolean hasAccess = accessControl.hasAccess(requesterIdentity, tableNames) && tableNames.stream()
        .allMatch(table -> accessControl.hasAccess(httpHeaders, TargetType.TABLE, table, Actions.Table.QUERY));
    if (!hasAccess) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.warn("Access denied for requestId {}", requestContext.getRequestId());
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
    }

    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - startTimeNs);

    return hasAccess;
  }

  /**
   * Returns true if the QPS quota of the tables has exceeded.
   */
  private boolean hasExceededQPSQuota(Set<String> tableNames, RequestContext requestContext) {
    for (String tableName : tableNames) {
      if (!_queryQuotaManager.acquire(tableName)) {
        LOGGER.warn("Request {}: query exceeds quota for table: {}", requestContext.getRequestId(), tableName);
        requestContext.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
        String rawTableName = TableNameBuilder.extractRawTableName(tableName);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1);
        return true;
      }
    }
    return false;
  }

  private void updatePhaseTimingForTables(Set<String> tableNames, BrokerQueryPhase phase, long time) {
    for (String tableName : tableNames) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      _brokerMetrics.addPhaseTiming(rawTableName, phase, time);
    }
  }

  private BrokerResponse constructMultistageExplainPlan(String sql, String plan) {
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{sql, plan});
    DataSchema multistageExplainResultSchema = new DataSchema(new String[]{"SQL", "PLAN"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    brokerResponse.setResultTable(new ResultTable(multistageExplainResultSchema, rows));
    return brokerResponse;
  }

  @Override
  protected BrokerResponse processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, Pair<List<String>, List<String>>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, Pair<List<String>, List<String>>> realtimeRoutingTable, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void shutDown() {
    _queryDispatcher.shutdown();
    _mailboxService.shutdown();
  }


  public static class MultiStageStatsTreeBuilder {
    private final List<PlanNode> _planNodes;
    private final List<? extends MultiStageQueryStats.StageStats> _queryStats;

    public MultiStageStatsTreeBuilder(List<PlanNode> planNodes,
        List<? extends MultiStageQueryStats.StageStats> queryStats) {
      _planNodes = planNodes;
      _queryStats = queryStats;
    }

    public ObjectNode jsonStatsByStage(int stage) {
      MultiStageQueryStats.StageStats stageStats = _queryStats.get(stage);
      PlanNode planNode = _planNodes.get(stage);
      InStageStatsTreeBuilder treeBuilder = new InStageStatsTreeBuilder(stageStats, this::jsonStatsByStage);
      return planNode.visit(treeBuilder, null);
    }
  }

  public static class InStageStatsTreeBuilder implements PlanNodeVisitor<ObjectNode, Void> {
    private final MultiStageQueryStats.StageStats _stageStats;
    private int _index;
    private static final String CHILDREN_KEY = "children";
    private final IntFunction<ObjectNode> _jsonStatsByStage;

    public InStageStatsTreeBuilder(MultiStageQueryStats.StageStats stageStats,
        IntFunction<ObjectNode> jsonStatsByStage) {
      _stageStats = stageStats;
      _index = stageStats.getLastOperatorIndex();
      _jsonStatsByStage = jsonStatsByStage;
    }

    private ObjectNode selfNode(MultiStageOperator.Type type) {
      ObjectNode json = JsonUtils.newObjectNode();
      json.put("type", type.toString());
      Iterator<Map.Entry<String, JsonNode>> statsIt = _stageStats.getOperatorStats(_index).asJson().fields();
      while (statsIt.hasNext()) {
        Map.Entry<String, JsonNode> entry = statsIt.next();
        json.set(entry.getKey(), entry.getValue());
      }
      return json;
    }

    private ObjectNode recursiveCase(AbstractPlanNode node, MultiStageOperator.Type expectedType) {
      MultiStageOperator.Type type = _stageStats.getOperatorType(_index);
      /*
       Sometimes the operator type is not what we expect, but we can still build the tree
       This always happen in stage 0, in which case we have two operators but we only have stats for the receive
       operator.
       This may also happen leaf stages, in which case the all the stage but the send operator will be compiled into
       a single leaf node.
      */
      if (type != expectedType) {
        if (type == MultiStageOperator.Type.LEAF) {
          return selfNode(MultiStageOperator.Type.LEAF);
        }
        List<PlanNode> inputs = node.getInputs();
        int childrenSize = inputs.size();
        LOGGER.warn("Skipping unexpected node {} when stat of type {} was found at index {}",
            node.getClass(), type, _index);
        switch (childrenSize) {
          case 0:
            return JsonUtils.newObjectNode();
          case 1:
            return inputs.get(0).visit(this, null);
          default:
            ObjectNode json = JsonUtils.newObjectNode();
            ArrayNode children = JsonUtils.newArrayNode();
            for (int i = 0; i < childrenSize; i++) {
              _index--;
              if (inputs.size() > i) {
                children.add(inputs.get(i).visit(this, null));
              }
            }
            json.set(CHILDREN_KEY, children);
            return json;
        }
      }
      ObjectNode json = selfNode(type);

      List<PlanNode> inputs = node.getInputs();
      ArrayNode children = JsonUtils.newArrayNode();
      int size = inputs.size();
      if (size > _index) {
        LOGGER.warn("Operator {} has {} inputs but only {} stats are left", type, size, _index);
        return json;
      }
      for (int i = size - 1; i >= 0; i--) {
        PlanNode planNode = inputs.get(i);
        _index--;
        JsonNode child = planNode.visit(this, null);
        children.add(child);
      }
      json.set(CHILDREN_KEY, children);
      return json;
    }

    @Override
    public ObjectNode visitAggregate(AggregateNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.AGGREGATE);
    }

    @Override
    public ObjectNode visitFilter(FilterNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.FILTER);
    }

    @Override
    public ObjectNode visitJoin(JoinNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.HASH_JOIN);
    }

    @Override
    public ObjectNode visitMailboxReceive(MailboxReceiveNode node, Void context) {
      ObjectNode json = selfNode(MultiStageOperator.Type.MAILBOX_RECEIVE);

      ArrayNode children = JsonUtils.newArrayNode();
      int senderStageId = node.getSenderStageId();
      children.add(_jsonStatsByStage.apply(senderStageId));
      json.set(CHILDREN_KEY, children);
      return json;
    }

    @Override
    public ObjectNode visitMailboxSend(MailboxSendNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.MAILBOX_SEND);
    }

    @Override
    public ObjectNode visitProject(ProjectNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.TRANSFORM);
    }

    @Override
    public ObjectNode visitSort(SortNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.SORT_OR_LIMIT);
    }

    @Override
    public ObjectNode visitTableScan(TableScanNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.SORT_OR_LIMIT);
    }

    @Override
    public ObjectNode visitValue(ValueNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.LITERAL);
    }

    @Override
    public ObjectNode visitWindow(WindowNode node, Void context) {
      return recursiveCase(node, MultiStageOperator.Type.WINDOW);
    }

    @Override
    public ObjectNode visitSetOp(SetOpNode node, Void context) {
      MultiStageOperator.Type type;
      switch (node.getSetOpType()) {
        case UNION:
          type = MultiStageOperator.Type.UNION;
          break;
        case INTERSECT:
          type = MultiStageOperator.Type.INTERSECT;
          break;
        case MINUS:
          type = MultiStageOperator.Type.MINUS;
          break;
        default:
          throw new IllegalStateException("Unexpected set op type: " + node.getSetOpType());
      }
      return recursiveCase(node, type);
    }

    @Override
    public ObjectNode visitExchange(ExchangeNode node, Void context) {
      throw new UnsupportedOperationException("ExchangeNode should not be visited");
    }
  }
}
