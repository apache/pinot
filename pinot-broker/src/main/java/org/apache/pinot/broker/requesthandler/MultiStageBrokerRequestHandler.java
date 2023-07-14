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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.BrokerResponseStats;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.executor.RoundRobinScheduler;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);
  private final String _reducerHostname;
  private final int _reducerPort;
  private final long _defaultBrokerTimeoutMs;

  private final MailboxService _mailboxService;
  private final OpChainSchedulerService _reducerScheduler;

  private final QueryEnvironment _queryEnvironment;
  private final QueryDispatcher _queryDispatcher;

  public MultiStageBrokerRequestHandler(PinotConfiguration config, String brokerIdFromConfig,
      BrokerRoutingManager routingManager, AccessControlFactory accessControlFactory,
      QueryQuotaManager queryQuotaManager, TableCache tableCache, BrokerMetrics brokerMetrics) {
    super(config, brokerIdFromConfig, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        brokerMetrics);
    LOGGER.info("Using Multi-stage BrokerRequestHandler.");
    String reducerHostname = config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME);
    if (reducerHostname == null) {
      // use broker ID as host name, but remove the
      String brokerId = brokerIdFromConfig;
      brokerId = brokerId.startsWith(CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE) ? brokerId.substring(
          CommonConstants.Helix.BROKER_INSTANCE_PREFIX_LENGTH) : brokerId;
      brokerId = StringUtils.split(brokerId, "_").length > 1 ? StringUtils.split(brokerId, "_")[0] : brokerId;
      reducerHostname = brokerId;
    }
    _reducerHostname = reducerHostname;
    // This config has to be set to a valid port number.
    _reducerPort = Integer.parseInt(config.getProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT));
    _defaultBrokerTimeoutMs = config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS,
        CommonConstants.Broker.DEFAULT_BROKER_TIMEOUT_MS);
    _queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(tableCache)),
        new WorkerManager(_reducerHostname, _reducerPort, routingManager), _tableCache);
    _queryDispatcher = new QueryDispatcher();

    long releaseMs = config.getProperty(QueryConfig.KEY_OF_SCHEDULER_RELEASE_TIMEOUT_MS,
        QueryConfig.DEFAULT_SCHEDULER_RELEASE_TIMEOUT_MS);
    _reducerScheduler = new OpChainSchedulerService(new RoundRobinScheduler(releaseMs),
        Executors.newCachedThreadPool(new NamedThreadFactory("query_broker_reducer_" + _reducerPort + "_port")));
    _mailboxService = new MailboxService(_reducerHostname, _reducerPort, config, _reducerScheduler::onDataAvailable);

    // TODO: move this to a startUp() function.
    _reducerScheduler.startAsync();
    _mailboxService.start();
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext)
      throws Exception {
    long requestId = _brokerIdGenerator.get();
    requestContext.setRequestId(requestId);
    requestContext.setRequestArrivalTimeMillis(System.currentTimeMillis());

    // First-stage access control to prevent unauthenticated requests from using up resources. Secondary table-level
    // check comes later.
    boolean hasAccess = _accessControlFactory.create().hasAccess(requesterIdentity);
    if (!hasAccess) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.info("Access denied for requestId {}", requestId);
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
    }

    JsonNode sql = request.get(CommonConstants.Broker.Request.SQL);
    if (sql == null) {
      throw new BadQueryRequestException("Failed to find 'sql' in the request: " + request);
    }
    String query = sql.asText();
    requestContext.setQuery(query);
    return handleRequest(requestId, query, sqlNodeAndOptions, request, requesterIdentity, requestContext);
  }

  private BrokerResponse handleRequest(long requestId, String query, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext)
      throws Exception {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    long compilationStartTimeNs;
    long queryTimeoutMs;
    QueryEnvironment.QueryPlannerResult queryPlanResult;
    try {
      // Parse the request
      sqlNodeAndOptions = sqlNodeAndOptions != null ? sqlNodeAndOptions : RequestUtils.parseQuery(query, request);
      Long timeoutMsFromQueryOption = QueryOptionsUtils.getTimeoutMs(sqlNodeAndOptions.getOptions());
      queryTimeoutMs = timeoutMsFromQueryOption == null ? _defaultBrokerTimeoutMs : timeoutMsFromQueryOption;
      // Compile the request
      compilationStartTimeNs = System.nanoTime();
      switch (sqlNodeAndOptions.getSqlNode().getKind()) {
        case EXPLAIN:
          queryPlanResult = _queryEnvironment.explainQuery(query, sqlNodeAndOptions);
          String plan = queryPlanResult.getExplainPlan();
          Set<String> tableNames = queryPlanResult.getTableNames();
          if (!hasTableAccess(requesterIdentity, tableNames, requestId, requestContext)) {
            return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
          }

          return constructMultistageExplainPlan(query, plan);
        case SELECT:
        default:
          queryPlanResult = _queryEnvironment.planQuery(query, sqlNodeAndOptions, requestId);
          break;
      }
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling SQL request {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e.getMessage()));
    }

    DispatchableSubPlan dispatchableSubPlan = queryPlanResult.getQueryPlan();
    Set<String> tableNames = queryPlanResult.getTableNames();

    // Compilation Time. This includes the time taken for parsing, compiling, create stage plans and assigning workers.
    long compilationEndTimeNs = System.nanoTime();
    long compilationTimeNs = (compilationEndTimeNs - compilationStartTimeNs) + sqlNodeAndOptions.getParseTimeNs();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.REQUEST_COMPILATION, compilationTimeNs);

    // Validate table access.
    if (!hasTableAccess(requesterIdentity, tableNames, requestId, requestContext)) {
      return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
    }
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - compilationEndTimeNs);

    // Validate QPS quota
    if (hasExceededQPSQuota(tableNames, requestId, requestContext)) {
      String errorMessage = String.format("Request %d: %s exceeds query quota.", requestId, query);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
    }

    boolean traceEnabled = Boolean.parseBoolean(
        request.has(CommonConstants.Broker.Request.TRACE) ? request.get(CommonConstants.Broker.Request.TRACE).asText()
            : "false");

    ResultTable queryResults;
    Map<Integer, ExecutionStatsAggregator> stageIdStatsMap = new HashMap<>();
    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      stageIdStatsMap.put(stageId, new ExecutionStatsAggregator(traceEnabled));
    }

    long executionStartTimeNs = System.nanoTime();
    try {
      queryResults = _queryDispatcher.submitAndReduce(requestId, dispatchableSubPlan, _mailboxService,
          _reducerScheduler,
          queryTimeoutMs, sqlNodeAndOptions.getOptions(), stageIdStatsMap, traceEnabled);
    } catch (Throwable t) {
      LOGGER.error("query execution failed: " + t);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, t));
    }

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    long executionEndTimeNs = System.nanoTime();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.QUERY_EXECUTION, executionEndTimeNs - executionStartTimeNs);

    // Set total query processing time
    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(
        sqlNodeAndOptions.getParseTimeNs() + (executionEndTimeNs - compilationStartTimeNs));
    brokerResponse.setTimeUsedMs(totalTimeMs);
    brokerResponse.setResultTable(queryResults);
    brokerResponse.setRequestId(String.valueOf(requestId));

    for (Map.Entry<Integer, ExecutionStatsAggregator> entry : stageIdStatsMap.entrySet()) {
      if (entry.getKey() == 0) {
        // Root stats are aggregated and added separately to broker response for backward compatibility
        entry.getValue().setStats(brokerResponse);
        continue;
      }

      BrokerResponseStats brokerResponseStats = new BrokerResponseStats();
      if (!tableNames.isEmpty()) {
        //TODO: Only using first table to assign broker metrics
        // find a way to split metrics in case of multiple table
        String rawTableName = TableNameBuilder.extractRawTableName(tableNames.iterator().next());
        entry.getValue().setStageLevelStats(rawTableName, brokerResponseStats, _brokerMetrics);
      } else {
        entry.getValue().setStageLevelStats(null, brokerResponseStats, null);
      }
      brokerResponse.addStageStat(entry.getKey(), brokerResponseStats);
    }

    requestContext.setQueryProcessingTime(totalTimeMs);
    augmentStatistics(requestContext, brokerResponse);
    return brokerResponse;
  }

  /**
   * Validates whether the requester has access to all the tables.
   */
  private boolean hasTableAccess(RequesterIdentity requesterIdentity, Set<String> tableNames, long requestId,
      RequestContext requestContext) {
    boolean hasAccess = _accessControlFactory.create().hasAccess(requesterIdentity, tableNames);
    if (!hasAccess) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.warn("Access denied for requestId {}", requestId);
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return false;
    }

    return true;
  }

  /**
   * Returns true if the QPS quota of the tables has exceeded.
   */
  private boolean hasExceededQPSQuota(Set<String> tableNames, long requestId, RequestContext requestContext) {
    for (String tableName : tableNames) {
      if (!_queryQuotaManager.acquire(tableName)) {
        LOGGER.warn("Request {}: query exceeds quota for table: {}", requestId, tableName);
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

  private BrokerResponseNative constructMultistageExplainPlan(String sql, String plan) {
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{sql, plan});
    DataSchema multistageExplainResultSchema = new DataSchema(new String[]{"SQL", "PLAN"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    brokerResponse.setResultTable(new ResultTable(multistageExplainResultSchema, rows));
    return brokerResponse;
  }

  @Override
  protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> offlineRoutingTable, @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable, long timeoutMs, ServerStats serverStats,
      RequestContext requestContext)
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
    _reducerScheduler.stopAsync();
  }
}
