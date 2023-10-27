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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import jnr.ffi.Runtime;
import org.apache.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
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
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListener;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);

  private final QueryEnvironment _queryEnvironment;
  private final MailboxService _mailboxService;
  private final QueryDispatcher _queryDispatcher;


  public MultiStageBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRoutingManager routingManager, AccessControlFactory accessControlFactory,
      QueryQuotaManager queryQuotaManager, TableCache tableCache, BrokerMetrics brokerMetrics,
      BrokerQueryEventListener brokerQueryEventListener) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        brokerMetrics, brokerQueryEventListener);
    LOGGER.info("Using Multi-stage BrokerRequestHandler.");
    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    int port = Integer.parseInt(config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT));
    _queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
        CalciteSchemaBuilder.asRootSchema(new PinotCatalog(tableCache)),
        new WorkerManager(hostname, port, routingManager), _tableCache);
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
      // Compile the request
      compilationStartTimeNs = System.nanoTime();
      switch (sqlNodeAndOptions.getSqlNode().getKind()) {
        case EXPLAIN:
          queryPlanResult = _queryEnvironment.explainQuery(query, sqlNodeAndOptions, requestId);
          String plan = queryPlanResult.getExplainPlan();
          Set<String> tableNames = queryPlanResult.getTableNames();
          if (!hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders)) {
            throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
          }

          return constructMultistageExplainPlan(query, plan);
        case SELECT:
        default:
          queryPlanResult = _queryEnvironment.planQuery(query, sqlNodeAndOptions, requestId);
          break;
      }
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
    requestContext.setTableNames(List.copyOf(tableNames));

    // Compilation Time. This includes the time taken for parsing, compiling, create stage plans and assigning workers.
    long compilationEndTimeNs = System.nanoTime();
    long compilationTimeNs = (compilationEndTimeNs - compilationStartTimeNs) + sqlNodeAndOptions.getParseTimeNs();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.REQUEST_COMPILATION, compilationTimeNs);

    // Validate table access.
    if (!hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders)) {
      throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
    }
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - compilationEndTimeNs);

    // Validate QPS quota
    if (hasExceededQPSQuota(tableNames, requestContext)) {
      String errorMessage = String.format("Request %d: %s exceeds query quota.", requestId, query);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
    }

    Map<String, String> queryOptions = sqlNodeAndOptions.getOptions();
    boolean traceEnabled = Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));

    ResultTable queryResults;
    Map<Integer, ExecutionStatsAggregator> stageIdStatsMap = new HashMap<>();
    for (int stageId = 0; stageId < dispatchableSubPlan.getQueryStageList().size(); stageId++) {
      stageIdStatsMap.put(stageId, new ExecutionStatsAggregator(traceEnabled));
    }

    long executionStartTimeNs = System.nanoTime();
    try {
      queryResults = _queryDispatcher.submitAndReduce(requestContext, dispatchableSubPlan, queryTimeoutMs, queryOptions,
          stageIdStatsMap);
    } catch (TimeoutException e) {
      for(String table : tableNames) {
        _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
      }
      throw new RuntimeException(e);
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
    brokerResponse.setResultTable(queryResults);

    // Attach unavailable segments
    int numUnavailableSegments = 0;
    for (Map.Entry<String, Set<String>> entry : dispatchableSubPlan.getTableToUnavailableSegmentsMap().entrySet()) {
      String tableName = entry.getKey();
      Set<String> unavailableSegments = entry.getValue();
      numUnavailableSegments += unavailableSegments.size();
      brokerResponse.addToExceptions(new QueryProcessingException(QueryException.SERVER_SEGMENT_MISSING_ERROR_CODE,
          String.format("Find unavailable segments: %s for table: %s", unavailableSegments, tableName)));
    }

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

    // Set total query processing time
    // TODO: Currently we don't emit metric for QUERY_TOTAL_TIME_MS
    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(
        sqlNodeAndOptions.getParseTimeNs() + (executionEndTimeNs - compilationStartTimeNs));
    brokerResponse.setTimeUsedMs(totalTimeMs);
    requestContext.setQueryProcessingTime(totalTimeMs);
    requestContext.setTraceInfo(brokerResponse.getTraceInfo());
    augmentStatistics(requestContext, brokerResponse);

    // Log query and stats
    _queryLogger.log(
        new QueryLogger.QueryLogParams(requestId, query, requestContext, tableNames.toString(), numUnavailableSegments,
            null, brokerResponse, totalTimeMs, requesterIdentity));

    return brokerResponse;
  }

  /**
   * Validates whether the requester has access to all the tables.
   */
  private boolean hasTableAccess(RequesterIdentity requesterIdentity, Set<String> tableNames,
      RequestContext requestContext, HttpHeaders httpHeaders) {
    AccessControl accessControl = _accessControlFactory.create();
    boolean hasAccess = accessControl.hasAccess(requesterIdentity, tableNames) && tableNames.stream()
        .allMatch(table -> accessControl.hasAccess(httpHeaders, TargetType.TABLE, table, Actions.Table.QUERY));
    if (!hasAccess) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.warn("Access denied for requestId {}", requestContext.getRequestId());
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return false;
    }
    return true;
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
      RequestContext requestContext) {
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
}
