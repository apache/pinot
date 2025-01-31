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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.common.utils.Timer;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.query.ImmutableQueryEnvironment;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.explain.AskingServerStageExplainer;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.runtime.MultiStageStatsTreeBuilder;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);

  private static final int NUM_UNAVAILABLE_SEGMENTS_TO_LOG = 10;

  private final WorkerManager _workerManager;
  private final QueryDispatcher _queryDispatcher;
  private final boolean _explainAskingServerDefault;
  private final MultiStageQueryThrottler _queryThrottler;

  public MultiStageBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      MultiStageQueryThrottler queryThrottler) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    int port = Integer.parseInt(config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT));
    _workerManager = new WorkerManager(hostname, port, _routingManager);
    TlsConfig tlsConfig = config.getProperty(
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_TLS_ENABLED,
        CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_TLS_ENABLED) ? TlsUtils.extractTlsConfig(config,
        CommonConstants.Broker.BROKER_TLS_PREFIX) : null;
    _queryDispatcher = new QueryDispatcher(new MailboxService(hostname, port, config, tlsConfig), tlsConfig);
    LOGGER.info("Initialized MultiStageBrokerRequestHandler on host: {}, port: {} with broker id: {}, timeout: {}ms, "
            + "query log max length: {}, query log max rate: {}", hostname, port, _brokerId, _brokerTimeoutMs,
        _queryLogger.getMaxQueryLengthToLog(), _queryLogger.getLogRateLimit());
    _explainAskingServerDefault = _config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN,
        CommonConstants.MultiStageQueryRunner.DEFAULT_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN);
    _queryThrottler = queryThrottler;
  }

  @Override
  public void start() {
    _queryDispatcher.start();
  }

  @Override
  public void shutDown() {
    _queryDispatcher.shutdown();
  }
  @Override
  protected BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      HttpHeaders httpHeaders, AccessControl accessControl) {
    try {
      BrokerResponse brokerResponse = handleRequestFailing(requestId, query, sqlNodeAndOptions, requesterIdentity,
          requestContext, httpHeaders);
      if (!brokerResponse.getExceptions().isEmpty()) {
        // a _green_ error (see handleRequestFailing javadoc)
        LOGGER.info("Request failed in a controlled manner {}: {}", requestId, brokerResponse.getExceptions());
        onFailedRequest(brokerResponse.getExceptions());
      }
      return brokerResponse;
    } catch (WebApplicationException e) {
      // a _yellow_ error (see handleRequestFailing javadoc)
      LOGGER.info("Request {} failed as HTTP request", requestId, e);
      throw e;
    } catch (QException e) {
      // a _yellow_ error (see handleRequestFailing javadoc)
      LOGGER.info("Request {} failed as Pinot exception", requestId, e);
      BrokerResponseNative brokerResponseNative = new BrokerResponseNative(e.getErrorCode(), e.getMessage());
      onFailedRequest(brokerResponseNative.getExceptions());
      return brokerResponseNative;
    } catch (RuntimeException e) {
      // a _red_ error (see handleRequestFailing javadoc)
      LOGGER.warn("Request {} failed in an uncontrolled way", requestId, e);
      String subStackTrace = ExceptionUtils.consolidateExceptionMessages(e);
      BrokerResponseNative brokerResponseNative = new BrokerResponseNative(QException.UNKNOWN_ERROR_CODE, subStackTrace);
      onFailedRequest(brokerResponseNative.getExceptions());
      return brokerResponseNative;
    }
  }


  private void onFailedRequest(List<QueryProcessingException> exs) {
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);

    for (QueryProcessingException ex : exs) {
      switch (ex.getErrorCode()) {
        case QueryException.QUERY_VALIDATION_ERROR_CODE:
          _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
          break;
        case QueryException.UNKNOWN_COLUMN_ERROR_CODE:
          _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNKNOWN_COLUMN_EXCEPTIONS, 1);
          break;
      }
    }
  }

  /**
   * Handles the request that is expected to fail in a controlled way. 
   * 
   * The query may be a select or an explain and it can fail finish in the following ways:
   * <ul>
   *   <li>Successfully, the BrokerResponse will contain no errors</li>
   *   <li>With green error, the BrokerResponse will contain error messages.
   *   In that case, it is understood that the request failed in a controlled way. The error message will be sent
   *   to the user and these error messages will be logged without stack trace.</li>
   *   <li>With yellow error, the method fails throwing either a QException or a WebApplicationException.
   *   In that case, it is understood that the request failed in a controlled but infrequent way.
   *   The exception message will be sent to the user and these exception will be logged with stack trace.</li>
   *   <li>With red error, the method fails throwing any other RuntimeException.
   *   In that case, it is understood that the request failed in an uncontrolled way.
   *   The exception message and part of the stack trace will be sent to the user and these exception will be logged
   *   with stack trace. These exceptions should be rare and should be fixed/converted into yellow as soon as possible.
   *   </li>
   * </ul>
   *
   * @return the broker response, which may contain error messages.
   * @throws QException a <em>yellow</em> exception. The exception message will be sent to the user as a BrokerResponse
   * and these exception will be logged with stack trace.
   * @throws WebApplicationException a <em>yellow</em>. The exception message will be sent to the user as an HTTP error
   * response instead of a broker response. For example, if we want to fail with a 403 Forbidden error.
   * @throws RuntimeException a <em>red</em> exception. The exception message and part of the stack trace will be sent
   * to the user as a BrokerResponse and these exception will be logged with stack trace.
   */
  protected BrokerResponse handleRequestFailing(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders)
      throws QException, WebApplicationException {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    // Compile the request
    Map<String, String> queryOptions = sqlNodeAndOptions.getOptions();

    long compilationStartTimeNs = System.nanoTime();

    ImmutableQueryEnvironment.Config queryEnvConf = safeParse(() -> getQueryEnvConf(httpHeaders, queryOptions));

    if (sqlNodeAndOptions.getSqlNode().getKind() == SqlKind.EXPLAIN) {
      return explain(requestId, query, sqlNodeAndOptions, requesterIdentity, requestContext, httpHeaders,
          queryEnvConf, queryOptions);
    } else {
      return query(requestId, query, sqlNodeAndOptions, requesterIdentity, requestContext, httpHeaders,
          compilationStartTimeNs, queryEnvConf, queryOptions);
    }
  }

  /**
   * A utility function to wrap some code that may throw exceptions that will be treated as query compilation errors.
   * @param supplier may throw WebApplicationException or QException, which will be rethrown or RuntimeException, which
   *                 will be wrapped in a QException.
   * @throws WebApplicationException
   * @throws QException
   */
  private <E> E safeParse(Supplier<E> supplier)
      throws WebApplicationException, QException {
    try {
      return supplier.get();
    } catch (WebApplicationException e) {
      throw e;
    } catch (QException e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      throw e;
    } catch (RuntimeException e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      throw new QException(QueryException.QUERY_PLANNING_ERROR_CODE, e);
    }
  }

  private ImmutableQueryEnvironment.Config getQueryEnvConf(HttpHeaders httpHeaders, Map<String, String> queryOptions) {
    String database = DatabaseUtils.extractDatabaseFromQueryRequest(queryOptions, httpHeaders);
    boolean inferPartitionHint = _config.getProperty(CommonConstants.Broker.CONFIG_OF_INFER_PARTITION_HINT,
        CommonConstants.Broker.DEFAULT_INFER_PARTITION_HINT);
    boolean defaultUseSpool = _config.getProperty(CommonConstants.Broker.CONFIG_OF_SPOOLS,
        CommonConstants.Broker.DEFAULT_OF_SPOOLS);
    return QueryEnvironment.configBuilder()
        .database(database)
        .tableCache(_tableCache)
        .workerManager(_workerManager)
        .defaultInferPartitionHint(inferPartitionHint)
        .defaultUseSpools(defaultUseSpool)
        .build();
  }

  private long getTimeout(Map<String, String> queryOptions) {
    Long timeoutMsFromQueryOption = QueryOptionsUtils.getTimeoutMs(queryOptions);
    return timeoutMsFromQueryOption != null ? timeoutMsFromQueryOption : _brokerTimeoutMs;
  }

  /**
   * Explains the query and returns the broker response.
   *
   * Throws using the same conventions as handleRequestFailing.
   */
  private BrokerResponse explain(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders,
      QueryEnvironment.Config queryEnvConf, Map<String, String> queryOptions)
  throws WebApplicationException, QException {
    QueryEnvironment.QueryPlannerResult queryPlanResult = safeParse(() -> {
      boolean askServers = QueryOptionsUtils.isExplainAskingServers(queryOptions)
          .orElse(_explainAskingServerDefault);
      @Nullable
      AskingServerStageExplainer.OnServerExplainer fragmentToPlanNode = askServers
          ? fragment -> requestPhysicalPlan(fragment, requestContext, queryOptions)
          : null;

      QueryEnvironment queryEnvironment = new QueryEnvironment(queryEnvConf);
      QueryEnvironment.QueryPlannerResult result =
          queryEnvironment.explainQuery(query, sqlNodeAndOptions, requestId, fragmentToPlanNode);

      Set<String> tableNames = result.getTableNames();
      TableAuthorizationResult tableAuthorizationResult =
          hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders);
      // TODO: We need to verify access before we start planning the query to avoid timing attacks.
      if (!tableAuthorizationResult.hasAccess()) {
        String failureMessage = tableAuthorizationResult.getFailureMessage();
        if (StringUtils.isNotBlank(failureMessage)) {
          failureMessage = "Reason: " + failureMessage;
        }
        throw new WebApplicationException("Permission denied. " + failureMessage, Response.Status.FORBIDDEN);
      }
      return result;
    });

    String plan = queryPlanResult.getExplainPlan();
    return constructMultistageExplainPlan(query, plan);
  }

  /**
   * Executes the query and returns the broker response.
   *
   * Throws using the same conventions as handleRequestFailing.
   */
  private BrokerResponse query(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders,
      long compilationStartTimeNs, QueryEnvironment.Config queryEnvConf,
      Map<String, String> queryOptions)
    throws QException, WebApplicationException {

    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_STAGE_QUERIES_GLOBAL, 1);

    QueryEnvironment.QueryPlannerResult queryPlanResult = safeParse(() -> {
      QueryEnvironment queryEnvironment = new QueryEnvironment(queryEnvConf);
      return queryEnvironment.planQuery(query, sqlNodeAndOptions, requestId);
    });

    DispatchableSubPlan dispatchableSubPlan = queryPlanResult.getQueryPlan();

    Set<QueryServerInstance> servers = new HashSet<>();
    for (DispatchablePlanFragment planFragment: dispatchableSubPlan.getQueryStageList()) {
      servers.addAll(planFragment.getServerInstances());
    }

    Set<String> tableNames = queryPlanResult.getTableNames();
    for (String tableName : tableNames) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.MULTI_STAGE_QUERIES, 1);
    }

    requestContext.setTableNames(List.copyOf(tableNames));

    // Compilation Time. This includes the time taken for parsing, compiling, create stage plans and assigning workers.
    long compilationEndTimeNs = System.nanoTime();
    long compilationTimeNs = (compilationEndTimeNs - compilationStartTimeNs) + sqlNodeAndOptions.getParseTimeNs();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.REQUEST_COMPILATION, compilationTimeNs);

    // Validate table access.
    TableAuthorizationResult tableAuthorizationResult =
        hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders);
    // TODO: We need to verify access before we start planning the query to avoid timing attacks.
    if (!tableAuthorizationResult.hasAccess()) {
      String failureMessage = tableAuthorizationResult.getFailureMessage();
      if (StringUtils.isNotBlank(failureMessage)) {
        failureMessage = "Reason: " + failureMessage;
      }
      throw new WebApplicationException("Permission denied." + failureMessage, Response.Status.FORBIDDEN);
    }

    // Validate QPS quota
    if (hasExceededQPSQuota(queryEnvConf.getDatabase(), tableNames, requestContext)) {
      String errorMessage = String.format("Request %d: %s exceeds query quota.", requestId, query);
      throw new QException(QueryException.TOO_MANY_REQUESTS_ERROR_CODE, errorMessage);
    }

    long queryTimeoutMs = getTimeout(queryOptions);
    Timer queryTimer = new Timer(queryTimeoutMs);
    int estimatedNumQueryThreads = dispatchableSubPlan.getEstimatedNumQueryThreads();
    try {
      // It's fine to block in this thread because we use a separate thread pool from the main Jersey server to process
      // these requests.
      if (!_queryThrottler.tryAcquire(estimatedNumQueryThreads, queryTimeoutMs, TimeUnit.MILLISECONDS)) {
        return new BrokerResponseNative(QException.BROKER_TIMEOUT_ERROR_CODE,
            "Request " + requestId + ": timed out waiting in the execution queue.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new QException(QException.INTERNAL_ERROR_CODE);
    }

    try {
      Tracing.ThreadAccountantOps.setupRunner(String.valueOf(requestId), ThreadExecutionContext.TaskType.MSE);

      long executionStartTimeNs = System.nanoTime();
      QueryDispatcher.QueryResult queryResults;
      try {
        queryResults =
            _queryDispatcher.submitAndReduce(requestContext, dispatchableSubPlan, queryTimer.getRemainingTime(),
                queryOptions);
      } catch (QException e) {
        // this is a case we may want to log as a warning, because it is unexpected
        if (e.getErrorCode() == QException.INTERNAL_ERROR_CODE || e.getErrorCode() == QException.UNKNOWN_ERROR_CODE) {
          LOGGER.warn("Request {} failed in the query dispatcher on an unexpected way", requestId, e);
        }
        // In other cases (ie something that fail in a server), we don't need the stack trace in the broker log
        // given that the server log will have it.
        return new BrokerResponseNative(e.getErrorCode(), e.getMessage());
      } catch (TimeoutException e) {
        for (String table : tableNames) {
          _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
        }
        String errMsg = "Request " + requestId + ": timed out while broker was waiting for results from servers";
        // Is the stack trace useful here? Probably not, but it is an error detected by the broker, so we log it
        // until it is proven that it is not useful.
        LOGGER.warn(errMsg, e);
        return new BrokerResponseNative(QException.BROKER_TIMEOUT_ERROR_CODE, errMsg);
      } finally {
        Tracing.getThreadAccountant().clear();
      }
      long executionEndTimeNs = System.nanoTime();
      updatePhaseTimingForTables(tableNames, BrokerQueryPhase.QUERY_EXECUTION,
          executionEndTimeNs - executionStartTimeNs);

      BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
      brokerResponse.setResultTable(queryResults.getResultTable());
      brokerResponse.setTablesQueried(tableNames);
      brokerResponse.setBrokerReduceTimeMs(queryResults.getBrokerReduceTimeMs());
      // MSE cannot finish if a single queried server did not respond, so we can use the same count for
      // both the queried and responded stats. Minus one prevents the broker to be included in the count
      // (it will always be included because of the root of the query plan)
      brokerResponse.setNumServersQueried(servers.size() - 1);
      brokerResponse.setNumServersResponded(servers.size() - 1);

      // Attach unavailable segments
      int numUnavailableSegments = 0;
      for (Map.Entry<String, Set<String>> entry : dispatchableSubPlan.getTableToUnavailableSegmentsMap().entrySet()) {
        String tableName = entry.getKey();
        Set<String> unavailableSegments = entry.getValue();
        int unavailableSegmentsInSubPlan = unavailableSegments.size();
        numUnavailableSegments += unavailableSegmentsInSubPlan;
        brokerResponse.addException(QueryException.getException(QueryException.SERVER_SEGMENT_MISSING_ERROR,
            String.format("Found %d unavailable segments for table %s: %s", unavailableSegmentsInSubPlan, tableName,
                toSizeLimitedString(unavailableSegments, NUM_UNAVAILABLE_SEGMENTS_TO_LOG))));
      }
      requestContext.setNumUnavailableSegments(numUnavailableSegments);

      fillOldBrokerResponseStats(brokerResponse, queryResults.getQueryStats(), dispatchableSubPlan);

      // Set total query processing time
      // TODO: Currently we don't emit metric for QUERY_TOTAL_TIME_MS
      long totalTimeMs = System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis();
      brokerResponse.setTimeUsedMs(totalTimeMs);
      augmentStatistics(requestContext, brokerResponse);
      if (QueryOptionsUtils.shouldDropResults(queryOptions)) {
        brokerResponse.setResultTable(null);
      }

      // Log query and stats
      _queryLogger.log(
          new QueryLogger.QueryLogParams(requestContext, tableNames.toString(), brokerResponse, requesterIdentity,
              null));

      return brokerResponse;
    } finally {
      _queryThrottler.release(estimatedNumQueryThreads);
    }
  }

  private Collection<PlanNode> requestPhysicalPlan(DispatchablePlanFragment fragment,
      RequestContext requestContext, Map<String, String> queryOptions) {
    List<PlanNode> stagePlans;
    try {
      long queryTimeoutMs = getTimeout(queryOptions);
      stagePlans = _queryDispatcher.explain(requestContext, fragment, queryTimeoutMs, queryOptions);
    } catch (Exception e) {
      PlanNode fragmentRoot = fragment.getPlanFragment().getFragmentRoot();
      throw new RuntimeException("Cannot obtain physical plan for fragment " + fragmentRoot.explain(), e);
    }

    return stagePlans;
  }

  private void fillOldBrokerResponseStats(BrokerResponseNativeV2 brokerResponse,
      List<MultiStageQueryStats.StageStats.Closed> queryStats, DispatchableSubPlan dispatchableSubPlan) {
    try {
      List<DispatchablePlanFragment> stagePlans = dispatchableSubPlan.getQueryStageList();

      MultiStageStatsTreeBuilder treeBuilder = new MultiStageStatsTreeBuilder(stagePlans, queryStats);
      brokerResponse.setStageStats(treeBuilder.jsonStatsByStage(0));
      for (MultiStageQueryStats.StageStats.Closed stageStats : queryStats) {
        if (stageStats != null) { // for example pipeline breaker may not have stats
          stageStats.forEach((type, stats) -> type.mergeInto(brokerResponse, stats));
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error encountered while collecting multi-stage stats", e);
      brokerResponse.setStageStats(JsonNodeFactory.instance.objectNode()
          .put("error", "Error encountered while collecting multi-stage stats - " + e));
    }
  }

  /**
   * Validates whether the requester has access to all the tables.
   */
  private TableAuthorizationResult hasTableAccess(RequesterIdentity requesterIdentity, Set<String> tableNames,
      RequestContext requestContext, HttpHeaders httpHeaders) {
    final long startTimeNs = System.nanoTime();
    AccessControl accessControl = _accessControlFactory.create();

    TableAuthorizationResult tableAuthorizationResult = accessControl.authorize(requesterIdentity, tableNames);

    Set<String> failedTables = tableNames.stream()
        .filter(table -> !accessControl.hasAccess(httpHeaders, TargetType.TABLE, table, Actions.Table.QUERY))
        .collect(Collectors.toSet());

    failedTables.addAll(tableAuthorizationResult.getFailedTables());

    if (!failedTables.isEmpty()) {
      tableAuthorizationResult = new TableAuthorizationResult(failedTables);
    } else {
      tableAuthorizationResult = TableAuthorizationResult.success();
    }

    if (!tableAuthorizationResult.hasAccess()) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.warn("Access denied for requestId {}", requestContext.getRequestId());
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
    }

    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - startTimeNs);

    return tableAuthorizationResult;
  }

  /**
   * Returns true if the QPS quota of query tables, database or application has been exceeded.
   */
  private boolean hasExceededQPSQuota(@Nullable String database, Set<String> tableNames,
      RequestContext requestContext) {
    if (database != null && !_queryQuotaManager.acquireDatabase(database)) {
      LOGGER.warn("Request {}: query exceeds quota for database: {}", requestContext.getRequestId(), database);
      requestContext.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
      return true;
    }
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
  public Map<Long, String> getRunningQueries() {
    // TODO: Support running query tracking for multi-stage engine
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses) {
    // TODO: Support query cancellation for multi-stage engine
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the string representation of the Set of Strings with a limit on the number of elements.
   * @param setOfStrings Set of strings
   * @param limit Limit on the number of elements
   * @return String representation of the set of the form [a,b,c...].
   */
  private static String toSizeLimitedString(Set<String> setOfStrings, int limit) {
    return setOfStrings.stream().limit(limit)
        .collect(Collectors.joining(", ", "[", setOfStrings.size() > limit ? "...]" : "]"));
  }
}
