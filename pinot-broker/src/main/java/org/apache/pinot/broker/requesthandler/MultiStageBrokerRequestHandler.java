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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerGauge;
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
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.Timer;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.core.transport.ServerInstance;
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
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class serves as the broker entry-point for handling incoming multi-stage query requests and dispatching them
 * to servers.
 */
public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);

  private static final int NUM_UNAVAILABLE_SEGMENTS_TO_LOG = 10;

  private final WorkerManager _workerManager;
  private final QueryDispatcher _queryDispatcher;
  private final boolean _explainAskingServerDefault;
  private final MultiStageQueryThrottler _queryThrottler;
  private final ExecutorService _queryCompileExecutor;

  public MultiStageBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      MultiStageQueryThrottler queryThrottler, FailureDetector failureDetector) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    int port = Integer.parseInt(config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT));
    _workerManager = new WorkerManager(_brokerId, hostname, port, _routingManager);
    TlsConfig tlsConfig = config.getProperty(
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_TLS_ENABLED,
        CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_TLS_ENABLED) ? TlsUtils.extractTlsConfig(config,
        CommonConstants.Broker.BROKER_TLS_PREFIX) : null;

    failureDetector.registerUnhealthyServerRetrier(this::retryUnhealthyServer);
    _queryDispatcher =
        new QueryDispatcher(new MailboxService(hostname, port, config, tlsConfig), failureDetector, tlsConfig,
            this.isQueryCancellationEnabled());
    LOGGER.info("Initialized MultiStageBrokerRequestHandler on host: {}, port: {} with broker id: {}, timeout: {}ms, "
            + "query log max length: {}, query log max rate: {}, query cancellation enabled: {}", hostname, port,
        _brokerId, _brokerTimeoutMs, _queryLogger.getMaxQueryLengthToLog(), _queryLogger.getLogRateLimit(),
        this.isQueryCancellationEnabled());
    _explainAskingServerDefault = _config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN,
        CommonConstants.MultiStageQueryRunner.DEFAULT_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN);
    _queryThrottler = queryThrottler;
    _queryCompileExecutor = QueryThreadContext.contextAwareExecutorService(
            Executors.newFixedThreadPool(
                Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
                new NamedThreadFactory("multi-stage-query-compile-executor")));
  }

  @Override
  public void start() {
    _queryDispatcher.start();
  }

  @Override
  public void shutDown() {
    _queryCompileExecutor.shutdown();
    _queryDispatcher.shutdown();
  }

  @Override
  protected void onQueryStart(long requestId, String clientRequestId, String query, Object... extras) {
    super.onQueryStart(requestId, clientRequestId, query, extras);
    QueryThreadContext.setQueryEngine("mse");
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      HttpHeaders httpHeaders, AccessControl accessControl) {
    try {
      BrokerResponse brokerResponse = handleRequestThrowing(requestId, query, sqlNodeAndOptions, requesterIdentity,
          requestContext, httpHeaders);
      if (!brokerResponse.getExceptions().isEmpty()) {
        // a _green_ error (see handleRequestThrowing javadoc)
        LOGGER.info("Request {} failed in a controlled manner: {}", requestId, brokerResponse.getExceptions());
        onFailedRequest(brokerResponse.getExceptions());
      }
      return brokerResponse;
    } catch (WebApplicationException e) {
      // a _yellow_ error (see handleRequestThrowing javadoc)
      LOGGER.info("Request {} failed as HTTP request", requestId, e);
      throw e;
    } catch (QueryException e) {
      if (isYellowError(e)) {
        // a _yellow_ error (see handleRequestThrowing javadoc)
        LOGGER.warn("Request {} failed with exception", requestId, e);
      } else {
        // a _green_ error (see handleRequestThrowing javadoc)
        LOGGER.info("Request {} failed with message {}", requestId, e.getMessage());
      }
      BrokerResponseNative brokerResponseNative = new BrokerResponseNative(e.getErrorCode(), e.getMessage());
      onFailedRequest(brokerResponseNative.getExceptions());
      return brokerResponseNative;
    } catch (RuntimeException e) {
      // a _red_ error (see handleRequestThrowing javadoc)
      LOGGER.warn("Request {} failed in an uncontrolled manner", requestId, e);
      String subStackTrace = ExceptionUtils.consolidateExceptionMessages(e);
      BrokerResponseNative brokerResponseNative = new BrokerResponseNative(QueryErrorCode.UNKNOWN, subStackTrace);
      onFailedRequest(brokerResponseNative.getExceptions());
      return brokerResponseNative;
    }
  }

  private void onFailedRequest(List<QueryProcessingException> exs) {
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1);

    for (QueryProcessingException ex : exs) {
      try {
        switch (QueryErrorCode.fromErrorCode(ex.getErrorCode())) {
          case QUERY_VALIDATION:
            _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
            break;
          case UNKNOWN_COLUMN:
            _brokerMetrics.addMeteredGlobalValue(BrokerMeter.UNKNOWN_COLUMN_EXCEPTIONS, 1);
            break;
          default:
            break;
        }
      } catch (IllegalArgumentException e) {
        // If the error code is not recognized, does nothing
      }
    }
  }

  /// Handles the request that can fail in a controlled way.
  ///
  /// The query may be a select or an explain and it can finish in the following ways:
  /// 1. Successfully
  /// 2. With green error: The request failed in a controlled. Usually a user error.
  ///   - Examples:
  ///     - A table that doesn't exist is used,
  ///     - A table not authorized to read is used
  ///     - An exception during function execution due to errors in the data
  ///       (ie a division by zero or casting an illegal string as int)
  ///   - The error message will be sent to the user and the error messages will be logged without stack trace.
  /// 3. With yellow error: The request failed in a way that is controlled but probably internal.
  ///   - The error message will be sent to the user and the error message will be logged with stack trace.
  /// 4. With red error: The request failed in an unexpected way.
  ///   - The error message and part of the stack trace will be sent to the user and the error message will be logged
  ///     with stack trace.
  ///   - Example: a NullPointerException that leaked from the code.
  ///
  /// How to classify responses:
  /// - If the method returns a [BrokerResponse] that contains no errors, it is considered successful.
  /// - If the method returns a [BrokerResponse] that contains error messages, it is considered a green error.
  /// - If the method throws [QueryException]
  ///   - If the exception is considered a [yellow error][#isYellowError(QueryException)], it is considered a yellow
  ///     error.
  ///   - Otherwise, it is considered a green error.
  /// - If the method throws a [WebApplicationException], it is considered a yellow error.
  /// - If the method throws any other exception, it is considered a red error.
  protected BrokerResponse handleRequestThrowing(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders)
      throws QueryException, WebApplicationException {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    long queryTimeoutMs = getTimeout(sqlNodeAndOptions.getOptions());
    Timer queryTimer = new Timer(queryTimeoutMs, TimeUnit.MILLISECONDS);

    try (QueryEnvironment.CompiledQuery compiledQuery =
        compileQuery(requestId, query, sqlNodeAndOptions, httpHeaders, queryTimer)) {

      checkAuthorization(requesterIdentity, requestContext, httpHeaders, compiledQuery);

      if (sqlNodeAndOptions.getSqlNode().getKind() == SqlKind.EXPLAIN) {
        return explain(compiledQuery, requestId, requestContext, queryTimer);
      } else {
        return query(compiledQuery, requestId, requesterIdentity, requestContext, httpHeaders, queryTimer);
      }
    }
  }

  /// Compiles the query.
  ///
  /// In this phase the query can be either planned or explained
  private QueryEnvironment.CompiledQuery compileQuery(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      HttpHeaders httpHeaders, Timer queryTimer) {
    Map<String, String> queryOptions = sqlNodeAndOptions.getOptions();

    try {
      ImmutableQueryEnvironment.Config queryEnvConf = getQueryEnvConf(httpHeaders, queryOptions);
      QueryEnvironment queryEnv = new QueryEnvironment(queryEnvConf);
      return callAsync(requestId, query, () -> queryEnv.compile(query, sqlNodeAndOptions), queryTimer);
    } catch (WebApplicationException e) {
      throw e;
    } catch (QueryException e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      throw e;
    } catch (RuntimeException e) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      throw QueryErrorCode.QUERY_PLANNING.asException(e);
    } catch (Throwable t) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      throw t;
    }
  }

  private void checkAuthorization(RequesterIdentity requesterIdentity, RequestContext requestContext,
      HttpHeaders httpHeaders, QueryEnvironment.CompiledQuery compiledQuery) {
    Set<String> tables = compiledQuery.getTableNames();
    if (tables != null && !tables.isEmpty()) {
      TableAuthorizationResult tableAuthorizationResult =
          hasTableAccess(requesterIdentity, tables, requestContext, httpHeaders);
      if (!tableAuthorizationResult.hasAccess()) {
        throwTableAccessError(tableAuthorizationResult);
      }
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
   * Throws using the same conventions as handleRequestThrowing.
   */
  private BrokerResponse explain(QueryEnvironment.CompiledQuery query, long requestId, RequestContext requestContext,
      Timer timer)
      throws WebApplicationException, QueryException {
    Map<String, String> queryOptions = query.getOptions();

    boolean askServers = QueryOptionsUtils.isExplainAskingServers(queryOptions)
        .orElse(_explainAskingServerDefault);
    @Nullable
    AskingServerStageExplainer.OnServerExplainer fragmentToPlanNode = askServers
        ? fragment -> requestPhysicalPlan(fragment, requestContext, timer.getRemainingTimeMs(), queryOptions)
        : null;

    QueryEnvironment.QueryPlannerResult queryPlanResult = callAsync(requestId, query.getTextQuery(),
        () -> query.explain(requestId, fragmentToPlanNode), timer);
    String plan = queryPlanResult.getExplainPlan();
    Set<String> tableNames = queryPlanResult.getTableNames();
    Map<String, String> extraFields = queryPlanResult.getExtraFields();
    return constructMultistageExplainPlan(query.getTextQuery(), plan, extraFields);
  }

  private BrokerResponse query(QueryEnvironment.CompiledQuery query, long requestId,
      RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders, Timer timer)
      throws QueryException, WebApplicationException {
    QueryEnvironment.QueryPlannerResult queryPlanResult = callAsync(requestId, query.getTextQuery(),
        () -> query.planQuery(requestId), timer);

    DispatchableSubPlan dispatchableSubPlan = queryPlanResult.getQueryPlan();

    Set<QueryServerInstance> servers = new HashSet<>();
    for (DispatchablePlanFragment planFragment : dispatchableSubPlan.getQueryStageMap().values()) {
      servers.addAll(planFragment.getServerInstances());
    }

    Set<String> tableNames = queryPlanResult.getTableNames();
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_STAGE_QUERIES_GLOBAL, 1);
    for (String tableName : tableNames) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.MULTI_STAGE_QUERIES, 1);
    }

    requestContext.setTableNames(List.copyOf(tableNames));

    // Compilation Time. This includes the time taken for parsing, compiling, create stage plans and assigning workers.
    long compilationTimeNs = timer.timeElapsed(TimeUnit.NANOSECONDS) + query.getSqlNodeAndOptions().getParseTimeNs();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.REQUEST_COMPILATION, compilationTimeNs);

    // Validate table access.
    TableAuthorizationResult tableAuthorizationResult =
        hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders);
    if (!tableAuthorizationResult.hasAccess()) {
      throwTableAccessError(tableAuthorizationResult);
    }

    // Validate QPS quota
    if (hasExceededQPSQuota(query.getDatabase(), tableNames, requestContext)) {
      String errorMessage = String.format("Request %d: %s exceeds query quota.", requestId, query);
      return new BrokerResponseNative(QueryErrorCode.TOO_MANY_REQUESTS, errorMessage);
    }

    int estimatedNumQueryThreads = dispatchableSubPlan.getEstimatedNumQueryThreads();
    try {
      // It's fine to block in this thread because we use a separate thread pool from the main Jersey server to process
      // these requests.
      if (!_queryThrottler.tryAcquire(estimatedNumQueryThreads, timer.getRemainingTimeMs(),
          TimeUnit.MILLISECONDS)) {
        LOGGER.warn("Timed out waiting to execute request {}: {}", requestId, query);
        requestContext.setErrorCode(QueryErrorCode.EXECUTION_TIMEOUT);
        return new BrokerResponseNative(QueryErrorCode.EXECUTION_TIMEOUT);
      }
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ESTIMATED_MSE_SERVER_THREADS,
          _queryThrottler.currentQueryServerThreads());
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupt received while waiting to execute request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryErrorCode.EXECUTION_TIMEOUT);
      return new BrokerResponseNative(QueryErrorCode.EXECUTION_TIMEOUT);
    }

    String clientRequestId = extractClientRequestId(query.getSqlNodeAndOptions());
    onQueryStart(requestId, clientRequestId, query.getTextQuery());

    try {
      Tracing.ThreadAccountantOps.setupRunner(String.valueOf(requestId), ThreadExecutionContext.TaskType.MSE);

      long executionStartTimeNs = System.nanoTime();
      QueryDispatcher.QueryResult queryResults;
      try {
        queryResults = _queryDispatcher.submitAndReduce(requestContext, dispatchableSubPlan, timer.getRemainingTimeMs(),
                query.getOptions());
      } catch (TimeoutException e) {
        for (String table : tableNames) {
          _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
        }
        LOGGER.warn("Timed out executing request {}: {}", requestId, query);
        requestContext.setErrorCode(QueryErrorCode.EXECUTION_TIMEOUT);
        return new BrokerResponseNative(QueryErrorCode.EXECUTION_TIMEOUT);
      } catch (QueryException e) {
        throw e;
      } catch (Throwable t) {
        QueryErrorCode queryErrorCode = QueryErrorCode.QUERY_EXECUTION;
        String consolidatedMessage = ExceptionUtils.consolidateExceptionMessages(t);
        LOGGER.error("Caught exception executing request {}: {}, {}", requestId, query, consolidatedMessage);
        requestContext.setErrorCode(queryErrorCode);
        return new BrokerResponseNative(queryErrorCode, consolidatedMessage);
      } finally {
        Tracing.ThreadAccountantOps.clear();
        onQueryFinish(requestId);
      }
      long executionEndTimeNs = System.nanoTime();
      updatePhaseTimingForTables(tableNames, BrokerQueryPhase.QUERY_EXECUTION,
          executionEndTimeNs - executionStartTimeNs);

      BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
      brokerResponse.setClientRequestId(clientRequestId);
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
        QueryProcessingException errMsg = new QueryProcessingException(QueryErrorCode.SERVER_SEGMENT_MISSING,
            "Found " + unavailableSegmentsInSubPlan + " unavailable segments for table " + tableName + ": "
                + toSizeLimitedString(unavailableSegments, NUM_UNAVAILABLE_SEGMENTS_TO_LOG));
        brokerResponse.addException(errMsg);
      }
      requestContext.setNumUnavailableSegments(numUnavailableSegments);

      fillOldBrokerResponseStats(brokerResponse, queryResults.getQueryStats(), dispatchableSubPlan);

      // Track number of queries with number of groups limit reached
      if (brokerResponse.isNumGroupsLimitReached()) {
        for (String table : tableNames) {
          _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED, 1);
        }
      }

      // Set total query processing time
      // TODO: Currently we don't emit metric for QUERY_TOTAL_TIME_MS
      long totalTimeMs = System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis();
      brokerResponse.setTimeUsedMs(totalTimeMs);
      augmentStatistics(requestContext, brokerResponse);
      if (QueryOptionsUtils.shouldDropResults(query.getOptions())) {
        brokerResponse.setResultTable(null);
      }

      // Log query and stats
      _queryLogger.log(
          new QueryLogger.QueryLogParams(requestContext, tableNames.toString(), brokerResponse,
              QueryLogger.QueryLogParams.QueryEngine.MULTI_STAGE, requesterIdentity, null));

      return brokerResponse;
    } finally {
      _queryThrottler.release(estimatedNumQueryThreads);
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ESTIMATED_MSE_SERVER_THREADS,
          _queryThrottler.currentQueryServerThreads());
    }
  }

  private static void throwTableAccessError(TableAuthorizationResult tableAuthorizationResult) {
    String failureMessage = tableAuthorizationResult.getFailureMessage();
    if (StringUtils.isNotBlank(failureMessage)) {
      failureMessage = "Reason: " + failureMessage;
    }
    throw new WebApplicationException("Permission denied." + failureMessage, Response.Status.FORBIDDEN);
  }

  /**
   * Calls the given callable in a separate thread and enforces a timeout on it.
   *
   * The only exception that can be thrown by this method is a QueryException. All other exceptions are caught and
   * wrapped in a QueryException. Specifically, {@link TimeoutException} is caught and wrapped in a QueryException with
   * the error code {@link QueryErrorCode#BROKER_TIMEOUT} and other exceptions are treated as internal errors.K
   */
  private <E> E callAsync(long requestId, String query, Callable<E> queryPlannerResultCallable, Timer timer)
      throws QueryException {
    Future<E> queryPlanResultFuture = _queryCompileExecutor.submit(queryPlannerResultCallable);
    try {
      return queryPlanResultFuture.get(timer.getRemainingTimeMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      String errorMsg = "Timed out while planning query";
      LOGGER.warn(errorMsg + " {}", query, e);
      queryPlanResultFuture.cancel(true);
      throw QueryErrorCode.BROKER_TIMEOUT.asException(errorMsg);
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupt received while planning query {}: {}", requestId, query);
      throw QueryErrorCode.INTERNAL.asException("Interrupted while planning query");
    } catch (ExecutionException e) {
      if (e.getCause() instanceof QueryException) {
        throw (QueryException) e.getCause();
      } else {
        LOGGER.warn("Error while planning query {}: {}", query, e.getCause());
        throw QueryErrorCode.INTERNAL.asException("Error while planning query", e.getCause());
      }
    }
  }

  private Collection<PlanNode> requestPhysicalPlan(DispatchablePlanFragment fragment,
      RequestContext requestContext, long queryTimeoutMs, Map<String, String> queryOptions) {
    List<PlanNode> stagePlans;
    try {
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
      Map<Integer, DispatchablePlanFragment> queryStageMap = dispatchableSubPlan.getQueryStageMap();

      MultiStageStatsTreeBuilder treeBuilder = new MultiStageStatsTreeBuilder(queryStageMap, queryStats);
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

  private BrokerResponse constructMultistageExplainPlan(String sql, String plan, Map<String, String> extraFields) {
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    int totalFieldCount = extraFields.size() + 2;
    String[] fieldNames = new String[totalFieldCount];
    Object[] fieldValues = new Object[totalFieldCount];
    fieldNames[0] = "SQL";
    fieldValues[0] = sql;
    fieldNames[1] = "PLAN";
    fieldValues[1] = plan;
    int i = 2;
    for (Map.Entry<String, String> entry : extraFields.entrySet()) {
      fieldNames[i] = entry.getKey().toUpperCase();
      fieldValues[i] = entry.getValue();
      i++;
    }
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[totalFieldCount];
    Arrays.fill(columnDataTypes, DataSchema.ColumnDataType.STRING);
    DataSchema multistageExplainResultSchema = new DataSchema(fieldNames, columnDataTypes);
    List<Object[]> rows = new ArrayList<>(1);
    rows.add(fieldValues);
    brokerResponse.setResultTable(new ResultTable(multistageExplainResultSchema, rows));
    return brokerResponse;
  }

  @Override
  protected boolean handleCancel(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses) {
    return _queryDispatcher.cancel(queryId);
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

  /**
   * Check if a server that was previously detected as unhealthy is now healthy.
   */
  public FailureDetector.ServerState retryUnhealthyServer(String instanceId) {
    LOGGER.info("Checking gRPC connection to unhealthy server: {}", instanceId);
    ServerInstance serverInstance = _routingManager.getEnabledServerInstanceMap().get(instanceId);
    if (serverInstance == null) {
      LOGGER.info("Failed to find enabled server: {} in routing manager, skipping the retry", instanceId);
      return FailureDetector.ServerState.UNHEALTHY;
    }

    return _queryDispatcher.checkConnectivityToInstance(serverInstance);
  }

  public static boolean isYellowError(QueryException e) {
    switch (e.getErrorCode()) {
      case QUERY_SCHEDULING_TIMEOUT:
      case EXECUTION_TIMEOUT:
      case INTERNAL:
      case UNKNOWN:
      case MERGE_RESPONSE:
      case BROKER_TIMEOUT:
      case BROKER_REQUEST_SEND:
      case SERVER_NOT_RESPONDING:
        return true;
      default:
        return false;
    }
  }
}
