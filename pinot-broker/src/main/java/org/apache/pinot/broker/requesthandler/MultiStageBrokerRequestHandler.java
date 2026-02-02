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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.TlsConfig;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.failuredetector.FailureDetector;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.EagerToLazyBrokerResponseAdaptor;
import org.apache.pinot.common.response.StreamingBrokerResponse;
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
import org.apache.pinot.common.utils.request.QueryFingerprintUtils;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
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
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.QueryFingerprint;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;


/**
 * This class serves as the broker entry-point for handling incoming multi-stage query requests and dispatching them
 * to servers.
 */
public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);
  /// Disabled by default, but can be enabled with
  ///```xml
  ///  <MarkerFilter marker="MSE_STATS_MARKER" onMatch="ACCEPT" onMismatch="NEUTRAL"/>
  ///  ...
  ///  <Loggers>
  ///    <Logger name="org.apache.pinot" level="debug" additivity="false">
  ///      <AppenderRef ref="console">
  ///        <MarkerFilter marker="MSE_STATS_MARKER"/>
  ///      </AppenderRef>
  ///    </Logger>
  ///  </Loggers>
  ///```
  private static final Marker MSE_STATS_MARKER = MarkerFactory.getMarker("MSE_STATS_MARKER");

  private static final int NUM_UNAVAILABLE_SEGMENTS_TO_LOG = 10;

  private final WorkerManager _workerManager;
  private final WorkerManager _multiClusterWorkerManager;
  private final QueryDispatcher _queryDispatcher;
  private final boolean _explainAskingServerDefault;
  private final MultiStageQueryThrottler _queryThrottler;
  private final ExecutorService _queryCompileExecutor;
  private final Set<String> _defaultDisabledPlannerRules;
  protected final long _extraPassiveTimeoutMs;
  protected final boolean _enableQueryFingerprinting;

  protected final PinotMeter _stagesStartedMeter = BrokerMeter.MSE_STAGES_STARTED.getGlobalMeter();
  protected final PinotMeter _stagesFinishedMeter = BrokerMeter.MSE_STAGES_COMPLETED.getGlobalMeter();
  protected final PinotMeter _opchainsStartedMeter = BrokerMeter.MSE_OPCHAINS_STARTED.getGlobalMeter();
  protected final PinotMeter _opchainsCompletedMeter = BrokerMeter.MSE_OPCHAINS_COMPLETED.getGlobalMeter();

  public MultiStageBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRequestIdGenerator requestIdGenerator, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      MultiStageQueryThrottler queryThrottler, FailureDetector failureDetector, ThreadAccountant threadAccountant,
      MultiClusterRoutingContext multiClusterRoutingContext) {
    super(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
        threadAccountant, multiClusterRoutingContext);
    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    int port = Integer.parseInt(config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT));

    _workerManager = new WorkerManager(_brokerId, hostname, port, _routingManager);
    if (multiClusterRoutingContext != null) {
      _multiClusterWorkerManager = new WorkerManager(_brokerId, hostname, port,
          multiClusterRoutingContext.getMultiClusterRoutingManager());
    } else {
      // if multi-cluster routing is not enabled, use the same worker manager.
      _multiClusterWorkerManager = _workerManager;
    }

    TlsConfig tlsConfig = config.getProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_TLS_ENABLED,
        CommonConstants.Helix.DEFAULT_MULTI_STAGE_ENGINE_TLS_ENABLED) ? TlsUtils.extractTlsConfig(config,
        CommonConstants.Broker.BROKER_TLS_PREFIX) : null;

    _extraPassiveTimeoutMs = config.getProperty(
        CommonConstants.Broker.CONFIG_OF_EXTRA_PASSIVE_TIMEOUT_MS,
        CommonConstants.Broker.DEFAULT_EXTRA_PASSIVE_TIMEOUT_MS);

    failureDetector.registerUnhealthyServerRetrier(this::retryUnhealthyServer);
    long cancelMillis = config.getProperty(
        CommonConstants.MultiStageQueryRunner.KEY_OF_CANCEL_TIMEOUT_MS,
        CommonConstants.MultiStageQueryRunner.DEFAULT_OF_CANCEL_TIMEOUT_MS);
    Duration cancelTimeout = Duration.ofMillis(cancelMillis);
    _queryDispatcher =
        new QueryDispatcher(new MailboxService(hostname, port, InstanceType.BROKER, config, tlsConfig), failureDetector,
            tlsConfig, isQueryCancellationEnabled(), cancelTimeout);
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
    _defaultDisabledPlannerRules =
        _config.containsKey(CommonConstants.Broker.CONFIG_OF_BROKER_MSE_PLANNER_DISABLED_RULES) ? Set.copyOf(
            _config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_MSE_PLANNER_DISABLED_RULES, List.of()))
            : CommonConstants.Broker.DEFAULT_DISABLED_RULES;
    _enableQueryFingerprinting = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_QUERY_FINGERPRINTING,
        CommonConstants.Broker.DEFAULT_BROKER_ENABLE_QUERY_FINGERPRINTING);
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
  protected BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl) {
    @SuppressWarnings("resource")
    StreamingBrokerResponse streamingBrokerResponse = handleStreamingRequest(requestId, query, sqlNodeAndOptions,
        request, requesterIdentity, requestContext, httpHeaders, accessControl);
    return streamingBrokerResponse.asEagerBrokerResponse();
  }

  @Override
  protected StreamingBrokerResponse handleStreamingRequest(long requestId, String query,
      SqlNodeAndOptions sqlNodeAndOptions, JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext, @Nullable HttpHeaders httpHeaders, AccessControl accessControl) {
    try {
      StreamingBrokerResponse plainResponse = handleRequestThrowing(requestId, query, sqlNodeAndOptions,
          requesterIdentity, requestContext, httpHeaders);
      return postDecorate(plainResponse, sqlNodeAndOptions);
    } catch (QueryException e) {
      String exceptionMessage = ExceptionUtils.consolidateExceptionMessages(e);
      if (isYellowError(e)) {
        // a _yellow_ error (see handleRequestThrowing javadoc)
        LOGGER.warn("Request {} failed before dispatching with exception", requestId, e);
      } else {
        // a _green_ error (see handleRequestThrowing javadoc)
        LOGGER.info("Request {} failed before dispatching with messages {}", requestId, exceptionMessage);
      }
      StreamingBrokerResponse.Metainfo.Error error =
          new StreamingBrokerResponse.Metainfo.Error(e.getErrorCode(), exceptionMessage);
      onFailedRequest(error.getExceptions());
      return new StreamingBrokerResponse.EarlyResponse(error);
    } catch (RuntimeException e) {
      // a _red_ error (see handleRequestThrowing javadoc)
      LOGGER.warn("Request {} failed in an uncontrolled manner", requestId, e);
      String errorMessage = ExceptionUtils.consolidateExceptionMessages(e);
      StreamingBrokerResponse.Metainfo.Error error =
          new StreamingBrokerResponse.Metainfo.Error(QueryErrorCode.UNKNOWN, errorMessage);
      onFailedRequest(error.getExceptions());
      return new StreamingBrokerResponse.EarlyResponse(error);
    }
  }

  private static boolean explicitSummarizeLogRequested(SqlNodeAndOptions sqlNodeAndOptions) {
    return Boolean.parseBoolean(
        sqlNodeAndOptions.getOptions()
            .getOrDefault(CommonConstants.MultiStageQueryRunner.KEY_OF_LOG_STATS, "false")
            .toLowerCase(Locale.US));
  }

  private void summarizeQuery(StreamingBrokerResponse.Metainfo metainfo, boolean explicitSummarizeLogRequested) {
    String completionStatus = metainfo.getExceptions().isEmpty()
        ? "successfully"
        : "with errors " + metainfo.getExceptions();
    String logTemplate = "Request finished {} in {}ms. Stats: {}";
    ObjectNode json = metainfo.asJson();
    long timeUsedMs = json.get("timeUsedMs").asLong(0);

    JsonNode stageStats = json.get("stageStats");
    if (metainfo.getExceptions().isEmpty() && !explicitSummarizeLogRequested) {
      LOGGER.debug(MSE_STATS_MARKER, logTemplate, completionStatus, timeUsedMs, stageStats);
    } else {
      LOGGER.info(MSE_STATS_MARKER, logTemplate, completionStatus, timeUsedMs, stageStats);
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
  /// The query may be a select or an explain, and it can finish in the following ways:
  /// 1. Successfully
  /// 2. With green error: The request failed in a controlled. Usually a user error.
  ///   - Examples:
  ///     - A table that doesn't exist is used,
  ///     - A table not authorized to read is used
  ///     - An exception during function execution due to errors in the data
  ///       (ie a division by zero or casting an illegal string as int)
  ///     - Query is too heavy and reaches the allowed timeout.
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
  protected StreamingBrokerResponse handleRequestThrowing(
      long requestId,
      String query,
      SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders
  ) throws QueryException, WebApplicationException {
    boolean queryWasLogged = _queryLogger.logQueryReceived(requestId, query);

    String queryHash = CommonConstants.Broker.DEFAULT_QUERY_HASH;
    if (_enableQueryFingerprinting) {
      try {
        QueryFingerprint queryFingerprint = QueryFingerprintUtils.generateFingerprint(sqlNodeAndOptions);
        if (queryFingerprint != null) {
          queryHash = queryFingerprint.getQueryHash();
          requestContext.setQueryFingerprint(queryFingerprint);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to generate query fingerprint for request {}: {}. {}", requestId, query, e.getMessage());
      }
    }

    String cid = extractClientRequestId(sqlNodeAndOptions);
    if (cid == null) {
      cid = Long.toString(requestId);
    }
    Map<String, String> options = sqlNodeAndOptions.getOptions();
    String workloadName = QueryOptionsUtils.getWorkloadName(sqlNodeAndOptions.getOptions());
    long startTimeMs = requestContext.getRequestArrivalTimeMillis();
    long timeoutMs = getTimeoutMs(options);
    Timer queryTimer = new Timer(timeoutMs, TimeUnit.MILLISECONDS);
    long activeDeadlineMs = startTimeMs + timeoutMs;
    long passiveDeadlineMs = activeDeadlineMs + getExtraPassiveTimeoutMs(options);

    QueryExecutionContext executionContext =
        new QueryExecutionContext(QueryExecutionContext.QueryType.MSE, requestId, cid, workloadName, startTimeMs,
            activeDeadlineMs, passiveDeadlineMs, _brokerId, _brokerId, queryHash);
    QueryThreadContext.MseWorkerInfo mseWorkerInfo = new QueryThreadContext.MseWorkerInfo(0, 0);
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext, mseWorkerInfo, _threadAccountant);
        QueryEnvironment.CompiledQuery compiledQuery = compileQuery(requestId, query, sqlNodeAndOptions, requestContext,
            httpHeaders, queryTimer)) {
      AtomicBoolean rlsFiltersApplied = new AtomicBoolean(false);
      checkAuthorization(requesterIdentity, requestContext, httpHeaders, compiledQuery, rlsFiltersApplied);

      if (sqlNodeAndOptions.getSqlNode().getKind() == SqlKind.EXPLAIN) {
        return explain(compiledQuery, requestId, requestContext, queryTimer).toStreamingResponse();
      } else {
        StreamingBrokerResponse plainResponse = query(compiledQuery, requestId, requesterIdentity, requestContext,
            httpHeaders, queryTimer, queryWasLogged);
        if (rlsFiltersApplied.get()) {
          return plainResponse.withDecoratedMetainfo(stats -> {
            stats.put("rlsFiltersApplied", true);
          });
        } else {
          return plainResponse;
        }
      }
    }
  }

  /// Decorates a given response to apply an additional layer of decoration to the response to handle logging and error
  /// handling.
  ///
  /// This cannot be done in [MseHandlerStreamingBrokerResponse] because sometimes the query finished before even
  /// getting to the data consumption phase (ie authorization error or quota exceeded). Also, this ensures that even if
  /// the exception is thrown during post-reduce processing, it is still caught and handled properly.
  private StreamingBrokerResponse postDecorate(StreamingBrokerResponse response, SqlNodeAndOptions sqlNodeAndOptions) {
    return new StreamingBrokerResponse.Delegator(response) {
      Metainfo _metainfo;

      @Override
      public Metainfo consumeData(DataConsumer consumer)
          throws InterruptedException {
        long requestId = QueryThreadContext.get().getExecutionContext().getRequestId();
        try {
          Metainfo metainfo = _delegate.consumeData(consumer);
          if (!metainfo.getExceptions().isEmpty()) {
            // a _green_ error (see handleRequestThrowing javadoc)
            onFailedRequest(metainfo.getExceptions());
          }
          summarizeQuery(metainfo, explicitSummarizeLogRequested(sqlNodeAndOptions));
          _metainfo = metainfo;
          return metainfo;
        } catch (QueryException e) {
          String errorMessage = ExceptionUtils.consolidateExceptionMessages(e);
          if (isYellowError(e)) {
            // a _yellow_ error (see handleRequestThrowing javadoc)
            LOGGER.warn("Request {} failed during reduction with exception", requestId, e);
          } else {
            // a _green_ error (see handleRequestThrowing javadoc)
            LOGGER.info("Request {} failed during reduction with messages {}", requestId, errorMessage);
          }
          _metainfo = new Metainfo.Error(e.getErrorCode(), errorMessage);
          onFailedRequest(_metainfo.getExceptions());
          return _metainfo;
        } catch (RuntimeException e) {
          // a _red_ error (see handleRequestThrowing javadoc)
          LOGGER.warn("Request {} failed in an uncontrolled manner", requestId, e);
          _metainfo = new Metainfo.Error(QueryErrorCode.UNKNOWN, ExceptionUtils.consolidateExceptionMessages(e));
          onFailedRequest(_metainfo.getExceptions());
          return _metainfo;
        }
      }

      @Override
      public Metainfo getMetaInfo() {
        Preconditions.checkState(_metainfo != null, "getMetadata() called before consumeData()");
        return _metainfo;
      }
    };
  }

  /// Compiles the query.
  ///
  /// In this phase the query can be either planned or explained
  private QueryEnvironment.CompiledQuery compileQuery(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      RequestContext requestContext, @Nullable HttpHeaders httpHeaders, Timer queryTimer) {
    // Add queryHash to query options so it gets passed to multi-stage workers
    if (_enableQueryFingerprinting) {
      QueryFingerprint queryFingerprint = requestContext.getQueryFingerprint();
      if (queryFingerprint != null) {
        sqlNodeAndOptions.getOptions().put(
            CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH,
            queryFingerprint.getQueryHash());
      }
    }

    Map<String, String> queryOptions = sqlNodeAndOptions.getOptions();

    try {
      ImmutableQueryEnvironment.Config queryEnvConf = getQueryEnvConf(httpHeaders, queryOptions, requestId);
      QueryEnvironment queryEnv = new QueryEnvironment(queryEnvConf, _multiClusterRoutingContext);
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
      HttpHeaders httpHeaders, QueryEnvironment.CompiledQuery compiledQuery, AtomicBoolean rlsFiltersApplied) {
    Set<String> tables = compiledQuery.getTableNames();
    if (tables != null && !tables.isEmpty()) {
      TableAuthorizationResult tableAuthorizationResult =
          hasTableAccess(requesterIdentity, tables, requestContext, httpHeaders);
      if (!tableAuthorizationResult.hasAccess()) {
        throwTableAccessError(tableAuthorizationResult);
      }
      if (_enableRowColumnLevelAuth) {
        AccessControl accessControl = _accessControlFactory.create();
        for (String tableName : tables) {
          accessControl.getRowColFilters(requesterIdentity, tableName).getRLSFilters()
              .ifPresent(rowFilters -> {
                rlsFiltersApplied.set(true);
                String combinedFilters =
                    rowFilters.stream().map(filter -> "( " + filter + " )").collect(Collectors.joining(" AND "));
                String key = RlsUtils.buildRlsFilterKey(tableName);
                compiledQuery.getOptions().put(key, combinedFilters);
                _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.RLS_FILTERS_APPLIED, 1);
              });
        }
      }
    }
  }

  private ImmutableQueryEnvironment.Config getQueryEnvConf(HttpHeaders httpHeaders, Map<String, String> queryOptions,
      long requestId) {
    String database = DatabaseUtils.extractDatabaseFromQueryRequest(queryOptions, httpHeaders);
    boolean inferPartitionHint = _config.getProperty(CommonConstants.Broker.CONFIG_OF_INFER_PARTITION_HINT,
        CommonConstants.Broker.DEFAULT_INFER_PARTITION_HINT);
    boolean defaultUseSpool = _config.getProperty(CommonConstants.Broker.CONFIG_OF_SPOOLS,
        CommonConstants.Broker.DEFAULT_OF_SPOOLS);
    boolean defaultUseLeafServerForIntermediateStage = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE,
        CommonConstants.Broker.DEFAULT_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE);
    boolean defaultEnableGroupTrim = _config.getProperty(CommonConstants.Broker.CONFIG_OF_MSE_ENABLE_GROUP_TRIM,
        CommonConstants.Broker.DEFAULT_MSE_ENABLE_GROUP_TRIM);
    boolean defaultEnableDynamicFilteringSemiJoin = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_BROKER_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN,
        CommonConstants.Broker.DEFAULT_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN);
    boolean defaultUsePhysicalOptimizer = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_PHYSICAL_OPTIMIZER,
        CommonConstants.Broker.DEFAULT_USE_PHYSICAL_OPTIMIZER);
    boolean defaultUseLiteMode = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_LITE_MODE,
        CommonConstants.Broker.DEFAULT_USE_LITE_MODE);
    boolean defaultRunInBroker = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_RUN_IN_BROKER,
        CommonConstants.Broker.DEFAULT_RUN_IN_BROKER);
    boolean defaultUseBrokerPruning = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_USE_BROKER_PRUNING,
        CommonConstants.Broker.DEFAULT_USE_BROKER_PRUNING);
    int defaultLiteModeLeafStageLimit = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_LITE_MODE_LEAF_STAGE_LIMIT,
        CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_LIMIT);
    int defaultLiteModeFanoutAdjustedLimit = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_LITE_MODE_LEAF_STAGE_FANOUT_ADJUSTED_LIMIT,
        CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_FAN_OUT_ADJUSTED_LIMIT);
    boolean defaultLiteModeEnableJoins = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_LITE_MODE_ENABLE_JOINS,
        CommonConstants.Broker.DEFAULT_LITE_MODE_ENABLE_JOINS);
    String defaultHashFunction = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_BROKER_DEFAULT_HASH_FUNCTION,
        CommonConstants.Broker.DEFAULT_BROKER_DEFAULT_HASH_FUNCTION);
    boolean caseSensitive = !_config.getProperty(
        CommonConstants.Helix.ENABLE_CASE_INSENSITIVE_KEY,
        CommonConstants.Helix.DEFAULT_ENABLE_CASE_INSENSITIVE
    );
    int sortExchangeCopyThreshold = _config.getProperty(
        CommonConstants.Broker.CONFIG_OF_SORT_EXCHANGE_COPY_THRESHOLD,
        CommonConstants.Broker.DEFAULT_SORT_EXCHANGE_COPY_THRESHOLD);
    WorkerManager workerManager = QueryOptionsUtils.isMultiClusterRoutingEnabled(queryOptions, false)
        ? _multiClusterWorkerManager : _workerManager;
    return QueryEnvironment.configBuilder()
        .requestId(requestId)
        .database(database)
        .tableCache(_tableCache)
        .workerManager(workerManager)
        .isCaseSensitive(caseSensitive)
        .isNullHandlingEnabled(QueryOptionsUtils.isNullHandlingEnabled(queryOptions))
        .defaultInferPartitionHint(inferPartitionHint)
        .defaultUseSpools(defaultUseSpool)
        .defaultUseLeafServerForIntermediateStage(defaultUseLeafServerForIntermediateStage)
        .defaultEnableGroupTrim(defaultEnableGroupTrim)
        .defaultEnableDynamicFilteringSemiJoin(defaultEnableDynamicFilteringSemiJoin)
        .defaultUsePhysicalOptimizer(defaultUsePhysicalOptimizer)
        .defaultUseLiteMode(defaultUseLiteMode)
        .defaultRunInBroker(defaultRunInBroker)
        .defaultUseBrokerPruning(defaultUseBrokerPruning)
        .defaultLiteModeLeafStageLimit(defaultLiteModeLeafStageLimit)
        .defaultLiteModeLeafStageFanOutAdjustedLimit(defaultLiteModeFanoutAdjustedLimit)
        .defaultLiteModeEnableJoins(defaultLiteModeEnableJoins)
        .defaultHashFunction(defaultHashFunction)
        .defaultDisabledPlannerRules(_defaultDisabledPlannerRules)
        .defaultSortExchangeCopyLimit(sortExchangeCopyThreshold)
        .build();
  }

  private long getTimeoutMs(Map<String, String> queryOptions) {
    Long timeoutMsFromQueryOption = QueryOptionsUtils.getTimeoutMs(queryOptions);
    return timeoutMsFromQueryOption != null ? timeoutMsFromQueryOption : _brokerTimeoutMs;
  }

  private long getExtraPassiveTimeoutMs(Map<String, String> queryOptions) {
    Long extraPassiveTimeoutMsFromQueryOption = QueryOptionsUtils.getExtraPassiveTimeoutMs(queryOptions);
    return extraPassiveTimeoutMsFromQueryOption != null ? extraPassiveTimeoutMsFromQueryOption : _extraPassiveTimeoutMs;
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
    Map<String, String> extraFields = queryPlanResult.getExtraFields();
    return constructMultistageExplainPlan(query.getTextQuery(), plan, extraFields);
  }

  private StreamingBrokerResponse query(QueryEnvironment.CompiledQuery query, long requestId,
      RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders, Timer timer,
      boolean queryWasLogged)
      throws QueryException, WebApplicationException {
    QueryEnvironment.QueryPlannerResult queryPlanResult = callAsync(requestId, query.getTextQuery(),
        () -> query.planQuery(requestId), timer);

    DispatchableSubPlan dispatchableSubPlan = queryPlanResult.getQueryPlan();

    // Optionally set ignoreMissingSegments query option based on broker config if not already set.
    if (_config.getProperty(CommonConstants.Broker.CONFIG_OF_IGNORE_MISSING_SEGMENTS,
        CommonConstants.Broker.DEFAULT_IGNORE_MISSING_SEGMENTS)) {
      query.getOptions().putIfAbsent(CommonConstants.Broker.Request.QueryOptionKey.IGNORE_MISSING_SEGMENTS, "true");
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
      return StreamingBrokerResponse.error(QueryErrorCode.TOO_MANY_REQUESTS, errorMessage);
    }

    int estimatedNumQueryThreads = dispatchableSubPlan.getEstimatedNumQueryThreads();
    try {
      // It's fine to block in this thread because we use a separate thread pool from the main Jersey server to process
      // these requests.
      if (!_queryThrottler.tryAcquire(estimatedNumQueryThreads, timer.getRemainingTimeMs(),
          TimeUnit.MILLISECONDS)) {
        LOGGER.warn("Timed out waiting to execute request {}: {}", requestId, query);
        requestContext.setErrorCode(QueryErrorCode.EXECUTION_TIMEOUT);
        return StreamingBrokerResponse.error(QueryErrorCode.EXECUTION_TIMEOUT);
      }
      _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ESTIMATED_MSE_SERVER_THREADS,
          _queryThrottler.currentQueryServerThreads());
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupt received while waiting to execute request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryErrorCode.EXECUTION_TIMEOUT);
      return StreamingBrokerResponse.error(QueryErrorCode.EXECUTION_TIMEOUT);
    }

    _stagesStartedMeter.mark(countStages(dispatchableSubPlan));
    _opchainsStartedMeter.mark(countOpChain(dispatchableSubPlan));

    String clientRequestId = extractClientRequestId(query.getSqlNodeAndOptions());
    onQueryStart(requestId, clientRequestId, query.getTextQuery());
    try {

      QueryDispatcher.DispatcherStreamingBrokerResponse dispatcherResponse = _queryDispatcher.submit(
          requestContext, dispatchableSubPlan, timer.getRemainingTimeMs(), query.getOptions());
      return new MseHandlerStreamingBrokerResponse(dispatcherResponse, requestContext, estimatedNumQueryThreads,
          dispatchableSubPlan, query, requesterIdentity, tableNames, queryWasLogged, query.getOptions());
    } catch (QueryException e) {
      onQueryFinish(requestId);
      throw e;
    } catch (Throwable t) {
      onQueryFinish(requestId);

      QueryErrorCode queryErrorCode = t instanceof TimeoutException
          ? QueryErrorCode.EXECUTION_TIMEOUT
          : QueryErrorCode.QUERY_EXECUTION;
      String consolidatedMessage = ExceptionUtils.consolidateExceptionTraces(t);
      LOGGER.error("Caught exception dispatching request {}: {}, {}", requestId, query, consolidatedMessage);
      requestContext.setErrorCode(queryErrorCode);
      return StreamingBrokerResponse.error(queryErrorCode, consolidatedMessage);
    }
  }

  private int countStages(DispatchableSubPlan dispatchableSubPlan) {
    return dispatchableSubPlan.getQueryStageMap().size();
  }

  private int countOpChain(DispatchableSubPlan dispatchableSubPlan) {
    return dispatchableSubPlan.getQueryStageMap().values().stream()
        .mapToInt(stage -> stage.getWorkerMetadataList().size())
        .sum();
  }

  /// A StreamingBrokerResponse that wraps another a [QueryDispatcher.DispatcherStreamingBrokerResponse] and
  /// performs some pre- and post-processing around it.
  ///
  /// The code is probably a bit messy, but it wasn't refactored during the streaming broker refactor to make the
  /// diff easier to read.
  private class MseHandlerStreamingBrokerResponse extends StreamingBrokerResponse.Delegator {
    private final RequestContext _requestContext;
    private final int _estimatedNumQueryThreads;
    private final DispatchableSubPlan _dispatchableSubPlan;
    private final QueryEnvironment.CompiledQuery _query;
    private final RequesterIdentity _requesterIdentity;
    private final Set<String> _tableNames;
    private final boolean _queryWasLogged;
    private final Map<String, String> _queryOpts;

    public MseHandlerStreamingBrokerResponse(QueryDispatcher.DispatcherStreamingBrokerResponse delegate,
        RequestContext requestContext, int estimatedNumQueryThreads, DispatchableSubPlan dispatchableSubPlan,
        QueryEnvironment.CompiledQuery query, RequesterIdentity requesterIdentity, Set<String> tableNames,
        boolean queryWasLogged, Map<String, String> queryOpts) {
      super(delegate);
      _requestContext = requestContext;
      _estimatedNumQueryThreads = estimatedNumQueryThreads;
      _dispatchableSubPlan = dispatchableSubPlan;
      _query = query;
      _requesterIdentity = requesterIdentity;
      _tableNames = tableNames;
      _queryWasLogged = queryWasLogged;
      _queryOpts = queryOpts;
    }

    @Override
    public Metainfo consumeData(DataConsumer consumer)
        throws InterruptedException {

      EagerToLazyBrokerResponseAdaptor.EagerBrokerResponseToMetainfo metainfo;
      try {
        QueryDispatcher.DispatcherStreamingBrokerResponse delegate =
            (QueryDispatcher.DispatcherStreamingBrokerResponse) _delegate;

        // Here is when the reduction phase actually happens
        metainfo = delegate.consumeData(consumer);
      } catch (InterruptedException e) {
        LOGGER.warn("Request {} interrupted while reducing results", _requestContext.getRequestId(), e);
        throw e;
      } catch (QueryException e) {
        return new Metainfo.Error(e.getErrorCode(), ExceptionUtils.consolidateExceptionMessages(e));
      } catch (Exception e) {
        LOGGER.warn("Request {} failed while reducing results", _requestContext.getRequestId(), e);
        return new Metainfo.Error(QueryErrorCode.INTERNAL, ExceptionUtils.consolidateExceptionMessages(e));
      } finally {
        _queryThrottler.release(_estimatedNumQueryThreads);
        _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.ESTIMATED_MSE_SERVER_THREADS,
            _queryThrottler.currentQueryServerThreads());
        onQueryFinish(_requestContext.getRequestId());
      }

      if (!(metainfo.getBrokerResponse() instanceof BrokerResponseNativeV2)) {
        LOGGER.warn("Unexpected broker response type: {}", metainfo.getBrokerResponse().getClass().getName());
      } else {
        BrokerResponseNativeV2 brokerResponse = (BrokerResponseNativeV2) metainfo.getBrokerResponse();
        try {
          postExecution(brokerResponse);
        } catch (Exception e) {
          LOGGER.warn("Request {} failed during post reduction", _requestContext.getRequestId(), e);
          QueryProcessingException error =
              new QueryProcessingException(QueryErrorCode.INTERNAL, "Error during post processing");
          brokerResponse.addException(error);
        }
      }

      return metainfo;
    }

    private void postExecution(BrokerResponseNativeV2 brokerResponse) {
      _stagesFinishedMeter.mark(countStages(_dispatchableSubPlan));
      _opchainsCompletedMeter.mark(countOpChain(_dispatchableSubPlan));

      long executionStartTimeNs = System.nanoTime();
      QueryExecutionContext executionContext = QueryThreadContext.get().getExecutionContext();
      long requestId = executionContext.getRequestId();
      String clientRequestId = executionContext.getCid();

      for (QueryProcessingException processingException : brokerResponse.getExceptions()) {
        brokerResponse.addException(processingException);
        QueryErrorCode errorCode = QueryErrorCode.fromErrorCode(processingException.getErrorCode());
        if (errorCode == QueryErrorCode.EXECUTION_TIMEOUT) {
          for (String table : _tableNames) {
            _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
          }
          LOGGER.warn("Timed out executing request {}: {}", requestId, _query);
        }
        _requestContext.setErrorCode(errorCode);
      }
      if (brokerResponse.getExceptions().isEmpty()) {
        brokerResponse.setResultTable(brokerResponse.getResultTable());
        long executionEndTimeNs = System.nanoTime();
        updatePhaseTimingForTables(_tableNames, BrokerQueryPhase.QUERY_EXECUTION,
            executionEndTimeNs - executionStartTimeNs);
      }

      brokerResponse.setClientRequestId(clientRequestId);
      brokerResponse.setTablesQueried(_tableNames);

      Set<QueryServerInstance> servers = new HashSet<>();
      for (DispatchablePlanFragment planFragment : _dispatchableSubPlan.getQueryStageMap().values()) {
        servers.addAll(planFragment.getServerInstances());
      }
      // MSE cannot finish if a single queried server did not respond, so we can use the same count for
      // both the queried and responded stats. Minus one prevents the broker to be included in the count
      // (it will always be included because of the root of the query plan)
      brokerResponse.setNumServersQueried(servers.size() - 1);
      brokerResponse.setNumServersResponded(servers.size() - 1);

      // Attach unavailable segments (unless configured to ignore missing segments)
      int numUnavailableSegments = 0;
      if (!QueryOptionsUtils.isIgnoreMissingSegments(_query.getOptions())) {
        Set<Map.Entry<String, Set<String>>> entries = _dispatchableSubPlan.getTableToUnavailableSegmentsMap()
            .entrySet();
        for (Map.Entry<String, Set<String>> entry : entries) {
          String tableName = entry.getKey();
          Set<String> unavailableSegments = entry.getValue();
          int unavailableSegmentsInSubPlan = unavailableSegments.size();
          numUnavailableSegments += unavailableSegmentsInSubPlan;
          QueryProcessingException errMsg = new QueryProcessingException(QueryErrorCode.SERVER_SEGMENT_MISSING,
              "Found " + unavailableSegmentsInSubPlan + " unavailable segments for table " + tableName + ": "
                  + toSizeLimitedString(unavailableSegments, NUM_UNAVAILABLE_SEGMENTS_TO_LOG));
          brokerResponse.addException(errMsg);
        }
      }
      _requestContext.setNumUnavailableSegments(numUnavailableSegments);

      // Add warnings for unavailable remote clusters in multi-cluster routing
      if (_multiClusterRoutingContext != null && QueryOptionsUtils.isMultiClusterRoutingEnabled(_queryOpts, false)) {
        for (QueryProcessingException clusterException
            : _multiClusterRoutingContext.getUnavailableClusterExceptions()) {
          brokerResponse.addException(clusterException);
        }
      }

      long totalTimeMs = System.currentTimeMillis() - _requestContext.getRequestArrivalTimeMillis();
      _brokerMetrics.addTimedValue(BrokerTimer.MULTI_STAGE_QUERY_TOTAL_TIME_MS, totalTimeMs, TimeUnit.MILLISECONDS);

      for (String table : _tableNames) {
        _brokerMetrics.addTimedTableValue(
            table, BrokerTimer.MULTI_STAGE_QUERY_TOTAL_TIME_MS, totalTimeMs, TimeUnit.MILLISECONDS);
        if (brokerResponse.isNumGroupsLimitReached()) {
          _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED, 1);
        }
        if (brokerResponse.isGroupsTrimmed()) {
          _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_GROUPS_TRIMMED, 1);
        }
      }

      brokerResponse.setTimeUsedMs(totalTimeMs);
      augmentStatistics(_requestContext, brokerResponse);
      if (QueryOptionsUtils.shouldDropResults(_query.getOptions())) {
        brokerResponse.setResultTable(null);
      }

      // Log query and stats
      _queryLogger.logQueryCompleted(
          new QueryLogger.QueryLogParams(_requestContext, _tableNames.toString(), brokerResponse,
              QueryLogger.QueryLogParams.QueryEngine.MULTI_STAGE, _requesterIdentity, null), _queryWasLogged);
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
   * the error code {@link QueryErrorCode#BROKER_TIMEOUT} and other exceptions are treated as internal errors.
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

  public QueryDispatcher getQueryDispatcher() {
    return _queryDispatcher;
  }
}
