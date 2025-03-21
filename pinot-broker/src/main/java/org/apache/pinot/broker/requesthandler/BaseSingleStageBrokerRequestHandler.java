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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestResponse;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.metrics.BrokerTimer;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.query.parser.utils.ParserUtils;
import org.apache.pinot.segment.local.function.GroovyFunctionEvaluator;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseSingleStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseSingleStageBrokerRequestHandler.class);
  private static final String IN_SUBQUERY = "insubquery";
  private static final String IN_ID_SET = "inidset";
  private static final Expression FALSE = RequestUtils.getLiteralExpression(false);
  private static final Expression TRUE = RequestUtils.getLiteralExpression(true);
  private static final Expression STAR = RequestUtils.getIdentifierExpression("*");
  private static final int MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION = 10;
  private static final Map<String, String> DISTINCT_MV_COL_FUNCTION_OVERRIDE_MAP =
      ImmutableMap.<String, String>builder().put("distinctcount", "distinctcountmv")
          .put("distinctcountbitmap", "distinctcountbitmapmv").put("distinctcounthll", "distinctcounthllmv")
          .put("distinctcountrawhll", "distinctcountrawhllmv").put("distinctsum", "distinctsummv")
          .put("distinctavg", "distinctavgmv").put("count", "countmv").put("min", "minmv").put("max", "maxmv")
          .put("avg", "avgmv").put("sum", "summv").put("minmaxrange", "minmaxrangemv")
          .put("distinctcounthllplus", "distinctcounthllplusmv")
          .put("distinctcountrawhllplus", "distinctcountrawhllplusmv").build();

  protected final QueryOptimizer _queryOptimizer = new QueryOptimizer();
  protected final boolean _disableGroovy;
  protected final boolean _useApproximateFunction;
  protected final int _defaultHllLog2m;
  protected final boolean _enableQueryLimitOverride;
  protected final boolean _enableDistinctCountBitmapOverride;
  protected final int _queryResponseLimit;
  // maps broker-generated query id with the servers that are running the query
  protected final Map<Long, QueryServers> _serversById;
  // if >= 0, then overrides default limit of 10, otherwise setting is ignored
  protected final int _defaultQueryLimit;
  protected final boolean _enableMultistageMigrationMetric;
  protected final boolean _useMSEToFillEmptyResponseSchema;
  protected ExecutorService _multistageCompileExecutor;
  protected BlockingQueue<Pair<String, String>> _multistageCompileQueryQueue;

  public BaseSingleStageBrokerRequestHandler(PinotConfiguration config, String brokerId,
      BrokerRoutingManager routingManager, AccessControlFactory accessControlFactory,
      QueryQuotaManager queryQuotaManager, TableCache tableCache) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    _disableGroovy = _config.getProperty(Broker.DISABLE_GROOVY, Broker.DEFAULT_DISABLE_GROOVY);
    _useApproximateFunction = _config.getProperty(Broker.USE_APPROXIMATE_FUNCTION, false);
    _defaultHllLog2m = _config.getProperty(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY,
        CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
    _enableQueryLimitOverride = _config.getProperty(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, false);
    _enableDistinctCountBitmapOverride =
        _config.getProperty(CommonConstants.Helix.ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY, false);
    _queryResponseLimit =
        config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT, Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
    if (this.isQueryCancellationEnabled()) {
      _serversById = new ConcurrentHashMap<>();
    } else {
      _serversById = null;
    }
    _defaultQueryLimit = config.getProperty(Broker.CONFIG_OF_BROKER_DEFAULT_QUERY_LIMIT,
        Broker.DEFAULT_BROKER_QUERY_LIMIT);
    boolean enableQueryCancellation =
        Boolean.parseBoolean(config.getProperty(Broker.CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION));

    _enableMultistageMigrationMetric = _config.getProperty(Broker.CONFIG_OF_BROKER_ENABLE_MULTISTAGE_MIGRATION_METRIC,
        Broker.DEFAULT_ENABLE_MULTISTAGE_MIGRATION_METRIC);
    if (_enableMultistageMigrationMetric) {
      _multistageCompileExecutor = Executors.newSingleThreadExecutor();
      _multistageCompileQueryQueue = new LinkedBlockingQueue<>(1000);
    }

    _useMSEToFillEmptyResponseSchema = _config.getProperty(Broker.USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA,
        Broker.DEFAULT_USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA);

    LOGGER.info("Initialized {} with broker id: {}, timeout: {}ms, query response limit: {}, "
            + "default query limit {}, query log max length: {}, query log max rate: {}, query cancellation "
            + "enabled: {}", getClass().getSimpleName(), _brokerId, _brokerTimeoutMs, _queryResponseLimit,
        _defaultQueryLimit, _queryLogger.getMaxQueryLengthToLog(), _queryLogger.getLogRateLimit(),
        enableQueryCancellation);
  }

  @Override
  public void start() {
    if (_enableMultistageMigrationMetric) {
      _multistageCompileExecutor.submit(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          Pair<String, String> query;
          try {
            query = _multistageCompileQueryQueue.take();
          } catch (InterruptedException e) {
            // Exit gracefully when the thread is interrupted, presumably when this single thread executor is shutdown.
            // Since this task is all that this single thread is doing, there's no need to preserve the thread's
            // interrupt status flag.
            return;
          }
          String queryString = query.getLeft();
          String database = query.getRight();

          // Check if the query is a v2 supported query
          if (!ParserUtils.canCompileWithMultiStageEngine(queryString, database, _tableCache)) {
            _brokerMetrics.addMeteredGlobalValue(BrokerMeter.SINGLE_STAGE_QUERIES_INVALID_MULTI_STAGE, 1);
          }
        }
      });
    }
  }

  @Override
  public void shutDown() {
    if (_enableMultistageMigrationMetric) {
      _multistageCompileExecutor.shutdownNow();
    }
  }

  @VisibleForTesting
  Set<ServerInstance> getRunningServers(long requestId) {
    Preconditions.checkState(isQueryCancellationEnabled(), "Query cancellation is not enabled on broker");
    QueryServers queryServers = _serversById.get(requestId);
    return queryServers != null ? queryServers._servers : Collections.emptySet();
  }

  @Override
  protected void onQueryFinish(long requestId) {
    super.onQueryFinish(requestId);
    if (isQueryCancellationEnabled()) {
      _serversById.remove(requestId);
    }
  }

  @Override
  protected boolean handleCancel(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    QueryServers queryServers = _serversById.get(queryId);
    if (queryServers == null) {
      return false;
    }

    // TODO: Use different global query id for OFFLINE and REALTIME table after releasing 0.12.0. See QueryIdUtils for
    //       details
    String globalQueryId = getGlobalQueryId(queryId);
    List<Pair<String, String>> serverUrls = new ArrayList<>();
    for (ServerInstance serverInstance : queryServers._servers) {
      // TODO: how should we add the cid here? Maybe as a query param?
      //  we can get the cid from QueryThreadContext
      serverUrls.add(Pair.of(String.format("%s/query/%s", serverInstance.getAdminEndpoint(), globalQueryId), null));
    }
    LOGGER.debug("Cancelling the query: {} via server urls: {}", queryServers._query, serverUrls);
    CompletionService<MultiHttpRequestResponse> completionService =
        new MultiHttpRequest(executor, connMgr).execute(serverUrls, null, timeoutMs, "DELETE", HttpDelete::new);
    List<String> errMsgs = new ArrayList<>(serverUrls.size());
    for (int i = 0; i < serverUrls.size(); i++) {
      MultiHttpRequestResponse httpRequestResponse = null;
      try {
        // Wait for all requests to respond before returning to be sure that the servers have handled the cancel
        // requests. The completion order is different from serverUrls, thus use uri in the response.
        httpRequestResponse = completionService.take().get();
        URI uri = httpRequestResponse.getURI();
        int status = httpRequestResponse.getResponse().getCode();
        // Unexpected server responses are collected and returned as exception.
        if (status != 200 && status != 404) {
          String responseString = EntityUtils.toString(httpRequestResponse.getResponse().getEntity());
          throw new Exception(
              String.format("Unexpected status=%d and response='%s' from uri='%s'", status, responseString, uri));
        }
        if (serverResponses != null) {
          serverResponses.put(uri.getHost() + ":" + uri.getPort(), status);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to cancel query: {}", queryServers._query, e);
        // Can't just throw exception from here as there is a need to release the other connections.
        // So just collect the error msg to throw them together after the for-loop.
        errMsgs.add(e.getMessage());
      } finally {
        if (httpRequestResponse != null) {
          httpRequestResponse.close();
        }
      }
    }
    if (errMsgs.size() > 0) {
      throw new Exception("Unexpected responses from servers: " + StringUtils.join(errMsgs, ","));
    }
    return true;
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    //Start instrumentation context. This must not be moved further below interspersed into the code.
    Tracing.ThreadAccountantOps.setupRunner(String.valueOf(requestId));

    try {
      return doHandleRequest(requestId, query, sqlNodeAndOptions, request, requesterIdentity, requestContext,
          httpHeaders, accessControl);
    } finally {
      Tracing.ThreadAccountantOps.clear();
    }
  }

  /**
   * CompileResult holds the result of the compilation phase. Compilation may or may not be successful. If compilation
   * is successful then all member variables other than BrokerResponse will be available. If compilation is not
   * successful, then only the BrokerResponse is set. This is done to keep the current behaviour as is.
   * It became hard to keep the current behaviour if we were to throw an exception from the compileRequest method.
   * The only exception is that a BrokerResponse is returned for a literal-only query.
   */
  private static class CompileResult {
    final PinotQuery _pinotQuery;
    final PinotQuery _serverPinotQuery;
    final Schema _schema;
    final String _tableName;
    final String _rawTableName;
    final BrokerResponse _errorOrLiteralOnlyBrokerResponse;

    public CompileResult(PinotQuery pinotQuery, PinotQuery serverPinotQuery, Schema schema, String tableName,
        String rawTableName) {
      _pinotQuery = pinotQuery;
      _serverPinotQuery = serverPinotQuery;
      _schema = schema;
      _tableName = tableName;
      _rawTableName = rawTableName;
      _errorOrLiteralOnlyBrokerResponse = null;
    }

    public CompileResult(BrokerResponse errorOrLiteralOnlyBrokerResponse) {
      _pinotQuery = null;
      _serverPinotQuery = null;
      _schema = null;
      _tableName = null;
      _rawTableName = null;
      _errorOrLiteralOnlyBrokerResponse = errorOrLiteralOnlyBrokerResponse;
    }
  }

  protected BrokerResponse doHandleRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl)
      throws Exception {
    // Compile the request into PinotQuery
    long compilationStartTimeNs = System.nanoTime();
    CompileResult compileResult =
          compileRequest(requestId, query, sqlNodeAndOptions, request, requesterIdentity, requestContext, httpHeaders,
              accessControl);

    if (compileResult._errorOrLiteralOnlyBrokerResponse != null) {
      /*
       * If the compileRequest method sets the BrokerResponse field, then it is either an error response or
       * a literal-only query. In either case, we can return the response directly.
       */
      return compileResult._errorOrLiteralOnlyBrokerResponse;
    }

    Schema schema = compileResult._schema;
    String tableName = compileResult._tableName;
    String rawTableName = compileResult._rawTableName;
    PinotQuery pinotQuery = compileResult._pinotQuery;
    PinotQuery serverPinotQuery = compileResult._serverPinotQuery;
    String database = DatabaseUtils.extractDatabaseFromFullyQualifiedTableName(tableName);

    long compilationEndTimeNs = System.nanoTime();
    // full request compile time = compilationTimeNs + parserTimeNs
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REQUEST_COMPILATION,
        (compilationEndTimeNs - compilationStartTimeNs) + sqlNodeAndOptions.getParseTimeNs());

    AuthorizationResult authorizationResult =
        hasTableAccess(requesterIdentity, Set.of(tableName), requestContext, httpHeaders);
    if (!authorizationResult.hasAccess()) {
      throwAccessDeniedError(requestId, query, requestContext, tableName, authorizationResult);
    }

    // Validate QPS
    if (hasExceededQPSQuota(database, Set.of(tableName), requestContext)) {
      String errorMessage = String.format("Request %d: %s exceeds query quota.", requestId, query);
      return new BrokerResponseNative(QueryErrorCode.TOO_MANY_REQUESTS, errorMessage);
    }

    // Validate the request
    try {
      validateRequest(serverPinotQuery, _queryResponseLimit);
    } catch (Exception e) {
      LOGGER.info("Caught exception while validating request {}: {}, {}", requestId, query, e.getMessage());
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      return new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, e.getMessage());
    }

    BrokerRequest brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
    BrokerRequest serverBrokerRequest =
        serverPinotQuery == pinotQuery ? brokerRequest : CalciteSqlCompiler.convertToBrokerRequest(serverPinotQuery);

    // Get the tables hit by the request
    String offlineTableName = null;
    String realtimeTableName = null;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == TableType.OFFLINE) {
      // Offline table
      if (_routingManager.routingExists(tableName)) {
        offlineTableName = tableName;
      }
    } else if (tableType == TableType.REALTIME) {
      // Realtime table
      if (_routingManager.routingExists(tableName)) {
        realtimeTableName = tableName;
      }
    } else {
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      if (_routingManager.routingExists(offlineTableNameToCheck)) {
        offlineTableName = offlineTableNameToCheck;
      }
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      if (_routingManager.routingExists(realtimeTableNameToCheck)) {
        realtimeTableName = realtimeTableNameToCheck;
      }
    }

    TableConfig offlineTableConfig =
        _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    TableConfig realtimeTableConfig =
        _tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));

    if (offlineTableName == null && realtimeTableName == null) {
      // No table matches the request
      if (realtimeTableConfig == null && offlineTableConfig == null) {
        LOGGER.info("Table not found for request {}: {}", requestId, query);
        requestContext.setErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST);
        return BrokerResponseNative.TABLE_DOES_NOT_EXIST;
      }
      LOGGER.info("No table matches for request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryErrorCode.BROKER_RESOURCE_MISSING);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1);
      return BrokerResponseNative.NO_TABLE_RESULT;
    }

    // Handle query rewrite that can be overridden by the table configs
    if (offlineTableName == null) {
      offlineTableConfig = null;
    }
    if (realtimeTableName == null) {
      realtimeTableConfig = null;
    }

    HandlerContext handlerContext = getHandlerContext(offlineTableConfig, realtimeTableConfig);
    validateGroovyScript(serverPinotQuery, handlerContext._disableGroovy);
    if (handlerContext._useApproximateFunction) {
      handleApproximateFunctionOverride(serverPinotQuery);
    }

    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERIES, 1);
    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_GLOBAL, 1);
    _brokerMetrics.addValueToTableGauge(rawTableName, BrokerGauge.REQUEST_SIZE, query.length());

    if (!pinotQuery.isExplain() && _enableMultistageMigrationMetric) {
      // Check if the query is a v2 supported query
      database = DatabaseUtils.extractDatabaseFromQueryRequest(sqlNodeAndOptions.getOptions(), httpHeaders);
      // Attempt to add the query to the compile queue; drop if queue is full
      if (!_multistageCompileQueryQueue.offer(Pair.of(query, database))) {
        LOGGER.trace("Not compiling query `{}` using the multi-stage query engine because the query queue is full",
            query);
      }
    }

    // Prepare OFFLINE and REALTIME requests
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    TimeBoundaryInfo timeBoundaryInfo = null;
    if (offlineTableName != null && realtimeTableName != null) {
      // Time boundary info might be null when there is no segment in the offline table, query real-time side only
      timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
      if (timeBoundaryInfo == null) {
        LOGGER.debug("No time boundary info found for hybrid table: {}", rawTableName);
        offlineTableName = null;
      }
    }
    if (offlineTableName != null && realtimeTableName != null) {
      // Hybrid
      PinotQuery offlinePinotQuery = serverPinotQuery.deepCopy();
      offlinePinotQuery.getDataSource().setTableName(offlineTableName);
      attachTimeBoundary(offlinePinotQuery, timeBoundaryInfo, true);
      handleExpressionOverride(offlinePinotQuery, _tableCache.getExpressionOverrideMap(offlineTableName));
      handleTimestampIndexOverride(offlinePinotQuery, offlineTableConfig);
      _queryOptimizer.optimize(offlinePinotQuery, offlineTableConfig, schema);
      offlineBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(offlinePinotQuery);

      PinotQuery realtimePinotQuery = serverPinotQuery.deepCopy();
      realtimePinotQuery.getDataSource().setTableName(realtimeTableName);
      attachTimeBoundary(realtimePinotQuery, timeBoundaryInfo, false);
      handleExpressionOverride(realtimePinotQuery, _tableCache.getExpressionOverrideMap(realtimeTableName));
      handleTimestampIndexOverride(realtimePinotQuery, realtimeTableConfig);
      _queryOptimizer.optimize(realtimePinotQuery, realtimeTableConfig, schema);
      realtimeBrokerRequest = CalciteSqlCompiler.convertToBrokerRequest(realtimePinotQuery);

      requestContext.setFanoutType(RequestContext.FanoutType.HYBRID);
      requestContext.setOfflineServerTenant(getServerTenant(offlineTableName));
      requestContext.setRealtimeServerTenant(getServerTenant(realtimeTableName));
    } else if (offlineTableName != null) {
      // OFFLINE only
      setTableName(serverBrokerRequest, offlineTableName);
      handleExpressionOverride(serverPinotQuery, _tableCache.getExpressionOverrideMap(offlineTableName));
      handleTimestampIndexOverride(serverPinotQuery, offlineTableConfig);
      _queryOptimizer.optimize(serverPinotQuery, offlineTableConfig, schema);
      offlineBrokerRequest = serverBrokerRequest;

      requestContext.setFanoutType(RequestContext.FanoutType.OFFLINE);
      requestContext.setOfflineServerTenant(getServerTenant(offlineTableName));
    } else {
      // REALTIME only
      setTableName(serverBrokerRequest, realtimeTableName);
      handleExpressionOverride(serverPinotQuery, _tableCache.getExpressionOverrideMap(realtimeTableName));
      handleTimestampIndexOverride(serverPinotQuery, realtimeTableConfig);
      _queryOptimizer.optimize(serverPinotQuery, realtimeTableConfig, schema);
      realtimeBrokerRequest = serverBrokerRequest;

      requestContext.setFanoutType(RequestContext.FanoutType.REALTIME);
      requestContext.setRealtimeServerTenant(getServerTenant(realtimeTableName));
    }

    // Check if response can be sent without server query evaluation.
    if (offlineBrokerRequest != null && isFilterAlwaysFalse(offlineBrokerRequest.getPinotQuery())) {
      // We don't need to evaluate offline request
      offlineBrokerRequest = null;
    }
    if (realtimeBrokerRequest != null && isFilterAlwaysFalse(realtimeBrokerRequest.getPinotQuery())) {
      // We don't need to evaluate realtime request
      realtimeBrokerRequest = null;
    }

    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      return getEmptyBrokerOnlyResponse(pinotQuery, requestContext, tableName, requesterIdentity, schema, query,
          database);
    }

    if (offlineBrokerRequest != null && isFilterAlwaysTrue(offlineBrokerRequest.getPinotQuery())) {
      // Drop offline request filter since it is always true
      offlineBrokerRequest.getPinotQuery().setFilterExpression(null);
    }
    if (realtimeBrokerRequest != null && isFilterAlwaysTrue(realtimeBrokerRequest.getPinotQuery())) {
      // Drop realtime request filter since it is always true
      realtimeBrokerRequest.getPinotQuery().setFilterExpression(null);
    }

    // Calculate routing table for the query
    // TODO: Modify RoutingManager interface to directly take PinotQuery
    long routingStartTimeNs = System.nanoTime();
    Map<ServerInstance, ServerRouteInfo> offlineRoutingTable = null;
    Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable = null;
    List<String> unavailableSegments = new ArrayList<>();
    int numPrunedSegmentsTotal = 0;
    boolean offlineTableDisabled = false;
    boolean realtimeTableDisabled = false;
    List<QueryProcessingException> errorMsgs = new ArrayList<>();
    if (offlineBrokerRequest != null) {
      offlineTableDisabled = _routingManager.isTableDisabled(offlineTableName);
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!offlineTableDisabled) {
        routingTable = _routingManager.getRoutingTable(offlineBrokerRequest, requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineRoutingTable = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      realtimeTableDisabled = _routingManager.isTableDisabled(realtimeTableName);
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = null;
      if (!realtimeTableDisabled) {
        routingTable = _routingManager.getRoutingTable(realtimeBrokerRequest, requestId);
      }
      if (routingTable != null) {
        unavailableSegments.addAll(routingTable.getUnavailableSegments());
        Map<ServerInstance, ServerRouteInfo> serverInstanceToSegmentsMap =
            routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeRoutingTable = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
        numPrunedSegmentsTotal += routingTable.getNumPrunedSegments();
      } else {
        realtimeBrokerRequest = null;
      }
    }

    if (offlineTableDisabled || realtimeTableDisabled) {
      String errorMessage = null;
      if (((realtimeTableConfig != null && offlineTableConfig != null) && (offlineTableDisabled
          && realtimeTableDisabled)) || (offlineTableConfig == null && realtimeTableDisabled) || (
          realtimeTableConfig == null && offlineTableDisabled)) {
        requestContext.setErrorCode(QueryErrorCode.TABLE_IS_DISABLED);
        return BrokerResponseNative.TABLE_IS_DISABLED;
      } else if ((realtimeTableConfig != null && offlineTableConfig != null) && realtimeTableDisabled) {
        errorMessage = "Realtime table is disabled in hybrid table";
      } else if ((realtimeTableConfig != null && offlineTableConfig != null) && offlineTableDisabled) {
        errorMessage = "Offline table is disabled in hybrid table";
      }
      errorMsgs.add(new QueryProcessingException(QueryErrorCode.TABLE_IS_DISABLED, errorMessage));
    }

    int numUnavailableSegments = unavailableSegments.size();
    requestContext.setNumUnavailableSegments(numUnavailableSegments);

    if (numUnavailableSegments > 0) {
      String errorMessage;
      if (numUnavailableSegments > MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION) {
        errorMessage = String.format("%d segments unavailable, sampling %d: %s", numUnavailableSegments,
            MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION,
            unavailableSegments.subList(0, MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION));
      } else {
        errorMessage = String.format("%d segments unavailable: %s", numUnavailableSegments, unavailableSegments);
      }
      String realtimeRoutingPolicy = realtimeBrokerRequest != null ? getRoutingPolicy(realtimeTableConfig) : null;
      String offlineRoutingPolicy = offlineBrokerRequest != null ? getRoutingPolicy(offlineTableConfig) : null;
      errorMessage = addRoutingPolicyInErrMsg(errorMessage, realtimeRoutingPolicy, offlineRoutingPolicy);
      errorMsgs.add(new QueryProcessingException(QueryErrorCode.BROKER_SEGMENT_UNAVAILABLE, errorMessage));
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_UNAVAILABLE_SEGMENTS, 1);
    }

    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      if (!errorMsgs.isEmpty()) {
        QueryProcessingException firstErrorMsg = errorMsgs.get(0);
        String logTail = errorMsgs.size() > 1 ? (errorMsgs.size()) + " errorMsgs found. Logging only the first one"
            : "1 exception found";
        LOGGER.info("No server found for request {}: {}. {} {}", requestId, query, logTail, firstErrorMsg);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
        return BrokerResponseNative.fromBrokerErrors(errorMsgs);
      } else {
        // When all segments have been pruned, we can just return an empty response.
        return getEmptyBrokerOnlyResponse(pinotQuery, requestContext, tableName, requesterIdentity, schema, query,
            database);
      }
    }
    long routingEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_ROUTING,
        routingEndTimeNs - routingStartTimeNs);

    // Set timeout in the requests
    long timeSpentMs = TimeUnit.NANOSECONDS.toMillis(routingEndTimeNs - compilationStartTimeNs);
    // Remaining time in milliseconds for the server query execution
    // NOTE: For hybrid use case, in most cases offline table and real-time table should have the same query timeout
    //       configured, but if necessary, we also allow different timeout for them.
    //       If the timeout is not the same for offline table and real-time table, use the max of offline table
    //       remaining time and realtime table remaining time. Server side will have different remaining time set for
    //       each table type, and broker should wait for both types to return.
    long remainingTimeMs = 0;
    try {
      if (offlineBrokerRequest != null) {
        remainingTimeMs =
            setQueryTimeout(offlineTableName, offlineBrokerRequest.getPinotQuery().getQueryOptions(), timeSpentMs);
      }
      if (realtimeBrokerRequest != null) {
        remainingTimeMs = Math.max(remainingTimeMs,
            setQueryTimeout(realtimeTableName, realtimeBrokerRequest.getPinotQuery().getQueryOptions(), timeSpentMs));
      }
    } catch (TimeoutException e) {
      String errorMessage = e.getMessage();
      LOGGER.info("{} {}: {}", errorMessage, requestId, query);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_TIMEOUT_BEFORE_SCATTERED_EXCEPTIONS, 1);
      errorMsgs.add(new QueryProcessingException(QueryErrorCode.BROKER_TIMEOUT, errorMessage));
      return BrokerResponseNative.fromBrokerErrors(errorMsgs);
    }

    // Set the maximum serialized response size per server, and ask server to directly return final response when only
    // one server is queried
    int numServers = 0;
    if (offlineRoutingTable != null) {
      numServers += offlineRoutingTable.size();
    }
    if (realtimeRoutingTable != null) {
      numServers += realtimeRoutingTable.size();
    }
    if (offlineBrokerRequest != null) {
      Map<String, String> queryOptions = offlineBrokerRequest.getPinotQuery().getQueryOptions();
      setMaxServerResponseSizeBytes(numServers, queryOptions, offlineTableConfig);
      // Set the query option to directly return final result for single server query unless it is explicitly disabled
      if (numServers == 1) {
        // Set the same flag in the original server request to be used in the reduce phase for hybrid table
        if (queryOptions.putIfAbsent(QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true") == null
            && offlineBrokerRequest != serverBrokerRequest) {
          serverBrokerRequest.getPinotQuery().getQueryOptions()
              .put(QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true");
        }
      }
    }
    if (realtimeBrokerRequest != null) {
      Map<String, String> queryOptions = realtimeBrokerRequest.getPinotQuery().getQueryOptions();
      setMaxServerResponseSizeBytes(numServers, queryOptions, realtimeTableConfig);
      // Set the query option to directly return final result for single server query unless it is explicitly disabled
      if (numServers == 1) {
        // Set the same flag in the original server request to be used in the reduce phase for hybrid table
        if (queryOptions.putIfAbsent(QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true") == null
            && realtimeBrokerRequest != serverBrokerRequest) {
          serverBrokerRequest.getPinotQuery().getQueryOptions()
              .put(QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true");
        }
      }
    }

    // Execute the query
    // TODO: Replace ServerStats with ServerRoutingStatsEntry.
    ServerStats serverStats = new ServerStats();
    // TODO: Handle broker specific operations for explain plan queries such as:
    //       - Alias handling
    //       - Compile time function invocation
    //       - Literal only queries
    //       - Any rewrites
    if (pinotQuery.isExplain()) {
      // Update routing tables to only send request to offline servers for OFFLINE and HYBRID tables.
      // TODO: Assess if the Explain Plan Query should also be routed to REALTIME servers for HYBRID tables
      if (offlineRoutingTable != null) {
        // For OFFLINE and HYBRID tables, don't send EXPLAIN query to realtime servers.
        realtimeBrokerRequest = null;
        realtimeRoutingTable = null;
      }
    }
    BrokerResponseNative brokerResponse;
    if (isQueryCancellationEnabled()) {
      // Start to track the running query for cancellation just before sending it out to servers to avoid any
      // potential failures that could happen before sending it out, like failures to calculate the routing table etc.
      // TODO: Even tracking the query as late as here, a potential race condition between calling cancel API and
      //       query being sent out to servers can still happen. If cancel request arrives earlier than query being
      //       sent out to servers, the servers miss the cancel request and continue to run the queries. The users
      //       can always list the running queries and cancel query again until it ends. Just that such race
      //       condition makes cancel API less reliable. This should be rare as it assumes sending queries out to
      //       servers takes time, but will address later if needed.
      String clientRequestId = extractClientRequestId(sqlNodeAndOptions);
      onQueryStart(
          requestId, clientRequestId, query, new QueryServers(query, offlineRoutingTable, realtimeRoutingTable));
      try {
        brokerResponse = processBrokerRequest(requestId, brokerRequest, serverBrokerRequest, offlineBrokerRequest,
            offlineRoutingTable, realtimeBrokerRequest, realtimeRoutingTable, remainingTimeMs, serverStats,
            requestContext);
        brokerResponse.setClientRequestId(clientRequestId);
      } finally {
        onQueryFinish(requestId);
        LOGGER.debug("Remove track of running query: {}", requestId);
      }
    } else {
      brokerResponse = processBrokerRequest(requestId, brokerRequest, serverBrokerRequest, offlineBrokerRequest,
          offlineRoutingTable, realtimeBrokerRequest, realtimeRoutingTable, remainingTimeMs, serverStats,
          requestContext);
    }
    brokerResponse.setTablesQueried(Set.of(rawTableName));

    for (QueryProcessingException errorMsg : errorMsgs) {
      brokerResponse.addException(errorMsg);
    }
    brokerResponse.setNumSegmentsPrunedByBroker(numPrunedSegmentsTotal);
    long executionEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_EXECUTION,
        executionEndTimeNs - routingEndTimeNs);

    // Track number of queries with number of groups limit reached
    if (brokerResponse.isNumGroupsLimitReached()) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED,
          1);
    }

    // server returns STRING as default dataType for all columns in (some) scenarios where no rows are returned
    // this is an attempt to return more faithful information based on other sources
    if (brokerResponse.getNumRowsResultSet() == 0) {
      boolean useMSE = QueryOptionsUtils.isUseMSEToFillEmptySchema(
          pinotQuery.getQueryOptions(), _useMSEToFillEmptyResponseSchema);
      ParserUtils.fillEmptyResponseSchema(useMSE, brokerResponse, _tableCache, schema, database, query);
    }

    // Set total query processing time
    long totalTimeMs = System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis();
    brokerResponse.setTimeUsedMs(totalTimeMs);
    augmentStatistics(requestContext, brokerResponse);
    // include both broker side errorMsgs and server side errorMsgs
    List<QueryProcessingException> brokerExceptions = brokerResponse.getExceptions();
    brokerExceptions.stream()
        .filter(exception -> exception.getErrorCode() == QueryErrorCode.QUERY_VALIDATION.getId())
        .findFirst()
        .ifPresent(exception -> _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1));
    if (!pinotQuery.isExplain() && QueryOptionsUtils.shouldDropResults(pinotQuery.getQueryOptions())) {
      brokerResponse.setResultTable(null);
    }
    if (QueryOptionsUtils.isSecondaryWorkload(pinotQuery.getQueryOptions())) {
      _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.SECONDARY_WORKLOAD_QUERY_TOTAL_TIME_MS, totalTimeMs,
          TimeUnit.MILLISECONDS);
      _brokerMetrics.addTimedValue(BrokerTimer.SECONDARY_WORKLOAD_QUERY_TOTAL_TIME_MS, totalTimeMs,
          TimeUnit.MILLISECONDS);
    } else {
      _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.QUERY_TOTAL_TIME_MS, totalTimeMs,
          TimeUnit.MILLISECONDS);
      _brokerMetrics.addTimedValue(BrokerTimer.QUERY_TOTAL_TIME_MS, totalTimeMs, TimeUnit.MILLISECONDS);
    }

    // Log query and stats
    _queryLogger.log(
        new QueryLogger.QueryLogParams(requestContext, tableName, brokerResponse,
            QueryLogger.QueryLogParams.QueryEngine.SINGLE_STAGE, requesterIdentity, serverStats));

    return brokerResponse;
  }

  private CompileResult compileRequest(long requestId, String query, SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      @Nullable HttpHeaders httpHeaders, AccessControl accessControl) {
    PinotQuery pinotQuery;
    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling SQL request {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
      // Check if the query is a v2 supported query
      String database = DatabaseUtils.extractDatabaseFromQueryRequest(sqlNodeAndOptions.getOptions(), httpHeaders);
      if (ParserUtils.canCompileWithMultiStageEngine(query, database, _tableCache)) {
        return new CompileResult(new BrokerResponseNative(QueryErrorCode.SQL_PARSING,
            "It seems that the query is only supported by the multi-stage query engine, please retry the query "
                    + "using "
                    + "the multi-stage query engine "
                    + "(https://docs.pinot.apache.org/developers/advanced/v2-multi-stage-query-engine)"));
      } else {
        return new CompileResult(
            new BrokerResponseNative(QueryErrorCode.SQL_PARSING, e.getMessage()));
      }
    }

    if (isDefaultQueryResponseLimitEnabled() && !pinotQuery.isSetLimit()) {
      pinotQuery.setLimit(_defaultQueryLimit);
    }

    if (isLiteralOnlyQuery(pinotQuery)) {
      LOGGER.debug("Request {} contains only Literal, skipping server query: {}", requestId, query);
      try {
        if (pinotQuery.isExplain()) {
          // EXPLAIN PLAN results to show that query is evaluated exclusively by Broker.
          return new CompileResult(BrokerResponseNative.BROKER_ONLY_EXPLAIN_PLAN_OUTPUT);
        }
        return new CompileResult(processLiteralOnlyQuery(requestId, pinotQuery, requestContext));
      } catch (Exception e) {
        // TODO: refine the exceptions here to early termination the queries won't requires to send to servers.
        LOGGER.warn("Unable to execute literal request {}: {} at broker, fallback to server query. {}", requestId,
            query, e.getMessage());
      }
    }

    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    DataSource dataSource = serverPinotQuery.getDataSource();
    if (dataSource == null) {
      LOGGER.info("Data source (FROM clause) not found in request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      return new CompileResult(new BrokerResponseNative(
          QueryErrorCode.QUERY_VALIDATION, "Data source (FROM clause) not found"));
    }
    if (dataSource.getJoin() != null) {
      LOGGER.info("JOIN is not supported in request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      return new CompileResult(new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "JOIN is not supported"));
    }
    if (dataSource.getTableName() == null) {
      LOGGER.info("Table name not found in request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      return new CompileResult(new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, "Table name not found"));
    }

    try {
      handleSubquery(serverPinotQuery, requestId, request, requesterIdentity, requestContext, httpHeaders,
          accessControl);
    } catch (Exception e) {
      LOGGER.info("Caught exception while handling the subquery in request {}: {}, {}", requestId, query,
          e.getMessage());
      requestContext.setErrorCode(QueryErrorCode.QUERY_EXECUTION);
      return new CompileResult(
          new BrokerResponseNative(QueryErrorCode.QUERY_EXECUTION, e.getMessage()));
    }

    boolean ignoreCase = _tableCache.isIgnoreCase();
    String tableName;
    try {
      tableName =
          getActualTableName(DatabaseUtils.translateTableName(dataSource.getTableName(), httpHeaders, ignoreCase),
              _tableCache);
    } catch (DatabaseConflictException e) {
      LOGGER.info("{}. Request {}: {}", e.getMessage(), requestId, query);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      requestContext.setErrorCode(QueryErrorCode.QUERY_VALIDATION);
      return new CompileResult(
          new BrokerResponseNative(QueryErrorCode.QUERY_VALIDATION, e.getMessage()));
    }
    dataSource.setTableName(tableName);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    requestContext.setTableName(rawTableName);

    try {
      Map<String, String> columnNameMap = _tableCache.getColumnNameMap(rawTableName);
      if (columnNameMap != null) {
        updateColumnNames(rawTableName, serverPinotQuery, ignoreCase, columnNameMap);
      }
    } catch (Exception e) {
      // Throw exceptions with column in-existence error.
      if (e instanceof BadQueryRequestException) {
        if (tableName != null) {
          // First check if table permissions are in place to not leak schema information.
          AuthorizationResult authorizationResult =
              hasTableAccess(requesterIdentity, Set.of(tableName), requestContext, httpHeaders);
          if (!authorizationResult.hasAccess()) {
            throwAccessDeniedError(requestId, query, requestContext, tableName, authorizationResult);
          }
        }
        LOGGER.info("Caught exception while checking column names in request {}: {}, {}", requestId, query,
            e.getMessage());
        requestContext.setErrorCode(QueryErrorCode.UNKNOWN_COLUMN);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.UNKNOWN_COLUMN_EXCEPTIONS, 1);
        return new CompileResult(new BrokerResponseNative(QueryErrorCode.UNKNOWN_COLUMN, e.getMessage()));
      }
      LOGGER.warn("Caught exception while updating column names in request {}: {}, {}", requestId, query,
          e.getMessage());
    }

    if (_defaultHllLog2m > 0) {
      handleHLLLog2mOverride(serverPinotQuery, _defaultHllLog2m);
    }
    if (_enableQueryLimitOverride) {
      handleQueryLimitOverride(serverPinotQuery, _queryResponseLimit);
    }
    handleSegmentPartitionedDistinctCountOverride(serverPinotQuery,
        getSegmentPartitionedColumns(_tableCache, tableName));
    if (_enableDistinctCountBitmapOverride) {
      handleDistinctCountBitmapOverride(serverPinotQuery);
    }

    Schema schema = _tableCache.getSchema(rawTableName);
    if (schema != null) {
      handleDistinctMultiValuedOverride(serverPinotQuery, schema);
    }

    return new CompileResult(pinotQuery, serverPinotQuery, schema, tableName, rawTableName);
  }

  private void throwAccessDeniedError(long requestId, String query, RequestContext requestContext, String tableName,
      AuthorizationResult authorizationResult) {
    _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
    LOGGER.info("Access denied for request {}: {}, table: {}, reason :{}", requestId, query, tableName,
        authorizationResult.getFailureMessage());

    requestContext.setErrorCode(QueryErrorCode.ACCESS_DENIED);
    String failureMessage = authorizationResult.getFailureMessage();
    if (StringUtils.isNotBlank(failureMessage)) {
      failureMessage = "Reason: " + failureMessage;
    }
    throw new WebApplicationException("Permission denied." + failureMessage, Response.Status.FORBIDDEN);
  }

  private boolean isDefaultQueryResponseLimitEnabled() {
    return _defaultQueryLimit > -1;
  }

  @VisibleForTesting
  static String addRoutingPolicyInErrMsg(String errorMessage, String realtimeRoutingPolicy,
      String offlineRoutingPolicy) {
    if (realtimeRoutingPolicy != null && offlineRoutingPolicy != null) {
      return errorMessage + ", with routing policy: " + realtimeRoutingPolicy + " [realtime], " + offlineRoutingPolicy
          + " [offline]";
    }
    if (realtimeRoutingPolicy != null) {
      return errorMessage + ", with routing policy: " + realtimeRoutingPolicy + " [realtime]";
    }
    if (offlineRoutingPolicy != null) {
      return errorMessage + ", with routing policy: " + offlineRoutingPolicy + " [offline]";
    }
    return errorMessage;
  }

  @Override
  protected void onQueryStart(long requestId, String clientRequestId, String query, Object... extras) {
    super.onQueryStart(requestId, clientRequestId, query, extras);
    QueryThreadContext.setQueryEngine("sse");
    if (isQueryCancellationEnabled() && extras.length > 0 && extras[0] instanceof QueryServers) {
      _serversById.put(requestId, (QueryServers) extras[0]);
    }
  }

  private static String getRoutingPolicy(TableConfig tableConfig) {
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    if (routingConfig == null) {
      return RoutingConfig.DEFAULT_INSTANCE_SELECTOR_TYPE;
    }
    String selectorType = routingConfig.getInstanceSelectorType();
    return selectorType != null ? selectorType : RoutingConfig.DEFAULT_INSTANCE_SELECTOR_TYPE;
  }

  private BrokerResponseNative getEmptyBrokerOnlyResponse(PinotQuery pinotQuery, RequestContext requestContext,
      String tableName, @Nullable RequesterIdentity requesterIdentity, Schema schema, String query, String database) {
    if (pinotQuery.isExplain()) {
      // EXPLAIN PLAN results to show that query is evaluated exclusively by Broker.
      return BrokerResponseNative.BROKER_ONLY_EXPLAIN_PLAN_OUTPUT;
    }

    // Send empty response since we don't need to evaluate either offline or realtime request.
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    boolean useMSE = QueryOptionsUtils.isUseMSEToFillEmptySchema(
        pinotQuery.getQueryOptions(), _useMSEToFillEmptyResponseSchema);
    ParserUtils.fillEmptyResponseSchema(useMSE, brokerResponse, _tableCache, schema, database, query);
    brokerResponse.setTimeUsedMs(System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis());
    _queryLogger.log(
        new QueryLogger.QueryLogParams(requestContext, tableName, brokerResponse,
            QueryLogger.QueryLogParams.QueryEngine.SINGLE_STAGE, requesterIdentity, null));
    return brokerResponse;
  }

  private void handleTimestampIndexOverride(PinotQuery pinotQuery, @Nullable TableConfig tableConfig) {
    if (tableConfig == null || tableConfig.getFieldConfigList() == null) {
      return;
    }

    Set<String> timestampIndexColumns = _tableCache.getTimestampIndexColumns(tableConfig.getTableName());
    if (CollectionUtils.isEmpty(timestampIndexColumns)) {
      return;
    }
    for (Expression expression : pinotQuery.getSelectList()) {
      setTimestampIndexExpressionOverrideHints(expression, timestampIndexColumns, pinotQuery);
    }
    setTimestampIndexExpressionOverrideHints(pinotQuery.getFilterExpression(), timestampIndexColumns, pinotQuery);
    setTimestampIndexExpressionOverrideHints(pinotQuery.getHavingExpression(), timestampIndexColumns, pinotQuery);
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (CollectionUtils.isNotEmpty(groupByList)) {
      groupByList.forEach(
          expression -> setTimestampIndexExpressionOverrideHints(expression, timestampIndexColumns, pinotQuery));
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (CollectionUtils.isNotEmpty(orderByList)) {
      orderByList.forEach(
          expression -> setTimestampIndexExpressionOverrideHints(expression, timestampIndexColumns, pinotQuery));
    }
  }

  private void setTimestampIndexExpressionOverrideHints(@Nullable Expression expression,
      Set<String> timestampIndexColumns, PinotQuery pinotQuery) {
    if (expression == null || expression.getFunctionCall() == null) {
      return;
    }
    Function function = expression.getFunctionCall();
    RequestUtils.applyTimestampIndexOverrideHints(expression, pinotQuery, timestampIndexColumns::contains);
    function.getOperands()
        .forEach(operand -> setTimestampIndexExpressionOverrideHints(operand, timestampIndexColumns, pinotQuery));
  }

  /** Given a {@link PinotQuery}, check if the WHERE clause will always evaluate to false. */
  private boolean isFilterAlwaysFalse(PinotQuery pinotQuery) {
    return FALSE.equals(pinotQuery.getFilterExpression());
  }

  /** Given a {@link PinotQuery}, check if the WHERE clause will always evaluate to true. */
  private boolean isFilterAlwaysTrue(PinotQuery pinotQuery) {
    return TRUE.equals(pinotQuery.getFilterExpression());
  }

  private String getServerTenant(String tableNameWithType) {
    TableConfig tableConfig = _tableCache.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.debug("Table config is not available for table {}", tableNameWithType);
      return "unknownTenant";
    }
    return tableConfig.getTenantConfig().getServer();
  }

  /**
   * Handles the subquery in the given query.
   * <p>Currently only supports subquery within the filter.
   */
  private void handleSubquery(PinotQuery pinotQuery, long requestId, JsonNode jsonRequest,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, @Nullable HttpHeaders httpHeaders,
      AccessControl accessControl)
      throws Exception {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      handleSubquery(filterExpression, requestId, jsonRequest, requesterIdentity, requestContext, httpHeaders,
          accessControl);
    }
  }

  /**
   * Handles the subquery in the given expression.
   * <p>When subquery is detected, first executes the subquery and gets the response, then rewrites the expression with
   * the subquery response.
   * <p>Currently only supports ID_SET subquery within the IN_SUBQUERY transform function, which will be rewritten to an
   * IN_ID_SET transform function.
   */
  private void handleSubquery(Expression expression, long requestId, JsonNode jsonRequest,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, @Nullable HttpHeaders httpHeaders,
      AccessControl accessControl)
      throws Exception {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    List<Expression> operands = function.getOperands();
    if (function.getOperator().equals(IN_SUBQUERY)) {
      Preconditions.checkState(operands.size() == 2, "IN_SUBQUERY requires 2 arguments: expression, subquery");
      Literal subqueryLiteral = operands.get(1).getLiteral();
      Preconditions.checkState(subqueryLiteral != null, "Second argument of IN_SUBQUERY must be a literal (subquery)");
      String subquery = subqueryLiteral.getStringValue();

      SqlNodeAndOptions sqlNodeAndOptions;
      try {
        sqlNodeAndOptions = RequestUtils.parseQuery(subquery, jsonRequest);
      } catch (Exception e) {
        // Do not log or emit metric here because it is pure user error
        requestContext.setErrorCode(QueryErrorCode.SQL_PARSING);
        throw new RuntimeException("Failed to parse subquery: " + subquery, e);
      }

      // Add null handling option from broker config only if there is no override in the query
      if (_enableNullHandling != null) {
        sqlNodeAndOptions.getOptions()
            .putIfAbsent(Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING, _enableNullHandling);
      }

      BrokerResponse response =
          doHandleRequest(requestId, subquery, sqlNodeAndOptions, jsonRequest, requesterIdentity, requestContext,
              httpHeaders, accessControl);
      if (response.getExceptionsSize() != 0) {
        throw new RuntimeException("Caught exception while executing subquery: " + subquery);
      }
      String serializedIdSet = (String) response.getResultTable().getRows().get(0)[0];
      function.setOperator(IN_ID_SET);
      operands.set(1, RequestUtils.getLiteralExpression(serializedIdSet));
    } else {
      for (Expression operand : operands) {
        handleSubquery(operand, requestId, jsonRequest, requesterIdentity, requestContext, httpHeaders, accessControl);
      }
    }
  }

  /**
   * Resolves the actual table name for:
   * - Case-insensitive cluster
   *
   * @param tableName the table name in the query
   * @param tableCache the table case-sensitive cache
   * @return table name if the table name is found in Pinot registry.
   */
  @VisibleForTesting
  static String getActualTableName(String tableName, TableCache tableCache) {
    String actualTableName = tableCache.getActualTableName(tableName);
    if (actualTableName != null) {
      return actualTableName;
    }
    return tableName;
  }

  /**
   * Retrieve segment partitioned columns for a table.
   * For a hybrid table, a segment partitioned column has to be the intersection of both offline and realtime tables.
   *
   * @param tableCache
   * @param tableName
   * @return segment partitioned columns belong to both offline and realtime tables.
   */
  private static Set<String> getSegmentPartitionedColumns(TableCache tableCache, String tableName) {
    final TableConfig offlineTableConfig =
        tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    final TableConfig realtimeTableConfig =
        tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    if (offlineTableConfig == null) {
      return getSegmentPartitionedColumns(realtimeTableConfig);
    }
    if (realtimeTableConfig == null) {
      return getSegmentPartitionedColumns(offlineTableConfig);
    }
    Set<String> segmentPartitionedColumns = getSegmentPartitionedColumns(offlineTableConfig);
    segmentPartitionedColumns.retainAll(getSegmentPartitionedColumns(realtimeTableConfig));
    return segmentPartitionedColumns;
  }

  private static Set<String> getSegmentPartitionedColumns(@Nullable TableConfig tableConfig) {
    Set<String> segmentPartitionedColumns = new HashSet<>();
    if (tableConfig == null) {
      return segmentPartitionedColumns;
    }
    List<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
    if (fieldConfigs != null) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        if (fieldConfig.getProperties() != null && Boolean.parseBoolean(
            fieldConfig.getProperties().get(FieldConfig.IS_SEGMENT_PARTITIONED_COLUMN_KEY))) {
          segmentPartitionedColumns.add(fieldConfig.getName());
        }
      }
    }
    return segmentPartitionedColumns;
  }

  /**
   * Retrieve multivalued columns for a table.
   * From the table Schema , we get the multi valued columns of dimension fields.
   *
   * @param tableSchema
   * @param columnName
   * @return multivalued columns of the table .
   */
  private static boolean isMultiValueColumn(Schema tableSchema, String columnName) {

    DimensionFieldSpec dimensionFieldSpec = tableSchema.getDimensionSpec(columnName);
    return dimensionFieldSpec != null && !dimensionFieldSpec.isSingleValueField();
  }

  /**
   * Sets the table name in the given broker request.
   * NOTE: Set table name in broker request because it is used for access control, query routing etc.
   */
  private void setTableName(BrokerRequest brokerRequest, String tableName) {
    brokerRequest.getQuerySource().setTableName(tableName);
    brokerRequest.getPinotQuery().getDataSource().setTableName(tableName);
  }

  /**
   * Sets HyperLogLog log2m for DistinctCountHLL functions if not explicitly set for the given query.
   */
  private static void handleHLLLog2mOverride(PinotQuery pinotQuery, int hllLog2mOverride) {
    List<Expression> selectList = pinotQuery.getSelectList();
    for (Expression expression : selectList) {
      handleHLLLog2mOverride(expression, hllLog2mOverride);
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        handleHLLLog2mOverride(expression.getFunctionCall().getOperands().get(0), hllLog2mOverride);
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      handleHLLLog2mOverride(havingExpression, hllLog2mOverride);
    }
  }

  /**
   * Sets HyperLogLog log2m for DistinctCountHLL functions if not explicitly set for the given expression.
   */
  private static void handleHLLLog2mOverride(Expression expression, int hllLog2mOverride) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    switch (function.getOperator()) {
      case "distinctcounthll":
      case "distinctcounthllmv":
      case "distinctcountrawhll":
      case "distinctcountrawhllmv":
        if (function.getOperandsSize() == 1) {
          function.addToOperands(RequestUtils.getLiteralExpression(hllLog2mOverride));
        }
        return;
      default:
        break;
    }
    for (Expression operand : function.getOperands()) {
      handleHLLLog2mOverride(operand, hllLog2mOverride);
    }
  }

  /**
   * Overrides the LIMIT of the given query if it exceeds the query limit.
   */
  @VisibleForTesting
  static void handleQueryLimitOverride(PinotQuery pinotQuery, int queryLimit) {
    if (queryLimit > 0 && pinotQuery.getLimit() > queryLimit) {
      pinotQuery.setLimit(queryLimit);
    }
  }

  /**
   * Rewrites 'DistinctCount' to 'SegmentPartitionDistinctCount' for the given query.
   */
  @VisibleForTesting
  static void handleSegmentPartitionedDistinctCountOverride(PinotQuery pinotQuery,
      Set<String> segmentPartitionedColumns) {
    if (segmentPartitionedColumns.isEmpty()) {
      return;
    }
    for (Expression expression : pinotQuery.getSelectList()) {
      handleSegmentPartitionedDistinctCountOverride(expression, segmentPartitionedColumns);
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (orderByExpressions != null) {
      for (Expression expression : orderByExpressions) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        handleSegmentPartitionedDistinctCountOverride(expression.getFunctionCall().getOperands().get(0),
            segmentPartitionedColumns);
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      handleSegmentPartitionedDistinctCountOverride(havingExpression, segmentPartitionedColumns);
    }
  }

  /**
   * Rewrites 'DistinctCount' to 'SegmentPartitionDistinctCount' for the given expression.
   */
  private static void handleSegmentPartitionedDistinctCountOverride(Expression expression,
      Set<String> segmentPartitionedColumns) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    if (function.getOperator().equals("distinctcount")) {
      List<Expression> operands = function.getOperands();
      if (operands.size() == 1 && operands.get(0).isSetIdentifier() && segmentPartitionedColumns.contains(
          operands.get(0).getIdentifier().getName())) {
        function.setOperator("segmentpartitioneddistinctcount");
      }
    } else {
      for (Expression operand : function.getOperands()) {
        handleSegmentPartitionedDistinctCountOverride(operand, segmentPartitionedColumns);
      }
    }
  }

  /**
   * Rewrites 'DistinctCount' to 'DistinctCountBitmap' for the given query.
   */
  private static void handleDistinctCountBitmapOverride(PinotQuery pinotQuery) {
    for (Expression expression : pinotQuery.getSelectList()) {
      handleDistinctCountBitmapOverride(expression);
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (orderByExpressions != null) {
      for (Expression expression : orderByExpressions) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        handleDistinctCountBitmapOverride(expression.getFunctionCall().getOperands().get(0));
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      handleDistinctCountBitmapOverride(havingExpression);
    }
  }

  /**
   * Rewrites selected 'Distinct' prefixed function to 'Distinct----MV' function for the field of multivalued type.
   */
  @VisibleForTesting
  static void handleDistinctMultiValuedOverride(PinotQuery pinotQuery, Schema tableSchema) {
    for (Expression expression : pinotQuery.getSelectList()) {
      handleDistinctMultiValuedOverride(expression, tableSchema);
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (orderByExpressions != null) {
      for (Expression expression : orderByExpressions) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        handleDistinctMultiValuedOverride(expression.getFunctionCall().getOperands().get(0), tableSchema);
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      handleDistinctMultiValuedOverride(havingExpression, tableSchema);
    }
  }

  /**
   * Rewrites selected 'Distinct' prefixed function to 'Distinct----MV' function for the field of multivalued type.
   */
  private static void handleDistinctMultiValuedOverride(Expression expression, Schema tableSchema) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }

    String overrideOperator = DISTINCT_MV_COL_FUNCTION_OVERRIDE_MAP.get(function.getOperator());
    if (overrideOperator != null) {
      List<Expression> operands = function.getOperands();
      if (operands.size() >= 1 && operands.get(0).isSetIdentifier() && isMultiValueColumn(tableSchema,
          operands.get(0).getIdentifier().getName())) {
        // we are only checking the first operand that if its a MV column as all the overriding agg. fn.'s have
        // first operator is column name
        function.setOperator(overrideOperator);
      }
    } else {
      for (Expression operand : function.getOperands()) {
        handleDistinctMultiValuedOverride(operand, tableSchema);
      }
    }
  }

  /**
   * Rewrites 'DistinctCount' to 'DistinctCountBitmap' for the given expression.
   */
  private static void handleDistinctCountBitmapOverride(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    if (function.getOperator().equals("distinctcount")) {
      function.setOperator("distinctcountbitmap");
    } else {
      for (Expression operand : function.getOperands()) {
        handleDistinctCountBitmapOverride(operand);
      }
    }
  }

  private HandlerContext getHandlerContext(@Nullable TableConfig offlineTableConfig,
      @Nullable TableConfig realtimeTableConfig) {
    Boolean disableGroovyOverride = null;
    Boolean useApproximateFunctionOverride = null;
    if (offlineTableConfig != null && offlineTableConfig.getQueryConfig() != null) {
      QueryConfig offlineTableQueryConfig = offlineTableConfig.getQueryConfig();
      Boolean disableGroovyOfflineTableOverride = offlineTableQueryConfig.getDisableGroovy();
      if (disableGroovyOfflineTableOverride != null) {
        disableGroovyOverride = disableGroovyOfflineTableOverride;
      }
      Boolean useApproximateFunctionOfflineTableOverride = offlineTableQueryConfig.getUseApproximateFunction();
      if (useApproximateFunctionOfflineTableOverride != null) {
        useApproximateFunctionOverride = useApproximateFunctionOfflineTableOverride;
      }
    }
    if (realtimeTableConfig != null && realtimeTableConfig.getQueryConfig() != null) {
      QueryConfig realtimeTableQueryConfig = realtimeTableConfig.getQueryConfig();
      Boolean disableGroovyRealtimeTableOverride = realtimeTableQueryConfig.getDisableGroovy();
      if (disableGroovyRealtimeTableOverride != null) {
        if (disableGroovyOverride == null) {
          disableGroovyOverride = disableGroovyRealtimeTableOverride;
        } else {
          // Disable Groovy if either offline or realtime table config disables Groovy
          disableGroovyOverride |= disableGroovyRealtimeTableOverride;
        }
      }
      Boolean useApproximateFunctionRealtimeTableOverride = realtimeTableQueryConfig.getUseApproximateFunction();
      if (useApproximateFunctionRealtimeTableOverride != null) {
        if (useApproximateFunctionOverride == null) {
          useApproximateFunctionOverride = useApproximateFunctionRealtimeTableOverride;
        } else {
          // Use approximate function if both offline and realtime table config uses approximate function
          useApproximateFunctionOverride &= useApproximateFunctionRealtimeTableOverride;
        }
      }
    }

    boolean disableGroovy = disableGroovyOverride != null ? disableGroovyOverride : _disableGroovy;
    boolean useApproximateFunction =
        useApproximateFunctionOverride != null ? useApproximateFunctionOverride : _useApproximateFunction;
    return new HandlerContext(disableGroovy, useApproximateFunction);
  }

  private static class HandlerContext {
    final boolean _disableGroovy;
    final boolean _useApproximateFunction;

    HandlerContext(boolean disableGroovy, boolean useApproximateFunction) {
      _disableGroovy = disableGroovy;
      _useApproximateFunction = useApproximateFunction;
    }
  }

  /**
   * Verifies that no groovy is present in the PinotQuery when disabled.
   */
  @VisibleForTesting
  static void validateGroovyScript(PinotQuery pinotQuery, boolean disableGroovy) {
    List<Expression> selectList = pinotQuery.getSelectList();
    for (Expression expression : selectList) {
      validateGroovyScript(expression, disableGroovy);
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        validateGroovyScript(expression.getFunctionCall().getOperands().get(0), disableGroovy);
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      validateGroovyScript(havingExpression, disableGroovy);
    }
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      validateGroovyScript(filterExpression, disableGroovy);
    }
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null) {
      for (Expression expression : groupByList) {
        validateGroovyScript(expression, disableGroovy);
      }
    }
  }

  private static void validateGroovyScript(Expression expression, boolean disableGroovy) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    if (function.getOperator().equals("groovy")) {
      if (disableGroovy) {
        throw new BadQueryRequestException("Groovy transform functions are disabled for queries");
      } else {
        groovySecureAnalysis(function);
      }
    }
    for (Expression operandExpression : function.getOperands()) {
      validateGroovyScript(operandExpression, disableGroovy);
    }
  }

  private static void groovySecureAnalysis(Function function) {
    List<Expression> operands = function.getOperands();
    if (operands == null || operands.size() < 2) {
      throw new BadQueryRequestException("Groovy transform function must have at least 2 argument");
    }
    // second argument in the groovy function is groovy script
    String script = operands.get(1).getLiteral().getStringValue();
    GroovyFunctionEvaluator.parseGroovyScript(String.format("groovy({%s})", script));
  }

  /**
   * Rewrites potential expensive functions to their approximation counterparts.
   * - DISTINCT_COUNT -> DISTINCT_COUNT_SMART_HLL
   * - PERCENTILE -> PERCENTILE_SMART_TDIGEST
   */
  @VisibleForTesting
  static void handleApproximateFunctionOverride(PinotQuery pinotQuery) {
    for (Expression expression : pinotQuery.getSelectList()) {
      handleApproximateFunctionOverride(expression);
    }
    List<Expression> orderByExpressions = pinotQuery.getOrderByList();
    if (orderByExpressions != null) {
      for (Expression expression : orderByExpressions) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        handleApproximateFunctionOverride(expression.getFunctionCall().getOperands().get(0));
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      handleApproximateFunctionOverride(havingExpression);
    }
  }

  private static void handleApproximateFunctionOverride(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    String functionName = function.getOperator();
    if (functionName.equals("distinctcount") || functionName.equals("distinctcountmv")) {
      function.setOperator("distinctcountsmarthll");
    } else if (functionName.startsWith("percentile")) {
      String remainingFunctionName = functionName.substring(10);
      if (remainingFunctionName.isEmpty() || remainingFunctionName.equals("mv")) {
        function.setOperator("percentilesmarttdigest");
      } else if (remainingFunctionName.matches("\\d+")) {
        try {
          int percentile = Integer.parseInt(remainingFunctionName);
          function.setOperator("percentilesmarttdigest");
          function.addToOperands(RequestUtils.getLiteralExpression(percentile));
        } catch (Exception e) {
          throw new BadQueryRequestException("Illegal function name: " + functionName);
        }
      } else if (remainingFunctionName.matches("\\d+mv")) {
        try {
          int percentile = Integer.parseInt(remainingFunctionName.substring(0, remainingFunctionName.length() - 2));
          function.setOperator("percentilesmarttdigest");
          function.addToOperands(RequestUtils.getLiteralExpression(percentile));
        } catch (Exception e) {
          throw new BadQueryRequestException("Illegal function name: " + functionName);
        }
      }
    } else {
      for (Expression operand : function.getOperands()) {
        handleApproximateFunctionOverride(operand);
      }
    }
  }

  private static void handleExpressionOverride(PinotQuery pinotQuery,
      @Nullable Map<Expression, Expression> expressionOverrideMap) {
    if (expressionOverrideMap == null) {
      return;
    }
    pinotQuery.getSelectList().replaceAll(o -> handleExpressionOverride(o, expressionOverrideMap));
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      pinotQuery.setFilterExpression(handleExpressionOverride(filterExpression, expressionOverrideMap));
    }
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null) {
      groupByList.replaceAll(o -> handleExpressionOverride(o, expressionOverrideMap));
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        expression.getFunctionCall().getOperands().replaceAll(o -> handleExpressionOverride(o, expressionOverrideMap));
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      pinotQuery.setHavingExpression(handleExpressionOverride(havingExpression, expressionOverrideMap));
    }
  }

  private static Expression handleExpressionOverride(Expression expression,
      Map<Expression, Expression> expressionOverrideMap) {
    Expression override = expressionOverrideMap.get(expression);
    if (override != null) {
      return new Expression(override);
    }
    Function function = expression.getFunctionCall();
    if (function != null) {
      function.getOperands().replaceAll(o -> handleExpressionOverride(o, expressionOverrideMap));
    }
    return expression;
  }

  /**
   * Returns {@code true} if the given query only contains literals, {@code false} otherwise.
   */
  @VisibleForTesting
  static boolean isLiteralOnlyQuery(PinotQuery pinotQuery) {
    for (Expression expression : pinotQuery.getSelectList()) {
      if (!CalciteSqlParser.isLiteralOnlyExpression(expression)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Processes the literal only query.
   */
  private BrokerResponseNative processLiteralOnlyQuery(long requestId, PinotQuery pinotQuery,
      RequestContext requestContext) {
    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    List<Expression> selectList = pinotQuery.getSelectList();
    int numColumns = selectList.size();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnTypes = new ColumnDataType[numColumns];
    Object[] values = new Object[numColumns];
    for (int i = 0; i < numColumns; i++) {
      computeResultsForExpression(selectList.get(i), columnNames, columnTypes, values, i);
      values[i] = columnTypes[i].format(values[i]);
    }
    DataSchema dataSchema = new DataSchema(columnNames, columnTypes);
    List<Object[]> rows = new ArrayList<>(1);
    rows.add(values);
    ResultTable resultTable = new ResultTable(dataSchema, rows);
    brokerResponse.setResultTable(resultTable);
    brokerResponse.setTimeUsedMs(System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis());
    augmentStatistics(requestContext, brokerResponse);
    if (!pinotQuery.isExplain() && QueryOptionsUtils.shouldDropResults(pinotQuery.getQueryOptions())) {
      brokerResponse.setResultTable(null);
    }
    return brokerResponse;
  }

  // TODO(xiangfu): Move Literal function computation here from Calcite Parser.
  private void computeResultsForExpression(Expression expression, String[] columnNames, ColumnDataType[] columnTypes,
      Object[] values, int index) {
    ExpressionType type = expression.getType();
    if (type == ExpressionType.LITERAL) {
      computeResultsForLiteral(expression.getLiteral(), columnNames, columnTypes, values, index);
    } else if (type == ExpressionType.FUNCTION) {
      Function function = expression.getFunctionCall();
      String operator = function.getOperator();
      if (operator.equals("as")) {
        List<Expression> operands = function.getOperands();
        computeResultsForExpression(operands.get(0), columnNames, columnTypes, values, index);
        columnNames[index] = operands.get(1).getIdentifier().getName();
      } else {
        throw new IllegalStateException("No able to compute results for function - " + operator);
      }
    }
  }

  private void computeResultsForLiteral(Literal literal, String[] columnNames, ColumnDataType[] columnTypes,
      Object[] values, int index) {
    columnNames[index] = RequestUtils.prettyPrint(literal);
    Pair<ColumnDataType, Object> typeAndValue = RequestUtils.getLiteralTypeAndValue(literal);
    columnTypes[index] = typeAndValue.getLeft();
    values[index] = typeAndValue.getRight();
  }

  /**
   * Fixes the column names to the actual column names in the given query.
   */
  @VisibleForTesting
  static void updateColumnNames(String rawTableName, PinotQuery pinotQuery, boolean isCaseInsensitive,
      Map<String, String> columnNameMap) {
    if (pinotQuery != null) {
      boolean hasStar = false;
      for (Expression expression : pinotQuery.getSelectList()) {
        fixColumnName(rawTableName, expression, columnNameMap, isCaseInsensitive);
        //check if the select expression is '*'
        if (!hasStar && expression.equals(STAR)) {
          hasStar = true;
        }
      }
      //if query has a '*' selection along with other columns
      if (hasStar) {
        expandStarExpressionsToActualColumns(pinotQuery, columnNameMap);
      }
      Expression filterExpression = pinotQuery.getFilterExpression();
      if (filterExpression != null) {
        // We don't support alias in filter expression, so we don't need to pass aliasMap
        fixColumnName(rawTableName, filterExpression, columnNameMap, isCaseInsensitive);
      }
      List<Expression> groupByList = pinotQuery.getGroupByList();
      if (groupByList != null) {
        for (Expression expression : groupByList) {
          fixColumnName(rawTableName, expression, columnNameMap, isCaseInsensitive);
        }
      }
      List<Expression> orderByList = pinotQuery.getOrderByList();
      if (orderByList != null) {
        for (Expression expression : orderByList) {
          // NOTE: Order-by is always a Function with the ordering of the Expression
          fixColumnName(rawTableName, expression.getFunctionCall().getOperands().get(0), columnNameMap,
              isCaseInsensitive);
        }
      }
      Expression havingExpression = pinotQuery.getHavingExpression();
      if (havingExpression != null) {
        fixColumnName(rawTableName, havingExpression, columnNameMap, isCaseInsensitive);
      }
    }
  }

  private static void expandStarExpressionsToActualColumns(PinotQuery pinotQuery, Map<String, String> columnNameMap) {
    List<Expression> originalSelections = pinotQuery.getSelectList();
    //expand '*'
    List<Expression> expandedSelections = new ArrayList<>();
    for (String tableCol : columnNameMap.values()) {
      Expression newSelection = RequestUtils.getIdentifierExpression(tableCol);
      //we exclude default virtual columns
      if (tableCol.charAt(0) != '$') {
        expandedSelections.add(newSelection);
      }
    }
    //sort naturally
    expandedSelections.sort(null);
    List<Expression> newSelections = new ArrayList<>();
    for (Expression originalSelection : originalSelections) {
      if (originalSelection.equals(STAR)) {
        newSelections.addAll(expandedSelections);
      } else {
        newSelections.add(originalSelection);
      }
    }
    pinotQuery.setSelectList(newSelections);
  }

  /**
   * Fixes the column names to the actual column names in the given expression.
   */
  private static void fixColumnName(String rawTableName, Expression expression, Map<String, String> columnNameMap,
      boolean ignoreCase) {
    ExpressionType expressionType = expression.getType();
    if (expressionType == ExpressionType.IDENTIFIER) {
      Identifier identifier = expression.getIdentifier();
      identifier.setName(getActualColumnName(rawTableName, identifier.getName(), columnNameMap, ignoreCase));
    } else if (expressionType == ExpressionType.FUNCTION) {
      final Function functionCall = expression.getFunctionCall();
      switch (functionCall.getOperator()) {
        case "as":
          fixColumnName(rawTableName, functionCall.getOperands().get(0), columnNameMap, ignoreCase);
          break;
        case "lookup":
          // LOOKUP function looks up another table's schema, skip the check for now.
          break;
        default:
          for (Expression operand : functionCall.getOperands()) {
            fixColumnName(rawTableName, operand, columnNameMap, ignoreCase);
          }
          break;
      }
    }
  }

  /**
   * Returns the actual column name for the given column name for:
   * - Case-insensitive cluster
   * - Column name in the format of [{@code rawTableName}].[column_name]
   * - Column name in the format of [logical_table_name].[column_name] while {@code rawTableName} is a translated name
   */
  @VisibleForTesting
  static String getActualColumnName(String rawTableName, String columnName, @Nullable Map<String, String> columnNameMap,
      boolean ignoreCase) {
    if ("*".equals(columnName)) {
      return columnName;
    }
    String columnNameToCheck = trimTableName(rawTableName, columnName, ignoreCase);
    if (ignoreCase) {
      columnNameToCheck = columnNameToCheck.toLowerCase();
    }
    if (columnNameMap != null) {
      String actualColumnName = columnNameMap.get(columnNameToCheck);
      if (actualColumnName != null) {
        return actualColumnName;
      }
    }
    if (columnName.charAt(0) == '$') {
      return columnName;
    }
    throw new BadQueryRequestException("Unknown columnName '" + columnName + "' found in the query");
  }

  private static String trimTableName(String rawTableName, String columnName, boolean ignoreCase) {
    int columnNameLength = columnName.length();
    int rawTableNameLength = rawTableName.length();
    if (columnNameLength > rawTableNameLength && columnName.charAt(rawTableNameLength) == '.'
        && columnName.regionMatches(ignoreCase, 0, rawTableName, 0, rawTableNameLength)) {
      return columnName.substring(rawTableNameLength + 1);
    }
    // Check if raw table name is translated name ([database_name].[logical_table_name]])
    String[] split = StringUtils.split(rawTableName, '.');
    if (split.length == 2) {
      String logicalTableName = split[1];
      int logicalTableNameLength = logicalTableName.length();
      if (columnNameLength > logicalTableNameLength && columnName.charAt(logicalTableNameLength) == '.'
          && columnName.regionMatches(ignoreCase, 0, logicalTableName, 0, logicalTableNameLength)) {
        return columnName.substring(logicalTableNameLength + 1);
      }
    }
    return columnName;
  }

  /**
   * Helper function to decide whether to force the log
   *
   * TODO: come up with other criteria for forcing a log and come up with better numbers
   */
  private boolean forceLog(BrokerResponse brokerResponse, long totalTimeMs) {
    if (brokerResponse.isNumGroupsLimitReached()) {
      return true;
    }

    if (brokerResponse.getExceptionsSize() > 0) {
      return true;
    }

    // If response time is more than 1 sec, force the log
    return totalTimeMs > 1000L;
  }

  /**
   * Sets the query timeout (remaining time in milliseconds) into the query options, and returns the remaining time in
   * milliseconds.
   * <p>For the overall query timeout, use query-level timeout (in the query options) if exists, or use table-level
   * timeout (in the table config) if exists, or use instance-level timeout (in the broker config).
   */
  private long setQueryTimeout(String tableNameWithType, Map<String, String> queryOptions, long timeSpentMs)
      throws TimeoutException {
    long queryTimeoutMs;
    Long queryLevelTimeoutMs = QueryOptionsUtils.getTimeoutMs(queryOptions);
    if (queryLevelTimeoutMs != null) {
      // Use query-level timeout if exists
      queryTimeoutMs = queryLevelTimeoutMs;
    } else {
      Long tableLevelTimeoutMs = _routingManager.getQueryTimeoutMs(tableNameWithType);
      if (tableLevelTimeoutMs != null) {
        // Use table-level timeout if exists
        queryTimeoutMs = tableLevelTimeoutMs;
      } else {
        // Use instance-level timeout
        queryTimeoutMs = _brokerTimeoutMs;
      }
    }

    long remainingTimeMs = queryTimeoutMs - timeSpentMs;
    if (remainingTimeMs <= 0) {
      String errorMessage =
          String.format("Query timed out (time spent: %dms, timeout: %dms) for table: %s before scattering the request",
              timeSpentMs, queryTimeoutMs, tableNameWithType);
      throw new TimeoutException(errorMessage);
    }
    queryOptions.put(QueryOptionKey.TIMEOUT_MS, Long.toString(remainingTimeMs));
    return remainingTimeMs;
  }

  /**
   * Sets a query option indicating the maximum response size that can be sent from a server to the broker. This size
   * is measured for the serialized response.
   *
   * The overriding order of priority is:
   * 1. QueryOption  -> maxServerResponseSizeBytes
   * 2. QueryOption  -> maxQueryResponseSizeBytes
   * 3. TableConfig  -> maxServerResponseSizeBytes
   * 4. TableConfig  -> maxQueryResponseSizeBytes
   * 5. BrokerConfig -> maxServerResponseSizeBytes
   * 6. BrokerConfig -> maxServerResponseSizeBytes
   */
  private void setMaxServerResponseSizeBytes(int numServers, Map<String, String> queryOptions,
      @Nullable TableConfig tableConfig) {
    // QueryOption
    if (QueryOptionsUtils.getMaxServerResponseSizeBytes(queryOptions) != null) {
      return;
    }
    Long maxQueryResponseSizeQueryOption = QueryOptionsUtils.getMaxQueryResponseSizeBytes(queryOptions);
    if (maxQueryResponseSizeQueryOption != null) {
      queryOptions.put(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES,
          Long.toString(maxQueryResponseSizeQueryOption / numServers));
      return;
    }

    // TableConfig
    if (tableConfig != null && tableConfig.getQueryConfig() != null) {
      QueryConfig queryConfig = tableConfig.getQueryConfig();
      if (queryConfig.getMaxServerResponseSizeBytes() != null) {
        queryOptions.put(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES,
            Long.toString(queryConfig.getMaxServerResponseSizeBytes()));
        return;
      }
      if (queryConfig.getMaxQueryResponseSizeBytes() != null) {
        queryOptions.put(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES,
            Long.toString(queryConfig.getMaxQueryResponseSizeBytes() / numServers));
        return;
      }
    }

    // BrokerConfig
    String maxServerResponseSizeBrokerConfig = _config.getProperty(Broker.CONFIG_OF_MAX_SERVER_RESPONSE_SIZE_BYTES);
    if (maxServerResponseSizeBrokerConfig != null) {
      queryOptions.put(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES,
          Long.toString(DataSizeUtils.toBytes(maxServerResponseSizeBrokerConfig)));
      return;
    }

    String maxQueryResponseSizeBrokerConfig = _config.getProperty(Broker.CONFIG_OF_MAX_QUERY_RESPONSE_SIZE_BYTES);
    if (maxQueryResponseSizeBrokerConfig != null) {
      queryOptions.put(QueryOptionKey.MAX_SERVER_RESPONSE_SIZE_BYTES,
          Long.toString(DataSizeUtils.toBytes(maxQueryResponseSizeBrokerConfig) / numServers));
    }
  }

  /**
   * Broker side validation on the query.
   * <p>Throw exception if query does not pass validation.
   * <p>Current validations are:
   * <ul>
   *   <li>Value for 'LIMIT' <= configured value</li>
   *   <li>Query options must be set to SQL mode</li>
   *   <li>Check if numReplicaGroupsToQuery option provided is valid</li>
   * </ul>
   */
  @VisibleForTesting
  static void validateRequest(PinotQuery pinotQuery, int queryResponseLimit) {
    // Verify LIMIT
    int limit = pinotQuery.getLimit();
    if (limit > queryResponseLimit) {
      throw new IllegalStateException(
          "Value for 'LIMIT' (" + limit + ") exceeds maximum allowed value of " + queryResponseLimit);
    }
    QueryOptionsUtils.getNumReplicaGroupsToQuery(pinotQuery.getQueryOptions());
    if (pinotQuery.getDataSource().getSubquery() != null) {
      validateRequest(pinotQuery.getDataSource().getSubquery(), queryResponseLimit);
    }
  }

  /**
   * Helper method to attach the time boundary to the given PinotQuery.
   */
  private static void attachTimeBoundary(PinotQuery pinotQuery, TimeBoundaryInfo timeBoundaryInfo,
      boolean isOfflineRequest) {
    String functionName = isOfflineRequest ? FilterKind.LESS_THAN_OR_EQUAL.name() : FilterKind.GREATER_THAN.name();
    String timeColumn = timeBoundaryInfo.getTimeColumn();
    String timeValue = timeBoundaryInfo.getTimeValue();
    Expression timeFilterExpression =
        RequestUtils.getFunctionExpression(functionName, RequestUtils.getIdentifierExpression(timeColumn),
            RequestUtils.getLiteralExpression(timeValue));

    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      pinotQuery.setFilterExpression(
          RequestUtils.getFunctionExpression(FilterKind.AND.name(), filterExpression, timeFilterExpression));
    } else {
      pinotQuery.setFilterExpression(timeFilterExpression);
    }
  }

  /**
   * Processes the optimized broker requests for both OFFLINE and REALTIME table.
   * TODO: Directly take PinotQuery
   */
  protected abstract BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable, long timeoutMs,
      ServerStats serverStats, RequestContext requestContext)
      throws Exception;

  private String getGlobalQueryId(long requestId) {
    return _brokerId + "_" + requestId;
  }

  /**
   * Helper class to pass the per server statistics.
   */
  public static class ServerStats {
    private String _serverStats;

    public String getServerStats() {
      return _serverStats;
    }

    public void setServerStats(String serverStats) {
      _serverStats = serverStats;
    }
  }

  /**
   * Helper class to track the query plaintext and the requested servers.
   */
  private static class QueryServers {
    final String _query;
    final Set<ServerInstance> _servers = new HashSet<>();

    QueryServers(String query, @Nullable Map<ServerInstance, ServerRouteInfo> offlineRoutingTable,
        @Nullable Map<ServerInstance, ServerRouteInfo> realtimeRoutingTable) {
      _query = query;
      if (offlineRoutingTable != null) {
        _servers.addAll(offlineRoutingTable.keySet());
      }
      if (realtimeRoutingTable != null) {
        _servers.addAll(realtimeRoutingTable.keySet());
      }
    }
  }
}
