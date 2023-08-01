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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
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
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.TimestampIndexUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseBrokerRequestHandler implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerRequestHandler.class);
  private static final String IN_SUBQUERY = "insubquery";
  private static final String IN_ID_SET = "inidset";
  private static final Expression FALSE = RequestUtils.getLiteralExpression(false);
  private static final Expression TRUE = RequestUtils.getLiteralExpression(true);
  private static final Expression STAR = RequestUtils.getIdentifierExpression("*");
  private static final int MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION = 10;

  protected final PinotConfiguration _config;
  protected final BrokerRoutingManager _routingManager;
  protected final AccessControlFactory _accessControlFactory;
  protected final QueryQuotaManager _queryQuotaManager;
  protected final TableCache _tableCache;
  protected final BrokerMetrics _brokerMetrics;

  protected final BrokerRequestIdGenerator _brokerIdGenerator;
  protected final QueryOptimizer _queryOptimizer = new QueryOptimizer();

  protected final String _brokerId;
  protected final long _brokerTimeoutMs;
  protected final int _queryResponseLimit;

  private final QueryLogger _queryLogger;
  private final boolean _disableGroovy;
  private final boolean _useApproximateFunction;
  private final int _defaultHllLog2m;
  private final boolean _enableQueryLimitOverride;
  private final boolean _enableDistinctCountBitmapOverride;
  private final Map<Long, QueryServers> _queriesById;

  public BaseBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics) {
    _brokerId = brokerId;
    _brokerIdGenerator = new BrokerRequestIdGenerator(brokerId);
    _config = config;
    _routingManager = routingManager;
    _accessControlFactory = accessControlFactory;
    _queryQuotaManager = queryQuotaManager;
    _tableCache = tableCache;
    _brokerMetrics = brokerMetrics;
    _disableGroovy = _config.getProperty(Broker.DISABLE_GROOVY, Broker.DEFAULT_DISABLE_GROOVY);
    _useApproximateFunction = _config.getProperty(Broker.USE_APPROXIMATE_FUNCTION, false);
    _defaultHllLog2m = _config.getProperty(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY,
        CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
    _enableQueryLimitOverride = _config.getProperty(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, false);
    _enableDistinctCountBitmapOverride =
        _config.getProperty(CommonConstants.Helix.ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY, false);

    _brokerTimeoutMs = config.getProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, Broker.DEFAULT_BROKER_TIMEOUT_MS);
    _queryResponseLimit =
        config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT, Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
    _queryLogger = new QueryLogger(config);
    boolean enableQueryCancellation =
        Boolean.parseBoolean(config.getProperty(Broker.CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION));
    _queriesById = enableQueryCancellation ? new ConcurrentHashMap<>() : null;
    LOGGER.info(
        "Broker Id: {}, timeout: {}ms, query response limit: {}, query log length: {}, query log max rate: {}qps, "
            + "enabling query cancellation: {}", _brokerId, _brokerTimeoutMs, _queryResponseLimit,
        _queryLogger.getMaxQueryLengthToLog(), _queryLogger.getLogRateLimit(), enableQueryCancellation);
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    Preconditions.checkState(_queriesById != null, "Query cancellation is not enabled on broker");
    return _queriesById.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()._query));
  }

  @VisibleForTesting
  Set<ServerInstance> getRunningServers(long requestId) {
    Preconditions.checkState(_queriesById != null, "Query cancellation is not enabled on broker");
    QueryServers queryServers = _queriesById.get(requestId);
    return queryServers != null ? queryServers._servers : Collections.emptySet();
  }

  @Override
  public boolean cancelQuery(long requestId, int timeoutMs, Executor executor, HttpConnectionManager connMgr,
      Map<String, Integer> serverResponses)
      throws Exception {
    Preconditions.checkState(_queriesById != null, "Query cancellation is not enabled on broker");
    QueryServers queryServers = _queriesById.get(requestId);
    if (queryServers == null) {
      return false;
    }
    // TODO: Use different global query id for OFFLINE and REALTIME table after releasing 0.12.0. See QueryIdUtils for
    //       details
    String globalQueryId = getGlobalQueryId(requestId);
    List<String> serverUrls = new ArrayList<>();
    for (ServerInstance serverInstance : queryServers._servers) {
      serverUrls.add(String.format("%s/query/%s", serverInstance.getAdminEndpoint(), globalQueryId));
    }
    LOGGER.debug("Cancelling the query: {} via server urls: {}", queryServers._query, serverUrls);
    CompletionService<DeleteMethod> completionService =
        new MultiHttpRequest(executor, connMgr).execute(serverUrls, null, timeoutMs, "DELETE", DeleteMethod::new);
    List<String> errMsgs = new ArrayList<>(serverUrls.size());
    for (int i = 0; i < serverUrls.size(); i++) {
      DeleteMethod deleteMethod = null;
      try {
        // Wait for all requests to respond before returning to be sure that the servers have handled the cancel
        // requests. The completion order is different from serverUrls, thus use uri in the response.
        deleteMethod = completionService.take().get();
        URI uri = deleteMethod.getURI();
        int status = deleteMethod.getStatusCode();
        // Unexpected server responses are collected and returned as exception.
        if (status != 200 && status != 404) {
          throw new Exception(String.format("Unexpected status=%d and response='%s' from uri='%s'", status,
              deleteMethod.getResponseBodyAsString(), uri));
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
        if (deleteMethod != null) {
          deleteMethod.releaseConnection();
        }
      }
    }
    if (errMsgs.size() > 0) {
      throw new Exception("Unexpected responses from servers: " + StringUtils.join(errMsgs, ","));
    }
    return true;
  }

  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext, HttpHeaders httpHeaders)
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
      throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
    }

    JsonNode sql = request.get(Broker.Request.SQL);
    if (sql == null) {
      throw new BadQueryRequestException("Failed to find 'sql' in the request: " + request);
    }
    String query = sql.asText();
    requestContext.setQuery(query);
    BrokerResponse brokerResponse = handleRequest(requestId, query, sqlNodeAndOptions, request,
            requesterIdentity, requestContext, httpHeaders);

    if (request.has(Broker.Request.QUERY_OPTIONS)) {
      String queryOptions = request.get(Broker.Request.QUERY_OPTIONS).asText();
      Map<String, String> optionsFromString = RequestUtils.getOptionsFromString(queryOptions);
      if (QueryOptionsUtils.shouldDropResults(optionsFromString)) {
        brokerResponse.setResultTable(null);
      }
    }

    brokerResponse.setRequestId(String.valueOf(requestId));
    brokerResponse.setBrokerId(_brokerId);
    brokerResponse.setBrokerReduceTimeMs(requestContext.getReduceTimeMillis());
    return brokerResponse;
  }

  private BrokerResponseNative handleRequest(long requestId, String query,
      @Nullable SqlNodeAndOptions sqlNodeAndOptions, JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext, HttpHeaders httpHeaders)
      throws Exception {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    //Start instrumentation context. This must not be moved further below interspersed into the code.
    Tracing.ThreadAccountantOps.setupRunner(String.valueOf(requestId));

    try {
      long compilationStartTimeNs;
      PinotQuery pinotQuery;
      try {
        // Parse the request
        sqlNodeAndOptions = sqlNodeAndOptions != null ? sqlNodeAndOptions : RequestUtils.parseQuery(query, request);
        // Compile the request into PinotQuery
        compilationStartTimeNs = System.nanoTime();
        pinotQuery = CalciteSqlParser.compileToPinotQuery(sqlNodeAndOptions);
      } catch (Exception e) {
        LOGGER.info("Caught exception while compiling SQL request {}: {}, {}", requestId, query, e.getMessage());
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
        requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
        return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR, e));
      }

      if (isLiteralOnlyQuery(pinotQuery)) {
        LOGGER.debug("Request {} contains only Literal, skipping server query: {}", requestId, query);
        try {
          if (pinotQuery.isExplain()) {
            // EXPLAIN PLAN results to show that query is evaluated exclusively by Broker.
            return BrokerResponseNative.BROKER_ONLY_EXPLAIN_PLAN_OUTPUT;
          }
          return processLiteralOnlyQuery(pinotQuery, compilationStartTimeNs, requestContext);
        } catch (Exception e) {
          // TODO: refine the exceptions here to early termination the queries won't requires to send to servers.
          LOGGER.warn("Unable to execute literal request {}: {} at broker, fallback to server query. {}", requestId,
              query, e.getMessage());
        }
      }

      PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
      DataSource dataSource = serverPinotQuery.getDataSource();
      if (dataSource == null) {
        LOGGER.info("Data source (FROM clause) not found in request {}: {}", request, query);
        requestContext.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
        return new BrokerResponseNative(
            QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, "Data source (FROM clause) not found"));
      }
      if (dataSource.getJoin() != null) {
        LOGGER.info("JOIN is not supported in request {}: {}", request, query);
        requestContext.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
        return new BrokerResponseNative(
            QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, "JOIN is not supported"));
      }
      if (dataSource.getTableName() == null) {
        LOGGER.info("Table name not found in request {}: {}", request, query);
        requestContext.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
        return new BrokerResponseNative(
            QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, "Table name not found"));
      }

      try {
        handleSubquery(serverPinotQuery, requestId, request, requesterIdentity, requestContext, httpHeaders);
      } catch (Exception e) {
        LOGGER.info("Caught exception while handling the subquery in request {}: {}, {}", requestId, query,
            e.getMessage());
        requestContext.setErrorCode(QueryException.QUERY_EXECUTION_ERROR_CODE);
        return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
      }

      String tableName = getActualTableName(dataSource.getTableName(), _tableCache);
      dataSource.setTableName(tableName);
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      requestContext.setTableName(rawTableName);

      try {
        boolean isCaseInsensitive = _tableCache.isIgnoreCase();
        Map<String, String> columnNameMap = _tableCache.getColumnNameMap(rawTableName);
        if (columnNameMap != null) {
          updateColumnNames(rawTableName, serverPinotQuery, isCaseInsensitive, columnNameMap);
        }
      } catch (Exception e) {
        // Throw exceptions with column in-existence error.
        if (e instanceof BadQueryRequestException) {
          LOGGER.info("Caught exception while checking column names in request {}: {}, {}", requestId, query,
              e.getMessage());
          requestContext.setErrorCode(QueryException.UNKNOWN_COLUMN_ERROR_CODE);
          _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.UNKNOWN_COLUMN_EXCEPTIONS, 1);
          return new BrokerResponseNative(QueryException.getException(QueryException.UNKNOWN_COLUMN_ERROR, e));
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

      long compilationEndTimeNs = System.nanoTime();
      // full request compile time = compilationTimeNs + parserTimeNs
      _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REQUEST_COMPILATION,
          (compilationEndTimeNs - compilationStartTimeNs) + sqlNodeAndOptions.getParseTimeNs());

      // Second-stage table-level access control
      // TODO: Modify AccessControl interface to directly take PinotQuery
      BrokerRequest brokerRequest = CalciteSqlCompiler.convertToBrokerRequest(pinotQuery);
      BrokerRequest serverBrokerRequest =
          serverPinotQuery == pinotQuery ? brokerRequest : CalciteSqlCompiler.convertToBrokerRequest(serverPinotQuery);
      AccessControl accessControl = _accessControlFactory.create();
      boolean hasTableAccess =
          accessControl.hasAccess(requesterIdentity, serverBrokerRequest) && accessControl.hasAccess(
              httpHeaders, TargetType.TABLE, tableName, Actions.Table.QUERY);
      if (!hasTableAccess) {
        _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
        LOGGER.info("Access denied for request {}: {}, table: {}", requestId, query, tableName);
        requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
        throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
      }
      _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.AUTHORIZATION,
          System.nanoTime() - compilationEndTimeNs);

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
          requestContext.setErrorCode(QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE);
          return BrokerResponseNative.TABLE_DOES_NOT_EXIST;
        }
        LOGGER.info("No table matches for request {}: {}", requestId, query);
        requestContext.setErrorCode(QueryException.BROKER_RESOURCE_MISSING_ERROR_CODE);
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
      if (handlerContext._disableGroovy) {
        rejectGroovyQuery(serverPinotQuery);
      }
      if (handlerContext._useApproximateFunction) {
        handleApproximateFunctionOverride(serverPinotQuery);
      }

      // Validate QPS quota
      if (!_queryQuotaManager.acquire(tableName)) {
        String errorMessage =
            String.format("Request %d: %s exceeds query quota for table: %s", requestId, query, tableName);
        LOGGER.info(errorMessage);
        requestContext.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1);
        return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
      }

      // Validate the request
      try {
        validateRequest(serverPinotQuery, _queryResponseLimit);
      } catch (Exception e) {
        LOGGER.info("Caught exception while validating request {}: {}, {}", requestId, query, e.getMessage());
        requestContext.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
        return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e));
      }

      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERIES, 1);
      _brokerMetrics.addValueToTableGauge(rawTableName, BrokerGauge.REQUEST_SIZE, query.length());

      // Prepare OFFLINE and REALTIME requests
      BrokerRequest offlineBrokerRequest = null;
      BrokerRequest realtimeBrokerRequest = null;
      TimeBoundaryInfo timeBoundaryInfo = null;
      Schema schema = _tableCache.getSchema(rawTableName);
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
        return getEmptyBrokerOnlyResponse(requestId, query, requesterIdentity, requestContext, pinotQuery, tableName);
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
      Map<ServerInstance, List<String>> offlineRoutingTable = null;
      Map<ServerInstance, List<String>> realtimeRoutingTable = null;
      List<String> unavailableSegments = new ArrayList<>();
      int numPrunedSegmentsTotal = 0;
      if (offlineBrokerRequest != null) {
        // NOTE: Routing table might be null if table is just removed
        RoutingTable routingTable = _routingManager.getRoutingTable(offlineBrokerRequest, requestId);
        if (routingTable != null) {
          unavailableSegments.addAll(routingTable.getUnavailableSegments());
          Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
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
        // NOTE: Routing table might be null if table is just removed
        RoutingTable routingTable = _routingManager.getRoutingTable(realtimeBrokerRequest, requestId);
        if (routingTable != null) {
          unavailableSegments.addAll(routingTable.getUnavailableSegments());
          Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
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
      int numUnavailableSegments = unavailableSegments.size();
      requestContext.setNumUnavailableSegments(numUnavailableSegments);

      List<ProcessingException> exceptions = new ArrayList<>();
      if (numUnavailableSegments > 0) {
        String errorMessage;
        if (numUnavailableSegments > MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION) {
          errorMessage = String.format("%d segments unavailable, sampling %d: %s", numUnavailableSegments,
              MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION,
              unavailableSegments.subList(0, MAX_UNAVAILABLE_SEGMENTS_TO_PRINT_IN_QUERY_EXCEPTION));
        } else {
          errorMessage = String.format("%d segments unavailable: %s", numUnavailableSegments, unavailableSegments);
        }
        exceptions.add(QueryException.getException(QueryException.BROKER_SEGMENT_UNAVAILABLE_ERROR, errorMessage));
      }

      if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
        if (!exceptions.isEmpty()) {
          LOGGER.info("No server found for request {}: {}", requestId, query);
          _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
          return new BrokerResponseNative(exceptions);
        } else {
          // When all segments have been pruned, we can just return an empty response.
          return getEmptyBrokerOnlyResponse(requestId, query, requesterIdentity, requestContext, pinotQuery,
              tableName);
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
        exceptions.add(QueryException.getException(QueryException.BROKER_TIMEOUT_ERROR, errorMessage));
        return new BrokerResponseNative(exceptions);
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
      if (_queriesById != null) {
        // Start to track the running query for cancellation just before sending it out to servers to avoid any
        // potential failures that could happen before sending it out, like failures to calculate the routing table etc.
        // TODO: Even tracking the query as late as here, a potential race condition between calling cancel API and
        //       query being sent out to servers can still happen. If cancel request arrives earlier than query being
        //       sent out to servers, the servers miss the cancel request and continue to run the queries. The users
        //       can always list the running queries and cancel query again until it ends. Just that such race
        //       condition makes cancel API less reliable. This should be rare as it assumes sending queries out to
        //       servers takes time, but will address later if needed.
        _queriesById.put(requestId, new QueryServers(query, offlineRoutingTable, realtimeRoutingTable));
        LOGGER.debug("Keep track of running query: {}", requestId);
        try {
          brokerResponse = processBrokerRequest(requestId, brokerRequest, serverBrokerRequest, offlineBrokerRequest,
              offlineRoutingTable, realtimeBrokerRequest, realtimeRoutingTable, remainingTimeMs, serverStats,
              requestContext);
        } finally {
          _queriesById.remove(requestId);
          LOGGER.debug("Remove track of running query: {}", requestId);
        }
      } else {
        brokerResponse =
            processBrokerRequest(requestId, brokerRequest, serverBrokerRequest, offlineBrokerRequest,
                offlineRoutingTable,
                realtimeBrokerRequest, realtimeRoutingTable, remainingTimeMs, serverStats, requestContext);
      }

      brokerResponse.setExceptions(exceptions);
      brokerResponse.setNumSegmentsPrunedByBroker(numPrunedSegmentsTotal);
      long executionEndTimeNs = System.nanoTime();
      _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_EXECUTION,
          executionEndTimeNs - routingEndTimeNs);

      // Track number of queries with number of groups limit reached
      if (brokerResponse.isNumGroupsLimitReached()) {
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED,
            1);
      }

      // Set total query processing time
      long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(executionEndTimeNs - compilationStartTimeNs);
      brokerResponse.setTimeUsedMs(totalTimeMs);
      requestContext.setQueryProcessingTime(totalTimeMs);
      augmentStatistics(requestContext, brokerResponse);
      _brokerMetrics.addTimedTableValue(rawTableName, BrokerTimer.QUERY_TOTAL_TIME_MS, totalTimeMs,
          TimeUnit.MILLISECONDS);

      // Extract source info from incoming request
      _queryLogger.log(
          new QueryLogger.QueryLogParams(requestId, query, requestContext, tableName, numUnavailableSegments,
              serverStats,
              brokerResponse, totalTimeMs, requesterIdentity));

      return brokerResponse;
    } finally {
      Tracing.ThreadAccountantOps.clear();
    }
  }

  private BrokerResponseNative getEmptyBrokerOnlyResponse(long requestId, String query,
      RequesterIdentity requesterIdentity, RequestContext requestContext, PinotQuery pinotQuery, String tableName) {
    if (pinotQuery.isExplain()) {
      // EXPLAIN PLAN results to show that query is evaluated exclusively by Broker.
      return BrokerResponseNative.BROKER_ONLY_EXPLAIN_PLAN_OUTPUT;
    }

    // Send empty response since we don't need to evaluate either offline or realtime request.
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    // Extract source info from incoming request
    _queryLogger.log(new QueryLogger.QueryLogParams(requestId, query, requestContext, tableName, 0, new ServerStats(),
        brokerResponse, System.nanoTime(), requesterIdentity));
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
    switch (function.getOperator()) {
      case "datetrunc":
        String granularString = function.getOperands().get(0).getLiteral().getStringValue().toUpperCase();
        Expression timeExpression = function.getOperands().get(1);
        if (((function.getOperandsSize() == 2) || (function.getOperandsSize() == 3 && "MILLISECONDS".equalsIgnoreCase(
            function.getOperands().get(2).getLiteral().getStringValue()))) && TimestampIndexUtils.isValidGranularity(
            granularString) && timeExpression.getIdentifier() != null) {
          String timeColumn = timeExpression.getIdentifier().getName();
          String timeColumnWithGranularity = TimestampIndexUtils.getColumnWithGranularity(timeColumn, granularString);
          if (timestampIndexColumns.contains(timeColumnWithGranularity)) {
            pinotQuery.putToExpressionOverrideHints(expression,
                RequestUtils.getIdentifierExpression(timeColumnWithGranularity));
          }
        }
        break;
      default:
        break;
    }
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
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      HttpHeaders httpHeaders)
      throws Exception {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      handleSubquery(filterExpression, requestId, jsonRequest, requesterIdentity, requestContext, httpHeaders);
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
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      HttpHeaders httpHeaders)
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
      BrokerResponseNative response =
          handleRequest(requestId, subquery, null, jsonRequest, requesterIdentity, requestContext, httpHeaders);
      if (response.getExceptionsSize() != 0) {
        throw new RuntimeException("Caught exception while executing subquery: " + subquery);
      }
      String serializedIdSet = (String) response.getResultTable().getRows().get(0)[0];
      function.setOperator(IN_ID_SET);
      operands.set(1, RequestUtils.getLiteralExpression(serializedIdSet));
    } else {
      for (Expression operand : operands) {
        handleSubquery(operand, requestId, jsonRequest, requesterIdentity, requestContext, httpHeaders);
      }
    }
  }

  /**
   * Resolves the actual table name for:
   * - Case-insensitive cluster
   * - Table name in the format of [database_name].[table_name]
   *
   * @param tableName the table name in the query
   * @param tableCache the table case-sensitive cache
   * @return table name if the table name is found in Pinot registry, drop the database_name in the format
   *  of [database_name].[table_name] if only [table_name] is found in Pinot registry.
   */
  @VisibleForTesting
  static String getActualTableName(String tableName, TableCache tableCache) {
    String actualTableName = tableCache.getActualTableName(tableName);
    if (actualTableName != null) {
      return actualTableName;
    }

    // Check if table is in the format of [database_name].[table_name]
    String[] tableNameSplits = StringUtils.split(tableName, ".", 2);
    if (tableNameSplits.length == 2) {
      actualTableName = tableCache.getActualTableName(tableNameSplits[1]);
      if (actualTableName != null) {
        return actualTableName;
      }
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
  static void rejectGroovyQuery(PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    for (Expression expression : selectList) {
      rejectGroovyQuery(expression);
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        rejectGroovyQuery(expression.getFunctionCall().getOperands().get(0));
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      rejectGroovyQuery(havingExpression);
    }
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      rejectGroovyQuery(filterExpression);
    }
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null) {
      for (Expression expression : groupByList) {
        rejectGroovyQuery(expression);
      }
    }
  }

  private static void rejectGroovyQuery(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    if (function.getOperator().equals("groovy")) {
      throw new BadQueryRequestException("Groovy transform functions are disabled for queries");
    }
    for (Expression operandExpression : function.getOperands()) {
      rejectGroovyQuery(operandExpression);
    }
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
          function.setOperands(
              Arrays.asList(function.getOperands().get(0), RequestUtils.getLiteralExpression(percentile)));
        } catch (Exception e) {
          throw new BadQueryRequestException("Illegal function name: " + functionName);
        }
      } else if (remainingFunctionName.matches("\\d+mv")) {
        try {
          int percentile = Integer.parseInt(remainingFunctionName.substring(0, remainingFunctionName.length() - 2));
          function.setOperator("percentilesmarttdigest");
          function.setOperands(
              Arrays.asList(function.getOperands().get(0), RequestUtils.getLiteralExpression(percentile)));
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
  private BrokerResponseNative processLiteralOnlyQuery(PinotQuery pinotQuery, long compilationStartTimeNs,
      RequestContext requestContext) {
    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    List<String> columnNames = new ArrayList<>();
    List<DataSchema.ColumnDataType> columnTypes = new ArrayList<>();
    List<Object> row = new ArrayList<>();
    for (Expression expression : pinotQuery.getSelectList()) {
      computeResultsForExpression(expression, columnNames, columnTypes, row);
    }
    DataSchema dataSchema =
        new DataSchema(columnNames.toArray(new String[0]), columnTypes.toArray(new DataSchema.ColumnDataType[0]));
    List<Object[]> rows = new ArrayList<>();
    rows.add(row.toArray());
    ResultTable resultTable = new ResultTable(dataSchema, rows);
    brokerResponse.setResultTable(resultTable);

    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - compilationStartTimeNs);
    brokerResponse.setTimeUsedMs(totalTimeMs);
    requestContext.setQueryProcessingTime(totalTimeMs);
    augmentStatistics(requestContext, brokerResponse);
    return brokerResponse;
  }

  // TODO(xiangfu): Move Literal function computation here from Calcite Parser.
  private void computeResultsForExpression(Expression e, List<String> columnNames,
      List<DataSchema.ColumnDataType> columnTypes, List<Object> row) {
    if (e.getType() == ExpressionType.LITERAL) {
      computeResultsForLiteral(e.getLiteral(), columnNames, columnTypes, row);
    }
    if (e.getType() == ExpressionType.FUNCTION) {
      if (e.getFunctionCall().getOperator().equals("as")) {
        String columnName = e.getFunctionCall().getOperands().get(1).getIdentifier().getName();
        computeResultsForExpression(e.getFunctionCall().getOperands().get(0), columnNames, columnTypes, row);
        columnNames.set(columnNames.size() - 1, columnName);
      } else {
        throw new IllegalStateException(
            "No able to compute results for function - " + e.getFunctionCall().getOperator());
      }
    }
  }

  private void computeResultsForLiteral(Literal literal, List<String> columnNames,
      List<DataSchema.ColumnDataType> columnTypes, List<Object> row) {
    Object fieldValue = literal.getFieldValue();
    columnNames.add(fieldValue.toString());
    switch (literal.getSetField()) {
      case BOOL_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.BOOLEAN);
        row.add(literal.getBoolValue());
        break;
      case BYTE_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.INT);
        row.add((int) literal.getByteValue());
        break;
      case SHORT_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.INT);
        row.add((int) literal.getShortValue());
        break;
      case INT_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.INT);
        row.add(literal.getIntValue());
        break;
      case LONG_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.LONG);
        row.add(literal.getLongValue());
        break;
      case DOUBLE_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.DOUBLE);
        row.add(literal.getDoubleValue());
        break;
      case STRING_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.STRING);
        row.add(literal.getStringValue());
        break;
      case BINARY_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.BYTES);
        row.add(BytesUtils.toHexString(literal.getBinaryValue()));
        break;
      case NULL_VALUE:
        columnTypes.add(DataSchema.ColumnDataType.UNKNOWN);
        row.add(null);
        break;
      default:
        break;
    }
  }

  /**
   * Fixes the column names to the actual column names in the given query.
   */
  @VisibleForTesting
  static void updateColumnNames(String rawTableName, PinotQuery pinotQuery, boolean isCaseInsensitive,
      Map<String, String> columnNameMap) {
    Map<String, String> aliasMap = new HashMap<>();
    if (pinotQuery != null) {
      boolean hasStar = false;
      for (Expression expression : pinotQuery.getSelectList()) {
        fixColumnName(rawTableName, expression, columnNameMap, aliasMap, isCaseInsensitive);
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
        fixColumnName(rawTableName, filterExpression, columnNameMap, aliasMap, isCaseInsensitive);
      }
      List<Expression> groupByList = pinotQuery.getGroupByList();
      if (groupByList != null) {
        for (Expression expression : groupByList) {
          fixColumnName(rawTableName, expression, columnNameMap, aliasMap, isCaseInsensitive);
        }
      }
      List<Expression> orderByList = pinotQuery.getOrderByList();
      if (orderByList != null) {
        for (Expression expression : orderByList) {
          // NOTE: Order-by is always a Function with the ordering of the Expression
          fixColumnName(rawTableName, expression.getFunctionCall().getOperands().get(0), columnNameMap, aliasMap,
              isCaseInsensitive);
        }
      }
      Expression havingExpression = pinotQuery.getHavingExpression();
      if (havingExpression != null) {
        fixColumnName(rawTableName, havingExpression, columnNameMap, aliasMap, isCaseInsensitive);
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
      Map<String, String> aliasMap, boolean ignoreCase) {
    ExpressionType expressionType = expression.getType();
    if (expressionType == ExpressionType.IDENTIFIER) {
      Identifier identifier = expression.getIdentifier();
      identifier.setName(getActualColumnName(rawTableName, identifier.getName(), columnNameMap, aliasMap, ignoreCase));
    } else if (expressionType == ExpressionType.FUNCTION) {
      final Function functionCall = expression.getFunctionCall();
      switch (functionCall.getOperator()) {
        case "as":
          fixColumnName(rawTableName, functionCall.getOperands().get(0), columnNameMap, aliasMap, ignoreCase);
          final Expression rightAsExpr = functionCall.getOperands().get(1);
          if (rightAsExpr.isSetIdentifier()) {
            String rightColumn = rightAsExpr.getIdentifier().getName();
            if (ignoreCase) {
              aliasMap.put(rightColumn.toLowerCase(), rightColumn);
            } else {
              aliasMap.put(rightColumn, rightColumn);
            }
          }
          break;
        case "lookup":
          // LOOKUP function looks up another table's schema, skip the check for now.
          break;
        default:
          for (Expression operand : functionCall.getOperands()) {
            fixColumnName(rawTableName, operand, columnNameMap, aliasMap, ignoreCase);
          }
          break;
      }
    }
  }

  /**
   * Returns the actual column name for the given column name for:
   * - Case-insensitive cluster
   * - Column name in the format of [table_name].[column_name]
   */
  @VisibleForTesting
  static String getActualColumnName(String rawTableName, String columnName, @Nullable Map<String, String> columnNameMap,
      @Nullable Map<String, String> aliasMap, boolean ignoreCase) {
    if ("*".equals(columnName)) {
      return columnName;
    }
    String columnNameToCheck;
    if (columnName.regionMatches(ignoreCase, 0, rawTableName, 0, rawTableName.length())
        && columnName.length() > rawTableName.length() && columnName.charAt(rawTableName.length()) == '.') {
      columnNameToCheck = ignoreCase ? columnName.substring(rawTableName.length() + 1).toLowerCase()
          : columnName.substring(rawTableName.length() + 1);
    } else {
      columnNameToCheck = ignoreCase ? columnName.toLowerCase() : columnName;
    }
    if (columnNameMap != null) {
      String actualColumnName = columnNameMap.get(columnNameToCheck);
      if (actualColumnName != null) {
        return actualColumnName;
      }
    }
    if (aliasMap != null) {
      String actualAlias = aliasMap.get(columnNameToCheck);
      if (actualAlias != null) {
        return actualAlias;
      }
    }
    if (columnName.charAt(0) == '$') {
      return columnName;
    }
    throw new BadQueryRequestException("Unknown columnName '" + columnName + "' found in the query");
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
    queryOptions.put(Broker.Request.QueryOptionKey.TIMEOUT_MS, Long.toString(remainingTimeMs));
    return remainingTimeMs;
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

    Map<String, String> queryOptions = pinotQuery.getQueryOptions();
    try {
      // throw errors if options is less than 1 or invalid
      Integer numReplicaGroupsToQuery = QueryOptionsUtils.getNumReplicaGroupsToQuery(queryOptions);
      if (numReplicaGroupsToQuery != null) {
        Preconditions.checkState(numReplicaGroupsToQuery > 0, "numReplicaGroups must be " + "positive number, got: %d",
            numReplicaGroupsToQuery);
      }
    } catch (NumberFormatException e) {
      String numReplicaGroupsToQuery = queryOptions.get(Broker.Request.QueryOptionKey.NUM_REPLICA_GROUPS_TO_QUERY);
      throw new IllegalStateException(
          String.format("numReplicaGroups must be a positive number, got: %s", numReplicaGroupsToQuery));
    }

    if (pinotQuery.getDataSource().getSubquery() != null) {
      validateRequest(pinotQuery.getDataSource().getSubquery(), queryResponseLimit);
    }
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
   * Processes the optimized broker requests for both OFFLINE and REALTIME table.
   * TODO: Directly take PinotQuery
   */
  protected abstract BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      BrokerRequest serverBrokerRequest, @Nullable BrokerRequest offlineBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> offlineRoutingTable, @Nullable BrokerRequest realtimeBrokerRequest,
      @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable, long timeoutMs, ServerStats serverStats,
      RequestContext requestContext)
      throws Exception;

  protected static void augmentStatistics(RequestContext statistics, BrokerResponse response) {
    statistics.setTotalDocs(response.getTotalDocs());
    statistics.setNumDocsScanned(response.getNumDocsScanned());
    statistics.setNumEntriesScannedInFilter(response.getNumEntriesScannedInFilter());
    statistics.setNumEntriesScannedPostFilter(response.getNumEntriesScannedPostFilter());
    statistics.setNumSegmentsQueried(response.getNumSegmentsQueried());
    statistics.setNumSegmentsProcessed(response.getNumSegmentsProcessed());
    statistics.setNumSegmentsMatched(response.getNumSegmentsMatched());
    statistics.setNumServersQueried(response.getNumServersQueried());
    statistics.setNumSegmentsProcessed(response.getNumSegmentsProcessed());
    statistics.setNumServersResponded(response.getNumServersResponded());
    statistics.setNumGroupsLimitReached(response.isNumGroupsLimitReached());
    statistics.setNumExceptions(response.getExceptionsSize());
    statistics.setOfflineThreadCpuTimeNs(response.getOfflineThreadCpuTimeNs());
    statistics.setRealtimeThreadCpuTimeNs(response.getRealtimeThreadCpuTimeNs());
    statistics.setOfflineSystemActivitiesCpuTimeNs(response.getOfflineSystemActivitiesCpuTimeNs());
    statistics.setRealtimeSystemActivitiesCpuTimeNs(response.getRealtimeSystemActivitiesCpuTimeNs());
    statistics.setOfflineResponseSerializationCpuTimeNs(response.getOfflineResponseSerializationCpuTimeNs());
    statistics.setRealtimeResponseSerializationCpuTimeNs(response.getRealtimeResponseSerializationCpuTimeNs());
    statistics.setOfflineTotalCpuTimeNs(response.getOfflineTotalCpuTimeNs());
    statistics.setRealtimeTotalCpuTimeNs(response.getRealtimeTotalCpuTimeNs());
    statistics.setNumRowsResultSet(response.getNumRowsResultSet());
  }

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

    QueryServers(String query, @Nullable Map<ServerInstance, List<String>> offlineRoutingTable,
        @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable) {
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
