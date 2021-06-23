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
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.RateLimiter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.requesthandler.PinotQueryParserFactory;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("UnstableApiUsage")
@ThreadSafe
public abstract class BaseBrokerRequestHandler implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerRequestHandler.class);
  private static final String IN_SUBQUERY = "inSubquery";
  private static final Expression FALSE = RequestUtils.getLiteralExpression(false);
  private static final Expression TRUE = RequestUtils.getLiteralExpression(true);

  protected final PinotConfiguration _config;
  protected final RoutingManager _routingManager;
  protected final AccessControlFactory _accessControlFactory;
  protected final QueryQuotaManager _queryQuotaManager;
  protected final TableCache _tableCache;
  protected final BrokerMetrics _brokerMetrics;

  protected final AtomicLong _requestIdGenerator = new AtomicLong();
  protected final QueryOptimizer _queryOptimizer = new QueryOptimizer();
  protected final BrokerReduceService _brokerReduceService;

  protected final String _brokerId;
  protected final long _brokerTimeoutMs;
  protected final int _queryResponseLimit;
  protected final int _queryLogLength;

  private final RateLimiter _queryLogRateLimiter;

  private final RateLimiter _numDroppedLogRateLimiter;
  private final AtomicInteger _numDroppedLog;

  private final int _defaultHllLog2m;
  private final boolean _enableQueryLimitOverride;
  private final boolean _enableDistinctCountBitmapOverride;

  public BaseBrokerRequestHandler(PinotConfiguration config, RoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache,
      BrokerMetrics brokerMetrics) {
    _config = config;
    _routingManager = routingManager;
    _accessControlFactory = accessControlFactory;
    _queryQuotaManager = queryQuotaManager;
    _tableCache = tableCache;
    _brokerMetrics = brokerMetrics;

    _defaultHllLog2m = _config.getProperty(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M_KEY,
        CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
    _enableQueryLimitOverride = _config.getProperty(Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, false);
    _enableDistinctCountBitmapOverride =
        _config.getProperty(CommonConstants.Helix.ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY, false);

    _brokerId = config.getProperty(Broker.CONFIG_OF_BROKER_ID, getDefaultBrokerId());
    _brokerTimeoutMs = config.getProperty(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, Broker.DEFAULT_BROKER_TIMEOUT_MS);
    _queryResponseLimit =
        config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT, Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
    _queryLogLength =
        config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_LOG_LENGTH, Broker.DEFAULT_BROKER_QUERY_LOG_LENGTH);
    _queryLogRateLimiter = RateLimiter.create(config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND,
        Broker.DEFAULT_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND));
    _numDroppedLog = new AtomicInteger(0);
    _numDroppedLogRateLimiter = RateLimiter.create(1.0);

    _brokerReduceService = new BrokerReduceService(_config);
    LOGGER
        .info("Broker Id: {}, timeout: {}ms, query response limit: {}, query log length: {}, query log max rate: {}qps",
            _brokerId, _brokerTimeoutMs, _queryResponseLimit, _queryLogLength, _queryLogRateLimiter.getRate());
  }

  private String getDefaultBrokerId() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting default broker Id", e);
      return "";
    }
  }

  @Override
  public BrokerResponseNative handleRequest(JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestStatistics requestStatistics)
      throws Exception {
    long requestId = _requestIdGenerator.incrementAndGet();
    requestStatistics.setBrokerId(_brokerId);
    requestStatistics.setRequestId(requestId);
    requestStatistics.setRequestArrivalTimeMillis(System.currentTimeMillis());

    // First-stage access control to prevent unauthenticated requests from using up resources. Secondary table-level
    // check comes later.
    boolean hasAccess = _accessControlFactory.create().hasAccess(requesterIdentity);
    if (!hasAccess) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.info("Access denied for requestId {}", requestId);
      requestStatistics.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
    }

    if (request.has(Broker.Request.SQL)) {
      return handleSQLRequest(requestId, request.get(Broker.Request.SQL).asText(), request, requesterIdentity,
          requestStatistics);
    } else {
      return handlePQLRequest(requestId, request.get(Broker.Request.PQL).asText(), request, requesterIdentity,
          requestStatistics);
    }
  }

  private BrokerResponseNative handleSQLRequest(long requestId, String query, JsonNode request,
      @Nullable RequesterIdentity requesterIdentity, RequestStatistics requestStatistics)
      throws Exception {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);
    requestStatistics.setPql(query);

    // Compile the request
    long compilationStartTimeNs = System.nanoTime();
    BrokerRequest brokerRequest;
    try {
      brokerRequest = PinotQueryParserFactory.parseSQLQuery(query);
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling SQL request {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestStatistics.setErrorCode(QueryException.PQL_PARSING_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.PQL_PARSING_ERROR, e));
    }
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    setOptions(pinotQuery, requestId, query, request);

    if (isLiteralOnlyQuery(pinotQuery)) {
      LOGGER.debug("Request {} contains only Literal, skipping server query: {}", requestId, query);
      try {
        return processLiteralOnlyQuery(pinotQuery, compilationStartTimeNs, requestStatistics);
      } catch (Exception e) {
        // TODO: refine the exceptions here to early termination the queries won't requires to send to servers.
        LOGGER
            .warn("Unable to execute literal request {}: {} at broker, fallback to server query. {}", requestId, query,
                e.getMessage());
      }
    }

    try {
      handleSubquery(pinotQuery, requestId, request, requesterIdentity, requestStatistics);
    } catch (Exception e) {
      LOGGER
          .info("Caught exception while handling the subquery in request {}: {}, {}", requestId, query, e.getMessage());
      requestStatistics.setErrorCode(QueryException.QUERY_EXECUTION_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    }

    String tableName = getActualTableName(pinotQuery.getDataSource().getTableName());
    setTableName(brokerRequest, tableName);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    requestStatistics.setTableName(rawTableName);

    try {
      updateColumnNames(rawTableName, pinotQuery);
    } catch (Exception e) {
      LOGGER
          .warn("Caught exception while updating column names in request {}: {}, {}", requestId, query, e.getMessage());
    }
    if (_defaultHllLog2m > 0) {
      handleHLLLog2mOverride(pinotQuery, _defaultHllLog2m);
    }
    if (_enableQueryLimitOverride) {
      handleQueryLimitOverride(pinotQuery, _queryResponseLimit);
    }
    if (_enableDistinctCountBitmapOverride) {
      handleDistinctCountBitmapOverride(pinotQuery);
    }

    long compilationEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REQUEST_COMPILATION,
        compilationEndTimeNs - compilationStartTimeNs);

    // Second-stage table-level access control
    boolean hasTableAccess = _accessControlFactory.create().hasAccess(requesterIdentity, brokerRequest);
    if (!hasTableAccess) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.info("Access denied for request {}: {}, table: {}", requestId, query, tableName);
      requestStatistics.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
    }
    _brokerMetrics
        .addPhaseTiming(rawTableName, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - compilationEndTimeNs);

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
    if ((offlineTableName == null) && (realtimeTableName == null)) {
      // No table matches the request
      if (_tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName)) == null
          && _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName)) == null) {
        LOGGER.info("Table not found for request {}: {}", requestId, query);
        requestStatistics.setErrorCode(QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE);
        return BrokerResponseNative.TABLE_DOES_NOT_EXIST;
      }
      LOGGER.info("No table matches for request {}: {}", requestId, query);
      requestStatistics.setErrorCode(QueryException.BROKER_RESOURCE_MISSING_ERROR_CODE);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1);
      return BrokerResponseNative.NO_TABLE_RESULT;
    }

    // Validate QPS quota
    if (!_queryQuotaManager.acquire(tableName)) {
      String errorMessage =
          String.format("Request %d: %s exceeds query quota for table: %s", requestId, query, tableName);
      LOGGER.info(errorMessage);
      requestStatistics.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
    }

    // Validate the request
    try {
      validateRequest(pinotQuery, _queryResponseLimit);
    } catch (Exception e) {
      LOGGER.info("Caught exception while validating request {}: {}, {}", requestId, query, e.getMessage());
      requestStatistics.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e));
    }

    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERIES, 1);
    _brokerMetrics.addValueToTableGauge(rawTableName, BrokerGauge.REQUEST_SIZE, query.length());

    // Prepare OFFLINE and REALTIME requests
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    Schema schema = _tableCache.getSchema(rawTableName);
    if (offlineTableName != null && realtimeTableName != null) {
      // Hybrid
      offlineBrokerRequest = getOfflineBrokerRequest(brokerRequest);
      _queryOptimizer
          .optimize(offlineBrokerRequest.getPinotQuery(), _tableCache.getTableConfig(offlineTableName), schema);
      realtimeBrokerRequest = getRealtimeBrokerRequest(brokerRequest);
      _queryOptimizer
          .optimize(realtimeBrokerRequest.getPinotQuery(), _tableCache.getTableConfig(realtimeTableName), schema);
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.HYBRID);
      requestStatistics.setOfflineServerTenant(getServerTenant(offlineTableName));
      requestStatistics.setRealtimeServerTenant(getServerTenant(realtimeTableName));
    } else if (offlineTableName != null) {
      // OFFLINE only
      setTableName(brokerRequest, offlineTableName);
      _queryOptimizer.optimize(pinotQuery, _tableCache.getTableConfig(offlineTableName), schema);
      offlineBrokerRequest = brokerRequest;
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.OFFLINE);
      requestStatistics.setOfflineServerTenant(getServerTenant(offlineTableName));
    } else {
      // REALTIME only
      setTableName(brokerRequest, realtimeTableName);
      _queryOptimizer.optimize(pinotQuery, _tableCache.getTableConfig(realtimeTableName), schema);
      realtimeBrokerRequest = brokerRequest;
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.REALTIME);
      requestStatistics.setRealtimeServerTenant(getServerTenant(realtimeTableName));
    }

    // Check if response can be send without server query evaluation.
    if (offlineBrokerRequest == null || offlineBrokerRequest.getPinotQuery() == null || isFilterAlwaysFalse(
        offlineBrokerRequest)) {
      // We don't need to evaluate offline request
      offlineBrokerRequest = null;
    }

    if (realtimeBrokerRequest == null || realtimeBrokerRequest.getPinotQuery() == null || isFilterAlwaysFalse(
        realtimeBrokerRequest)) {
      // We don't need to evaluate realtime request
      realtimeBrokerRequest = null;
    }

    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      // Send empty response since we don't need to evaluate either offline or realtime request.
      BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
      logBrokerResponse(requestId, query, requestStatistics, brokerRequest, 0, new ServerStats(), brokerResponse,
          System.nanoTime());

      return brokerResponse;
    }

    if (offlineBrokerRequest != null && offlineBrokerRequest.getPinotQuery() != null && isFilterAlwaysTrue(
        offlineBrokerRequest)) {
      // Drop offline request filter since it is always true
      offlineBrokerRequest.getPinotQuery().setFilterExpression(null);
    }

    if (realtimeBrokerRequest != null && realtimeBrokerRequest.getPinotQuery() != null && isFilterAlwaysTrue(
        realtimeBrokerRequest)) {
      // Drop realtime request filter since it is always true
      realtimeBrokerRequest.getPinotQuery().setFilterExpression(null);
    }

    // Calculate routing table for the query
    long routingStartTimeNs = System.nanoTime();
    Map<ServerInstance, List<String>> offlineRoutingTable = null;
    Map<ServerInstance, List<String>> realtimeRoutingTable = null;
    int numUnavailableSegments = 0;
    if (offlineBrokerRequest != null) {
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = _routingManager.getRoutingTable(offlineBrokerRequest);
      if (routingTable != null) {
        numUnavailableSegments += routingTable.getUnavailableSegments().size();
        Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineRoutingTable = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = _routingManager.getRoutingTable(realtimeBrokerRequest);
      if (routingTable != null) {
        numUnavailableSegments += routingTable.getUnavailableSegments().size();
        Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeRoutingTable = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
      } else {
        realtimeBrokerRequest = null;
      }
    }
    requestStatistics.setNumUnavailableSegments(numUnavailableSegments);

    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      LOGGER.info("No server found for request {}: {}", requestId, query);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
      return BrokerResponseNative.EMPTY_RESULT;
    }
    long routingEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_ROUTING, routingEndTimeNs - routingStartTimeNs);

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
      return new BrokerResponseNative(QueryException.getException(QueryException.BROKER_TIMEOUT_ERROR, errorMessage));
    }

    // Execute the query
    ServerStats serverStats = new ServerStats();
    BrokerResponseNative brokerResponse =
        processBrokerRequest(requestId, brokerRequest, offlineBrokerRequest, offlineRoutingTable, realtimeBrokerRequest,
            realtimeRoutingTable, remainingTimeMs, serverStats, requestStatistics);
    long executionEndTimeNs = System.nanoTime();
    _brokerMetrics
        .addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_EXECUTION, executionEndTimeNs - routingEndTimeNs);

    // Track number of queries with number of groups limit reached
    if (brokerResponse.isNumGroupsLimitReached()) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED, 1);
    }

    // Set total query processing time
    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(executionEndTimeNs - compilationStartTimeNs);
    brokerResponse.setTimeUsedMs(totalTimeMs);
    requestStatistics.setQueryProcessingTime(totalTimeMs);
    requestStatistics.setStatistics(brokerResponse);

    logBrokerResponse(requestId, query, requestStatistics, brokerRequest, numUnavailableSegments, serverStats,
        brokerResponse, totalTimeMs);
    return brokerResponse;
  }

  /** Given a {@link BrokerRequest}, check if the WHERE clause will always evaluate to false. */
  private boolean isFilterAlwaysFalse(BrokerRequest brokerRequest) {
    return FALSE.equals(brokerRequest.getPinotQuery().getFilterExpression());
  }

  /** Given a {@link BrokerRequest}, check if the WHERE clause will always evaluate to true. */
  private boolean isFilterAlwaysTrue(BrokerRequest brokerRequest) {
    return TRUE.equals(brokerRequest.getPinotQuery().getFilterExpression());
  }

  private void logBrokerResponse(long requestId, String query, RequestStatistics requestStatistics,
      BrokerRequest brokerRequest, int numUnavailableSegments, ServerStats serverStats,
      BrokerResponseNative brokerResponse, long totalTimeMs) {
    LOGGER.debug("Broker Response: {}", brokerResponse);

    // Please keep the format as name=value comma-separated with no spaces
    // Please keep all the name value pairs together, then followed by the query. To add a new entry, please add it to
    // the end of existing pairs, but before the query.
    if (_queryLogRateLimiter.tryAcquire() || forceLog(brokerResponse, totalTimeMs)) {
      // Table name might have been changed (with suffix _OFFLINE/_REALTIME appended)
      LOGGER.info("requestId={},table={},timeMs={},docs={}/{},entries={}/{},"
              + "segments(queried/processed/matched/consuming/unavailable):{}/{}/{}/{}/{},consumingFreshnessTimeMs={},"
              + "servers={}/{},groupLimitReached={},brokerReduceTimeMs={},exceptions={},serverStats={},"
              + "offlineThreadCpuTimeNs={},realtimeThreadCpuTimeNs={},query={}", requestId,
          brokerRequest.getQuerySource().getTableName(), totalTimeMs, brokerResponse.getNumDocsScanned(),
          brokerResponse.getTotalDocs(), brokerResponse.getNumEntriesScannedInFilter(),
          brokerResponse.getNumEntriesScannedPostFilter(), brokerResponse.getNumSegmentsQueried(),
          brokerResponse.getNumSegmentsProcessed(), brokerResponse.getNumSegmentsMatched(),
          brokerResponse.getNumConsumingSegmentsQueried(), numUnavailableSegments,
          brokerResponse.getMinConsumingFreshnessTimeMs(), brokerResponse.getNumServersResponded(),
          brokerResponse.getNumServersQueried(), brokerResponse.isNumGroupsLimitReached(),
          requestStatistics.getReduceTimeMillis(), brokerResponse.getExceptionsSize(), serverStats.getServerStats(),
          brokerResponse.getOfflineThreadCpuTimeNs(), brokerResponse.getRealtimeThreadCpuTimeNs(),
          StringUtils.substring(query, 0, _queryLogLength));

      // Limit the dropping log message at most once per second.
      if (_numDroppedLogRateLimiter.tryAcquire()) {
        // NOTE: the reported number may not be accurate since we will be missing some increments happened between
        // get() and set().
        int numDroppedLog = _numDroppedLog.get();
        if (numDroppedLog > 0) {
          LOGGER.info("{} logs were dropped. (log max rate per second: {})", numDroppedLog,
              _queryLogRateLimiter.getRate());
          _numDroppedLog.set(0);
        }
      }
    } else {
      // Increment the count for dropped log
      _numDroppedLog.incrementAndGet();
    }
  }

  private String getServerTenant(String tableNameWithType) {
    TableConfig tableConfig = _tableCache.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.debug("Table config is not available for table {}", tableNameWithType);
      return "unknownTenant";
    }
    return tableConfig.getTenantConfig().getServer();
  }

  @Deprecated
  private BrokerResponseNative handlePQLRequest(long requestId, String query, JsonNode request,
      @Nullable RequesterIdentity requesterIdentity, RequestStatistics requestStatistics)
      throws Exception {
    LOGGER.debug("PQL query for request {}: {}", requestId, query);
    requestStatistics.setPql(query);

    // Compile the request
    long compilationStartTimeNs = System.nanoTime();
    BrokerRequest brokerRequest;
    try {
      brokerRequest = PinotQueryParserFactory.parsePQLQuery(query);
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling PQL request {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestStatistics.setErrorCode(QueryException.PQL_PARSING_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.PQL_PARSING_ERROR, e));
    }
    setOptions(requestId, query, request, brokerRequest);

    try {
      handleSubquery(brokerRequest, requestId, request, requesterIdentity, requestStatistics);
    } catch (Exception e) {
      LOGGER
          .info("Caught exception while handling the subquery in request {}: {}, {}", requestId, query, e.getMessage());
      requestStatistics.setErrorCode(QueryException.QUERY_EXECUTION_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
    }

    String tableName = getActualTableName(brokerRequest.getQuerySource().getTableName());
    setTableName(brokerRequest, tableName);
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    requestStatistics.setTableName(rawTableName);

    try {
      updateColumnNames(rawTableName, brokerRequest);
    } catch (Exception e) {
      LOGGER
          .warn("Caught exception while updating column names in request {}: {}, {}", requestId, query, e.getMessage());
    }
    if (_defaultHllLog2m > 0) {
      handleHyperloglogLog2mOverride(brokerRequest, _defaultHllLog2m);
    }
    if (_enableQueryLimitOverride) {
      handleQueryLimitOverride(brokerRequest, _queryResponseLimit);
    }
    if (_enableDistinctCountBitmapOverride) {
      handleDistinctCountBitmapOverride(brokerRequest);
    }

    long compilationEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REQUEST_COMPILATION,
        compilationEndTimeNs - compilationStartTimeNs);

    // Second-stage table-level access control
    boolean hasTableAccess = _accessControlFactory.create().hasAccess(requesterIdentity, brokerRequest);
    if (!hasTableAccess) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.info("Access denied for requestId {}, table {}", requestId, tableName);
      requestStatistics.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
      return new BrokerResponseNative(QueryException.ACCESS_DENIED_ERROR);
    }
    _brokerMetrics
        .addPhaseTiming(rawTableName, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - compilationEndTimeNs);

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
    if ((offlineTableName == null) && (realtimeTableName == null)) {
      // No table matches the request
      if (_tableCache.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName)) == null
          && _tableCache.getTableConfig(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName)) == null) {
        LOGGER.info("Table not found for request {}: {}", requestId, query);
        requestStatistics.setErrorCode(QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE);
        return BrokerResponseNative.TABLE_DOES_NOT_EXIST;
      }
      LOGGER.info("No table matches for request {}: {}", requestId, query);
      requestStatistics.setErrorCode(QueryException.BROKER_RESOURCE_MISSING_ERROR_CODE);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1);
      return BrokerResponseNative.NO_TABLE_RESULT;
    }

    // Validate QPS quota
    if (!_queryQuotaManager.acquire(tableName)) {
      String errorMessage =
          String.format("Request %d exceeds query quota for table:%s, query:%s", requestId, tableName, query);
      LOGGER.info(errorMessage);
      requestStatistics.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
    }

    // Validate the request
    try {
      validateRequest(brokerRequest, _queryResponseLimit);
    } catch (Exception e) {
      LOGGER.info("Caught exception while validating request {}: {}, {}", requestId, query, e.getMessage());
      requestStatistics.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e));
    }

    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERIES, 1);
    _brokerMetrics.addValueToTableGauge(rawTableName, BrokerGauge.REQUEST_SIZE, query.length());

    // Prepare OFFLINE and REALTIME requests
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    Schema schema = _tableCache.getSchema(rawTableName);
    if (offlineTableName != null && realtimeTableName != null) {
      // Hybrid
      offlineBrokerRequest = getOfflineBrokerRequest(brokerRequest);
      _queryOptimizer.optimize(offlineBrokerRequest, schema);
      realtimeBrokerRequest = getRealtimeBrokerRequest(brokerRequest);
      _queryOptimizer.optimize(realtimeBrokerRequest, schema);
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.HYBRID);
    } else if (offlineTableName != null) {
      // OFFLINE only
      setTableName(brokerRequest, offlineTableName);
      _queryOptimizer.optimize(brokerRequest, schema);
      offlineBrokerRequest = brokerRequest;
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.OFFLINE);
    } else {
      // REALTIME only
      setTableName(brokerRequest, realtimeTableName);
      _queryOptimizer.optimize(brokerRequest, schema);
      realtimeBrokerRequest = brokerRequest;
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.REALTIME);
    }

    // Calculate routing table for the query
    long routingStartTimeNs = System.nanoTime();
    Map<ServerInstance, List<String>> offlineRoutingTable = null;
    Map<ServerInstance, List<String>> realtimeRoutingTable = null;
    int numUnavailableSegments = 0;
    if (offlineBrokerRequest != null) {
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = _routingManager.getRoutingTable(offlineBrokerRequest);
      if (routingTable != null) {
        numUnavailableSegments += routingTable.getUnavailableSegments().size();
        Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          offlineRoutingTable = serverInstanceToSegmentsMap;
        } else {
          offlineBrokerRequest = null;
        }
      } else {
        offlineBrokerRequest = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      // NOTE: Routing table might be null if table is just removed
      RoutingTable routingTable = _routingManager.getRoutingTable(realtimeBrokerRequest);
      if (routingTable != null) {
        numUnavailableSegments += routingTable.getUnavailableSegments().size();
        Map<ServerInstance, List<String>> serverInstanceToSegmentsMap = routingTable.getServerInstanceToSegmentsMap();
        if (!serverInstanceToSegmentsMap.isEmpty()) {
          realtimeRoutingTable = serverInstanceToSegmentsMap;
        } else {
          realtimeBrokerRequest = null;
        }
      } else {
        realtimeBrokerRequest = null;
      }
    }
    requestStatistics.setNumUnavailableSegments(numUnavailableSegments);

    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      LOGGER.info("No server found for request {}: {}", requestId, query);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
      return BrokerResponseNative.EMPTY_RESULT;
    }
    long routingEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_ROUTING, routingEndTimeNs - routingStartTimeNs);

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
        remainingTimeMs = setQueryTimeout(offlineTableName, offlineBrokerRequest.getQueryOptions(), timeSpentMs);
      }
      if (realtimeBrokerRequest != null) {
        remainingTimeMs = Math.max(remainingTimeMs,
            setQueryTimeout(realtimeTableName, realtimeBrokerRequest.getQueryOptions(), timeSpentMs));
      }
    } catch (TimeoutException e) {
      String errorMessage = e.getMessage();
      LOGGER.info("{} {}: {}", errorMessage, requestId, query);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.REQUEST_TIMEOUT_BEFORE_SCATTERED_EXCEPTIONS, 1);
      return new BrokerResponseNative(QueryException.getException(QueryException.BROKER_TIMEOUT_ERROR, errorMessage));
    }

    // Execute the query
    ServerStats serverStats = new ServerStats();
    BrokerResponseNative brokerResponse =
        processBrokerRequest(requestId, brokerRequest, offlineBrokerRequest, offlineRoutingTable, realtimeBrokerRequest,
            realtimeRoutingTable, remainingTimeMs, serverStats, requestStatistics);
    long executionEndTimeNs = System.nanoTime();
    _brokerMetrics
        .addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_EXECUTION, executionEndTimeNs - routingEndTimeNs);

    // Track number of queries with number of groups limit reached
    if (brokerResponse.isNumGroupsLimitReached()) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_NUM_GROUPS_LIMIT_REACHED, 1);
    }

    // Set total query processing time
    long totalTimeMs = TimeUnit.NANOSECONDS.toMillis(executionEndTimeNs - compilationStartTimeNs);
    brokerResponse.setTimeUsedMs(totalTimeMs);
    requestStatistics.setQueryProcessingTime(totalTimeMs);
    requestStatistics.setStatistics(brokerResponse);

    LOGGER.debug("Broker Response: {}", brokerResponse);

    // Please keep the format as name=value comma-separated with no spaces
    // Please add new entries at the end
    if (_queryLogRateLimiter.tryAcquire() || forceLog(brokerResponse, totalTimeMs)) {
      // Table name might have been changed (with suffix _OFFLINE/_REALTIME appended)
      LOGGER.info("requestId={},table={},timeMs={},docs={}/{},entries={}/{},"
              + "segments(queried/processed/matched/consuming/unavailable):{}/{}/{}/{}/{},consumingFreshnessTimeMs={},"
              + "servers={}/{},groupLimitReached={},brokerReduceTimeMs={},exceptions={},serverStats={},query={},"
              + "offlineThreadCpuTimeNs={},realtimeThreadCpuTimeNs={}", requestId,
          brokerRequest.getQuerySource().getTableName(), totalTimeMs, brokerResponse.getNumDocsScanned(),
          brokerResponse.getTotalDocs(), brokerResponse.getNumEntriesScannedInFilter(),
          brokerResponse.getNumEntriesScannedPostFilter(), brokerResponse.getNumSegmentsQueried(),
          brokerResponse.getNumSegmentsProcessed(), brokerResponse.getNumSegmentsMatched(),
          brokerResponse.getNumConsumingSegmentsQueried(), numUnavailableSegments,
          brokerResponse.getMinConsumingFreshnessTimeMs(), brokerResponse.getNumServersResponded(),
          brokerResponse.getNumServersQueried(), brokerResponse.isNumGroupsLimitReached(),
          requestStatistics.getReduceTimeMillis(), brokerResponse.getExceptionsSize(), serverStats.getServerStats(),
          StringUtils.substring(query, 0, _queryLogLength), brokerResponse.getOfflineThreadCpuTimeNs(),
          brokerResponse.getRealtimeThreadCpuTimeNs());

      // Limit the dropping log message at most once per second.
      if (_numDroppedLogRateLimiter.tryAcquire()) {
        // NOTE: the reported number may not be accurate since we will be missing some increments happened between
        // get() and set().
        int numDroppedLog = _numDroppedLog.get();
        if (numDroppedLog > 0) {
          LOGGER.info("{} logs were dropped. (log max rate per second: {})", numDroppedLog,
              _queryLogRateLimiter.getRate());
          _numDroppedLog.set(0);
        }
      }
    } else {
      // Increment the count for dropped log
      _numDroppedLog.incrementAndGet();
    }
    return brokerResponse;
  }

  /**
   * Handles the subquery in the given PQL query.
   * <p>Currently only supports subquery within the filter.
   */
  @Deprecated
  private void handleSubquery(BrokerRequest brokerRequest, long requestId, JsonNode jsonRequest,
      @Nullable RequesterIdentity requesterIdentity, RequestStatistics requestStatistics)
      throws Exception {
    FilterQueryMap filterSubQueryMap = brokerRequest.getFilterSubQueryMap();
    if (filterSubQueryMap != null) {
      for (FilterQuery filterQuery : filterSubQueryMap.getFilterQueryMap().values()) {
        String column = filterQuery.getColumn();
        if (column != null) {
          TransformExpressionTree expression = TransformExpressionTree.compileToExpressionTree(column);
          handleSubquery(expression, requestId, jsonRequest, requesterIdentity, requestStatistics);
          filterQuery.setColumn(expression.toString());
        }
      }
    }
  }

  /**
   * Handles the subquery in the given PQL expression.
   * <p>When subquery is detected, first executes the subquery and gets the response, then rewrites the expression with
   * the subquery response.
   * <p>Currently only supports ID_SET subquery within the IN_SUBQUERY transform function, which will be rewritten to an
   * IN_ID_SET transform function.
   */
  @Deprecated
  private void handleSubquery(TransformExpressionTree expression, long requestId, JsonNode jsonRequest,
      @Nullable RequesterIdentity requesterIdentity, RequestStatistics requestStatistics)
      throws Exception {
    if (expression.getExpressionType() != TransformExpressionTree.ExpressionType.FUNCTION) {
      return;
    }
    List<TransformExpressionTree> children = expression.getChildren();
    if (StringUtils.remove(expression.getValue(), '_').equalsIgnoreCase(IN_SUBQUERY)) {
      Preconditions.checkState(children.size() == 2, "IN_SUBQUERY requires 2 arguments: expression, subquery");
      TransformExpressionTree subqueryExpression = children.get(1);
      Preconditions.checkState(subqueryExpression.getExpressionType() == TransformExpressionTree.ExpressionType.LITERAL,
          "Second argument of IN_SUBQUERY must be a literal (subquery)");
      String subquery = subqueryExpression.getValue();
      BrokerResponseNative response =
          handlePQLRequest(requestId, subquery, jsonRequest, requesterIdentity, requestStatistics);
      if (response.getExceptionsSize() != 0) {
        throw new RuntimeException("Caught exception while executing subquery: " + subquery);
      }

      String serializedIdSet = (String) response.getAggregationResults().get(0).getValue();
      expression.setValue(TransformFunctionType.INIDSET.name());
      children
          .set(1, new TransformExpressionTree(TransformExpressionTree.ExpressionType.LITERAL, serializedIdSet, null));
    } else {
      for (TransformExpressionTree child : children) {
        handleSubquery(child, requestId, jsonRequest, requesterIdentity, requestStatistics);
      }
    }
  }

  /**
   * Handles the subquery in the given SQL query.
   * <p>Currently only supports subquery within the filter.
   */
  private void handleSubquery(PinotQuery pinotQuery, long requestId, JsonNode jsonRequest,
      @Nullable RequesterIdentity requesterIdentity, RequestStatistics requestStatistics)
      throws Exception {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      handleSubquery(filterExpression, requestId, jsonRequest, requesterIdentity, requestStatistics);
    }
  }

  /**
   * Handles the subquery in the given SQL expression.
   * <p>When subquery is detected, first executes the subquery and gets the response, then rewrites the expression with
   * the subquery response.
   * <p>Currently only supports ID_SET subquery within the IN_SUBQUERY transform function, which will be rewritten to an
   * IN_ID_SET transform function.
   */
  private void handleSubquery(Expression expression, long requestId, JsonNode jsonRequest,
      @Nullable RequesterIdentity requesterIdentity, RequestStatistics requestStatistics)
      throws Exception {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    List<Expression> operands = function.getOperands();
    if (StringUtils.remove(function.getOperator(), '_').equalsIgnoreCase(IN_SUBQUERY)) {
      Preconditions.checkState(operands.size() == 2, "IN_SUBQUERY requires 2 arguments: expression, subquery");
      Literal subqueryLiteral = operands.get(1).getLiteral();
      Preconditions.checkState(subqueryLiteral != null, "Second argument of IN_SUBQUERY must be a literal (subquery)");
      String subquery = subqueryLiteral.getStringValue();
      BrokerResponseNative response =
          handleSQLRequest(requestId, subquery, jsonRequest, requesterIdentity, requestStatistics);
      if (response.getExceptionsSize() != 0) {
        throw new RuntimeException("Caught exception while executing subquery: " + subquery);
      }
      String serializedIdSet = (String) response.getResultTable().getRows().get(0)[0];
      function.setOperator(TransformFunctionType.INIDSET.name());
      operands.set(1, RequestUtils.getLiteralExpression(serializedIdSet));
    } else {
      for (Expression operand : operands) {
        handleSubquery(operand, requestId, jsonRequest, requesterIdentity, requestStatistics);
      }
    }
  }

  /**
   * Resolves the actual table name for:
   * - Case-insensitive cluster
   * - Table name in the format of [database_name].[table_name]
   *
   * Drop the database part if there is no existing table in the format of [database_name].[table_name], but only
   * [table_name].
   */
  private String getActualTableName(String tableName) {
    // Use TableCache to handle case-insensitive table name
    if (_tableCache.isCaseInsensitive()) {
      String actualTableName = _tableCache.getActualTableName(tableName);
      if (actualTableName != null) {
        return actualTableName;
      }

      // Check if table is in the format of [database_name].[table_name]
      String[] tableNameSplits = StringUtils.split(tableName, ".", 2);
      if (tableNameSplits.length == 2) {
        actualTableName = _tableCache.getActualTableName(tableNameSplits[1]);
        if (actualTableName != null) {
          return actualTableName;
        }
      }

      return tableName;
    }

    // Check if table is in the format of [database_name].[table_name]
    String[] tableNameSplits = StringUtils.split(tableName, ".", 2);
    if (tableNameSplits.length != 2) {
      return tableName;
    }

    // Use RoutingManager to handle case-sensitive table name
    // Update table name if there is no existing table in the format of [database_name].[table_name] but only [table_name]
    if (TableNameBuilder.isTableResource(tableName)) {
      if (_routingManager.routingExists(tableNameSplits[1]) && !_routingManager.routingExists(tableName)) {
        return tableNameSplits[1];
      } else {
        return tableName;
      }
    }
    if (_routingManager.routingExists(TableNameBuilder.OFFLINE.tableNameWithType(tableNameSplits[1]))
        && !_routingManager.routingExists(TableNameBuilder.OFFLINE.tableNameWithType(tableName))) {
      return tableNameSplits[1];
    }
    if (_routingManager.routingExists(TableNameBuilder.REALTIME.tableNameWithType(tableNameSplits[1]))
        && !_routingManager.routingExists(TableNameBuilder.REALTIME.tableNameWithType(tableName))) {
      return tableNameSplits[1];
    }
    return tableName;
  }

  /**
   * Sets the table name in the given broker request (SQL and PQL)
   * NOTE: Set table name in broker request even for SQL query because it is used for access control, query routing etc.
   */
  private void setTableName(BrokerRequest brokerRequest, String tableName) {
    brokerRequest.getQuerySource().setTableName(tableName);
    if (brokerRequest.getPinotQuery() != null) {
      brokerRequest.getPinotQuery().getDataSource().setTableName(tableName);
    }
  }

  /**
   * Sets HyperLogLog log2m for DistinctCountHLL functions if not explicitly set.
   */
  @Deprecated
  private static void handleHyperloglogLog2mOverride(BrokerRequest brokerRequest, int hllLog2mOverride) {
    if (brokerRequest.getAggregationsInfo() != null) {
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        switch (aggregationInfo.getAggregationType().toUpperCase()) {
          case "DISTINCTCOUNTHLL":
          case "DISTINCTCOUNTHLLMV":
          case "DISTINCTCOUNTRAWHLL":
          case "DISTINCTCOUNTRAWHLLMV":
            if (aggregationInfo.getExpressionsSize() == 1) {
              aggregationInfo.addToExpressions(Integer.toString(hllLog2mOverride));
            }
        }
      }
    }
  }

  /**
   * Sets HyperLogLog log2m for DistinctCountHLL functions if not explicitly set for the given SQL query.
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
   * Sets HyperLogLog log2m for DistinctCountHLL functions if not explicitly set for the given SQL expression.
   */
  private static void handleHLLLog2mOverride(Expression expression, int hllLog2mOverride) {
    Function functionCall = expression.getFunctionCall();
    if (functionCall == null) {
      return;
    }
    switch (functionCall.getOperator().toUpperCase()) {
      case "DISTINCTCOUNTHLL":
      case "DISTINCTCOUNTHLLMV":
      case "DISTINCTCOUNTRAWHLL":
      case "DISTINCTCOUNTRAWHLLMV":
        if (functionCall.getOperandsSize() == 1) {
          functionCall.addToOperands(RequestUtils.getLiteralExpression(hllLog2mOverride));
        }
        return;
    }
    if (functionCall.getOperandsSize() > 0) {
      for (Expression operand : functionCall.getOperands()) {
        handleHLLLog2mOverride(operand, hllLog2mOverride);
      }
    }
  }

  /**
   * Overrides the LIMIT/TOP of the given PQL query if it exceeds the query limit.
   */
  @Deprecated
  @VisibleForTesting
  static void handleQueryLimitOverride(BrokerRequest brokerRequest, int queryLimit) {
    if (queryLimit > 0) {
      // Handle GroupBy for BrokerRequest
      if (brokerRequest.getGroupBy() != null) {
        if (brokerRequest.getGroupBy().getTopN() > queryLimit) {
          brokerRequest.getGroupBy().setTopN(queryLimit);
        }
      }

      // Handle Selection for BrokerRequest
      if (brokerRequest.getLimit() > queryLimit) {
        brokerRequest.setLimit(queryLimit);
      }
    }
  }

  /**
   * Overrides the LIMIT of the given SQL query if it exceeds the query limit.
   */
  @VisibleForTesting
  static void handleQueryLimitOverride(PinotQuery pinotQuery, int queryLimit) {
    if (queryLimit > 0 && pinotQuery.getLimit() > queryLimit) {
      pinotQuery.setLimit(queryLimit);
    }
  }

  /**
   * Rewrites 'DistinctCount' to 'DistinctCountBitmap' for the given PQL broker request.
   */
  @Deprecated
  private static void handleDistinctCountBitmapOverride(BrokerRequest brokerRequest) {
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo != null) {
      for (AggregationInfo aggregationInfo : aggregationsInfo) {
        if (StringUtils.remove(aggregationInfo.getAggregationType(), '_')
            .equalsIgnoreCase(AggregationFunctionType.DISTINCTCOUNT.name())) {
          aggregationInfo.setAggregationType(AggregationFunctionType.DISTINCTCOUNTBITMAP.name());
        }
      }
    }
  }

  /**
   * Rewrites 'DistinctCount' to 'DistinctCountBitmap' for the given SQL query.
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
   * Rewrites 'DistinctCount' to 'DistinctCountBitmap' for the given SQL expression.
   */
  private static void handleDistinctCountBitmapOverride(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    if (StringUtils.remove(function.getOperator(), '_')
        .equalsIgnoreCase(AggregationFunctionType.DISTINCTCOUNT.name())) {
      function.setOperator(AggregationFunctionType.DISTINCTCOUNTBITMAP.name());
    } else {
      for (Expression operand : function.getOperands()) {
        handleDistinctCountBitmapOverride(operand);
      }
    }
  }

  /**
   * Returns {@code true} if the given SQL query only contains literals, {@code false} otherwise.
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
   * Processes the literal only SQL query.
   */
  private BrokerResponseNative processLiteralOnlyQuery(PinotQuery pinotQuery, long compilationStartTimeNs,
      RequestStatistics requestStatistics) {
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
    requestStatistics.setQueryProcessingTime(totalTimeMs);
    requestStatistics.setStatistics(brokerResponse);
    return brokerResponse;
  }

  // TODO(xiangfu): Move Literal function computation here from Calcite Parser.
  private void computeResultsForExpression(Expression e, List<String> columnNames,
      List<DataSchema.ColumnDataType> columnTypes, List<Object> row) {
    if (e.getType() == ExpressionType.LITERAL) {
      computeResultsForLiteral(e.getLiteral(), columnNames, columnTypes, row);
    }
    if (e.getType() == ExpressionType.FUNCTION) {
      if (e.getFunctionCall().getOperator().equalsIgnoreCase(SqlKind.AS.toString())) {
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
    }
  }

  /**
   * Fixes the column names to the actual column names in the given SQL query.
   */
  private void updateColumnNames(String rawTableName, PinotQuery pinotQuery) {
    Map<String, String> columnNameMap =
        _tableCache.isCaseInsensitive() ? _tableCache.getColumnNameMap(rawTableName) : null;
    if (pinotQuery != null) {
      for (Expression expression : pinotQuery.getSelectList()) {
        fixColumnName(rawTableName, expression, columnNameMap);
      }
      Expression filterExpression = pinotQuery.getFilterExpression();
      if (filterExpression != null) {
        fixColumnName(rawTableName, filterExpression, columnNameMap);
      }
      List<Expression> groupByList = pinotQuery.getGroupByList();
      if (groupByList != null) {
        for (Expression expression : groupByList) {
          fixColumnName(rawTableName, expression, columnNameMap);
        }
      }
      List<Expression> orderByList = pinotQuery.getOrderByList();
      if (orderByList != null) {
        for (Expression expression : orderByList) {
          // NOTE: Order-by is always a Function with the ordering of the Expression
          fixColumnName(rawTableName, expression.getFunctionCall().getOperands().get(0), columnNameMap);
        }
      }
      Expression havingExpression = pinotQuery.getHavingExpression();
      if (havingExpression != null) {
        fixColumnName(rawTableName, havingExpression, columnNameMap);
      }
    }
  }

  /**
   * Fixes the column names to the actual column names in the given broker request.
   */
  @Deprecated
  private void updateColumnNames(String rawTableName, BrokerRequest brokerRequest) {
    Map<String, String> columnNameMap =
        _tableCache.isCaseInsensitive() ? _tableCache.getColumnNameMap(rawTableName) : null;

    if (brokerRequest.getFilterSubQueryMap() != null) {
      Collection<FilterQuery> values = brokerRequest.getFilterSubQueryMap().getFilterQueryMap().values();
      for (FilterQuery filterQuery : values) {
        if (filterQuery.getNestedFilterQueryIdsSize() == 0) {
          filterQuery.setColumn(fixColumnName(rawTableName, filterQuery.getColumn(), columnNameMap));
        }
      }
    }
    if (brokerRequest.isSetAggregationsInfo()) {
      for (AggregationInfo info : brokerRequest.getAggregationsInfo()) {
        String functionName = StringUtils.remove(info.getAggregationType(), '_');
        if (!functionName.equalsIgnoreCase(AggregationFunctionType.COUNT.getName())) {
          // Always read from backward compatible api in AggregationFunctionUtils.
          List<String> arguments = AggregationFunctionUtils.getArguments(info);

          if (functionName.equalsIgnoreCase(AggregationFunctionType.DISTINCT.getName())) {
            // For DISTINCT query, all arguments are expressions
            arguments.replaceAll(e -> fixColumnName(rawTableName, e, columnNameMap));
          } else {
            // For non-DISTINCT query, only the first argument is expression, others are literals
            // NOTE: We skip fixing the literal arguments because of the legacy behavior of PQL compiler treating string
            //       literal as identifier in the aggregation function.
            arguments.set(0, fixColumnName(rawTableName, arguments.get(0), columnNameMap));
          }
          info.setExpressions(arguments);
        }
      }
      if (brokerRequest.isSetGroupBy()) {
        List<String> expressions = brokerRequest.getGroupBy().getExpressions();
        for (int i = 0; i < expressions.size(); i++) {
          expressions.set(i, fixColumnName(rawTableName, expressions.get(i), columnNameMap));
        }
      }
    } else {
      Selection selection = brokerRequest.getSelections();
      List<String> selectionColumns = selection.getSelectionColumns();
      for (int i = 0; i < selectionColumns.size(); i++) {
        String expression = selectionColumns.get(i);
        if (!expression.equals("*")) {
          selectionColumns.set(i, fixColumnName(rawTableName, expression, columnNameMap));
        }
      }
    }
    if (brokerRequest.isSetOrderBy()) {
      List<SelectionSort> orderBy = brokerRequest.getOrderBy();
      for (SelectionSort selectionSort : orderBy) {
        String expression = selectionSort.getColumn();
        selectionSort.setColumn(fixColumnName(rawTableName, expression, columnNameMap));
      }
    }
  }

  @Deprecated
  private String fixColumnName(String rawTableName, String expression, @Nullable Map<String, String> columnNameMap) {
    TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
    fixColumnName(rawTableName, expressionTree, columnNameMap);
    return expressionTree.toString();
  }

  @Deprecated
  private void fixColumnName(String rawTableName, TransformExpressionTree expression,
      @Nullable Map<String, String> columnNameMap) {
    TransformExpressionTree.ExpressionType expressionType = expression.getExpressionType();
    if (expressionType == TransformExpressionTree.ExpressionType.IDENTIFIER) {
      expression.setValue(getActualColumnName(rawTableName, expression.getValue(), columnNameMap));
    } else if (expressionType == TransformExpressionTree.ExpressionType.FUNCTION) {
      for (TransformExpressionTree child : expression.getChildren()) {
        fixColumnName(rawTableName, child, columnNameMap);
      }
    }
  }

  /**
   * Fixes the column names to the actual column names in the given SQL expression.
   */
  private void fixColumnName(String rawTableName, Expression expression, @Nullable Map<String, String> columnNameMap) {
    ExpressionType expressionType = expression.getType();
    if (expressionType == ExpressionType.IDENTIFIER) {
      Identifier identifier = expression.getIdentifier();
      identifier.setName(getActualColumnName(rawTableName, identifier.getName(), columnNameMap));
    } else if (expressionType == ExpressionType.FUNCTION) {
      for (Expression operand : expression.getFunctionCall().getOperands()) {
        fixColumnName(rawTableName, operand, columnNameMap);
      }
    }
  }

  /**
   * Returns the actual column name for the given column name for:
   * - Case-insensitive cluster
   * - Column name in the format of [table_name].[column_name]
   */
  private String getActualColumnName(String rawTableName, String columnName,
      @Nullable Map<String, String> columnNameMap) {
    // Check if column is in the format of [table_name].[column_name]
    String[] splits = StringUtils.split(columnName, ".", 2);
    if (_tableCache.isCaseInsensitive()) {
      if (splits.length == 2 && rawTableName.equalsIgnoreCase(splits[0])) {
        columnName = splits[1];
      }
      if (columnNameMap != null) {
        return columnNameMap.getOrDefault(columnName.toLowerCase(), columnName);
      } else {
        return columnName;
      }
    } else {
      if (splits.length == 2 && rawTableName.equals(splits[0])) {
        columnName = splits[1];
      }
      return columnName;
    }
  }

  private static Map<String, String> getOptionsFromJson(JsonNode request, String optionsKey) {
    return Splitter.on(';').omitEmptyStrings().trimResults().withKeyValueSeparator('=')
        .split(request.get(optionsKey).asText());
  }

  /**
   * Helper function to decide whether to force the log
   *
   * TODO: come up with other criteria for forcing a log and come up with better numbers
   *
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
   * Sets extra options for the given SQL query.
   */
  @VisibleForTesting
  static void setOptions(PinotQuery pinotQuery, long requestId, String query, JsonNode jsonRequest) {
    Map<String, String> queryOptions = new HashMap<>();
    if (jsonRequest.has(Broker.Request.QUERY_OPTIONS)) {
      Map<String, String> queryOptionsFromJson = getOptionsFromJson(jsonRequest, Broker.Request.QUERY_OPTIONS);
      queryOptions.putAll(queryOptionsFromJson);
    }
    Map<String, String> queryOptionsFromQuery = pinotQuery.getQueryOptions();
    if (queryOptionsFromQuery != null) {
      queryOptions.putAll(queryOptionsFromQuery);
    }
    boolean enableTrace = jsonRequest.has(Broker.Request.TRACE) && jsonRequest.get(Broker.Request.TRACE).asBoolean();
    if (enableTrace) {
      queryOptions.put(Broker.Request.TRACE, "true");
    }
    // NOTE: Always set query options because we will put 'timeoutMs' later
    pinotQuery.setQueryOptions(queryOptions);
    if (!queryOptions.isEmpty()) {
      LOGGER.debug("Query options are set to: {} for request {}: {}", queryOptions, requestId, query);
    }

    if (jsonRequest.has(Broker.Request.DEBUG_OPTIONS)) {
      Map<String, String> debugOptions = getOptionsFromJson(jsonRequest, Broker.Request.DEBUG_OPTIONS);
      if (!debugOptions.isEmpty()) {
        LOGGER.debug("Debug options are set to: {} for request {}: {}", debugOptions, requestId, query);
        pinotQuery.setDebugOptions(debugOptions);
      }
    }
  }

  /**
   * Sets extra options for the given PQL query.
   */
  @Deprecated
  @VisibleForTesting
  static void setOptions(long requestId, String query, JsonNode jsonRequest, BrokerRequest brokerRequest) {
    Map<String, String> queryOptions = new HashMap<>();
    if (jsonRequest.has(Broker.Request.QUERY_OPTIONS)) {
      Map<String, String> queryOptionsFromJson = getOptionsFromJson(jsonRequest, Broker.Request.QUERY_OPTIONS);
      queryOptions.putAll(queryOptionsFromJson);
    }
    Map<String, String> queryOptionsFromBrokerRequest = brokerRequest.getQueryOptions();
    if (queryOptionsFromBrokerRequest != null) {
      queryOptions.putAll(queryOptionsFromBrokerRequest);
    }
    boolean enableTrace = jsonRequest.has(Broker.Request.TRACE) && jsonRequest.get(Broker.Request.TRACE).asBoolean();
    if (enableTrace) {
      queryOptions.put(Broker.Request.TRACE, "true");
    }
    // NOTE: Always set query options because we will put 'timeoutMs' later
    brokerRequest.setQueryOptions(queryOptions);
    if (!queryOptions.isEmpty()) {
      LOGGER.debug("Query options are set to: {} for request {}: {}", queryOptions, requestId, query);
    }

    if (jsonRequest.has(Broker.Request.DEBUG_OPTIONS)) {
      Map<String, String> debugOptions = getOptionsFromJson(jsonRequest, Broker.Request.DEBUG_OPTIONS);
      if (!debugOptions.isEmpty()) {
        LOGGER.debug("Debug options are set to: {} for request {}: {}", debugOptions, requestId, query);
        brokerRequest.setDebugOptions(debugOptions);
      }
    }
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
    Long queryLevelTimeoutMs = QueryOptions.getTimeoutMs(queryOptions);
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
      String errorMessage = String
          .format("Query timed out (time spent: %dms, timeout: %dms) for table: %s before scattering the request",
              timeSpentMs, queryTimeoutMs, tableNameWithType);
      throw new TimeoutException(errorMessage);
    }
    queryOptions.put(Broker.Request.QueryOptionKey.TIMEOUT_MS, Long.toString(remainingTimeMs));
    return remainingTimeMs;
  }

  /**
   * Broker side validation on the PQL broker request.
   * <p>Throw exception if query does not pass validation.
   * <p>Current validations are:
   * <ul>
   *   <li>Value for 'TOP' for aggregation group-by query <= configured value</li>
   *   <li>Value for 'LIMIT' for selection/distinct query <= configured value</li>
   *   <li>Unsupported DISTINCT queries</li>
   * </ul>
   *
   * NOTES on validation for DISTINCT queries:
   *
   * These DISTINCT queries are not supported
   * (1) SELECT sum(col1), min(col2), DISTINCT(col3, col4) FROM foo
   * (2) SELECT sum(col1), DISTINCT(col2, col3), min(col4) FROM foo
   * (3) SELECT DISTINCT(col1, col2), DISTINCT(col3) FROM foo
   *
   * (4) SELECT DISTINCT(col1, col2), sum(col3), min(col4) FROM foo
   *
   * (5) SELECT DISTINCT(col1, col2) FROM foo ORDER BY col1, col2
   * (6) SELECT DISTINCT(col1, col2) FROM foo GROUP BY col1
   *
   * (1), (2) and (3) are not valid SQL queries and so PQL won't support
   * them too.
   *
   * (4) is a valid SQL query for multi column distinct. It will output
   * exactly 1 row by taking [col1, col2, sum(col3) and min(col4)] as one entire column
   * set as an input into DISTINCT. However, we can't support it
   * since DISTINCT, sum and min are implemented as independent aggregation
   * functions. So unless the output of sum and min is piped into DISTINCT,
   * we can't execute this query.
   *
   * DISTINCT is currently not supported with ORDER BY and GROUP BY and
   * so we throw exceptions for (5) and (6)
   *
   * NOTE: There are other two other types of queries that should ideally not be
   * supported but we let them go through since that behavior has been there
   * from a long time
   *
   * SELECT DISTINCT(col1), col2, col3 FROM foo
   *
   * The above query is both a selection and aggregation query.
   * The reason this is a bad query is that DISTINCT(col1) will output
   * potentially less number of rows than entire dataset, whereas all
   * column values from col2 and col3 will be selected. So the output
   * does not make sense.
   *
   * However, when the broker request is built in {@link org.apache.pinot.pql.parsers.pql2.ast.SelectAstNode},
   * we check if it has both aggregation and selections and set the latter to NULL.
   * Thus we execute such queries as if the user had only specified the aggregation in
   * the query.
   *
   * SELECT DISTINCT(COL1), transform_func(col2) FROM foo
   *
   * The same reason is applicable to the above query too. It is a combination
   * of aggregation (DISTINCT) and selection (transform) and we just execute
   * the aggregation.
   *
   * Note that DISTINCT(transform_func(col)) is supported as in this case,
   * the output of transform_func(col) is piped into DISTINCT.
   */
  @Deprecated
  @VisibleForTesting
  static void validateRequest(BrokerRequest brokerRequest, int queryResponseLimit) {
    // verify LIMIT
    // LIMIT is applicable to selection query or DISTINCT query
    // LIMIT is store in BrokerRequest
    int limit = brokerRequest.getLimit();
    if (limit > queryResponseLimit) {
      throw new IllegalStateException(
          "Value for 'LIMIT' (" + limit + ") exceeds maximum allowed value of " + queryResponseLimit);
    }

    boolean groupBy = false;

    // verify TOP
    if (brokerRequest.isSetGroupBy()) {
      groupBy = true;
      long topN = brokerRequest.getGroupBy().getTopN();
      if (topN > queryResponseLimit) {
        throw new IllegalStateException(
            "Value for 'TOP' (" + topN + ") exceeds maximum allowed value of " + queryResponseLimit);
      }
    }

    // The behavior of GROUP BY with multiple aggregations, is different in PQL vs SQL.
    // As a result, we have 2 groupByModes, to maintain backward compatibility.
    // The results of PQL groupByMode (if numAggregations > 1) cannot be returned in SQL responseFormat, as the results are non-tabular
    // Checking for this upfront, to avoid executing the query and wasting resources
    QueryOptions queryOptions = new QueryOptions(brokerRequest.getQueryOptions());
    if (brokerRequest.isSetAggregationsInfo() && brokerRequest.getGroupBy() != null) {
      if (brokerRequest.getAggregationsInfoSize() > 1 && queryOptions.isResponseFormatSQL() && !queryOptions
          .isGroupByModeSQL()) {
        throw new UnsupportedOperationException(
            "The results of a GROUP BY query with multiple aggregations in PQL is not tabular, and cannot be returned in SQL responseFormat");
      }
    }

    // verify the following for DISTINCT queries:
    // (1) User query does not have DISTINCT() along with any other aggregation function
    // (2) For DISTINCT(column set) with ORDER BY, the order by columns should be some/all columns in column set
    // (3) User query does not have DISTINCT() along with GROUP BY
    if (brokerRequest.isSetAggregationsInfo()) {
      List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();
      int numAggFunctions = aggregationInfos.size();
      for (AggregationInfo aggregationInfo : aggregationInfos) {
        if (aggregationInfo.getAggregationType().equalsIgnoreCase(AggregationFunctionType.DISTINCT.getName())) {
          if (numAggFunctions > 1) {
            throw new UnsupportedOperationException("Aggregation functions cannot be used with DISTINCT");
          }
          if (groupBy) {
            // TODO: Explore if DISTINCT should be supported with GROUP BY
            throw new UnsupportedOperationException("DISTINCT with GROUP BY is currently not supported");
          }
          if (brokerRequest.getLimit() == 0) {
            // TODO: Consider changing it to SELECTION query for LIMIT 0
            throw new UnsupportedOperationException("DISTINCT must have positive LIMIT");
          }
          if (brokerRequest.isSetOrderBy()) {
            Set<String> expressionSet = new HashSet<>(AggregationFunctionUtils.getArguments(aggregationInfo));
            List<SelectionSort> orderByColumns = brokerRequest.getOrderBy();
            for (SelectionSort selectionSort : orderByColumns) {
              if (!expressionSet.contains(selectionSort.getColumn())) {
                throw new UnsupportedOperationException(
                    "ORDER By should be only on some/all of the columns passed as arguments to DISTINCT");
              }
            }
          }
        }
      }
    }
  }

  /**
   * Broker side validation on the SQL query.
   * <p>Throw exception if query does not pass validation.
   * <p>Current validations are:
   * <ul>
   *   <li>Value for 'LIMIT' <= configured value</li>
   *   <li>Query options must be set to SQL mode</li>
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

    // SQL query should always have response format and group-by mode set to SQL
    // TODO: Remove these 2 options after deprecating PQL
    QueryOptions queryOptions = new QueryOptions(pinotQuery.getQueryOptions());
    if (!queryOptions.isGroupByModeSQL() || !queryOptions.isResponseFormatSQL()) {
      throw new IllegalStateException("SQL query should always have response format and group-by mode set to SQL");
    }
  }

  /**
   * Helper method to create an OFFLINE broker request from the given hybrid broker request (SQL or PQL).
   * <p>This step will attach the time boundary to the request.
   */
  private BrokerRequest getOfflineBrokerRequest(BrokerRequest hybridBrokerRequest) {
    BrokerRequest offlineRequest = hybridBrokerRequest.deepCopy();
    String rawTableName = hybridBrokerRequest.getQuerySource().getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    setTableName(offlineRequest, offlineTableName);
    attachTimeBoundary(rawTableName, offlineRequest, true);
    return offlineRequest;
  }

  /**
   * Helper method to create a REALTIME broker request from the given hybrid broker request (SQL or PQL).
   * <p>This step will attach the time boundary to the request.
   */
  private BrokerRequest getRealtimeBrokerRequest(BrokerRequest hybridBrokerRequest) {
    BrokerRequest realtimeRequest = hybridBrokerRequest.deepCopy();
    String rawTableName = hybridBrokerRequest.getQuerySource().getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    setTableName(realtimeRequest, realtimeTableName);
    attachTimeBoundary(rawTableName, realtimeRequest, false);
    return realtimeRequest;
  }

  /**
   * Helper method to attach the time boundary to the given broker request (SQL or PQL).
   */
  private void attachTimeBoundary(String rawTableName, BrokerRequest brokerRequest, boolean isOfflineRequest) {
    TimeBoundaryInfo timeBoundaryInfo =
        _routingManager.getTimeBoundaryInfo(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    if (timeBoundaryInfo == null) {
      LOGGER.warn("Failed to find time boundary info for hybrid table: {}", rawTableName);
      return;
    }

    String timeColumn = timeBoundaryInfo.getTimeColumn();
    String timeValue = timeBoundaryInfo.getTimeValue();
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    if (pinotQuery != null) {
      // SQL

      Expression timeFilterExpression = RequestUtils.getFunctionExpression(
          isOfflineRequest ? FilterKind.LESS_THAN_OR_EQUAL.name() : FilterKind.GREATER_THAN.name());
      timeFilterExpression.getFunctionCall().setOperands(Arrays
          .asList(RequestUtils.createIdentifierExpression(timeColumn), RequestUtils.getLiteralExpression(timeValue)));

      Expression filterExpression = pinotQuery.getFilterExpression();
      if (filterExpression != null) {
        Expression andFilterExpression = RequestUtils.getFunctionExpression(FilterKind.AND.name());
        andFilterExpression.getFunctionCall().setOperands(Arrays.asList(filterExpression, timeFilterExpression));
        pinotQuery.setFilterExpression(andFilterExpression);
      } else {
        pinotQuery.setFilterExpression(timeFilterExpression);
      }
    } else {
      // PQL

      // Create a time range filter
      FilterQuery timeFilterQuery = new FilterQuery();
      // Use -1 to prevent collision with other filters
      timeFilterQuery.setId(-1);
      timeFilterQuery.setColumn(timeColumn);
      String filterValue = isOfflineRequest ? Range.LOWER_UNBOUNDED + timeValue + Range.UPPER_INCLUSIVE
          : Range.LOWER_EXCLUSIVE + timeValue + Range.UPPER_UNBOUNDED;
      timeFilterQuery.setValue(Collections.singletonList(filterValue));
      timeFilterQuery.setOperator(FilterOperator.RANGE);
      timeFilterQuery.setNestedFilterQueryIds(Collections.emptyList());

      // Attach the time range filter to the current filters
      FilterQuery currentFilterQuery = brokerRequest.getFilterQuery();
      if (currentFilterQuery != null) {
        FilterQuery andFilterQuery = new FilterQuery();
        // Use -2 to prevent collision with other filters
        andFilterQuery.setId(-2);
        andFilterQuery.setOperator(FilterOperator.AND);
        List<Integer> nestedFilterQueryIds = new ArrayList<>(2);
        nestedFilterQueryIds.add(currentFilterQuery.getId());
        nestedFilterQueryIds.add(timeFilterQuery.getId());
        brokerRequest.setFilterQuery(andFilterQuery);

        andFilterQuery.setNestedFilterQueryIds(nestedFilterQueryIds);
        FilterQueryMap filterSubQueryMap = brokerRequest.getFilterSubQueryMap();
        filterSubQueryMap.putToFilterQueryMap(-1, timeFilterQuery);
        filterSubQueryMap.putToFilterQueryMap(-2, andFilterQuery);
        brokerRequest.setFilterSubQueryMap(filterSubQueryMap);
      } else {
        FilterQueryMap filterSubQueryMap = new FilterQueryMap();
        filterSubQueryMap.putToFilterQueryMap(-1, timeFilterQuery);
        brokerRequest.setFilterQuery(timeFilterQuery);
        brokerRequest.setFilterSubQueryMap(filterSubQueryMap);
      }
    }
  }

  /**
   * Processes the optimized broker requests for both OFFLINE and REALTIME table.
   */
  protected abstract BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable Map<ServerInstance, List<String>> offlineRoutingTable,
      @Nullable BrokerRequest realtimeBrokerRequest, @Nullable Map<ServerInstance, List<String>> realtimeRoutingTable,
      long timeoutMs, ServerStats serverStats, RequestStatistics requestStatistics)
      throws Exception;

  /**
   * Helper class to pass the per server statistics.
   */
  protected static class ServerStats {
    private String _serverStats;

    public String getServerStats() {
      return _serverStats;
    }

    public void setServerStats(String serverStats) {
      _serverStats = serverStats;
    }
  }
}
