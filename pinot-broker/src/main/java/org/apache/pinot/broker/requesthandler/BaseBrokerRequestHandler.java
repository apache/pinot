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
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.RateLimiter;
import java.net.InetAddress;
import java.util.ArrayList;
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
import org.apache.pinot.common.function.AggregationFunctionType;
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
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Broker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.helix.TableCache;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.requesthandler.BrokerRequestOptimizer;
import org.apache.pinot.core.requesthandler.PinotQueryParserFactory;
import org.apache.pinot.core.requesthandler.PinotQueryRequest;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseBrokerRequestHandler implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerRequestHandler.class);

  protected final PinotConfiguration _config;
  protected final RoutingManager _routingManager;
  protected final AccessControlFactory _accessControlFactory;
  protected final QueryQuotaManager _queryQuotaManager;
  protected final TableCache _tableCache;
  protected final BrokerMetrics _brokerMetrics;

  protected final AtomicLong _requestIdGenerator = new AtomicLong();
  protected final BrokerRequestOptimizer _brokerRequestOptimizer = new BrokerRequestOptimizer();
  protected final BrokerReduceService _brokerReduceService = new BrokerReduceService();

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

  @SuppressWarnings("Duplicates")
  @Override
  public BrokerResponse handleRequest(JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestStatistics requestStatistics)
      throws Exception {
    long requestId = _requestIdGenerator.incrementAndGet();
    requestStatistics.setBrokerId(_brokerId);
    requestStatistics.setRequestId(requestId);
    requestStatistics.setRequestArrivalTimeMillis(System.currentTimeMillis());

    PinotQueryRequest pinotQueryRequest = getPinotQueryRequest(request);
    String query = pinotQueryRequest.getQuery();
    LOGGER.debug("Query string for request {}: {}", requestId, pinotQueryRequest.getQuery());
    requestStatistics.setPql(query);

    // Compile the request
    long compilationStartTimeNs = System.nanoTime();
    BrokerRequest brokerRequest;
    try {
      brokerRequest = PinotQueryParserFactory.get(pinotQueryRequest.getQueryFormat()).compileToBrokerRequest(query);
    } catch (Exception e) {
      LOGGER.info("Caught exception while compiling request {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      requestStatistics.setErrorCode(QueryException.PQL_PARSING_ERROR_CODE);
      return new BrokerResponseNative(QueryException.getException(QueryException.PQL_PARSING_ERROR, e));
    }
    if (isLiteralOnlyQuery(brokerRequest)) {
      LOGGER.debug("Request {} contains only Literal, skipping server query: {}", requestId, query);
      try {
        return processLiteralOnlyBrokerRequest(brokerRequest, compilationStartTimeNs, requestStatistics);
      } catch (Exception e) {
        // TODO: refine the exceptions here to early termination the queries won't requires to send to servers.
        LOGGER
            .warn("Unable to execute literal request {}: {} at broker, fallback to server query. {}", requestId, query,
                e.getMessage());
      }
    }
    updateTableName(brokerRequest);
    try {
      updateColumnNames(brokerRequest);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while updating Column names in Query {}: {}, {}", requestId, query, e);
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
    String tableName = brokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    requestStatistics.setTableName(rawTableName);
    long compilationEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REQUEST_COMPILATION,
        compilationEndTimeNs - compilationStartTimeNs);
    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERIES, 1);

    // Check table access
    boolean hasAccess = _accessControlFactory.create().hasAccess(requesterIdentity, brokerRequest);
    if (!hasAccess) {
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

    // Set extra settings into broker request
    setOptions(requestId, query, request, brokerRequest);

    // Optimize the query
    // TODO: get time column name from schema or table config so that we can apply it for REALTIME only case
    // We get timeColumnName from time boundary service currently, which only exists for offline table
    String timeColumn = getTimeColumnName(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    if ((offlineTableName != null) && (realtimeTableName != null)) {
      // Hybrid
      offlineBrokerRequest = _brokerRequestOptimizer.optimize(getOfflineBrokerRequest(brokerRequest), timeColumn);
      realtimeBrokerRequest = _brokerRequestOptimizer.optimize(getRealtimeBrokerRequest(brokerRequest), timeColumn);
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.HYBRID);
    } else if (offlineTableName != null) {
      // OFFLINE only
      brokerRequest.getQuerySource().setTableName(offlineTableName);
      offlineBrokerRequest = _brokerRequestOptimizer.optimize(brokerRequest, timeColumn);
      requestStatistics.setFanoutType(RequestStatistics.FanoutType.OFFLINE);
    } else {
      // REALTIME only
      brokerRequest.getQuerySource().setTableName(realtimeTableName);
      realtimeBrokerRequest = _brokerRequestOptimizer.optimize(brokerRequest, timeColumn);
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
    BrokerResponse brokerResponse =
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
              + "servers={}/{},groupLimitReached={},brokerReduceTimeMs={},exceptions={},serverStats={},query={}", requestId,
          brokerRequest.getQuerySource().getTableName(), totalTimeMs, brokerResponse.getNumDocsScanned(),
          brokerResponse.getTotalDocs(), brokerResponse.getNumEntriesScannedInFilter(),
          brokerResponse.getNumEntriesScannedPostFilter(), brokerResponse.getNumSegmentsQueried(),
          brokerResponse.getNumSegmentsProcessed(), brokerResponse.getNumSegmentsMatched(),
          brokerResponse.getNumConsumingSegmentsQueried(), numUnavailableSegments,
          brokerResponse.getMinConsumingFreshnessTimeMs(), brokerResponse.getNumServersResponded(),
          brokerResponse.getNumServersQueried(), brokerResponse.isNumGroupsLimitReached(),
          requestStatistics.getReduceTimeMillis(), brokerResponse.getExceptionsSize(), serverStats.getServerStats(),
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
    return brokerResponse;
  }

  /**
   * Check if table is in the format of [database_name].[table_name].
   *
   * Only update TableName in QuerySource if there is no existing table in the format of [database_name].[table_name],
   * but only [table_name].
   */
  private void updateTableName(BrokerRequest brokerRequest) {
    String tableName = brokerRequest.getQuerySource().getTableName();

    // Use TableCache to handle case-insensitive table name
    if (_tableCache.isCaseInsensitive()) {
      String actualTableName = _tableCache.getActualTableName(tableName);
      if (actualTableName != null) {
        setTableName(brokerRequest, actualTableName);
        return;
      }

      // Check if table is in the format of [database_name].[table_name]
      String[] tableNameSplits = StringUtils.split(tableName, ".", 2);
      if (tableNameSplits.length == 2) {
        actualTableName = _tableCache.getActualTableName(tableNameSplits[1]);
        if (actualTableName != null) {
          setTableName(brokerRequest, actualTableName);
          return;
        }
      }

      return;
    }

    // Check if table is in the format of [database_name].[table_name]
    String[] tableNameSplits = StringUtils.split(tableName, ".", 2);
    if (tableNameSplits.length != 2) {
      return;
    }

    // Use RoutingManager to handle case-sensitive table name
    // Update table name if there is no existing table in the format of [database_name].[table_name] but only [table_name]
    if (TableNameBuilder.isTableResource(tableName)) {
      if (_routingManager.routingExists(tableNameSplits[1]) && !_routingManager.routingExists(tableName)) {
        setTableName(brokerRequest, tableNameSplits[1]);
      }
      return;
    }
    if (_routingManager.routingExists(TableNameBuilder.OFFLINE.tableNameWithType(tableNameSplits[1]))
        && !_routingManager.routingExists(TableNameBuilder.OFFLINE.tableNameWithType(tableName))) {
      setTableName(brokerRequest, tableNameSplits[1]);
      return;
    }
    if (_routingManager.routingExists(TableNameBuilder.REALTIME.tableNameWithType(tableNameSplits[1]))
        && !_routingManager.routingExists(TableNameBuilder.REALTIME.tableNameWithType(tableName))) {
      setTableName(brokerRequest, tableNameSplits[1]);
    }
  }

  private void setTableName(BrokerRequest brokerRequest, String tableName) {
    brokerRequest.getQuerySource().setTableName(tableName);
    if (brokerRequest.getPinotQuery() != null) {
      brokerRequest.getPinotQuery().getDataSource().setTableName(tableName);
    }
  }

  /**
   * Set Log2m value for DistinctCountHLL Function
   * @param brokerRequest
   * @param hllLog2mOverride
   */
  static void handleHyperloglogLog2mOverride(BrokerRequest brokerRequest, int hllLog2mOverride) {
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
    if (brokerRequest.getPinotQuery() != null && brokerRequest.getPinotQuery().getSelectList() != null) {
      for (Expression expr : brokerRequest.getPinotQuery().getSelectList()) {
        updateDistinctCountHllExpr(expr, hllLog2mOverride);
      }
    }
  }

  private static void updateDistinctCountHllExpr(Expression expr, int hllLog2mOverride) {
    Function functionCall = expr.getFunctionCall();
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
        updateDistinctCountHllExpr(operand, hllLog2mOverride);
      }
    }
  }

  /**
   * Reset limit for selection query if it exceeds maxQuerySelectionLimit.
   * @param brokerRequest
   * @param queryLimit
   *
   */
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

      // Handle Selection & GroupBy for PinotQuery
      if (brokerRequest.getPinotQuery() != null) {
        if (brokerRequest.getPinotQuery().getLimit() > queryLimit) {
          brokerRequest.getPinotQuery().setLimit(queryLimit);
        }
      }
    }
  }

  /**
   * Helper method to rewrite 'DistinctCount' with 'DistinctCountBitmap' for the given broker request.
   */
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
    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
    if (pinotQuery != null) {
      for (Expression expression : pinotQuery.getSelectList()) {
        handleDistinctCountBitmapOverride(expression);
      }
      List<Expression> orderByExpressions = pinotQuery.getOrderByList();
      if (orderByExpressions != null) {
        for (Expression expression : orderByExpressions) {
          handleDistinctCountBitmapOverride(expression);
        }
      }
      Expression havingExpression = pinotQuery.getHavingExpression();
      if (havingExpression != null) {
        handleDistinctCountBitmapOverride(havingExpression);
      }
    }
  }

  /**
   * Helper method to rewrite 'DistinctCount' with 'DistinctCountBitmap' for the given expression.
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
   * Check if a SQL parsed BrokerRequest is a literal only query.
   * @param brokerRequest
   * @return true if this query selects only Literals
   *
   */
  @VisibleForTesting
  static boolean isLiteralOnlyQuery(BrokerRequest brokerRequest) {
    if (brokerRequest.getPinotQuery() != null) {
      for (Expression e : brokerRequest.getPinotQuery().getSelectList()) {
        if (!CalciteSqlParser.isLiteralOnlyExpression(e)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Compute BrokerResponse for literal only query.
   *
   * @param brokerRequest
   * @param compilationStartTimeNs
   * @param requestStatistics
   * @return BrokerResponse
   */
  private BrokerResponse processLiteralOnlyBrokerRequest(BrokerRequest brokerRequest, long compilationStartTimeNs,
      RequestStatistics requestStatistics)
      throws IllegalStateException {
    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    List<String> columnNames = new ArrayList<>();
    List<DataSchema.ColumnDataType> columnTypes = new ArrayList<>();
    List<Object> row = new ArrayList<>();
    for (Expression e : brokerRequest.getPinotQuery().getSelectList()) {
      computeResultsForExpression(e, columnNames, columnTypes, row);
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
        columnTypes.add(DataSchema.ColumnDataType.STRING);
        row.add(Boolean.toString(literal.getBoolValue()));
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
   * Fixes the column names to the actual column names in the given broker request.
   */
  private void updateColumnNames(BrokerRequest brokerRequest) {
    String rawTableName = TableNameBuilder.extractRawTableName(brokerRequest.getQuerySource().getTableName());
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

    PinotQuery pinotQuery = brokerRequest.getPinotQuery();
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
          fixColumnName(rawTableName, expression, columnNameMap);
        }
      }
      Expression havingExpression = pinotQuery.getHavingExpression();
      if (havingExpression != null) {
        fixColumnName(rawTableName, havingExpression, columnNameMap);
      }
    }
  }

  private String fixColumnName(String rawTableName, String expression, @Nullable Map<String, String> columnNameMap) {
    TransformExpressionTree expressionTree = TransformExpressionTree.compileToExpressionTree(expression);
    fixColumnName(rawTableName, expressionTree, columnNameMap);
    return expressionTree.toString();
  }

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

  private String getActualColumnName(String rawTableName, String columnName,
      @Nullable Map<String, String> columnNameMap) {
    // Check if column is in the format of [table_name].[column_name]
    String[] splits = StringUtils.split(columnName, ".", 2);
    if (_tableCache.isCaseInsensitive()) {
      if (splits.length == 2 && rawTableName.equalsIgnoreCase(splits[0])) {
        columnName = splits[1];
      }
      if (columnNameMap != null) {
        return columnNameMap.getOrDefault(columnName, columnName);
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

  private PinotQueryRequest getPinotQueryRequest(JsonNode request) {
    if (request.has(Broker.Request.SQL)) {
      return new PinotQueryRequest(Broker.Request.SQL, request.get(Broker.Request.SQL).asText());
    }
    return new PinotQueryRequest(Broker.Request.PQL, request.get(Broker.Request.PQL).asText());
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
   * Sets brokerRequest extra options
   */
  @VisibleForTesting
  static void setOptions(long requestId, String query, JsonNode jsonRequest, BrokerRequest brokerRequest) {
    if (jsonRequest.has(Broker.Request.TRACE) && jsonRequest.get(Broker.Request.TRACE).asBoolean()) {
      LOGGER.debug("Enable trace for request {}: {}", requestId, query);
      brokerRequest.setEnableTrace(true);
    }

    if (jsonRequest.has(Broker.Request.DEBUG_OPTIONS)) {
      Map<String, String> debugOptions = getOptionsFromJson(jsonRequest, Broker.Request.DEBUG_OPTIONS);
      LOGGER.debug("Debug options are set to: {} for request {}: {}", debugOptions, requestId, query);
      brokerRequest.setDebugOptions(debugOptions);
    }

    Map<String, String> queryOptions = new HashMap<>();
    if (jsonRequest.has(Broker.Request.QUERY_OPTIONS)) {
      Map<String, String> queryOptionsFromJson = getOptionsFromJson(jsonRequest, Broker.Request.QUERY_OPTIONS);
      queryOptions.putAll(queryOptionsFromJson);
    }
    Map<String, String> queryOptionsFromBrokerRequest = brokerRequest.getQueryOptions();
    if (queryOptionsFromBrokerRequest != null) {
      queryOptions.putAll(queryOptionsFromBrokerRequest);
    }
    // NOTE: Always set query options because we will put 'timeoutMs' later
    brokerRequest.setQueryOptions(queryOptions);
    if (!queryOptions.isEmpty()) {
      LOGGER
          .debug("Query options are set to: {} for request {}: {}", brokerRequest.getQueryOptions(), requestId, query);
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
              timeSpentMs, queryLevelTimeoutMs, tableNameWithType);
      throw new TimeoutException(errorMessage);
    }
    queryOptions.put(Broker.Request.QueryOptionKey.TIMEOUT_MS, Long.toString(remainingTimeMs));
    return remainingTimeMs;
  }

  /**
   * Broker side validation on the broker request.
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
   * Helper method to get the time column name for the OFFLINE table name from the time boundary service, or
   * <code>null</code> if the time boundary service does not have the information.
   */
  private String getTimeColumnName(String offlineTableName) {
    TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
    return timeBoundaryInfo != null ? timeBoundaryInfo.getTimeColumn() : null;
  }

  /**
   * Helper method to create an OFFLINE broker request from the given hybrid broker request.
   * <p>This step will attach the time boundary to the request.
   */
  private BrokerRequest getOfflineBrokerRequest(BrokerRequest hybridBrokerRequest) {
    BrokerRequest offlineRequest = hybridBrokerRequest.deepCopy();
    String rawTableName = hybridBrokerRequest.getQuerySource().getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    offlineRequest.getQuerySource().setTableName(offlineTableName);
    attachTimeBoundary(rawTableName, offlineRequest, true);
    return offlineRequest;
  }

  /**
   * Helper method to create a REALTIME broker request from the given hybrid broker request.
   * <p>This step will attach the time boundary to the request.
   */
  private BrokerRequest getRealtimeBrokerRequest(BrokerRequest hybridBrokerRequest) {
    BrokerRequest realtimeRequest = hybridBrokerRequest.deepCopy();
    String rawTableName = hybridBrokerRequest.getQuerySource().getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    realtimeRequest.getQuerySource().setTableName(realtimeTableName);
    attachTimeBoundary(rawTableName, realtimeRequest, false);
    return realtimeRequest;
  }

  /**
   * Helper method to attach time boundary to a broker request.
   */
  private void attachTimeBoundary(String rawTableName, BrokerRequest brokerRequest, boolean isOfflineRequest) {
    TimeBoundaryInfo timeBoundaryInfo =
        _routingManager.getTimeBoundaryInfo(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    if (timeBoundaryInfo == null) {
      LOGGER.warn("Failed to find time boundary info for hybrid table: {}", rawTableName);
      return;
    }

    // Create a time range filter
    FilterQuery timeFilterQuery = new FilterQuery();
    // Use -1 to prevent collision with other filters
    timeFilterQuery.setId(-1);
    timeFilterQuery.setColumn(timeBoundaryInfo.getTimeColumn());
    String timeValue = timeBoundaryInfo.getTimeValue();
    String filterValue = isOfflineRequest ? "(*\t\t" + timeValue + "]" : "(" + timeValue + "\t\t*)";
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

  /**
   * Processes the optimized broker requests for both OFFLINE and REALTIME table.
   */
  protected abstract BrokerResponse processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
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
