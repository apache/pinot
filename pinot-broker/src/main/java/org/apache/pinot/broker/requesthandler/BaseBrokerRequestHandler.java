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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.RequestStatistics;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.RoutingTableLookupRequest;
import org.apache.pinot.broker.routing.TimeBoundaryService;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Broker;
import org.apache.pinot.core.query.reduce.BrokerReduceService;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.pql.parsers.pql2.ast.FunctionCallAstNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseBrokerRequestHandler implements BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrokerRequestHandler.class);

  protected final Configuration _config;
  protected final RoutingTable _routingTable;
  protected final TimeBoundaryService _timeBoundaryService;
  protected final AccessControlFactory _accessControlFactory;
  protected final QueryQuotaManager _queryQuotaManager;
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

  public BaseBrokerRequestHandler(Configuration config, RoutingTable routingTable,
      TimeBoundaryService timeBoundaryService, AccessControlFactory accessControlFactory,
      QueryQuotaManager queryQuotaManager, BrokerMetrics brokerMetrics) {
    _config = config;
    _routingTable = routingTable;
    _timeBoundaryService = timeBoundaryService;
    _accessControlFactory = accessControlFactory;
    _queryQuotaManager = queryQuotaManager;
    _brokerMetrics = brokerMetrics;

    _brokerId = config.getString(Broker.CONFIG_OF_BROKER_ID, getDefaultBrokerId());
    _brokerTimeoutMs = config.getLong(Broker.CONFIG_OF_BROKER_TIMEOUT_MS, Broker.DEFAULT_BROKER_TIMEOUT_MS);
    _queryResponseLimit =
        config.getInt(Broker.CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT, Broker.DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
    _queryLogLength = config.getInt(Broker.CONFIG_OF_BROKER_QUERY_LOG_LENGTH, Broker.DEFAULT_BROKER_QUERY_LOG_LENGTH);
    _queryLogRateLimiter = RateLimiter.create(config.getDouble(Broker.CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND,
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
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      // Offline table
      if (_routingTable.routingTableExists(tableName)) {
        offlineTableName = tableName;
      }
    } else if (tableType == CommonConstants.Helix.TableType.REALTIME) {
      // Realtime table
      if (_routingTable.routingTableExists(tableName)) {
        realtimeTableName = tableName;
      }
    } else {
      // Hybrid table (check both OFFLINE and REALTIME)
      String offlineTableNameToCheck = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      if (_routingTable.routingTableExists(offlineTableNameToCheck)) {
        offlineTableName = offlineTableNameToCheck;
      }
      String realtimeTableNameToCheck = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      if (_routingTable.routingTableExists(realtimeTableNameToCheck)) {
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
    if (offlineBrokerRequest != null) {
      offlineRoutingTable = _routingTable.getRoutingTable(new RoutingTableLookupRequest(offlineBrokerRequest));
      if (offlineRoutingTable.isEmpty()) {
        LOGGER.debug("No OFFLINE server found for request {}: {}", requestId, query);
        offlineBrokerRequest = null;
        offlineRoutingTable = null;
      }
    }
    if (realtimeBrokerRequest != null) {
      realtimeRoutingTable = _routingTable.getRoutingTable(new RoutingTableLookupRequest(realtimeBrokerRequest));
      if (realtimeRoutingTable.isEmpty()) {
        LOGGER.debug("No REALTIME server found for request {}: {}", requestId, query);
        realtimeBrokerRequest = null;
        realtimeRoutingTable = null;
      }
    }
    if (offlineBrokerRequest == null && realtimeBrokerRequest == null) {
      LOGGER.info("No server found for request {}: {}", requestId, query);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
      return BrokerResponseNative.EMPTY_RESULT;
    }
    long routingEndTimeNs = System.nanoTime();
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_ROUTING, routingEndTimeNs - routingStartTimeNs);

    // Execute the query
    long remainingTimeMs = _brokerTimeoutMs - TimeUnit.NANOSECONDS.toMillis(routingEndTimeNs - compilationStartTimeNs);
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

    if (_queryLogRateLimiter.tryAcquire() || forceLog(brokerResponse, totalTimeMs)) {
      // Table name might have been changed (with suffix _OFFLINE/_REALTIME appended)
      LOGGER.info("RequestId:{}, table:{}, timeMs:{}, docs:{}/{}, entries:{}/{},"
              + " segments(queried/processed/matched/consuming):{}/{}/{}/{}, consumingFreshnessTimeMs:{},"
              + " servers:{}/{}, groupLimitReached:{}, exceptions:{}, serverStats:{}, query:{}", requestId,
          brokerRequest.getQuerySource().getTableName(), totalTimeMs, brokerResponse.getNumDocsScanned(),
          brokerResponse.getTotalDocs(), brokerResponse.getNumEntriesScannedInFilter(),
          brokerResponse.getNumEntriesScannedPostFilter(), brokerResponse.getNumSegmentsQueried(),
          brokerResponse.getNumSegmentsProcessed(), brokerResponse.getNumSegmentsMatched(),
          brokerResponse.getNumConsumingSegmentsQueried(), brokerResponse.getMinConsumingFreshnessTimeMs(),
          brokerResponse.getNumServersResponded(), brokerResponse.getNumServersQueried(),
          brokerResponse.isNumGroupsLimitReached(), brokerResponse.getExceptionsSize(), serverStats.getServerStats(),
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
    if (!queryOptions.isEmpty()) {
      brokerRequest.setQueryOptions(queryOptions);
      LOGGER
          .debug("Query options are set to: {} for request {}: {}", brokerRequest.getQueryOptions(), requestId, query);
    }
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
          if (brokerRequest.isSetOrderBy()) {
            String column = aggregationInfo.getAggregationParams().get(FunctionCallAstNode.COLUMN_KEY_IN_AGGREGATION_INFO);
            String[] columns = column.split(FunctionCallAstNode.DISTINCT_MULTI_COLUMN_SEPARATOR);
            Set<String> set = new HashSet<>(Arrays.asList(columns));
            List<SelectionSort> orderByColumns = brokerRequest.getOrderBy();
            for (SelectionSort selectionSort : orderByColumns) {
              if (!set.contains(selectionSort.getColumn())) {
                throw new UnsupportedOperationException("ORDER By should be only on some/all of the columns passed as arguments to DISTINCT");
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
    TimeBoundaryService.TimeBoundaryInfo timeBoundaryInfo =
        _timeBoundaryService.getTimeBoundaryInfoFor(offlineTableName);
    return (timeBoundaryInfo != null) ? timeBoundaryInfo.getTimeColumn() : null;
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
    TimeBoundaryService.TimeBoundaryInfo timeBoundaryInfo =
        _timeBoundaryService.getTimeBoundaryInfoFor(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
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
