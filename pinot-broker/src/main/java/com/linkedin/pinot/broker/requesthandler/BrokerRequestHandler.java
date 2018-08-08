/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.requesthandler;

import com.google.common.base.Splitter;
import com.linkedin.pinot.broker.api.RequesterIdentity;
import com.linkedin.pinot.broker.broker.AccessControlFactory;
import com.linkedin.pinot.broker.pruner.SegmentZKMetadataPrunerService;

import com.linkedin.pinot.broker.queryquota.TableQueryQuotaManager;
import com.linkedin.pinot.broker.routing.RoutingTable;
import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.broker.routing.TimeBoundaryService;
import com.linkedin.pinot.broker.routing.TimeBoundaryService.TimeBoundaryInfo;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.query.ReduceServiceRegistry;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.BrokerResponseFactory;
import com.linkedin.pinot.common.response.BrokerResponseFactory.ResponseType;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableFactory;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.linkedin.pinot.transport.scattergather.ScatterGatherStats;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.protocol.TCompactProtocol;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.common.utils.CommonConstants.Broker.*;
import static com.linkedin.pinot.common.utils.CommonConstants.Broker.Request.*;


/**
 * The <code>BrokerRequestHandler</code> class is a thread-safe broker request handler. Clients can submit multiple
 * requests to be processed parallel.
 */
@ThreadSafe
public class BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRequestHandler.class);
  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();

  private final SegmentZKMetadataPrunerService _segmentPrunerService;
  private final int _queryLogLength;
  private final AccessControlFactory _accessControlFactory;
  private final RoutingTable _routingTable;
  private final ScatterGather _scatterGatherer;
  private final ReduceServiceRegistry _reduceServiceRegistry;
  private final BrokerMetrics _brokerMetrics;
  private final TimeBoundaryService _timeBoundaryService;
  private final long _brokerTimeOutMs;
  private final BrokerRequestOptimizer _optimizer;
  private final int _queryResponseLimit;
  private final AtomicLong _requestIdGenerator;
  private final String _brokerId;
  private final TableQueryQuotaManager _tableQueryQuotaManager;

  public BrokerRequestHandler(RoutingTable table, TimeBoundaryService timeBoundaryService,
      ScatterGather scatterGatherer, ReduceServiceRegistry reduceServiceRegistry,
      SegmentZKMetadataPrunerService segmentPrunerService, BrokerMetrics brokerMetrics, Configuration config,
      AccessControlFactory accessControlFactory, TableQueryQuotaManager tableQueryQuotaManager) {
    _routingTable = table;
    _timeBoundaryService = timeBoundaryService;
    _reduceServiceRegistry = reduceServiceRegistry;
    _scatterGatherer = scatterGatherer;
    _brokerMetrics = brokerMetrics;
    _optimizer = new BrokerRequestOptimizer();
    _requestIdGenerator = new AtomicLong(0);
    _queryResponseLimit = config.getInt(CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT, DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
    _queryLogLength = config.getInt(CONFIG_OF_BROKER_QUERY_LOG_LENGTH, DEFAULT_BROKER_QUERY_LOG_LENGTH);
    _brokerTimeOutMs = config.getLong(CONFIG_OF_BROKER_TIMEOUT_MS, DEFAULT_BROKER_TIMEOUT_MS);
    _brokerId = config.getString(CONFIG_OF_BROKER_ID, getDefaultBrokerId());
    _segmentPrunerService = segmentPrunerService;
    _accessControlFactory = accessControlFactory;
    _tableQueryQuotaManager = tableQueryQuotaManager;

    LOGGER.info("Broker response limit is: " + _queryResponseLimit);
    LOGGER.info("Broker timeout is - " + _brokerTimeOutMs + " ms");
    LOGGER.info("Broker id: " + _brokerId);
  }

  private String getDefaultBrokerId() {
    String defaultBrokerId = "";
    try {
      defaultBrokerId = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOGGER.error("Caught exception while getting default broker id", e);
    }
    return defaultBrokerId;
  }

  /**
   * Process a JSON format request.
   *
   * @param request JSON format request to be processed.
   * @param requesterIdentity
   * @return broker response.
   * @throws Exception
   */
  @Nonnull
  public BrokerResponse handleRequest(@Nonnull JSONObject request, RequesterIdentity requesterIdentity)
      throws Exception {
    long requestId = _requestIdGenerator.incrementAndGet();
    String query = request.getString(PQL);
    LOGGER.debug("Query string for requestId {}: {}", requestId, query);

    // Compile the request
    final long compilationStartTime = System.nanoTime();
    BrokerRequest brokerRequest;
    try {
      brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
    } catch (Exception e) {
      LOGGER.info("Parsing error on requestId {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1L);
      return BrokerResponseFactory.getBrokerResponseWithException(DEFAULT_BROKER_RESPONSE_TYPE,
          QueryException.getException(QueryException.PQL_PARSING_ERROR, e));
    }
    final String tableName = brokerRequest.getQuerySource().getTableName();
    final String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.REQUEST_COMPILATION,
        System.nanoTime() - compilationStartTime);

    final long authStartTime = System.nanoTime();
    try {
      boolean hasAccess = _accessControlFactory.create().hasAccess(requesterIdentity, brokerRequest);
      if (!hasAccess) {
        _brokerMetrics.addMeteredTableValue(brokerRequest.getQuerySource().getTableName(),
            BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
        return BrokerResponseFactory.getBrokerResponseWithException(DEFAULT_BROKER_RESPONSE_TYPE,
            QueryException.ACCESS_DENIED_ERROR);
      }
    } finally {
      _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - authStartTime);
    }

    // Get the resources hit by the request
    String offlineTableName = null;
    String realtimeTableName = null;
    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      // Offline table name
      if (_routingTable.routingTableExists(tableName)) {
        offlineTableName = tableName;
      }
    } else if (tableType == CommonConstants.Helix.TableType.REALTIME) {
      // Realtime table name
      if (_routingTable.routingTableExists(tableName)) {
        realtimeTableName = tableName;
      }
    } else {
      // Raw table name (check both OFFLINE and REALTIME)
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
      LOGGER.info("No table matches the name: {}", tableName);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1L);
      return BrokerResponseFactory.getStaticNoTableHitBrokerResponse(ResponseType.BROKER_RESPONSE_TYPE_NATIVE);
    }

    // Validate qps quota.
    if (!_tableQueryQuotaManager.acquire(tableName)) {
      String msg = String.format("Query quota exceeded on requestId %d. TableName: %s. Broker Id: %s", requestId, rawTableName, _brokerId);
      LOGGER.error(msg);
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1L);
      return BrokerResponseFactory.getBrokerResponseWithException(DEFAULT_BROKER_RESPONSE_TYPE,
          QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, msg));
    }

    // Validate the request
    try {
      validateRequest(brokerRequest);
    } catch (Exception e) {
      LOGGER.info("Validation error on requestId {}: {}, {}", requestId, query, e.getMessage());
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1L);
      return BrokerResponseFactory.getBrokerResponseWithException(DEFAULT_BROKER_RESPONSE_TYPE,
          QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e));
    }

    // Set extra settings into broker request
    if (request.has(TRACE) && request.getBoolean(TRACE)) {
      LOGGER.debug("Enable trace for requestId {}: {}", requestId, query);
      brokerRequest.setEnableTrace(true);
    }
    if (request.has(DEBUG_OPTIONS)) {
      Map<String, String> debugOptions = Splitter.on(';')
          .omitEmptyStrings()
          .trimResults()
          .withKeyValueSeparator('=')
          .split(request.getString(DEBUG_OPTIONS));
      LOGGER.debug("Debug options are set to: {} for requestId {}: {}", debugOptions, requestId, query);
      brokerRequest.setDebugOptions(debugOptions);
    }
    brokerRequest.setResponseFormat(ResponseType.BROKER_RESPONSE_TYPE_NATIVE.name());

    _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERIES, 1L);

    // Execute the query.
    long executionStartTime = System.nanoTime();
    ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
    BrokerResponse brokerResponse =
        processBrokerRequest(brokerRequest, offlineTableName, realtimeTableName, scatterGatherStats, requestId);
    _brokerMetrics.addPhaseTiming(rawTableName, BrokerQueryPhase.QUERY_EXECUTION,
        System.nanoTime() - executionStartTime);

    // Set total query processing time.
    long totalTimeMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - compilationStartTime, TimeUnit.NANOSECONDS);
    brokerResponse.setTimeUsedMs(totalTimeMs);

    LOGGER.debug("Broker Response: {}", brokerResponse);
    // Table name might have been changed (with suffix _OFFLINE/_REALTIME appended).
    LOGGER.info("RequestId: {}, table: {}, totalTimeMs: {}, numDocsScanned: {}, numEntriesScannedInFilter: {}, "
            + "numEntriesScannedPostFilter: {}, totalDocs: {}, scatterGatherStats: {}, query: {}", requestId,
        brokerRequest.getQuerySource().getTableName(), totalTimeMs, brokerResponse.getNumDocsScanned(),
        brokerResponse.getNumEntriesScannedInFilter(), brokerResponse.getNumEntriesScannedPostFilter(),
        brokerResponse.getTotalDocs(), scatterGatherStats, StringUtils.substring(query, 0, _queryLogLength));

    return brokerResponse;
  }

  /**
   * Broker side validation on the broker request.
   * <p>Throw RuntimeException if query does not pass validation.
   * <p>Current validations are:
   * <ul>
   *   <li>Value for 'TOP' for aggregation group-by query is <= configured value.</li>
   *   <li>Value for 'LIMIT' for selection query is <= configured value.</li>
   * </ul>
   *
   * @param brokerRequest broker request to be validated.
   */
  public void validateRequest(@Nonnull BrokerRequest brokerRequest) {
    if (brokerRequest.isSetAggregationsInfo()) {
      if (brokerRequest.isSetGroupBy()) {
        long topN = brokerRequest.getGroupBy().getTopN();
        if (topN > _queryResponseLimit) {
          throw new RuntimeException(
              "Value for 'TOP' " + topN + " exceeded maximum allowed value of " + _queryResponseLimit);
        }
      }
    } else {
      int limit = brokerRequest.getSelections().getSize();
      if (limit > _queryResponseLimit) {
        throw new RuntimeException(
            "Value for 'LIMIT' " + limit + " exceeded maximum allowed value of " + _queryResponseLimit);
      }
    }
  }

  /**
   * Main method to process the request.
   * <p>Following lifecycle stages:
   * <ul>
   *   <li>1. Find the candidate servers to be queried for each set of segments from the routing table.</li>
   *   <li>2. Select servers for each segment set and scatter request to the servers.</li>
   *   <li>3. Gather responses from the servers.</li>
   *   <li>4. Deserialize the server responses.</li>
   *   <li>5. Reduce (merge) the server responses and create a broker response to be returned.</li>
   * </ul>
   *
   * @param brokerRequest broker request to be processed.
   * @param offlineTableName offline table hit by the request.
   * @param realtimeTableName realtime table hit by the request.
   * @param scatterGatherStats scatter-gather statistics.
   * @param requestId broker request ID.
   * @return broker response.
   * @throws InterruptedException
   */
  @Nonnull
  public BrokerResponse processBrokerRequest(@Nonnull BrokerRequest brokerRequest, @Nullable String offlineTableName,
      @Nullable String realtimeTableName, @Nonnull ScatterGatherStats scatterGatherStats, long requestId)
      throws InterruptedException {
    ResponseType responseType = BrokerResponseFactory.getResponseType(brokerRequest.getResponseFormat());
    LOGGER.debug("Broker Response Type: {}", responseType.name());

    // TODO: get time column name from schema or table config so that we can apply it in realtime only use case.
    // We get timeColumnName from time boundary service currently, which only exists for offline table.
    String timeColumnName = (offlineTableName != null) ? getTimeColumnName(offlineTableName) : null;

    BrokerRequest offlineBrokerRequest = null;
    BrokerRequest realtimeBrokerRequest = null;
    if ((offlineTableName != null) && (realtimeTableName != null)) {
      // Hybrid
      offlineBrokerRequest = _optimizer.optimize(getOfflineBrokerRequest(brokerRequest), timeColumnName);
      realtimeBrokerRequest = _optimizer.optimize(getRealtimeBrokerRequest(brokerRequest), timeColumnName);
    } else if (offlineTableName != null) {
      // Offline only
      brokerRequest.getQuerySource().setTableName(offlineTableName);
      offlineBrokerRequest = _optimizer.optimize(brokerRequest, timeColumnName);
    } else {
      // Realtime only
      brokerRequest.getQuerySource().setTableName(realtimeTableName);
      realtimeBrokerRequest = _optimizer.optimize(brokerRequest, timeColumnName);
    }

    ReduceService reduceService = _reduceServiceRegistry.get(responseType);
    return processOptimizedBrokerRequests(brokerRequest, offlineBrokerRequest, realtimeBrokerRequest, reduceService,
        scatterGatherStats, requestId);
  }

  /**
   * Returns the time column name for the table name from the time boundary service.
   * Can return null if the time boundary service does not have the information.
   *
   * @param tableName Name of table for which to get the time column name
   * @return Time column name for the table.
   */
  @Nullable
  private String getTimeColumnName(@Nonnull String tableName) {
    TimeBoundaryInfo timeBoundary = _timeBoundaryService.getTimeBoundaryInfoFor(tableName);
    return (timeBoundary != null) ? timeBoundary.getTimeColumn() : null;
  }

  /**
   * Given a broker request, use it to create an offline broker request.
   *
   * @param brokerRequest original broker request.
   * @return offline broker request.
   */
  @Nonnull
  private BrokerRequest getOfflineBrokerRequest(@Nonnull BrokerRequest brokerRequest) {
    BrokerRequest offlineRequest = brokerRequest.deepCopy();
    String hybridTableName = brokerRequest.getQuerySource().getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(hybridTableName);
    offlineRequest.getQuerySource().setTableName(offlineTableName);
    attachTimeBoundary(hybridTableName, offlineRequest, true);
    return offlineRequest;
  }

  /**
   * Given a broker request, use it to create a realtime broker request.
   *
   * @param brokerRequest original broker request.
   * @return realtime broker request.
   */
  @Nonnull
  private BrokerRequest getRealtimeBrokerRequest(@Nonnull BrokerRequest brokerRequest) {
    BrokerRequest realtimeRequest = brokerRequest.deepCopy();
    String hybridTableName = brokerRequest.getQuerySource().getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(hybridTableName);
    realtimeRequest.getQuerySource().setTableName(realtimeTableName);
    attachTimeBoundary(hybridTableName, realtimeRequest, false);
    return realtimeRequest;
  }

  /**
   * Attach time boundary to a broker request.
   *
   * @param hybridTableName hybrid table name.
   * @param brokerRequest original broker request.
   * @param isOfflineRequest flag for offline/realtime request.
   */
  private void attachTimeBoundary(@Nonnull String hybridTableName, @Nonnull BrokerRequest brokerRequest,
      boolean isOfflineRequest) {
    TimeBoundaryInfo timeBoundaryInfo =
        _timeBoundaryService.getTimeBoundaryInfoFor(TableNameBuilder.OFFLINE.tableNameWithType(hybridTableName));
    if (timeBoundaryInfo == null || timeBoundaryInfo.getTimeColumn() == null
        || timeBoundaryInfo.getTimeValue() == null) {
      LOGGER.warn("No time boundary attached for table: {}", hybridTableName);
      return;
    }

    // Create a range filter based on the request type.
    String timeValue = timeBoundaryInfo.getTimeValue();
    FilterQuery timeFilterQuery = new FilterQuery();
    timeFilterQuery.setOperator(FilterOperator.RANGE);
    timeFilterQuery.setColumn(timeBoundaryInfo.getTimeColumn());
    timeFilterQuery.setNestedFilterQueryIds(new ArrayList<Integer>());
    List<String> values = new ArrayList<>();
    if (isOfflineRequest) {
      values.add("(*\t\t" + timeValue + ")");
    } else {
      values.add("[" + timeValue + "\t\t*)");
    }
    timeFilterQuery.setValue(values);
    timeFilterQuery.setId(-1);

    // Attach the range filter to the current filter.
    FilterQuery currentFilterQuery = brokerRequest.getFilterQuery();
    if (currentFilterQuery != null) {
      FilterQuery andFilterQuery = new FilterQuery();
      andFilterQuery.setOperator(FilterOperator.AND);
      List<Integer> nestedFilterQueryIds = new ArrayList<>();
      nestedFilterQueryIds.add(currentFilterQuery.getId());
      nestedFilterQueryIds.add(timeFilterQuery.getId());
      andFilterQuery.setNestedFilterQueryIds(nestedFilterQueryIds);
      andFilterQuery.setId(-2);
      FilterQueryMap filterSubQueryMap = brokerRequest.getFilterSubQueryMap();
      filterSubQueryMap.putToFilterQueryMap(timeFilterQuery.getId(), timeFilterQuery);
      filterSubQueryMap.putToFilterQueryMap(andFilterQuery.getId(), andFilterQuery);
      brokerRequest.setFilterQuery(andFilterQuery);
      brokerRequest.setFilterSubQueryMap(filterSubQueryMap);
    } else {
      FilterQueryMap filterSubQueryMap = new FilterQueryMap();
      filterSubQueryMap.putToFilterQueryMap(timeFilterQuery.getId(), timeFilterQuery);
      brokerRequest.setFilterQuery(timeFilterQuery);
      brokerRequest.setFilterSubQueryMap(filterSubQueryMap);
    }
  }

  /**
   * Process the optimized broker requests for both OFFLINE and REALTIME table.
   *
   * @param originalBrokerRequest original broker request.
   * @param offlineBrokerRequest broker request for OFFLINE table.
   * @param realtimeBrokerRequest broker request for REALTIME table.
   * @param reduceService reduce service.
   * @param scatterGatherStats scatter-gather statistics.
   * @param requestId request ID.
   * @return broker response.
   * @throws InterruptedException
   */
  @Nonnull
  private BrokerResponse processOptimizedBrokerRequests(@Nonnull BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable BrokerRequest realtimeBrokerRequest,
      @Nonnull ReduceService reduceService, @Nonnull ScatterGatherStats scatterGatherStats, long requestId)
      throws InterruptedException {
    ResponseType serverResponseType = BrokerResponseFactory.getResponseType(originalBrokerRequest.getResponseFormat());
    PhaseTimes phaseTimes = new PhaseTimes();

    // Step 1: find the candidate servers to be queried for each set of segments from the routing table.
    // Step 2: select servers for each segment set and scatter request to the servers.
    String offlineTableName = null;
    CompositeFuture<byte[]> offlineCompositeFuture = null;
    if (offlineBrokerRequest != null) {
      offlineTableName = offlineBrokerRequest.getQuerySource().getTableName();
      offlineCompositeFuture =
          routeAndScatterBrokerRequest(offlineBrokerRequest, phaseTimes, scatterGatherStats, true, requestId);
    }
    String realtimeTableName = null;
    CompositeFuture<byte[]> realtimeCompositeFuture = null;
    if (realtimeBrokerRequest != null) {
      realtimeTableName = realtimeBrokerRequest.getQuerySource().getTableName();
      realtimeCompositeFuture =
          routeAndScatterBrokerRequest(realtimeBrokerRequest, phaseTimes, scatterGatherStats, false, requestId);
    }
    if ((offlineCompositeFuture == null) && (realtimeCompositeFuture == null)) {
      // No server found in either OFFLINE or REALTIME table.
      return BrokerResponseFactory.getStaticEmptyBrokerResponse(serverResponseType);
    }

    // Step 3: gather response from the servers.
    int numServersQueried = 0;
    long gatherStartTime = System.nanoTime();
    List<ProcessingException> processingExceptions = new ArrayList<>();
    Map<ServerInstance, byte[]> offlineServerResponseMap = null;
    Map<ServerInstance, byte[]> realtimeServerResponseMap = null;
    if (offlineCompositeFuture != null) {
      numServersQueried += offlineCompositeFuture.getNumFutures();
      offlineServerResponseMap =
          gatherServerResponses(offlineCompositeFuture, scatterGatherStats, true, offlineTableName,
              processingExceptions);
    }
    if (realtimeCompositeFuture != null) {
      numServersQueried += realtimeCompositeFuture.getNumFutures();
      realtimeServerResponseMap =
          gatherServerResponses(realtimeCompositeFuture, scatterGatherStats, false, realtimeTableName,
              processingExceptions);
    }
    phaseTimes.addToGatherTime(System.nanoTime() - gatherStartTime);
    if ((offlineServerResponseMap == null) && (realtimeServerResponseMap == null)) {
      // No response gathered.
      return BrokerResponseFactory.getBrokerResponseWithExceptions(serverResponseType, processingExceptions);
    }

    //Step 4: deserialize the server responses.
    int numServersResponded = 0;
    long deserializationStartTime = System.nanoTime();
    Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();
    // Add a long variable to sum the total response sizes from both realtime and offline servers.
    long totalServerResponseSize = 0L;
    if (offlineServerResponseMap != null) {
      numServersResponded += offlineServerResponseMap.size();
      totalServerResponseSize +=
          deserializeServerResponses(offlineServerResponseMap, true, dataTableMap, offlineTableName,
              processingExceptions);
    }
    if (realtimeServerResponseMap != null) {
      numServersResponded += realtimeServerResponseMap.size();
      totalServerResponseSize +=
          deserializeServerResponses(realtimeServerResponseMap, false, dataTableMap, realtimeTableName,
              processingExceptions);
    }
    phaseTimes.addToDeserializationTime(System.nanoTime() - deserializationStartTime);

    // Step 5: reduce (merge) the server responses and create a broker response to be returned.
    long reduceStartTime = System.nanoTime();
    BrokerResponse brokerResponse =
        reduceService.reduceOnDataTable(originalBrokerRequest, dataTableMap, _brokerMetrics);
    phaseTimes.addToReduceTime(System.nanoTime() - reduceStartTime);

    // Set processing exceptions and number of servers queried/responded.
    brokerResponse.setExceptions(processingExceptions);
    brokerResponse.setNumServersQueried(numServersQueried);
    brokerResponse.setNumServersResponded(numServersResponded);

    // Update broker metrics.
    String rawTableName = TableNameBuilder.extractRawTableName(originalBrokerRequest.getQuerySource().getTableName());
    phaseTimes.addPhaseTimesToBrokerMetrics(_brokerMetrics, rawTableName);
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS, 1L);
    }
    if (numServersQueried > numServersResponded) {
      _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED,
          1L);
    }
    _brokerMetrics.addMeteredQueryValue(originalBrokerRequest, BrokerMeter.TOTAL_SERVER_RESPONSE_SIZE,
        totalServerResponseSize);

    return brokerResponse;
  }

  /**
   * Route and scatter the broker request.
   *
   * @return composite future used to gather responses.
   */
  @Nullable
  private CompositeFuture<byte[]> routeAndScatterBrokerRequest(@Nonnull BrokerRequest brokerRequest,
      @Nonnull PhaseTimes phaseTimes, @Nonnull ScatterGatherStats scatterGatherStats, boolean isOfflineTable,
      long requestId) throws InterruptedException {
    // Step 1: find the candidate servers to be queried for each set of segments from the routing table.
    // TODO: add checks for whether all segments are covered.
    long routingStartTime = System.nanoTime();
    Map<String, List<String>> routingTable =
        _routingTable.getRoutingTable(new RoutingTableLookupRequest(brokerRequest));
    phaseTimes.addToRoutingTime(System.nanoTime() - routingStartTime);
    if (routingTable == null || routingTable.isEmpty()) {
      String tableNameWithType = brokerRequest.getQuerySource().getTableName();
      LOGGER.info("No server found or all segments are pruned for table: {}", tableNameWithType);
      _brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1L);
      return null;
    }

    // Step 2: select servers for each segment set and scatter request to the servers.
    long scatterStartTime = System.nanoTime();
    ScatterGatherRequestImpl scatterRequest =
        new ScatterGatherRequestImpl(brokerRequest, routingTable, requestId, _brokerTimeOutMs, _brokerId);
    CompositeFuture<byte[]> compositeFuture =
        _scatterGatherer.scatterGather(scatterRequest, scatterGatherStats, isOfflineTable, _brokerMetrics);
    phaseTimes.addToScatterTime(System.nanoTime() - scatterStartTime);
    return compositeFuture;
  }

  /**
   * Gather responses from servers, append processing exceptions to the processing exception list passed in.
   *
   * @param compositeFuture composite future returned from scatter phase.
   * @param scatterGatherStats scatter-gather statistics.
   * @param isOfflineTable whether the scatter-gather target is an OFFLINE table.
   * @param tableNameWithType table name with type suffix.
   * @param processingExceptions list of processing exceptions.
   * @return server response map.
   */
  @Nullable
  private Map<ServerInstance, byte[]> gatherServerResponses(@Nonnull CompositeFuture<byte[]> compositeFuture,
      @Nonnull ScatterGatherStats scatterGatherStats, boolean isOfflineTable, @Nonnull String tableNameWithType,
      @Nonnull List<ProcessingException> processingExceptions) {
    try {
      Map<ServerInstance, byte[]> serverResponseMap = compositeFuture.get();
      Iterator<Entry<ServerInstance, byte[]>> iterator = serverResponseMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<ServerInstance, byte[]> entry = iterator.next();
        if (entry.getValue().length == 0) {
          LOGGER.warn("Got empty response from server: {]", entry.getKey().getShortHostName());
          iterator.remove();
        }
      }
      Map<ServerInstance, Long> responseTimes = compositeFuture.getResponseTimes();
      scatterGatherStats.setResponseTimeMillis(responseTimes, isOfflineTable);
      return serverResponseMap;
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching responses for table: {}", tableNameWithType, e);
      _brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1L);
      processingExceptions.add(QueryException.getException(QueryException.BROKER_GATHER_ERROR, e));
      return null;
    }
  }

  /**
   * Deserialize the server responses, put the de-serialized data table into the data table map passed in, append
   * processing exceptions to the processing exception list passed in, and return the total response size from pinot servers.
   * <p>For hybrid use case, multiple responses might be from the same instance. Use response sequence to distinguish
   * them.
   *
   * @param responseMap map from server to response.
   * @param isOfflineTable whether the responses are from an OFFLINE table.
   * @param dataTableMap map from server to data table.
   * @param tableNameWithType table name with type suffix.
   * @param processingExceptions list of processing exceptions.
   * @return total server response size.
   */
  private long deserializeServerResponses(@Nonnull Map<ServerInstance, byte[]> responseMap, boolean isOfflineTable,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap, @Nonnull String tableNameWithType,
      @Nonnull List<ProcessingException> processingExceptions) {
    long totalResponseSize = 0L;
    for (Entry<ServerInstance, byte[]> entry : responseMap.entrySet()) {
      ServerInstance serverInstance = entry.getKey();
      if (!isOfflineTable) {
        serverInstance = serverInstance.withSeq(1);
      }
      byte[] responseInBytes = entry.getValue();
      totalResponseSize += responseInBytes.length;
      try {
        dataTableMap.put(serverInstance, DataTableFactory.getDataTable(responseInBytes));
      } catch (Exception e) {
        LOGGER.error("Caught exceptions while deserializing response for table: {} from server: {}", tableNameWithType,
            serverInstance, e);
        _brokerMetrics.addMeteredTableValue(tableNameWithType, BrokerMeter.DATA_TABLE_DESERIALIZATION_EXCEPTIONS, 1L);
        processingExceptions.add(QueryException.getException(QueryException.DATA_TABLE_DESERIALIZATION_ERROR, e));
      }
    }
    return totalResponseSize;
  }

  /**
   * Container for time statistics in all phases.
   */
  private static class PhaseTimes {
    private long _routingTime = 0L;
    private long _scatterTime = 0L;
    private long _gatherTime = 0L;
    private long _deserializationTime = 0L;
    private long _reduceTime = 0L;

    public void addToRoutingTime(long routingTime) {
      _routingTime += routingTime;
    }

    public void addToScatterTime(long scatterTime) {
      _scatterTime += scatterTime;
    }

    public void addToGatherTime(long gatherTime) {
      _gatherTime += gatherTime;
    }

    public void addToDeserializationTime(long deserializationTime) {
      _deserializationTime += deserializationTime;
    }

    public void addToReduceTime(long reduceTime) {
      _reduceTime += reduceTime;
    }

    public void addPhaseTimesToBrokerMetrics(BrokerMetrics brokerMetrics, String tableName) {
      brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.QUERY_ROUTING, _routingTime);
      brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.SCATTER_GATHER, _scatterTime + _gatherTime);
      brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.DESERIALIZATION, _deserializationTime);
      brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.REDUCE, _reduceTime);
    }
  }

  private static class ScatterGatherRequestImpl implements ScatterGatherRequest {
    private final BrokerRequest _brokerRequest;
    private final Map<String, List<String>> _routingTable;
    private final long _requestId;
    private final long _requestTimeoutMs;
    private final String _brokerId;

    public ScatterGatherRequestImpl(BrokerRequest request, Map<String, List<String>> routingTable, long requestId,
        long requestTimeoutMs, String brokerId) {
      _brokerRequest = request;
      _routingTable = routingTable;
      _requestId = requestId;
      _requestTimeoutMs = requestTimeoutMs;
      _brokerId = brokerId;
    }

    @Override
    public Map<String, List<String>> getRoutingTable() {
      return _routingTable;
    }

    @Override
    public byte[] getRequestForService(List<String> segments) {
      InstanceRequest r = new InstanceRequest();
      r.setRequestId(_requestId);
      r.setEnableTrace(_brokerRequest.isEnableTrace());
      r.setQuery(_brokerRequest);
      r.setSearchSegments(segments);
      r.setBrokerId(_brokerId);
      // _serde is not threadsafe.
      return new SerDe(new TCompactProtocol.Factory()).serialize(r);
    }

    @Override
    public long getRequestId() {
      return _requestId;
    }

    @Override
    public long getRequestTimeoutMs() {
      return _requestTimeoutMs;
    }

    @Override
    public BrokerRequest getBrokerRequest() {
      return _brokerRequest;
    }
  }

  public String getRoutingTableSnapshot(String tableName) throws Exception {
    return _routingTable.dumpSnapshot(tableName);
  }
}
