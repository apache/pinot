/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableFactory;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.routing.RoutingTable;
import com.linkedin.pinot.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.routing.TimeBoundaryService;
import com.linkedin.pinot.routing.TimeBoundaryService.TimeBoundaryInfo;
import com.linkedin.pinot.serde.SerDe;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.RoundRobinReplicaSelection;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.linkedin.pinot.transport.scattergather.ScatterGatherStats;
import io.netty.buffer.ByteBuf;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.protocol.TCompactProtocol;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BrokerRequestHandler</code> class is a thread-safe broker request handler. Clients can submit multiple
 * requests to be processed parallel.
 */
@ThreadSafe
public class BrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRequestHandler.class);
  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();

  private static final int DEFAULT_BROKER_QUERY_RESPONSE_LIMIT = Integer.MAX_VALUE;
  private static final String BROKER_QUERY_RESPONSE_LIMIT_CONFIG = "pinot.broker.query.response.limit";
  public static final long DEFAULT_BROKER_TIME_OUT_MS = 10 * 1000L;
  private static final String BROKER_TIME_OUT_CONFIG = "pinot.broker.timeoutMs";
  private static final String DEFAULT_BROKER_ID;
  public static final String BROKER_ID_CONFIG_KEY = "pinot.broker.id";
  private static final ResponseType DEFAULT_BROKER_RESPONSE_TYPE = ResponseType.BROKER_RESPONSE_TYPE_NATIVE;

  static {
    String defaultBrokerId = "";
    try {
      defaultBrokerId = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOGGER.error("Failed to read default broker id.", e);
    }
    DEFAULT_BROKER_ID = defaultBrokerId;
  }

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
  // TODO: Currently only using RoundRobin selection. But, this can be allowed to be configured.
  private RoundRobinReplicaSelection _replicaSelection;

  public BrokerRequestHandler(RoutingTable table, TimeBoundaryService timeBoundaryService,
      ScatterGather scatterGatherer, ReduceServiceRegistry reduceServiceRegistry, BrokerMetrics brokerMetrics,
      Configuration config) {
    _routingTable = table;
    _timeBoundaryService = timeBoundaryService;
    _reduceServiceRegistry = reduceServiceRegistry;
    _scatterGatherer = scatterGatherer;
    _replicaSelection = new RoundRobinReplicaSelection();
    _brokerMetrics = brokerMetrics;
    _optimizer = new BrokerRequestOptimizer();
    _requestIdGenerator = new AtomicLong(0);
    _queryResponseLimit = config.getInt(BROKER_QUERY_RESPONSE_LIMIT_CONFIG, DEFAULT_BROKER_QUERY_RESPONSE_LIMIT);
    _brokerTimeOutMs = config.getLong(BROKER_TIME_OUT_CONFIG, DEFAULT_BROKER_TIME_OUT_MS);
    _brokerId = config.getString(BROKER_ID_CONFIG_KEY, DEFAULT_BROKER_ID);
    LOGGER.info("Broker response limit is: " + _queryResponseLimit);
    LOGGER.info("Broker timeout is - " + _brokerTimeOutMs + " ms");
    LOGGER.info("Broker id: " + _brokerId);
  }

  /**
   * Process a JSON format request.
   *
   * @param request JSON format request to be processed.
   * @return broker response.
   * @throws Exception
   */
  @Nonnull
  public BrokerResponse handleRequest(@Nonnull JSONObject request)
      throws Exception {
    long requestId = _requestIdGenerator.incrementAndGet();
    String pql = request.getString("pql");
    LOGGER.debug("Query string for requestId {}: {}", requestId, pql);

    boolean isTraceEnabled = false;
    if (request.has("trace")) {
      isTraceEnabled = Boolean.parseBoolean(request.getString("trace"));
      LOGGER.debug("Trace is set to: {} for requestId {}: {}", isTraceEnabled, requestId, pql);
    }

    Map<String, String> debugOptions = null;
    if (request.has("debugOptions")) {
      String routingOptionParameter = request.getString("debugOptions");
      debugOptions =
          Splitter.on(';').omitEmptyStrings().trimResults().withKeyValueSeparator('=').split(routingOptionParameter);
      LOGGER.debug("Debug options are set to: {} for requestId {}: {}", debugOptions, requestId, pql);
    }

    // Compile and validate the request.
    long compilationStartTime = System.nanoTime();
    BrokerRequest brokerRequest;
    try {
      brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pql);
    } catch (Exception e) {
      LOGGER.warn("Parsing error on requestId {}: {}", requestId, pql, e);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      return BrokerResponseFactory.getBrokerResponseWithException(DEFAULT_BROKER_RESPONSE_TYPE,
          QueryException.getException(QueryException.PQL_PARSING_ERROR, e));
    }
    String tableName = brokerRequest.getQuerySource().getTableName();
    try {
      validateRequest(brokerRequest);
    } catch (Exception e) {
      LOGGER.warn("Validation error on requestId {}: {}", requestId, pql, e);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      return BrokerResponseFactory.getBrokerResponseWithException(DEFAULT_BROKER_RESPONSE_TYPE,
          QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e));
    }
    if (isTraceEnabled) {
      brokerRequest.setEnableTrace(true);
    }
    if (debugOptions != null) {
      brokerRequest.setDebugOptions(debugOptions);
    }
    brokerRequest.setResponseFormat(ResponseType.BROKER_RESPONSE_TYPE_NATIVE.name());
    _brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.REQUEST_COMPILATION,
        System.nanoTime() - compilationStartTime);
    _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.QUERIES, 1);

    // Execute the query.
    long executionStartTime = System.nanoTime();
    ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
    BrokerResponse brokerResponse = processBrokerRequest(brokerRequest, scatterGatherStats, requestId);
    _brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.QUERY_EXECUTION, System.nanoTime() - executionStartTime);

    // Set total query processing time.
    long totalTimeMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - compilationStartTime, TimeUnit.NANOSECONDS);
    brokerResponse.setTimeUsedMs(totalTimeMs);

    LOGGER.debug("Broker Response: {}", brokerResponse);
    // Table name might have been changed (with suffix _OFFLINE/_REALTIME appended).
    LOGGER.info("RequestId: {}, table: {}, totalTimeMs: {}, numDocsScanned: {}, numEntriesScannedInFilter: {}, "
            + "numEntriesScannedPostFilter: {}, totalDocs: {}, scatterGatherStats: {}, query: {}", requestId,
        brokerRequest.getQuerySource().getTableName(), totalTimeMs, brokerResponse.getNumDocsScanned(),
        brokerResponse.getNumEntriesScannedInFilter(), brokerResponse.getNumEntriesScannedPostFilter(),
        brokerResponse.getTotalDocs(), scatterGatherStats, pql);

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
   * @param scatterGatherStats scatter-gather statistics.
   * @param requestId broker request ID.
   * @return broker response.
   * @throws InterruptedException
   */
  @Nonnull
  public BrokerResponse processBrokerRequest(@Nonnull BrokerRequest brokerRequest,
      @Nonnull ScatterGatherStats scatterGatherStats, long requestId)
      throws InterruptedException {
    String tableName = brokerRequest.getQuerySource().getTableName();
    ResponseType responseType = BrokerResponseFactory.getResponseType(brokerRequest.getResponseFormat());
    LOGGER.debug("Broker Response Type: {}", responseType.name());

    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    if (!_routingTable.routingTableExists(offlineTableName)) {
      offlineTableName = null;
    }
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    if (!_routingTable.routingTableExists(realtimeTableName)) {
      realtimeTableName = null;
    }

    if ((offlineTableName == null) && (realtimeTableName == null)) {
      // No table matches the broker request.
      LOGGER.warn("No table matches the name: {}", tableName);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1);
      return BrokerResponseFactory.getStaticNoTableHitBrokerResponse(responseType);
    } else {
      // At least one table matches the broker request.
      BrokerRequest offlineBrokerRequest = null;
      BrokerRequest realtimeBrokerRequest = null;
      if ((offlineTableName != null) && (realtimeTableName != null)) {
        // Hybrid table.
        offlineBrokerRequest = getOfflineBrokerRequest(brokerRequest);
        realtimeBrokerRequest = getRealtimeBrokerRequest(brokerRequest);
      } else if (offlineTableName != null) {
        // Offline table only.
        brokerRequest.getQuerySource().setTableName(offlineTableName);
        offlineBrokerRequest = _optimizer.optimize(brokerRequest);
      } else {
        // Realtime table only.
        brokerRequest.getQuerySource().setTableName(realtimeTableName);
        realtimeBrokerRequest = _optimizer.optimize(brokerRequest);
      }

      ReduceService reduceService = _reduceServiceRegistry.get(responseType);
      // TODO: wire up the customized BucketingSelection.
      return processOptimizedBrokerRequests(brokerRequest, offlineBrokerRequest, realtimeBrokerRequest, reduceService,
          scatterGatherStats, null, requestId);
    }
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
    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(hybridTableName);
    offlineRequest.getQuerySource().setTableName(offlineTableName);
    attachTimeBoundary(hybridTableName, offlineRequest, true);
    return _optimizer.optimize(offlineRequest);
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
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(hybridTableName);
    realtimeRequest.getQuerySource().setTableName(realtimeTableName);
    attachTimeBoundary(hybridTableName, realtimeRequest, false);
    return _optimizer.optimize(realtimeRequest);
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
    TimeBoundaryInfo timeBoundaryInfo = _timeBoundaryService.getTimeBoundaryInfoFor(
        TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(hybridTableName));
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
   * @param bucketingSelection customized bucketing selection.
   * @param scatterGatherStats scatter-gather statistics.
   * @param requestId request ID.
   * @return broker response.
   * @throws InterruptedException
   */
  @Nonnull
  private BrokerResponse processOptimizedBrokerRequests(@Nonnull BrokerRequest originalBrokerRequest,
      @Nullable BrokerRequest offlineBrokerRequest, @Nullable BrokerRequest realtimeBrokerRequest,
      @Nonnull ReduceService reduceService, @Nonnull ScatterGatherStats scatterGatherStats,
      @Nullable BucketingSelection bucketingSelection, long requestId)
      throws InterruptedException {
    String originalTableName = originalBrokerRequest.getQuerySource().getTableName();
    ResponseType serverResponseType = BrokerResponseFactory.getResponseType(originalBrokerRequest.getResponseFormat());
    PhaseTimes phaseTimes = new PhaseTimes();

    // Step 1: find the candidate servers to be queried for each set of segments from the routing table.
    // Step 2: select servers for each segment set and scatter request to the servers.
    String offlineTableName = null;
    CompositeFuture<ServerInstance, ByteBuf> offlineCompositeFuture = null;
    if (offlineBrokerRequest != null) {
      offlineTableName = offlineBrokerRequest.getQuerySource().getTableName();
      offlineCompositeFuture =
          routeAndScatterBrokerRequest(offlineBrokerRequest, phaseTimes, scatterGatherStats, bucketingSelection,
              requestId);
    }
    String realtimeTableName = null;
    CompositeFuture<ServerInstance, ByteBuf> realtimeCompositeFuture = null;
    if (realtimeBrokerRequest != null) {
      realtimeTableName = realtimeBrokerRequest.getQuerySource().getTableName();
      realtimeCompositeFuture =
          routeAndScatterBrokerRequest(realtimeBrokerRequest, phaseTimes, scatterGatherStats, bucketingSelection,
              requestId);
    }
    if ((offlineCompositeFuture == null) && (realtimeCompositeFuture == null)) {
      // No server found in either OFFLINE or REALTIME table.
      return BrokerResponseFactory.getStaticEmptyBrokerResponse(serverResponseType);
    }

    // Step 3: gather response from the servers.
    int numServersQueried = 0;
    long gatherStartTime = System.nanoTime();
    List<ProcessingException> processingExceptions = new ArrayList<>();
    Map<ServerInstance, ByteBuf> offlineServerResponseMap = null;
    Map<ServerInstance, ByteBuf> realtimeServerResponseMap = null;
    if (offlineCompositeFuture != null) {
      numServersQueried += offlineCompositeFuture.getNumFutures();
      offlineServerResponseMap =
          gatherServerResponses(offlineCompositeFuture, scatterGatherStats, true, offlineTableName,
              processingExceptions);
    }
    if (realtimeCompositeFuture != null) {
      numServersQueried += realtimeCompositeFuture.getNumFutures();
      realtimeServerResponseMap =
          gatherServerResponses(realtimeCompositeFuture, scatterGatherStats, true, realtimeTableName,
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
    if (offlineServerResponseMap != null) {
      numServersResponded += offlineServerResponseMap.size();
      deserializeServerResponses(offlineServerResponseMap, true, dataTableMap, offlineTableName, processingExceptions);
    }
    if (realtimeServerResponseMap != null) {
      numServersResponded += realtimeServerResponseMap.size();
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
    phaseTimes.addPhaseTimesToBrokerMetrics(_brokerMetrics, originalTableName);
    if (brokerResponse.getExceptionsSize() > 0) {
      _brokerMetrics.addMeteredTableValue(originalTableName, BrokerMeter.BROKER_RESPONSES_WITH_PROCESSING_EXCEPTIONS,
          1);
    }
    if (numServersQueried > numServersResponded) {
      _brokerMetrics.addMeteredTableValue(originalTableName,
          BrokerMeter.BROKER_RESPONSES_WITH_PARTIAL_SERVERS_RESPONDED, 1);
    }

    return brokerResponse;
  }

  /**
   * Route and scatter the broker request.
   *
   * @return composite future used to gather responses.
   */
  @Nullable
  private CompositeFuture<ServerInstance, ByteBuf> routeAndScatterBrokerRequest(@Nonnull BrokerRequest brokerRequest,
      @Nonnull PhaseTimes phaseTimes, @Nonnull ScatterGatherStats scatterGatherStats,
      @Nullable BucketingSelection bucketingSelection, long requestId)
      throws InterruptedException {
    // Step 1: find the candidate servers to be queried for each set of segments from the routing table.
    // TODO: add checks for whether all segments are covered.
    long routingStartTime = System.nanoTime();
    Map<ServerInstance, SegmentIdSet> segmentServices = findCandidateServers(brokerRequest);
    phaseTimes.addToRoutingTime(System.nanoTime() - routingStartTime);
    if (segmentServices == null || segmentServices.isEmpty()) {
      String tableName = brokerRequest.getQuerySource().getTableName();
      LOGGER.warn("No server found for table: {}", tableName);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
      return null;
    }

    // Step 2: select servers for each segment set and scatter request to the servers.
    long scatterStartTime = System.nanoTime();
    ScatterGatherRequestImpl scatterRequest =
        new ScatterGatherRequestImpl(brokerRequest, segmentServices, _replicaSelection,
            ReplicaSelectionGranularity.SEGMENT_ID_SET, brokerRequest.getBucketHashKey(), 0, bucketingSelection,
            requestId, _brokerTimeOutMs, _brokerId);
    CompositeFuture<ServerInstance, ByteBuf> compositeFuture =
        _scatterGatherer.scatterGather(scatterRequest, scatterGatherStats, true, _brokerMetrics);
    phaseTimes.addToScatterTime(System.nanoTime() - scatterStartTime);
    return compositeFuture;
  }

  /**
   * Find the candidate servers to be queried for each set of segments from the routing table.
   *
   * @param brokerRequest broker request.
   * @return map from server to set of segments.
   */
  @Nullable
  private Map<ServerInstance, SegmentIdSet> findCandidateServers(@Nonnull BrokerRequest brokerRequest) {
    String tableName = brokerRequest.getQuerySource().getTableName();
    List<String> routingOptions;
    Map<String, String> debugOptions = brokerRequest.getDebugOptions();
    if (debugOptions == null || !debugOptions.containsKey("routingOptions")) {
      routingOptions = Collections.emptyList();
    } else {
      routingOptions =
          Splitter.on(",").omitEmptyStrings().trimResults().splitToList(debugOptions.get("routingOptions"));
    }
    RoutingTableLookupRequest routingTableLookupRequest = new RoutingTableLookupRequest(tableName, routingOptions);
    return _routingTable.findServers(routingTableLookupRequest);
  }

  /**
   * Gather responses from servers, append processing exceptions to the processing exception list passed in.
   *
   * @param compositeFuture composite future returned from scatter phase.
   * @param scatterGatherStats scatter-gather statistics.
   * @param isOfflineTable whether the scatter-gather target is an OFFLINE table.
   * @param tableName table name.
   * @param processingExceptions list of processing exceptions.
   * @return server response map.
   */
  @Nullable
  private Map<ServerInstance, ByteBuf> gatherServerResponses(
      @Nonnull CompositeFuture<ServerInstance, ByteBuf> compositeFuture, @Nonnull ScatterGatherStats scatterGatherStats,
      boolean isOfflineTable, @Nonnull String tableName, @Nonnull List<ProcessingException> processingExceptions) {
    try {
      Map<ServerInstance, ByteBuf> serverResponseMap = compositeFuture.get();
      Map<String, Long> responseTimes = compositeFuture.getResponseTimes();
      scatterGatherStats.setResponseTimeMillis(responseTimes, isOfflineTable);
      return serverResponseMap;
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching responses for table: {}", tableName, e);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1);
      processingExceptions.add(QueryException.getException(QueryException.BROKER_GATHER_ERROR, e));
      return null;
    }
  }

  /**
   * Deserialize the server responses, put the de-serialized data table into the data table map passed in, append
   * processing exceptions to the processing exception list passed in.
   * <p>For hybrid use case, multiple responses might be from the same instance. Use response sequence to distinguish
   * them.
   *
   * @param responseMap map from server to response.
   * @param isOfflineTable whether the responses are from an OFFLINE table.
   * @param dataTableMap map from server to data table.
   * @param tableName table name.
   * @param processingExceptions list of processing exceptions.
   */
  private void deserializeServerResponses(@Nonnull Map<ServerInstance, ByteBuf> responseMap, boolean isOfflineTable,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap, @Nonnull String tableName,
      @Nonnull List<ProcessingException> processingExceptions) {
    for (Entry<ServerInstance, ByteBuf> entry : responseMap.entrySet()) {
      ServerInstance serverInstance = entry.getKey();
      if (!isOfflineTable) {
        serverInstance = new ServerInstance(serverInstance.getHostname(), serverInstance.getPort(), 1);
      }
      ByteBuf byteBuf = entry.getValue();
      try {
        byte[] byteArray = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(byteArray);
        dataTableMap.put(serverInstance, DataTableFactory.getDataTable(byteArray));
      } catch (Exception e) {
        LOGGER.error("Caught exceptions while deserializing response for table: {} from server: {}", tableName,
            serverInstance, e);
        _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.DATA_TABLE_DESERIALIZATION_EXCEPTIONS, 1);
        processingExceptions.add(QueryException.getException(QueryException.DATA_TABLE_DESERIALIZATION_ERROR, e));
      }
    }
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
    private final Map<ServerInstance, SegmentIdSet> _segmentServices;
    private final ReplicaSelection _replicaSelection;
    private final ReplicaSelectionGranularity _replicaSelectionGranularity;
    private final Object _hashKey;
    private final int _numSpeculativeRequests;
    private final BucketingSelection _bucketingSelection;
    private final long _requestId;
    private final long _requestTimeoutMs;
    private final String _brokerId;

    public ScatterGatherRequestImpl(BrokerRequest request, Map<ServerInstance, SegmentIdSet> segmentServices,
        ReplicaSelection replicaSelection, ReplicaSelectionGranularity replicaSelectionGranularity, Object hashKey,
        int numSpeculativeRequests, BucketingSelection bucketingSelection, long requestId, long requestTimeoutMs,
        String brokerId) {
      _brokerRequest = request;
      _segmentServices = segmentServices;
      _replicaSelection = replicaSelection;
      _replicaSelectionGranularity = replicaSelectionGranularity;
      _hashKey = hashKey;
      _numSpeculativeRequests = numSpeculativeRequests;
      _bucketingSelection = bucketingSelection;
      _requestId = requestId;
      _requestTimeoutMs = requestTimeoutMs;
      _brokerId = brokerId;
    }

    @Override
    public Map<ServerInstance, SegmentIdSet> getSegmentsServicesMap() {
      return _segmentServices;
    }

    @Override
    public byte[] getRequestForService(ServerInstance service, SegmentIdSet querySegments) {
      InstanceRequest r = new InstanceRequest();
      r.setRequestId(_requestId);
      r.setEnableTrace(_brokerRequest.isEnableTrace());
      r.setQuery(_brokerRequest);
      r.setSearchSegments(querySegments.getSegmentsNameList());
      r.setBrokerId(_brokerId);
      // _serde is not threadsafe.
      return getSerde().serialize(r);
      //      return _serde.serialize(r);
    }

    @Override
    public ReplicaSelection getReplicaSelection() {
      return _replicaSelection;
    }

    @Override
    public ReplicaSelectionGranularity getReplicaSelectionGranularity() {
      return _replicaSelectionGranularity;
    }

    @Override
    public Object getHashKey() {
      return _hashKey;
    }

    @Override
    public int getNumSpeculativeRequests() {
      return _numSpeculativeRequests;
    }

    @Override
    public BucketingSelection getPredefinedSelection() {
      return _bucketingSelection;
    }

    @Override
    public long getRequestId() {
      return _requestId;
    }

    @Override
    public long getRequestTimeoutMS() {
      return _requestTimeoutMs;
    }

    public SerDe getSerde() {
      return new SerDe(new TCompactProtocol.Factory());
    }

    @Override
    public BrokerRequest getBrokerRequest() {
      return _brokerRequest;
    }
  }

  public String getRoutingTableSnapshot(String tableName)
      throws Exception {
    return _routingTable.dumpSnapshot(tableName);
  }
}
