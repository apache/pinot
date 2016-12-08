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
package com.linkedin.pinot.requestHandler;

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
import java.util.Arrays;
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
   *   <li>3. Gather response from the servers.</li>
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

    List<String> matchedTables = getMatchedTables(tableName);
    int numMatchedTables = matchedTables.size();
    if (numMatchedTables == 0) {
      // No table matches the broker request.
      LOGGER.warn("No table matches the name: {}", tableName);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.RESOURCE_MISSING_EXCEPTIONS, 1);
      return BrokerResponseFactory.getStaticNoTableHitBrokerResponse(responseType);
    } else {
      ReduceService reduceService = _reduceServiceRegistry.get(responseType);
      // TODO: wire up the customized BucketingSelection.
      if (numMatchedTables == 1) {
        // Broker request hits one table.
        brokerRequest.getQuerySource().setTableName(matchedTables.get(0));
        return processSingleTableBrokerRequest(_optimizer.optimize(brokerRequest), reduceService, null,
            scatterGatherStats, requestId);
      } else {
        // Broker request hits multiple tables.
        List<BrokerRequest> perTableRequests =
            Arrays.asList(getOfflineBrokerRequest(brokerRequest), getRealtimeBrokerRequest(brokerRequest));
        return processFederatedBrokerRequest(brokerRequest, reduceService, perTableRequests, null, scatterGatherStats,
            requestId);
      }
    }
  }

  /**
   * Given a table name, look up routing table and get all tables (OFFLINE/REALTIME) matched.
   *
   * @param tableName table name to be looked up.
   * @return list of matched tables.
   */
  @Nonnull
  private List<String> getMatchedTables(@Nonnull String tableName) {
    List<String> matchedTables = new ArrayList<>();
    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
    if (_routingTable.routingTableExists(offlineTableName)) {
      matchedTables.add(offlineTableName);
    }
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
    if (_routingTable.routingTableExists(realtimeTableName)) {
      matchedTables.add(realtimeTableName);
    }
    // For backward compatible.
    if (matchedTables.isEmpty()) {
      if (_routingTable.routingTableExists(tableName)) {
        matchedTables.add(tableName);
      }
    }
    return matchedTables;
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
   * Process single broker request on single table.
   *
   * @param brokerRequest broker request with modified table name.
   * @param reduceService reduce service.
   * @param bucketingSelection customized bucketing selection.
   * @param scatterGatherStats scatter-gather statistics.
   * @param requestId request ID.
   * @return broker response.
   * @throws InterruptedException
   */
  @Nonnull
  private BrokerResponse processSingleTableBrokerRequest(@Nonnull BrokerRequest brokerRequest,
      @Nonnull ReduceService reduceService, @Nullable BucketingSelection bucketingSelection,
      @Nonnull ScatterGatherStats scatterGatherStats, long requestId)
      throws InterruptedException {
    String tableName = brokerRequest.getQuerySource().getTableName();
    ResponseType brokerResponseType = BrokerResponseFactory.getResponseType(brokerRequest.getResponseFormat());

    // Step 1: find the candidate servers to be queried for each set of segments from the routing table.
    // TODO: add checks for whether all segments are covered.
    long routingStartTime = System.nanoTime();
    Map<ServerInstance, SegmentIdSet> segmentServices = findCandidateServers(brokerRequest);
    if (segmentServices == null || segmentServices.isEmpty()) {
      LOGGER.warn("No server found for table: {}", tableName);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
      return BrokerResponseFactory.getStaticEmptyBrokerResponse(brokerResponseType);
    }
    _brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.QUERY_ROUTING, System.nanoTime() - routingStartTime);

    // Step 2: select servers for each segment set and scatter request to the servers.
    long scatterStartTime = System.nanoTime();
    ScatterGatherRequestImpl scatterRequest =
        new ScatterGatherRequestImpl(brokerRequest, segmentServices, _replicaSelection,
            ReplicaSelectionGranularity.SEGMENT_ID_SET, brokerRequest.getBucketHashKey(), 0, bucketingSelection,
            requestId, _brokerTimeOutMs, _brokerId);
    CompositeFuture<ServerInstance, ByteBuf> compositeFuture =
        _scatterGatherer.scatterGather(scatterRequest, scatterGatherStats, _brokerMetrics);

    // Step 3: gather response from the servers.
    Map<ServerInstance, ByteBuf> serverResponseMap;
    try {
      serverResponseMap = compositeFuture.get();
      Map<String, Long> serverResponseTimes = compositeFuture.getResponseTimes();
      scatterGatherStats.setResponseTimeMillis(serverResponseTimes);
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching responses.", e);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1);
      return BrokerResponseFactory.getBrokerResponseWithException(brokerResponseType,
          QueryException.getException(QueryException.BROKER_GATHER_ERROR, e));
    }
    _brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.SCATTER_GATHER, System.nanoTime() - scatterStartTime);

    //Step 4: deserialize the server responses.
    long deserializationStartTime = System.nanoTime();
    Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();
    List<ProcessingException> processingExceptions = new ArrayList<>();
    deserializeServerResponses(tableName, serverResponseMap, -1, dataTableMap, processingExceptions);
    _brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.DESERIALIZATION,
        System.nanoTime() - deserializationStartTime);

    // Step 5: reduce (merge) the server responses and create a broker response to be returned.
    long reduceStartTime = System.nanoTime();
    BrokerResponse brokerResponse = reduceService.reduceOnDataTable(brokerRequest, dataTableMap, _brokerMetrics);
    _brokerMetrics.addPhaseTiming(tableName, BrokerQueryPhase.REDUCE, System.nanoTime() - reduceStartTime);

    // Set processing exceptions and return.
    brokerResponse.setExceptions(processingExceptions);
    return brokerResponse;
  }

  /**
   * Process federated (hybrid) broker requests on both OFFLINE and REALTIME tables.
   *
   * @param originalBrokerRequest original broker request.
   * @param reduceService reduce service.
   * @param perTableRequests list of broker requests per table with modified table name.
   * @param bucketingSelection customized bucketing selection.
   * @param scatterGatherStats scatter-gather statistics.
   * @param requestId request ID.
   * @return broker response.
   * @throws InterruptedException
   */
  @Nonnull
  private BrokerResponse processFederatedBrokerRequest(@Nonnull BrokerRequest originalBrokerRequest,
      @Nonnull ReduceService reduceService, @Nonnull List<BrokerRequest> perTableRequests,
      @Nullable BucketingSelection bucketingSelection, @Nonnull ScatterGatherStats scatterGatherStats, long requestId)
      throws InterruptedException {
    // TODO: for hybrid use case, if both OFFLINE and REALTIME segments queried are on the same server, then the scatter
    // gather stats for that server will be override. Fix this by passing OFFLINE/REALTIME info into the scatter gather
    // stats and name server name for OFFLINE and REALTIME differently inside the map.

    String originalTableName = originalBrokerRequest.getQuerySource().getTableName();
    ResponseType serverResponseType = BrokerResponseFactory.getResponseType(originalBrokerRequest.getResponseFormat());
    long routingTime = 0L;
    long scatterGatherTime = 0L;
    Map<String, CompositeFuture<ServerInstance, ByteBuf>> compositeFutureMap = new HashMap<>(perTableRequests.size());

    for (BrokerRequest brokerRequest : perTableRequests) {
      String tableName = brokerRequest.getQuerySource().getTableName();

      // Step 1: find the candidate servers to be queried for each set of segments from the routing table.
      // TODO: add checks for whether all segments are covered.
      long routingStartTime = System.nanoTime();
      Map<ServerInstance, SegmentIdSet> segmentServices = findCandidateServers(brokerRequest);
      if (segmentServices == null || segmentServices.isEmpty()) {
        LOGGER.warn("No server found for table: {}", tableName);
        _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.NO_SERVER_FOUND_EXCEPTIONS, 1);
        continue;
      }
      routingTime += System.nanoTime() - routingStartTime;

      // Step 2: select servers for each segment set and scatter request to the servers.
      long scatterStartTime = System.nanoTime();
      ScatterGatherRequestImpl scatterRequest =
          new ScatterGatherRequestImpl(brokerRequest, segmentServices, _replicaSelection,
              ReplicaSelectionGranularity.SEGMENT_ID_SET, brokerRequest.getBucketHashKey(), 0, bucketingSelection,
              requestId, _brokerTimeOutMs, _brokerId);
      compositeFutureMap.put(tableName,
          _scatterGatherer.scatterGather(scatterRequest, scatterGatherStats, _brokerMetrics));
      scatterGatherTime += System.nanoTime() - scatterStartTime;
    }
    if (compositeFutureMap.isEmpty()) {
      // No server found for federated table.
      return BrokerResponseFactory.getStaticEmptyBrokerResponse(serverResponseType);
    }
    _brokerMetrics.addPhaseTiming(originalTableName, BrokerQueryPhase.QUERY_ROUTING, routingTime);

    // Step 3: gather response from the servers.
    long gatherStartTime = System.nanoTime();
    Map<String, Map<ServerInstance, ByteBuf>> serverResponseMaps = new HashMap<>(compositeFutureMap.size());
    List<ProcessingException> processingExceptions = new ArrayList<>();
    for (Entry<String, CompositeFuture<ServerInstance, ByteBuf>> entry : compositeFutureMap.entrySet()) {
      String tableName = entry.getKey();
      CompositeFuture<ServerInstance, ByteBuf> compositeFuture = entry.getValue();
      Map<ServerInstance, ByteBuf> serverResponseMap;
      try {
        serverResponseMap = compositeFuture.get();
        Map<String, Long> responseTimes = compositeFuture.getResponseTimes();
        scatterGatherStats.setResponseTimeMillis(responseTimes);
        serverResponseMaps.put(tableName, serverResponseMap);
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching responses for table: {}", tableName, e);
        _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.RESPONSE_FETCH_EXCEPTIONS, 1);
        processingExceptions.add(QueryException.getException(QueryException.BROKER_GATHER_ERROR, e));
      }
    }
    int numServerResponseMaps = serverResponseMaps.size();
    if (numServerResponseMaps == 0) {
      // No response gathered.
      return BrokerResponseFactory.getBrokerResponseWithExceptions(serverResponseType, processingExceptions);
    }
    scatterGatherTime += System.nanoTime() - gatherStartTime;
    _brokerMetrics.addPhaseTiming(originalTableName, BrokerQueryPhase.SCATTER_GATHER, scatterGatherTime);

    //Step 4: deserialize the server responses.
    long deserializationStartTime = System.nanoTime();
    Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();
    int responseSequence = 0;
    for (Entry<String, Map<ServerInstance, ByteBuf>> entry : serverResponseMaps.entrySet()) {
      String tableName = entry.getKey();
      Map<ServerInstance, ByteBuf> serverResponseMap = entry.getValue();
      deserializeServerResponses(tableName, serverResponseMap, responseSequence, dataTableMap, processingExceptions);
      responseSequence++;
    }
    _brokerMetrics.addPhaseTiming(originalTableName, BrokerQueryPhase.DESERIALIZATION,
        System.nanoTime() - deserializationStartTime);

    // Step 5: reduce (merge) the server responses and create a broker response to be returned.
    long reduceStartTime = System.nanoTime();
    BrokerResponse brokerResponse =
        reduceService.reduceOnDataTable(originalBrokerRequest, dataTableMap, _brokerMetrics);
    _brokerMetrics.addPhaseTiming(originalTableName, BrokerQueryPhase.REDUCE, System.nanoTime() - reduceStartTime);

    // Set processing exceptions and return.
    brokerResponse.setExceptions(processingExceptions);
    return brokerResponse;
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
   * Deserialize the server responses, put the de-serialized data table into the data table map passed in, append
   * processing exceptions to the processing exception list passed in.
   * <p>For federated (hybrid) request, multiple responses might be from the same instance. Use response sequence to
   * distinguish them.
   *
   * @param tableName table name.
   * @param responseMap map from server to response.
   * @param responseSequence response sequence for federated request.
   * @param dataTableMap map from server to data table.
   * @param processingExceptions list of processing exceptions.
   */
  private void deserializeServerResponses(@Nonnull String tableName, @Nonnull Map<ServerInstance, ByteBuf> responseMap,
      int responseSequence, @Nonnull Map<ServerInstance, DataTable> dataTableMap,
      @Nonnull List<ProcessingException> processingExceptions) {
    for (Entry<ServerInstance, ByteBuf> entry : responseMap.entrySet()) {
      ServerInstance serverInstance = entry.getKey();
      if (responseSequence >= 0) {
        serverInstance = new ServerInstance(serverInstance.getHostname(), serverInstance.getPort(), responseSequence);
      }
      ByteBuf byteBuf = entry.getValue();
      try {
        byte[] byteArray = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(byteArray);
        dataTableMap.put(serverInstance, new DataTable(byteArray));
      } catch (Exception e) {
        LOGGER.error("Caught exceptions while deserializing response for table: {} from server: {}", tableName,
            serverInstance, e);
        _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.DATA_TABLE_DESERIALIZATION_EXCEPTIONS, 1);
        processingExceptions.add(QueryException.getException(QueryException.DATA_TABLE_DESERIALIZATION_ERROR, e));
      }
    }
  }

  public static class ScatterGatherRequestImpl implements ScatterGatherRequest {
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

  public static class RequestProcessingException extends ProcessingException {
    private static int REQUEST_ERROR = -100;

    public RequestProcessingException(Throwable rootCause) {
      super(REQUEST_ERROR);
      initCause(rootCause);
      setMessage(rootCause.getMessage());
    }
  }

  public String getRoutingTableSnapshot(String tableName)
      throws Exception {
    return _routingTable.dumpSnapshot(tableName);
  }
}
