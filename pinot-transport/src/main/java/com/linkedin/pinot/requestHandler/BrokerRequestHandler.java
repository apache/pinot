/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.annotation.ThreadSafe;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
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


/**
 * Request Handler to serve a Broker Request. THis is thread-safe and clients
 * can concurrently submit requests to the main method.
 *
 */
@ThreadSafe
public class BrokerRequestHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerRequestHandler.class);

  private final RoutingTable _routingTable;
  private final ScatterGather _scatterGatherer;
  private final ReduceService _reduceService;
  private final BrokerMetrics _brokerMetrics;
  private final TimeBoundaryService _timeBoundaryService;
  private final long _brokerTimeOutMs;

  //TODO: Currently only using RoundRobin selection. But, this can be allowed to be configured.
  private RoundRobinReplicaSelection _replicaSelection;

  public BrokerRequestHandler(RoutingTable table, TimeBoundaryService timeBoundaryService,
      ScatterGather scatterGatherer, ReduceService reduceService, BrokerMetrics brokerMetrics, long brokerTimeOutMs) {
    _routingTable = table;
    _timeBoundaryService = timeBoundaryService;
    _scatterGatherer = scatterGatherer;
    _replicaSelection = new RoundRobinReplicaSelection();
    _reduceService = reduceService;
    _brokerMetrics = brokerMetrics;
    _brokerTimeOutMs = brokerTimeOutMs;
  }

  /**
   * Main method to process the request. Following lifecycle stages:
   * 1. This method will first find the candidate servers to be queried for each set of segments from the routing table
   * 2. The second stage will be to select servers for each segment set.
   * 3. Scatter-Gather of request
   * 4. Gather response from the servers.
   * 5. Deserialize the responses and errors.
   * 6. Reduce (Merge) the responses. Create a broker response to be returned.
   *
   * @param request Broker Request to be sent
   * @return Broker response
   * @throws InterruptedException
   */
  //TODO: Define a broker response class and return
  public Object processBrokerRequest(final BrokerRequest request, BucketingSelection overriddenSelection,
      final ScatterGatherStats scatterGatherStats, final long requestId)
      throws InterruptedException {
    if (request == null || request.getQuerySource() == null || request.getQuerySource().getTableName() == null) {
      LOGGER.info("Query contains null table.");
      return BrokerResponse.getNullBrokerResponse();
    }
    List<String> matchedTables = getMatchedTables(request);
    if (matchedTables.size() > 1) {
      return processFederatedBrokerRequest(request, overriddenSelection, scatterGatherStats, requestId);
    }
    if (matchedTables.size() == 1) {
      return processSingleTableBrokerRequest(request, matchedTables.get(0), overriddenSelection, scatterGatherStats, requestId);
    }
    return BrokerResponse.getNullBrokerResponse();
  }

  /**
   * Given a request, will look up routing table to see how many tables are matched there.
   *
   * @param request
   * @return
   */
  private List<String> getMatchedTables(BrokerRequest request) {
    List<String> matchedTables = new ArrayList<String>();
    String tableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(request.getQuerySource().getTableName());
    if (_routingTable.findServers(new RoutingTableLookupRequest(tableName)) != null) {
      matchedTables.add(tableName);
    }
    tableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(request.getQuerySource().getTableName());
    if (_routingTable.findServers(new RoutingTableLookupRequest(tableName)) != null) {
      matchedTables.add(tableName);
    }
    // For backward compatible
    if (matchedTables.isEmpty()) {
      tableName = request.getQuerySource().getTableName();
      if (_routingTable.findServers(new RoutingTableLookupRequest(tableName)) != null) {
        matchedTables.add(tableName);
      }
    }
    return matchedTables;
  }

  private Object processSingleTableBrokerRequest(final BrokerRequest request, String matchedTableName,
      BucketingSelection overriddenSelection, final ScatterGatherStats scatterGatherStats,
      final long requestId) throws InterruptedException {
    request.getQuerySource().setTableName(matchedTableName);
    return getDataTableFromBrokerRequest(request, null, scatterGatherStats, requestId);
  }

  private Object processFederatedBrokerRequest(final BrokerRequest request, BucketingSelection overriddenSelection,
      final ScatterGatherStats scatterGatherStats, final long requestId) {
    List<BrokerRequest> perTableRequests = new ArrayList<BrokerRequest>();
    perTableRequests.add(getRealtimeBrokerRequest(request));
    perTableRequests.add(getOfflineBrokerRequest(request));
    try {
      return getDataTableFromBrokerRequestList(request, perTableRequests, null, scatterGatherStats, requestId);
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing federated broker request", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  private BrokerRequest getOfflineBrokerRequest(BrokerRequest request) {
    BrokerRequest offlineRequest = request.deepCopy();
    String hybridTableName = request.getQuerySource().getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(hybridTableName);
    offlineRequest.getQuerySource().setTableName(offlineTableName);
    attachTimeBoundary(hybridTableName, offlineRequest, true);
    return offlineRequest;
  }

  private BrokerRequest getRealtimeBrokerRequest(BrokerRequest request) {
    BrokerRequest realtimeRequest = request.deepCopy();
    String hybridTableName = request.getQuerySource().getTableName();
    String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(hybridTableName);
    realtimeRequest.getQuerySource().setTableName(realtimeTableName);
    attachTimeBoundary(hybridTableName, realtimeRequest, false);
    return realtimeRequest;
  }

  private void attachTimeBoundary(String hybridTableName, BrokerRequest offlineRequest, boolean isOfflineRequest) {
    TimeBoundaryInfo timeBoundaryInfo =
        _timeBoundaryService.getTimeBoundaryInfoFor(TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER
            .forTable(hybridTableName));
    if (timeBoundaryInfo == null || timeBoundaryInfo.getTimeColumn() == null || timeBoundaryInfo.getTimeValue() == null) {
      return;
    }
    FilterQuery timeFilterQuery = new FilterQuery();
    timeFilterQuery.setOperator(FilterOperator.RANGE);
    timeFilterQuery.setColumn(timeBoundaryInfo.getTimeColumn());
    timeFilterQuery.setNestedFilterQueryIds(new ArrayList<Integer>());
    List<String> values = new ArrayList<String>();
    if (isOfflineRequest) {
      values.add("(*\t\t" + timeBoundaryInfo.getTimeValue() + ")");
    } else {
      values.add("[" + timeBoundaryInfo.getTimeValue() + "\t\t*)");
    }
    timeFilterQuery.setValue(values);
    timeFilterQuery.setId(-1);
    FilterQuery currentFilterQuery = offlineRequest.getFilterQuery();
    if (currentFilterQuery != null) {
      FilterQuery andFilterQuery = new FilterQuery();
      andFilterQuery.setOperator(FilterOperator.AND);
      List<Integer> nestedFilterQueryIds = new ArrayList<Integer>();
      nestedFilterQueryIds.add(currentFilterQuery.getId());
      nestedFilterQueryIds.add(timeFilterQuery.getId());
      andFilterQuery.setNestedFilterQueryIds(nestedFilterQueryIds);
      andFilterQuery.setId(-2);
      FilterQueryMap filterSubQueryMap = offlineRequest.getFilterSubQueryMap();
      filterSubQueryMap.putToFilterQueryMap(timeFilterQuery.getId(), timeFilterQuery);
      filterSubQueryMap.putToFilterQueryMap(andFilterQuery.getId(), andFilterQuery);

      offlineRequest.setFilterQuery(andFilterQuery);
      offlineRequest.setFilterSubQueryMap(filterSubQueryMap);
    } else {
      FilterQueryMap filterSubQueryMap = new FilterQueryMap();
      filterSubQueryMap.putToFilterQueryMap(timeFilterQuery.getId(), timeFilterQuery);
      offlineRequest.setFilterQuery(timeFilterQuery);
      offlineRequest.setFilterSubQueryMap(filterSubQueryMap);
    }
  }

  private Object getDataTableFromBrokerRequest(final BrokerRequest request, BucketingSelection overriddenSelection,
      final ScatterGatherStats scatterGatherStats, final long requestId)
      throws InterruptedException {
    // Step1
    final long routingStartTime = System.nanoTime();
    RoutingTableLookupRequest rtRequest = new RoutingTableLookupRequest(request.getQuerySource().getTableName());
    Map<ServerInstance, SegmentIdSet> segmentServices = _routingTable.findServers(rtRequest);
    if (segmentServices == null || segmentServices.isEmpty()) {
      LOGGER.warn("Not found ServerInstances to Segments Mapping:");
      return BrokerResponse.getEmptyBrokerResponse();
    }

    final long queryRoutingTime = System.nanoTime() - routingStartTime;
    _brokerMetrics.addPhaseTiming(request, BrokerQueryPhase.QUERY_ROUTING, queryRoutingTime);

    // Step 2-4
    final long scatterGatherStartTime = System.nanoTime();
    ScatterGatherRequestImpl scatterRequest =
        new ScatterGatherRequestImpl(request, segmentServices, _replicaSelection,
            ReplicaSelectionGranularity.SEGMENT_ID_SET, request.getBucketHashKey(), 0, //TODO: Speculative Requests not yet supported
            overriddenSelection, requestId, _brokerTimeOutMs);
    CompositeFuture<ServerInstance, ByteBuf> response = _scatterGatherer.scatterGather(scatterRequest, scatterGatherStats,
        _brokerMetrics);

    //Step 5 - Deserialize Responses and build instance response map
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    {
      Map<ServerInstance, ByteBuf> responses = null;
      try {
        responses = response.get();
        // The call above should have waited for all the responses to come in.
        Map<String, Long> responseTimes = response.getResponseTimes();
        scatterGatherStats.setResponseTimeMillis(responseTimes);
      } catch (ExecutionException e) {
        LOGGER.warn("Caught exception while fetching response", e);
        _brokerMetrics.addMeteredValue(request, BrokerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
      }

      final long scatterGatherTime = System.nanoTime() - scatterGatherStartTime;
      _brokerMetrics.addPhaseTiming(request, BrokerQueryPhase.SCATTER_GATHER, scatterGatherTime);

      final long deserializationStartTime = System.nanoTime();

      Map<ServerInstance, Throwable> errors = response.getError();

      if (null != responses) {
        for (Entry<ServerInstance, ByteBuf> e : responses.entrySet()) {
          try {
            ByteBuf b = e.getValue();
            byte[] b2 = new byte[b.readableBytes()];
            if (b2 == null || b2.length == 0) {
              continue;
            }
            b.readBytes(b2);
            DataTable r2 = new DataTable(b2);
            if (errors != null && errors.containsKey(e.getKey())) {
              Throwable throwable = errors.get(e.getKey());
              r2.getMetadata().put("exception", new RequestProcessingException(throwable).toString());
              _brokerMetrics.addMeteredValue(request, BrokerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
            }
            instanceResponseMap.put(e.getKey(), r2);
          } catch (Exception ex) {
            LOGGER.error("Got exceptions in collect query result for instance " + e.getKey() + ", error: "
                + ex.getMessage(), ex);
            _brokerMetrics.addMeteredValue(request, BrokerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
          }
        }
      }
      final long deserializationTime = System.nanoTime() - deserializationStartTime;
      _brokerMetrics.addPhaseTiming(request, BrokerQueryPhase.DESERIALIZATION, deserializationTime);
    }

    // Step 6 : Do the reduce and return
    try {
      return _brokerMetrics.timePhase(request, BrokerQueryPhase.REDUCE, new Callable<BrokerResponse>() {
        @Override
        public BrokerResponse call() {
          BrokerResponse returnValue = _reduceService.reduceOnDataTable(request, instanceResponseMap);
          _brokerMetrics.addMeteredValue(request, BrokerMeter.DOCUMENTS_SCANNED, returnValue.getNumDocsScanned());
          return returnValue;
        }
      });
    } catch (Exception e) {
      // Shouldn't happen, this is only here because timePhase() can throw a checked exception, even though the nested callable can't.
      LOGGER.error("Caught exception while processing return", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
    }
  }

  private Object getDataTableFromBrokerRequestList(final BrokerRequest federatedBrokerRequest,
      final List<BrokerRequest> requests, BucketingSelection overriddenSelection,
      final ScatterGatherStats scatterGatherStats, final long requestId) throws InterruptedException {
    // Step1
    long scatterGatherStartTime = System.nanoTime();
    long queryRoutingTime = 0;
    Map<BrokerRequest, Pair<CompositeFuture<ServerInstance, ByteBuf>, ScatterGatherStats>> responseFuturesList =
        new HashMap<BrokerRequest, Pair<CompositeFuture<ServerInstance, ByteBuf>, ScatterGatherStats>>();
    for (BrokerRequest request : requests) {
      final long routingStartTime = System.nanoTime();
      RoutingTableLookupRequest rtRequest = new RoutingTableLookupRequest(request.getQuerySource().getTableName());
      Map<ServerInstance, SegmentIdSet> segmentServices = _routingTable.findServers(rtRequest);
      if (segmentServices == null || segmentServices.isEmpty()) {
        LOGGER.info("Not found ServerInstances to Segments Mapping for Table - {}", rtRequest.getTableName());
        continue;
      }
      LOGGER.debug("Find ServerInstances to Segments Mapping for table - {}",  rtRequest.getTableName());
      for (ServerInstance serverInstance : segmentServices.keySet()) {
        LOGGER.debug("{} : {}", serverInstance, segmentServices.get(serverInstance));
      }
      queryRoutingTime += System.nanoTime() - routingStartTime;
      ScatterGatherStats respStats = new ScatterGatherStats();

      // Step 2-4
      scatterGatherStartTime = System.nanoTime();
      ScatterGatherRequestImpl scatterRequest =
          new ScatterGatherRequestImpl(request, segmentServices, _replicaSelection,
              ReplicaSelectionGranularity.SEGMENT_ID_SET, request.getBucketHashKey(), 0, //TODO: Speculative Requests not yet supported
              overriddenSelection, requestId, _brokerTimeOutMs);
      responseFuturesList.put(request, Pair.of(_scatterGatherer.scatterGather(scatterRequest, scatterGatherStats,
          _brokerMetrics), respStats));
    }
    _brokerMetrics.addPhaseTiming(federatedBrokerRequest, BrokerQueryPhase.QUERY_ROUTING, queryRoutingTime);

    long scatterGatherTime = 0;
    long deserializationTime = 0;
    //Step 5 - Deserialize Responses and build instance response map
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    final AtomicInteger responseSeq = new AtomicInteger(-1);
    {
      for (BrokerRequest request : responseFuturesList.keySet()) {
        CompositeFuture<ServerInstance, ByteBuf> compositeFuture = responseFuturesList.get(request).getKey();
        ScatterGatherStats respStats = responseFuturesList.get(request).getValue();

        Map<ServerInstance, ByteBuf> responseMap = null;
        try {
          responseMap = compositeFuture.get();
          // The 'get' call above waits for all the responses to come in before returning.
          // compositeFuture has the individual response times of each underlying future.
          // We get a map of server to the response time of the server here.
          Map<String, Long> responseTimeMap = compositeFuture.getResponseTimes();
          respStats.setResponseTimeMillis(responseTimeMap);
          scatterGatherStats.merge(respStats);
        } catch (ExecutionException e) {
          LOGGER.warn("Caught exception while fetching response", e);
          _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
        }

        scatterGatherTime += System.nanoTime() - scatterGatherStartTime;

        final long deserializationStartTime = System.nanoTime();

        Map<ServerInstance, Throwable> errors = compositeFuture.getError();

        if (null != responseMap) {
          for (Entry<ServerInstance, ByteBuf> responseEntry : responseMap.entrySet()) {
            try {
              ByteBuf b = responseEntry.getValue();
              byte[] b2 = new byte[b.readableBytes()];
              if (b2 == null || b2.length == 0) {
                continue;
              }
              b.readBytes(b2);
              DataTable r2 = new DataTable(b2);
              // Hybrid requests may get response from same instance, so we need to distinguish them.
              ServerInstance decoratedServerInstance =
                  new ServerInstance(responseEntry.getKey().getHostname(), responseEntry.getKey().getPort(), responseSeq.incrementAndGet());
              if (errors != null && errors.containsKey(responseEntry.getKey())) {
                Throwable throwable = errors.get(responseEntry.getKey());
                if (throwable != null) {
                  r2.getMetadata().put("exception", new RequestProcessingException(throwable).toString());
                  _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
                }
              }
              instanceResponseMap.put(decoratedServerInstance, r2);
            } catch (Exception ex) {
              LOGGER.error("Got exceptions in collect query result for instance " + responseEntry.getKey() + ", error: "
                  + ex.getMessage(), ex);
              _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
            }
          }
        }
        deserializationTime += System.nanoTime() - deserializationStartTime;
      }
    }
    _brokerMetrics.addPhaseTiming(federatedBrokerRequest, BrokerQueryPhase.SCATTER_GATHER, scatterGatherTime);
    _brokerMetrics.addPhaseTiming(federatedBrokerRequest, BrokerQueryPhase.DESERIALIZATION, deserializationTime);

    // Step 6 : Do the reduce and return
    try {
      return _brokerMetrics.timePhase(federatedBrokerRequest, BrokerQueryPhase.REDUCE, new Callable<BrokerResponse>() {
        @Override
        public BrokerResponse call() {
          BrokerResponse returnValue = _reduceService.reduceOnDataTable(federatedBrokerRequest, instanceResponseMap);
          _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.DOCUMENTS_SCANNED,
              returnValue.getNumDocsScanned());
          return returnValue;
        }
      });
    } catch (Exception e) {
      // Shouldn't happen, this is only here because timePhase() can throw a checked exception, even though the nested callable can't.
      LOGGER.error("Caught exception while processing query", e);
      Utils.rethrowException(e);
      throw new AssertionError("Should not reach this");
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

    public ScatterGatherRequestImpl(BrokerRequest request, Map<ServerInstance, SegmentIdSet> segmentServices,
        ReplicaSelection replicaSelection, ReplicaSelectionGranularity replicaSelectionGranularity, Object hashKey,
        int numSpeculativeRequests, BucketingSelection bucketingSelection, long requestId, long requestTimeoutMs) {
      _brokerRequest = request;
      _segmentServices = segmentServices;
      _replicaSelection = replicaSelection;
      _replicaSelectionGranularity = replicaSelectionGranularity;
      _hashKey = hashKey;
      _numSpeculativeRequests = numSpeculativeRequests;
      _bucketingSelection = bucketingSelection;
      _requestId = requestId;
      _requestTimeoutMs = requestTimeoutMs;
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
  
  public String getDebugInfo() throws Exception {
    return _routingTable.dumpSnapShot();
  }
}
