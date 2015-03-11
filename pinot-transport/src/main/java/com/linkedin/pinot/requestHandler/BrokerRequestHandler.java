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

import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.annotation.ThreadSafe;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TCompactProtocol;

import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
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


/**
 * Request Handler to serve a Broker Request. THis is thread-safe and clients
 * can concurrently submit requests to the main method.
 * @author bvaradar
 *
 */
@ThreadSafe
public class BrokerRequestHandler {

  private final RoutingTable _routingTable;
  private final ScatterGather _scatterGatherer;
  private final AtomicLong _requestIdGen;
  private final ReduceService _reduceService;
  private final BrokerMetrics _brokerMetrics;
  private final TimeBoundaryService _timeBoundaryService;
  private final static Logger LOGGER = Logger.getLogger(BrokerRequestHandler.class);

  //TODO: Currently only using RoundRobin selection. But, this can be allowed to be configured.
  private RoundRobinReplicaSelection _replicaSelection;

  public BrokerRequestHandler(RoutingTable table, TimeBoundaryService timeBoundaryService, ScatterGather scatterGatherer, ReduceService reduceService,
      BrokerMetrics brokerMetrics) {
    _routingTable = table;
    _timeBoundaryService = timeBoundaryService;
    _scatterGatherer = scatterGatherer;
    _requestIdGen = new AtomicLong(0);
    _replicaSelection = new RoundRobinReplicaSelection();
    _reduceService = reduceService;
    _brokerMetrics = brokerMetrics;
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
  public Object processBrokerRequest(final BrokerRequest request, BucketingSelection overriddenSelection)
      throws InterruptedException {
    if (request == null || request.getQuerySource() == null || request.getQuerySource().getResourceName() == null) {
      LOGGER.info("Query contains null resource.");
      return BrokerResponse.getNullBrokerResponse();
    }
    List<String> matchedResources = getMatchedResources(request);
    if (matchedResources.size() > 1) {
      return processFederatedBrokerRequest(request, overriddenSelection);
    }
    if (matchedResources.size() == 1) {
      return processSingleResourceBrokerRequest(request, matchedResources.get(0), overriddenSelection);
    }
    return BrokerResponse.getNullBrokerResponse();
  }

  /**
   * Given a request, will look up routing table to see how many resources are matched there.
   *
   * @param request
   * @return
   */
  private List<String> getMatchedResources(BrokerRequest request) {
    List<String> matchedResources = new ArrayList<String>();
    String resourceName =
        BrokerRequestUtils.getOfflineResourceNameForResource(request.getQuerySource().getResourceName());
    if (_routingTable.findServers(new RoutingTableLookupRequest(resourceName)) != null) {
      matchedResources.add(resourceName);
    }
    resourceName =
        BrokerRequestUtils.getRealtimeResourceNameForResource(request.getQuerySource().getResourceName());
    if (_routingTable.findServers(new RoutingTableLookupRequest(resourceName)) != null) {
      matchedResources.add(resourceName);
    }
    // For backward compatible
    if (matchedResources.isEmpty()) {
      resourceName =
          request.getQuerySource().getResourceName();
      if (_routingTable.findServers(new RoutingTableLookupRequest(resourceName)) != null) {
        matchedResources.add(resourceName);
      }
    }
    return matchedResources;
  }

  private Object processSingleResourceBrokerRequest(final BrokerRequest request, String matchedResourceName, BucketingSelection overriddenSelection) throws InterruptedException {
    request.getQuerySource().setResourceName(matchedResourceName);
    return getDataTableFromBrokerRequest(request, null);
  }

  private Object processFederatedBrokerRequest(final BrokerRequest request, BucketingSelection overriddenSelection) {
    List<BrokerRequest> perResourceRequests = new ArrayList<BrokerRequest>();
    perResourceRequests.add(getRealtimeBrokerRequest(request));
    perResourceRequests.add(getOfflineBrokerRequest(request));
    try {
      return getDataTableFromBrokerRequestList(request, perResourceRequests, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private BrokerRequest getOfflineBrokerRequest(BrokerRequest request) {
    BrokerRequest offlineRequest = request.deepCopy();
    String hybridResourceName = request.getQuerySource().getResourceName();
    String offlineResourceName = BrokerRequestUtils.getOfflineResourceNameForResource(hybridResourceName);
    offlineRequest.getQuerySource().setResourceName(offlineResourceName);
    attachTimeBoundary(hybridResourceName, offlineRequest, true);
    return offlineRequest;
  }

  private BrokerRequest getRealtimeBrokerRequest(BrokerRequest request) {
    BrokerRequest realtimeRequest = request.deepCopy();
    String hybridResourceName = request.getQuerySource().getResourceName();
    String realtimeResourceName = BrokerRequestUtils.getRealtimeResourceNameForResource(hybridResourceName);
    realtimeRequest.getQuerySource().setResourceName(realtimeResourceName);
    attachTimeBoundary(hybridResourceName, realtimeRequest, false);
    return realtimeRequest;
  }

  private void attachTimeBoundary(String hybridResourceName, BrokerRequest offlineRequest, boolean isOfflineRequest) {
    TimeBoundaryInfo timeBoundaryInfo = _timeBoundaryService.getTimeBoundaryInfoFor(hybridResourceName);
    if (timeBoundaryInfo == null || timeBoundaryInfo.getTimeColumn() == null || timeBoundaryInfo.getTimeValue() == null) {
      return;
    }
    FilterQuery timeFilterQuery = new FilterQuery();
    timeFilterQuery.setOperator(FilterOperator.RANGE);
    timeFilterQuery.setColumn(timeBoundaryInfo.getTimeColumn());
    List<String> values = new ArrayList<String>();
    if (isOfflineRequest) {
      values.add("(*," + timeBoundaryInfo.getTimeValue() + ")");
    } else {
      values.add("[" + timeBoundaryInfo.getTimeValue() + ", *)");
    }
    timeFilterQuery.setValue(values);
    FilterQuery currentFilterQuery = offlineRequest.getFilterQuery();

    FilterQuery andFilterQuery = new FilterQuery();
    andFilterQuery.setOperator(FilterOperator.AND);
    List<Integer> nestedFilterQueryIds = new ArrayList<Integer>();
    nestedFilterQueryIds.add(currentFilterQuery.getId());
    nestedFilterQueryIds.add(timeFilterQuery.getId());
    andFilterQuery.setNestedFilterQueryIds(nestedFilterQueryIds);
    andFilterQuery.setId(andFilterQuery.hashCode());

    FilterQueryMap filterSubQueryMap = offlineRequest.getFilterSubQueryMap();

    filterSubQueryMap.putToFilterQueryMap(timeFilterQuery.getId(), timeFilterQuery);
    filterSubQueryMap.putToFilterQueryMap(andFilterQuery.getId(), andFilterQuery);

    offlineRequest.setFilterQuery(andFilterQuery);
    offlineRequest.setFilterSubQueryMap(filterSubQueryMap);
  }

  private Object getDataTableFromBrokerRequest(final BrokerRequest request, BucketingSelection overriddenSelection)
      throws InterruptedException {
    // Step1
    final long routingStartTime = System.nanoTime();
    RoutingTableLookupRequest rtRequest = new RoutingTableLookupRequest(request.getQuerySource().getResourceName());
    Map<ServerInstance, SegmentIdSet> segmentServices = _routingTable.findServers(rtRequest);
    if (segmentServices == null) {
      LOGGER.info("Not found ServerInstances to Segments Mapping:");
      return null;
    }
    LOGGER.info("Find ServerInstances to Segments Mapping:");
    for (ServerInstance serverInstance : segmentServices.keySet()) {
      LOGGER.info(serverInstance + " : " + segmentServices.get(serverInstance));
    }

    final long queryRoutingTime = System.nanoTime() - routingStartTime;
    _brokerMetrics.addPhaseTiming(request, BrokerQueryPhase.QUERY_ROUTING, queryRoutingTime);

    // Step 2-4
    final long scatterGatherStartTime = System.nanoTime();
    ScatterGatherRequestImpl scatterRequest =
        new ScatterGatherRequestImpl(request, segmentServices, _replicaSelection,
            ReplicaSelectionGranularity.SEGMENT_ID_SET, request.getBucketHashKey(), 0, //TODO: Speculative Requests not yet supported
            overriddenSelection, _requestIdGen.incrementAndGet(), 10 * 1000L);
    CompositeFuture<ServerInstance, ByteBuf> response = _scatterGatherer.scatterGather(scatterRequest);

    //Step 5 - Deserialize Responses and build instance response map
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    {
      Map<ServerInstance, ByteBuf> responses = null;
      try {
        responses = response.get();
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
            instanceResponseMap.put(e.getKey(), r2);
          } catch (Exception ex) {
            LOGGER.error("Got exceptions in collect query result for instance " + e.getKey() + ", error: "
                + ex.getMessage());
            _brokerMetrics.addMeteredValue(request, BrokerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
          }
        }
      }
      if (null != errors) {
        for (Entry<ServerInstance, Throwable> e : errors.entrySet()) {
          DataTable r2 = new DataTable();
          r2.getMetadata().put("exception", new RequestProcessingException(e.getValue()).toString());
          instanceResponseMap.put(e.getKey(), r2);
          _brokerMetrics.addMeteredValue(request, BrokerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
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
      throw new RuntimeException(e);
    }
  }

  private Object getDataTableFromBrokerRequestList(final BrokerRequest federatedBrokerRequest, final List<BrokerRequest> requests, BucketingSelection overriddenSelection)
      throws InterruptedException {
    // Step1
    long scatterGatherStartTime = System.nanoTime();
    long queryRoutingTime = 0;
    Map<BrokerRequest, CompositeFuture<ServerInstance, ByteBuf>> responseFuturesList = new HashMap<BrokerRequest, CompositeFuture<ServerInstance, ByteBuf>>();
    for (BrokerRequest request : requests) {
      final long routingStartTime = System.nanoTime();
      RoutingTableLookupRequest rtRequest = new RoutingTableLookupRequest(request.getQuerySource().getResourceName());
      Map<ServerInstance, SegmentIdSet> segmentServices = _routingTable.findServers(rtRequest);
      if (segmentServices == null) {
        LOGGER.info("Not found ServerInstances to Segments Mapping:");
        return null;
      }
      LOGGER.info("Find ServerInstances to Segments Mapping:");
      for (ServerInstance serverInstance : segmentServices.keySet()) {
        LOGGER.info(serverInstance + " : " + segmentServices.get(serverInstance));
      }
      queryRoutingTime += System.nanoTime() - routingStartTime;

      // Step 2-4
      scatterGatherStartTime = System.nanoTime();
      ScatterGatherRequestImpl scatterRequest =
          new ScatterGatherRequestImpl(request, segmentServices, _replicaSelection,
              ReplicaSelectionGranularity.SEGMENT_ID_SET, request.getBucketHashKey(), 0, //TODO: Speculative Requests not yet supported
              overriddenSelection, _requestIdGen.incrementAndGet(), 10 * 1000L);
      responseFuturesList.put(request, _scatterGatherer.scatterGather(scatterRequest));
    }
    _brokerMetrics.addPhaseTiming(federatedBrokerRequest, BrokerQueryPhase.QUERY_ROUTING, queryRoutingTime);

    long scatterGatherTime = 0;
    long deserializationTime = 0;
    //Step 5 - Deserialize Responses and build instance response map
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    {
      for (BrokerRequest request : responseFuturesList.keySet()) {
        CompositeFuture<ServerInstance, ByteBuf> response = responseFuturesList.get(request);

        Map<ServerInstance, ByteBuf> responses = null;
        try {
          responses = response.get();
        } catch (ExecutionException e) {
          LOGGER.warn("Caught exception while fetching response", e);
          _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
        }

        scatterGatherTime += System.nanoTime() - scatterGatherStartTime;

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
              ServerInstance decoratedServerInstance =
                  new ServerInstance(e.getKey().getHostname() + "_" + request.hashCode(), e.getKey().getPort());
              instanceResponseMap.put(decoratedServerInstance, r2);
            } catch (Exception ex) {
              LOGGER.error("Got exceptions in collect query result for instance " + e.getKey() + ", error: "
                  + ex.getMessage());
              _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.REQUEST_DESERIALIZATION_EXCEPTIONS, 1);
            }
          }
        }
        if (null != errors) {
          for (Entry<ServerInstance, Throwable> e : errors.entrySet()) {
            DataTable r2 = new DataTable();
            r2.getMetadata().put("exception", new RequestProcessingException(e.getValue()).toString());
            ServerInstance decoratedServerInstance =
                new ServerInstance(e.getKey().getHostname() + "_" + request.hashCode(), e.getKey().getPort());
            instanceResponseMap.put(decoratedServerInstance, r2);
            _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.REQUEST_FETCH_EXCEPTIONS, 1);
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
          _brokerMetrics.addMeteredValue(federatedBrokerRequest, BrokerMeter.DOCUMENTS_SCANNED, returnValue.getNumDocsScanned());
          return returnValue;
        }
      });
    } catch (Exception e) {
      // Shouldn't happen, this is only here because timePhase() can throw a checked exception, even though the nested callable can't.
      throw new RuntimeException(e);
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

  }

  public static class RequestProcessingException extends ProcessingException {
    private static int REQUEST_ERROR = -100;

    public RequestProcessingException(Throwable rootCause) {
      super(REQUEST_ERROR);
      initCause(rootCause);
      setMessage(rootCause.getMessage());
    }
  }
}
