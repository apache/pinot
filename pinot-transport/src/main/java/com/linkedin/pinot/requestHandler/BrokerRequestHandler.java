package com.linkedin.pinot.requestHandler;

import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.annotation.ThreadSafe;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TCompactProtocol;

import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.routing.RoutingTable;
import com.linkedin.pinot.routing.RoutingTableLookupRequest;
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
  private final static Logger LOGGER = Logger.getLogger(BrokerRequestHandler.class);

  //TODO: Currently only using RoundRobin selection. But, this can be allowed to be configured.
  private RoundRobinReplicaSelection _replicaSelection;

  public BrokerRequestHandler(RoutingTable table, ScatterGather scatterGatherer, ReduceService reduceService) {
    _routingTable = table;
    _scatterGatherer = scatterGatherer;
    _requestIdGen = new AtomicLong(0);
    _replicaSelection = new RoundRobinReplicaSelection();
    _reduceService = reduceService;
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
  public Object processBrokerRequest(BrokerRequest request, BucketingSelection overriddenSelection)
      throws InterruptedException {
    // Step1
    if (request == null || request.getQuerySource() == null || request.getQuerySource().getResourceName() == null) {
      LOGGER.info("Query contains null resource.");
      return BrokerResponse.getNullBrokerResponse();
    }
    RoutingTableLookupRequest rtRequest = new RoutingTableLookupRequest(request.getQuerySource().getResourceName());
    // Map<SegmentIdSet, List<ServerInstance>> segmentServices = _routingTable.findServers(rtRequest);
    Map<ServerInstance, SegmentIdSet> segmentServices = _routingTable.findServers(rtRequest);
    if (segmentServices == null) {
      return BrokerResponse.getNullBrokerResponse();
    }
    LOGGER.info("Find ServerInstances to Segments Mapping:");
    for (ServerInstance serverInstance : segmentServices.keySet()) {
      LOGGER.info(serverInstance + " : " + segmentServices.get(serverInstance));
    }
    // Step 2-4
    ScatterGatherRequestImpl scatterRequest =
        new ScatterGatherRequestImpl(request, segmentServices, _replicaSelection,
            ReplicaSelectionGranularity.SEGMENT_ID_SET, request.getBucketHashKey(), 0, //TODO: Speculative Requests not yet supported
            overriddenSelection, _requestIdGen.incrementAndGet(), 10 * 1000L);
    CompositeFuture<ServerInstance, ByteBuf> response = _scatterGatherer.scatterGather(scatterRequest);

    //Step 5 - Deserialize Responses and build instance response map
    Map<ServerInstance, DataTable> instanceResponseMap = null;
    {
      Map<ServerInstance, ByteBuf> responses = null;
      try {
        responses = response.get();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      Map<ServerInstance, Throwable> errors = response.getError();

      instanceResponseMap = new HashMap<ServerInstance, DataTable>();
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
          }
        }
      }
      if (null != errors) {
        for (Entry<ServerInstance, Throwable> e : errors.entrySet()) {
          DataTable r2 = new DataTable();
          r2.getMetadata().put("exception", new RequestProcessingException(e.getValue()).toString());
          instanceResponseMap.put(e.getKey(), r2);
        }
      }
    }

    // Step 6 : Do the reduce and return
    return _reduceService.reduceOnDataTable(request, instanceResponseMap);

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
