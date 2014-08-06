package com.linkedin.pinot.requestHandler;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.annotation.ThreadSafe;
import org.apache.thrift.protocol.TCompactProtocol;

import com.linkedin.pinot.common.query.response.ReduceService;
import com.linkedin.pinot.common.query.response.ServerInstance;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.routing.RoutingTable;
import com.linkedin.pinot.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.RoundRobinReplicaSelection;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.linkedin.pinot.serde.SerDe;


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
    RoutingTableLookupRequest rtRequest = new RoutingTableLookupRequest(request.getQuerySource().getResourceName());
    Map<SegmentIdSet, List<ServerInstance>> segmentServices = _routingTable.findServers(rtRequest);

    // Step 2-4
    ScatterGatherRequestImpl scatterRequest =
        new ScatterGatherRequestImpl(request, segmentServices, _replicaSelection,
            ReplicaSelectionGranularity.SEGMENT_ID_SET, request.getBucketHashKey(), 0, //TODO: Speculative Requests not yet supported
            overriddenSelection, _requestIdGen.incrementAndGet(), 10 * 1000L);
    CompositeFuture<ServerInstance, ByteBuf> response = _scatterGatherer.scatterGather(scatterRequest);

    //Step 5 - Deserialize Responses and build instance response map
    Map<ServerInstance, InstanceResponse> instanceResponseMap = null;
    {
      Map<ServerInstance, ByteBuf> responses = null;
      try {
        responses = response.get();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      Map<ServerInstance, Throwable> errors = response.getError();

      instanceResponseMap = new HashMap<ServerInstance, InstanceResponse>();
      if (null != responses)
      {
        for (Entry<ServerInstance, ByteBuf> e : responses.entrySet())
        {
          ByteBuf b = e.getValue();
          byte[] b2 = new byte[b.readableBytes()];
          b.readBytes(b2);
          InstanceResponse r2 = new InstanceResponse();
          scatterRequest.getSerde().deserialize(r2, b2);
          instanceResponseMap.put(e.getKey(), r2);
        }
      }
      
      
      if ( null != errors)
      {
        for (Entry<ServerInstance, Throwable> e : errors.entrySet())
        {
          InstanceResponse r2 = new InstanceResponse();
          List<ProcessingException> exceptions  = new ArrayList<ProcessingException>();
          exceptions.add(new RequestProcessingException(e.getValue()));
          r2.setExceptions(exceptions);
          instanceResponseMap.put(e.getKey(), r2);
        }
      }
    }
    
    // Step 6 : Do the reduce and return
    return _reduceService.reduce(request, instanceResponseMap);
    
  }

  public static class ScatterGatherRequestImpl implements ScatterGatherRequest {
    private final BrokerRequest _brokerRequest;
    private final Map<SegmentIdSet, List<ServerInstance>> _segmentServices;
    private final ReplicaSelection _replicaSelection;
    private final ReplicaSelectionGranularity _replicaSelectionGranularity;
    private final Object _hashKey;
    private final int _numSpeculativeRequests;
    private final BucketingSelection _bucketingSelection;
    private final long _requestId;
    private final long _requestTimeoutMs;
    private final SerDe _serde;

    public ScatterGatherRequestImpl(BrokerRequest request, Map<SegmentIdSet, List<ServerInstance>> segmentServices,
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
      _serde = new SerDe(new TCompactProtocol.Factory());
    }

    @Override
    public Map<SegmentIdSet, List<ServerInstance>> getSegmentsServicesMap() {
      return _segmentServices;
    }

    @Override
    public byte[] getRequestForService(ServerInstance service, SegmentIdSet querySegments) {
      InstanceRequest r = new InstanceRequest();
      r.setRequestId(_requestId);
      r.setEnableTrace(_brokerRequest.isEnableTrace());
      r.setQuery(_brokerRequest);
      r.setSearchSegments(querySegments.getSegmentsNameList());
      return _serde.serialize(r);
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
      return _serde;
    }

  }
  
  public static class RequestProcessingException extends ProcessingException
  {    
    private static int REQUEST_ERROR = -100;
    public RequestProcessingException(Throwable rootCause)
    {
      super(REQUEST_ERROR);
      initCause(rootCause);
      setMessage(rootCause.getMessage());
    } 
  }
}
