package com.linkedin.pinot.transport.scattergather;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.CompositeFuture.GatherModeOnError;
import com.linkedin.pinot.transport.common.KeyedFuture;
import com.linkedin.pinot.transport.common.Partition;
import com.linkedin.pinot.transport.common.PartitionGroup;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.pool.KeyedPool;

public class ScatterGatherImpl implements ScatterGather {

  private final KeyedPool<ServerInstance, NettyClientConnection> _connPool;

  public ScatterGatherImpl(KeyedPool<ServerInstance, NettyClientConnection> pool)
  {
    _connPool = pool;
  }

  @Override
  public CompositeFuture<ServerInstance, ByteBuf> scatterGather(ScatterGatherRequest scatterRequest)
      throws InterruptedException {
    ScatterGatherRequestContext ctxt = new ScatterGatherRequestContext(scatterRequest);

    // Build services to partition group map
    buildInvertedMap(ctxt);

    System.out.println("Context : " + ctxt);
    // do Selection for each partition-group/partition
    selectServices(ctxt);

    return sendRequest(ctxt);
  }

  protected CompositeFuture<ServerInstance, ByteBuf> sendRequest( ScatterGatherRequestContext ctxt) throws InterruptedException
  {
    Map<ServerInstance, PartitionGroup> mp = ctxt.getSelectedServers();

    System.out.println("Map Size :" + mp.size());
    CountDownLatch requestDispatchLatch = new CountDownLatch(mp.size());

    ExecutorService executor = MoreExecutors.sameThreadExecutor();

    // async checkout of connections and then dispatch of request
    List<SingleRequestHandler> handlers = new ArrayList<SingleRequestHandler>(mp.size());
    for (Entry<ServerInstance, PartitionGroup> e : mp.entrySet())
    {
      System.out.println("Creating Request Handler !!");
      KeyedFuture<ServerInstance, NettyClientConnection> c = _connPool.checkoutObject(e.getKey());
      SingleRequestHandler handler = new SingleRequestHandler(e.getKey(), c, ctxt.getRequest(), e.getValue(), requestDispatchLatch);
      c.addListener(handler, executor);
      handlers.add(handler);
    }

    requestDispatchLatch.await(); //Todo : There should be bounded wait here to cancel the request

    // At this point the requests have been sent
    CompositeFuture<ServerInstance, ByteBuf> response = new CompositeFuture<ServerInstance,ByteBuf>("scatterRequest",
        GatherModeOnError.SHORTCIRCUIT_AND);

    List<KeyedFuture<ServerInstance,ByteBuf>> responseFutures = new ArrayList<KeyedFuture<ServerInstance,ByteBuf>>();
    for (SingleRequestHandler h : handlers)
    {
      responseFutures.add(h.getResponseFuture());
    }
    response.start(responseFutures);
    //RequestCompletionHandler completionHandler = new RequestCompletionHandler(handlers, _connPool);
    //response.addListener(completionHandler, executor);

    return response;
  }


  /**
   * Merge partition-groups which have the same set of servers. If 2 partitions have overlapping
   * set of partitions, they are not merged. If there is predefined-selection for a partition,
   * a separate entry is added for those in the inverted map.
   * @param request Scatter gather request
   */
  protected void buildInvertedMap(ScatterGatherRequestContext requestContext)
  {
    ScatterGatherRequest request = requestContext.getRequest();
    Map<PartitionGroup, List<ServerInstance>> partitionToInstanceMap = request.getPartitionServicesMap();

    Map<List<ServerInstance>, PartitionGroup> instanceToPartitionMap =
        new HashMap<List<ServerInstance>, PartitionGroup>();

    BucketingSelection sel = request.getPredefinedSelection();

    for (PartitionGroup pg : partitionToInstanceMap.keySet())
    {
      /**
       * If predefined selection is enabled, split the partitions in the partition group
       * into many partition-groups where one such partition-group will contain the non-preselected
       * partitions. For each preselected partition, a partition group instance is newly created
       * and merged on to the inverted Map. At the end of this method, we are guaranteed that each
       * pre-selected partition will have only one choice of the server which is the pre-selected one.
       */
      if ( null != sel)
      {
        PartitionGroup pg2 = new PartitionGroup();
        for(Partition p1 : pg.getPartitions())
        {
          ServerInstance s1 = sel.getPreSelectedServer(p1);
          if ( null != s1)
          {
            List<ServerInstance> servers1 = new ArrayList<ServerInstance>();
            servers1.add(s1);
            mergePartitionGroup(instanceToPartitionMap, servers1, p1);
          }  else {
            pg2.addPartition(p1);
          }
        }
        pg = pg2; // Now, pg points to the left-over partitions which have not been pre-selected.
      }

      if ( ! pg.getPartitions().isEmpty())
      {
        List<ServerInstance> instances1 = partitionToInstanceMap.get(pg);
        mergePartitionGroup(instanceToPartitionMap, instances1, pg);
      }
    }
    requestContext.setInvertedMap(instanceToPartitionMap);
  }

  private <T> void mergePartitionGroup(Map<T, PartitionGroup> instanceToPartitionMap, T instances, PartitionGroup pg)
  {
    PartitionGroup pg2 = instanceToPartitionMap.get(instances);
    if ( null != pg2)
    {
      pg2.addPartitions(pg.getPartitions());
    } else {
      instanceToPartitionMap.put(instances, pg);
    }
  }

  private <T> void mergePartitionGroup(Map<T, PartitionGroup> instanceToPartitionMap, T instances, Partition p)
  {
    PartitionGroup pg2 = instanceToPartitionMap.get(instances);
    if ( null != pg2)
    {
      pg2.addPartition(p);
    } else {
      PartitionGroup pg1 = new PartitionGroup();
      pg1.addPartition(p);
      instanceToPartitionMap.put(instances, pg1);
    }
  }

  protected void selectServices(ScatterGatherRequestContext requestContext)
  {
    ScatterGatherRequest request = requestContext.getRequest();


    if (request.getReplicaSelectionGranularity() == ReplicaSelectionGranularity.PARTITION_GROUP)
    {
      selectServicesPerPartitionGroup(requestContext);
    } else {
      //TODO Replica Selection Policy ==> Partition. Which increases the chances of fanning-out the query
    }
  }

  /**
   * For each partition-group in the instanceToPartitionMap, we select one (or more speculative) servers
   *
   * @param requestContext
   */
  private void selectServicesPerPartitionGroup(ScatterGatherRequestContext requestContext)
  {
    Map<ServerInstance, PartitionGroup> selectedServers = new HashMap<ServerInstance, PartitionGroup>();
    ScatterGatherRequest request = requestContext.getRequest();
    Map<List<ServerInstance>, PartitionGroup> instanceToPartitionMap = requestContext.getInvertedMap();
    int numDuplicateRequests = request.getNumSpeculativeRequests();
    ReplicaSelection selection = request.getReplicaSelection();
    for (Entry<List<ServerInstance>, PartitionGroup> e : instanceToPartitionMap.entrySet())
    {
      ServerInstance s = selection.selectServer(e.getValue().getOnePartition(), e.getKey(), null, request.getHashKey());
      mergePartitionGroup(selectedServers, s, e.getValue());

      int numServers = e.getKey().size();

      // Pick Unique servers for speculative request
      int numIterations = Math.min(numServers - 1, numDuplicateRequests);
      for (int i = 0, c = 0; i < numIterations; i++, c++)
      {
        ServerInstance s1 = e.getKey().get(c);
        if ( s.equals(s1))
        {
          c++;
          s1 = e.getKey().get(c);
        }
        mergePartitionGroup(selectedServers, s, e.getValue());
      }
    }
    requestContext.setSelectedServers(selectedServers);
  }


  public static class ScatterGatherRequestContext
  {
    private final ScatterGatherRequest _request;

    private Map<List<ServerInstance>, PartitionGroup> _invertedMap;

    private Map<ServerInstance, PartitionGroup> _selectedServers;

    private ScatterGatherRequestContext(ScatterGatherRequest request)
    {
      _request = request;
    }

    public Map<List<ServerInstance>, PartitionGroup> getInvertedMap() {
      return _invertedMap;
    }

    public void setInvertedMap(Map<List<ServerInstance>, PartitionGroup> invertedMap) {
      _invertedMap = invertedMap;
    }

    public ScatterGatherRequest getRequest() {
      return _request;
    }

    public Map<ServerInstance, PartitionGroup> getSelectedServers() {
      return _selectedServers;
    }

    public void setSelectedServers(Map<ServerInstance, PartitionGroup> selectedServers) {
      _selectedServers = selectedServers;
    }

    @Override
    public String toString() {
      return "ScatterGatherRequestContext [_request=" + _request + ", _invertedMap=" + _invertedMap
          + ", _selectedServers=" + _selectedServers + "]";
    }
  }

  /**
   * Runnable responsible for sending a request to the server once the connection is available
   * @author bvaradar
   *
   */
  public static class SingleRequestHandler implements Runnable
  {
    // Scatter Request
    private final ScatterGatherRequest _request;
    // List Of Partitions to be queried on the server
    private final PartitionGroup _partitions;
    // Server Instance to be queried
    private final ServerInstance _server;
    // Future for checking out a connection
    private final KeyedFuture<ServerInstance, NettyClientConnection> _connFuture;
    // Latch to signal completion of dispatching request
    private final CountDownLatch _requestDispatchLatch;
    // Future for the response
    private volatile ResponseFuture _responseFuture;

    public SingleRequestHandler (ServerInstance server,
        KeyedFuture<ServerInstance, NettyClientConnection> connFuture,
        ScatterGatherRequest request,
        PartitionGroup partitions,
        CountDownLatch latch)
    {
      _server = server;
      _connFuture = connFuture;
      _request = request;
      _partitions = partitions;
      _requestDispatchLatch = latch;
    }

    @Override
    public void run() {

      NettyClientConnection conn = null;
      try {
        byte[] serializedRequest = _request.getRequestForService(_server, _partitions);

        conn = _connFuture.getOne();

        ByteBuf req = Unpooled.wrappedBuffer(serializedRequest);
        _responseFuture = conn.sendRequest(req);
        System.out.println("1. Response Future is :" + _responseFuture);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        _requestDispatchLatch.countDown();
      }

    }

    public KeyedFuture<ServerInstance, NettyClientConnection> getConnFuture() {
      return _connFuture;
    }

    public ServerInstance getServer() {
      return _server;
    }

    public ResponseFuture getResponseFuture() {
      return _responseFuture;
    }
  }

  /**
   * This is used to checkin the connections once the responses/errors are obtained
   * @author bvaradar
   *
   */
  /*
   * This is not needed as The Client connection resources has the ability to
   * self-checkin back to the pool. Look at {@Link PooledNettyClientResourceManager.PooledClientConnection
   * 
  public static class RequestCompletionHandler implements Runnable
  {

    public final Collection<SingleRequestHandler> _handlers;
    public final KeyedPool<ServerInstance, NettyClientConnection> _connPool;

    public RequestCompletionHandler(Collection<SingleRequestHandler> handlers,
        KeyedPool<ServerInstance, NettyClientConnection> connPool)
    {
      _handlers = handlers;
      _connPool = connPool;
    }

    @Override
    public void run() {

      List<SingleRequestHandler> pendingRequestHandlers = new ArrayList<SingleRequestHandler>();

      // In the first pass checkin all completed connections
      for ( SingleRequestHandler h : _handlers)
      {
        ResponseFuture f = h.getResponseFuture();
        if (! f.getState().isCompleted())
        {
          pendingRequestHandlers.add(h);
        } else {
          try {
            _connPool.checkinObject(h.getServer(), h.getConnFuture().getOne());
          } catch (Exception e) {
            //Not expected
            throw new IllegalStateException("Not expected !!");
          }
        }
      }

      //In the second pass, wait for request completion for stragglers and checkin
      //TODO: Need to have a bounded wait and handle failures
      for (SingleRequestHandler p : pendingRequestHandlers )
      {
        try
        {
          // Wait for response to be returned
          p.getResponseFuture().getOne();
          _connPool.checkinObject(p.getServer(), p.getConnFuture().getOne());
        } catch (Exception e) {
          // Discard the connection if we get an exception
          try {
            _connPool.destroyObject(p.getServer(), p.getConnFuture().getOne());
            //TODO: We need to handle case when checking out of connFuture is blocking.
            // In that case, we need to cancel it and avoid race-condition (cancel vs valid checkout)
          } catch (Exception e1) {
            //Not expected
            throw new IllegalStateException("Not expected !!");
          }
        }
      }

    }

  }
   */
}
