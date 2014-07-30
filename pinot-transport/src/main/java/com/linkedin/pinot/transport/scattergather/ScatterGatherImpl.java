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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.CompositeFuture.GatherModeOnError;
import com.linkedin.pinot.transport.common.KeyedFuture;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.Partition;
import com.linkedin.pinot.transport.common.PartitionGroup;
import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.pool.KeyedPool;

/**
 * 
 * Scatter-Gather implementation
 * @author Balaji Varadarajan
 *
 */
public class ScatterGatherImpl implements ScatterGather {

  private static Logger LOGGER = LoggerFactory.getLogger(ScatterGatherImpl.class);

  /**
   * Connection Pool for sending scatter-gather requests
   */
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

    LOGGER.debug("Context : {}", ctxt);

    // do Selection for each partition-group/partition
    selectServices(ctxt);

    return sendRequest(ctxt);
  }

  /**
   * 
   * Helper Function to send scatter-request. This method should be called after the servers are selected
   * 
   * @param ctxt Scatter-Gather Request context with selected servers for each request.
   * @return a composite future representing the gather process.
   * @throws InterruptedException
   */
  protected CompositeFuture<ServerInstance, ByteBuf> sendRequest( ScatterGatherRequestContext ctxt) throws InterruptedException
  {
    // Servers are expected to be selected at this stage
    Map<ServerInstance, PartitionGroup> mp = ctxt.getSelectedServers();

    CountDownLatch requestDispatchLatch = new CountDownLatch(mp.size());

    //Use same thread to send request once the connection is available
    ExecutorService executor = MoreExecutors.sameThreadExecutor();

    // async checkout of connections and then dispatch of request
    List<SingleRequestHandler> handlers = new ArrayList<SingleRequestHandler>(mp.size());

    for (Entry<ServerInstance, PartitionGroup> e : mp.entrySet())
    {
      KeyedFuture<ServerInstance, NettyClientConnection> c = _connPool.checkoutObject(e.getKey());
      SingleRequestHandler handler = new SingleRequestHandler(_connPool, e.getKey(), c, ctxt.getRequest(), e.getValue(), ctxt.getRequest().getRequestTimeoutMS(), requestDispatchLatch);
      c.addListener(handler, executor);
      handlers.add(handler);
    }

    // Create the composite future for returning
    CompositeFuture<ServerInstance, ByteBuf> response = new CompositeFuture<ServerInstance,ByteBuf>("scatterRequest",
        GatherModeOnError.SHORTCIRCUIT_AND);

    // Wait for requests to be sent
    boolean sentSuccessfully = requestDispatchLatch.await(ctxt.getTimeRemaining(), TimeUnit.MILLISECONDS);

    if ( sentSuccessfully )
    {
      List<KeyedFuture<ServerInstance,ByteBuf>> responseFutures = new ArrayList<KeyedFuture<ServerInstance,ByteBuf>>();
      for (SingleRequestHandler h : handlers)
      {
        responseFutures.add(h.getResponseFuture());
      }
      response.start(responseFutures);
    } else {
      // Some requests were not event sent (possibly because of checkout !!)
      // and so we cancel all of them here
      for (SingleRequestHandler h : handlers)
      {
        h.cancel();
      }
    }
    return response;
  }


  /**
   * Merge partition-groups which have the same set of servers. If 2 partitions have overlapping
   * set of servers, they are not merged. If there is predefined-selection for a partition,
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
      List<ServerInstance> instances1 = partitionToInstanceMap.get(pg);

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
      selectServicesPerPartition(requestContext);
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
    //int numDuplicateRequests = request.getNumSpeculativeRequests();
    ReplicaSelection selection = request.getReplicaSelection();
    for (Entry<List<ServerInstance>, PartitionGroup> e : instanceToPartitionMap.entrySet())
    {
      ServerInstance s = selection.selectServer(e.getValue().getOnePartition(), e.getKey(), request.getHashKey());
      mergePartitionGroup(selectedServers, s, e.getValue());

      /**
       * TODO:
       * We can easily add speculative execution here. The below code will pick a distinct server
       * for the partition, This entry needs to be maintained in a separate container in ScatterGatherRequestContext
       * Then in sndRequest, we need to construct SelectingFuture for the pairs of Future corresponding to original
       * and speculative(duplicate) request.
       * 
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
        mergePartitionGroup(selectedServers, s1, e.getValue());
        //TODO: speculative servers need to be maintained in a separate entry in ScatterGatherRequestContext
      }
       **/
    }
    requestContext.setSelectedServers(selectedServers);
  }

  /**
   * For each partition in the instanceToPartitionMap, we select one (or more speculative) servers
   *
   * @param requestContext
   */
  private void selectServicesPerPartition(ScatterGatherRequestContext requestContext)
  {
    Map<ServerInstance, PartitionGroup> selectedServers = new HashMap<ServerInstance, PartitionGroup>();
    ScatterGatherRequest request = requestContext.getRequest();
    Map<List<ServerInstance>, PartitionGroup> instanceToPartitionMap = requestContext.getInvertedMap();
    ReplicaSelection selection = request.getReplicaSelection();
    for (Entry<List<ServerInstance>, PartitionGroup> e : instanceToPartitionMap.entrySet())
    {
      Partition firstPartition = null;
      for (Partition p: e.getValue().getPartitions())
      {
        /**
         * For selecting the server, we always use first partition in the group. This will provide
         * more chance for fanning out the query
         */
        if ( null == firstPartition)
        {
          firstPartition = p;
        }
        ServerInstance s = selection.selectServer(firstPartition, e.getKey(), request.getHashKey());
        System.out.println("Partition :" + p + ", server :" + s);

        mergePartitionGroup(selectedServers, s, p);
      }
    }
    requestContext.setSelectedServers(selectedServers);
  }

  public static class ScatterGatherRequestContext
  {
    private final long _startTimeMs;

    private final ScatterGatherRequest _request;

    private Map<List<ServerInstance>, PartitionGroup> _invertedMap;

    private Map<ServerInstance, PartitionGroup> _selectedServers;

    protected ScatterGatherRequestContext(ScatterGatherRequest request)
    {
      _request = request;
      _startTimeMs = System.currentTimeMillis();
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

    /**
     * Return time remaining in MS
     * @return
     */
    public long getTimeRemaining()
    {
      long timeout = _request.getRequestTimeoutMS();

      if (timeout < 0) {
        return Long.MAX_VALUE;
      }

      long timeElapsed = System.currentTimeMillis() - _startTimeMs;
      return (timeout- timeElapsed);
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

    // Connection Pool: Used if we need to checkin/destroy object in case of timeout
    private final KeyedPool<ServerInstance, NettyClientConnection> _connPool;

    // Track if request has been dispatched
    private final AtomicBoolean _isSent = new AtomicBoolean(false);

    // Cancel dispatching request
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);

    // Timeout MS
    private final long _timeoutMS;

    public SingleRequestHandler (KeyedPool<ServerInstance, NettyClientConnection> connPool,
        ServerInstance server,
        KeyedFuture<ServerInstance, NettyClientConnection> connFuture,
        ScatterGatherRequest request,
        PartitionGroup partitions,
        long timeoutMS,
        CountDownLatch latch)
    {
      _connPool = connPool;
      _server = server;
      _connFuture = connFuture;
      _request = request;
      _partitions = partitions;
      _requestDispatchLatch = latch;
      _timeoutMS = timeoutMS;
    }

    @Override
    public synchronized void run() {

      if ( _isCancelled.get())
      {
        LOGGER.error("Request {} to server {} dispatcher getting called despite cancelling the checkout !! Ignoring", _request.getRequestId(), _server);

        //Return the connection to pool without sending request
        try {
          _connPool.checkinObject(_server, _connFuture.getOne());
        } catch (Exception e) {
          LOGGER.error("Unable to get connection for server (" + _server + "). Unexpected !!", e);
        }

        return;
      }

      NettyClientConnection conn = null;
      try {
        byte[] serializedRequest = _request.getRequestForService(_server, _partitions);

        conn = _connFuture.getOne();
        ByteBuf req = Unpooled.wrappedBuffer(serializedRequest);
        _responseFuture = conn.sendRequest(req, _request.getRequestId(), _timeoutMS);
        _isSent.set(true);
        LOGGER.debug("Response Future is : {}",_responseFuture);
      } catch (Exception e) {
        LOGGER.error("Got exception sending request (" + _request.getRequestId() + "). Setting error future", e);
        _responseFuture = new ResponseFuture(_server, e);
      } finally {
        _requestDispatchLatch.countDown();
      }
    }

    /**
     * Cancel checking-out request if possible. If in unsafe state (request already sent),
     * discard the connection from the pool.
     */
    public synchronized void cancel()
    {
      if ( _isCancelled.get()) {
        return;
      }

      _isCancelled.set(true);

      if (! _isSent.get())
      {
        /**
         * If request has not been sent, we cancel the checkout
         */
        _connFuture.cancel(true);
      } else {
        /**
         * If the request has already been sent, we cancel the
         * response future. The connection will automatically be returned to the pool if response
         * arrived within timeout or discarded if timeout happened. No need to handle it here.
         */
        _responseFuture.cancel(true);
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
