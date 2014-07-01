package com.linkedin.pinot.transport.scattergather;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.Partition;
import com.linkedin.pinot.transport.common.PartitionGroup;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.yammer.metrics.core.MetricsRegistry;


public class TestScatterGather {

  protected static Logger LOG = LoggerFactory.getLogger(TestScatterGather.class);

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
  }

  @Test
  public void testSingleServer() throws Exception {

    MetricsRegistry registry = new MetricsRegistry();

    // Server start
    int serverPort = 7071;
    NettyTCPServer server1 = new NettyTCPServer(serverPort, new TestRequestHandlerFactory(0,1), null);
    Thread t1 = new Thread(server1);
    t1.start();

    //Client setup
    ScheduledExecutorService timedExecutor = new ScheduledThreadPoolExecutor(1);
    ExecutorService service = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new LinkedBlockingDeque<Runnable>());
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "client_");
    PooledNettyClientResourceManager rm = new PooledNettyClientResourceManager(eventLoopGroup, clientMetrics);
    KeyedPoolImpl<ServerInstance, NettyClientConnection> pool = new KeyedPoolImpl<ServerInstance, NettyClientConnection>(1, 1, 300000, 1, rm, timedExecutor, service, registry);
    rm.setPool(pool);

    ScatterGatherImpl scImpl = new ScatterGatherImpl(pool);

    PartitionGroup pg = new PartitionGroup();
    pg.addPartition(new Partition(0));
    ServerInstance serverInstance1 = new ServerInstance("localhost", serverPort);
    List<ServerInstance> instances = new ArrayList<ServerInstance>();
    instances.add(serverInstance1);
    Map<PartitionGroup, List<ServerInstance>> pgMap = new HashMap<PartitionGroup, List<ServerInstance>>();
    pgMap.put(pg,instances);
    String request = "request_0";
    Map<PartitionGroup, String> pgMapStr = new HashMap<PartitionGroup, String>();
    pgMapStr.put(pg, request);
    ScatterGatherRequest req = new TestScatterGatherRequest(pgMap, pgMapStr);

    CompositeFuture<ServerInstance, ByteBuf> fut = scImpl.scatterGather(req);
    Map<ServerInstance, ByteBuf> v = fut.get();
    ByteBuf b = v.get(serverInstance1);
    byte[] b2 = new byte[b.readableBytes()];
    b.readBytes(b2);
    String response = new String(b2);
    Assert.assertEquals("response_0_0",response);
    Assert.assertEquals(1,v.size());
    server1.shutdownGracefully();
  }

  @Test
  public void testMultipleServer() throws Exception {

    MetricsRegistry registry = new MetricsRegistry();

    // Server start
    int serverPort1 = 7071;
    int serverPort2 = 7072;
    int serverPort3 = 7073;
    int serverPort4 = 7074;
    NettyTCPServer server1 = new NettyTCPServer(serverPort1, new TestRequestHandlerFactory(0,1), null);
    NettyTCPServer server2 = new NettyTCPServer(serverPort2, new TestRequestHandlerFactory(1,1), null);
    NettyTCPServer server3 = new NettyTCPServer(serverPort3, new TestRequestHandlerFactory(2,1), null);
    NettyTCPServer server4 = new NettyTCPServer(serverPort4, new TestRequestHandlerFactory(3,1), null);

    Thread t1 = new Thread(server1);
    Thread t2 = new Thread(server2);
    Thread t3 = new Thread(server3);
    Thread t4 = new Thread(server4);
    t1.start();
    t2.start();
    t3.start();
    t4.start();

    //Client setup
    ScheduledExecutorService timedExecutor = new ScheduledThreadPoolExecutor(1);
    ExecutorService service = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new LinkedBlockingDeque<Runnable>());
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "client_");
    PooledNettyClientResourceManager rm = new PooledNettyClientResourceManager(eventLoopGroup, clientMetrics);
    KeyedPoolImpl<ServerInstance, NettyClientConnection> pool = new KeyedPoolImpl<ServerInstance, NettyClientConnection>(1, 1, 300000, 1, rm, timedExecutor, service, registry);
    rm.setPool(pool);

    PartitionGroup pg1 = new PartitionGroup();
    pg1.addPartition(new Partition(0));
    PartitionGroup pg2 = new PartitionGroup();
    pg2.addPartition(new Partition(1));
    PartitionGroup pg3 = new PartitionGroup();
    pg3.addPartition(new Partition(2));
    PartitionGroup pg4 = new PartitionGroup();
    pg4.addPartition(new Partition(3));

    ServerInstance serverInstance1 = new ServerInstance("localhost", serverPort1);
    ServerInstance serverInstance2 = new ServerInstance("localhost", serverPort2);
    ServerInstance serverInstance3 = new ServerInstance("localhost", serverPort3);
    ServerInstance serverInstance4 = new ServerInstance("localhost", serverPort4);

    List<ServerInstance> instances1 = new ArrayList<ServerInstance>();
    instances1.add(serverInstance1);
    List<ServerInstance> instances2 = new ArrayList<ServerInstance>();
    instances2.add(serverInstance2);
    List<ServerInstance> instances3 = new ArrayList<ServerInstance>();
    instances3.add(serverInstance3);
    List<ServerInstance> instances4 = new ArrayList<ServerInstance>();
    instances4.add(serverInstance4);
    Map<PartitionGroup, List<ServerInstance>> pgMap = new HashMap<PartitionGroup, List<ServerInstance>>();
    pgMap.put(pg1,instances1);
    pgMap.put(pg2,instances2);
    pgMap.put(pg3, instances3);
    pgMap.put(pg4, instances4);

    String request1 = "request_0";
    String request2 = "request_1";
    String request3 = "request_2";
    String request4 = "request_3";

    Map<PartitionGroup, String> pgMapStr = new HashMap<PartitionGroup, String>();
    pgMapStr.put(pg1, request1);
    pgMapStr.put(pg2, request2);
    pgMapStr.put(pg3, request3);
    pgMapStr.put(pg4, request4);

    ScatterGatherRequest req = new TestScatterGatherRequest(pgMap, pgMapStr);
    ScatterGatherImpl scImpl = new ScatterGatherImpl(pool);
    CompositeFuture<ServerInstance, ByteBuf> fut = scImpl.scatterGather(req);
    Map<ServerInstance, ByteBuf> v = fut.get();
    Assert.assertEquals(4,v.size());

    ByteBuf b = v.get(serverInstance1);
    byte[] b2 = new byte[b.readableBytes()];
    b.readBytes(b2);
    String response = new String(b2);
    Assert.assertEquals("response_0_0",response);
    b = v.get(serverInstance2);
    b2 = new byte[b.readableBytes()];
    b.readBytes(b2);
    response = new String(b2);
    Assert.assertEquals("response_1_0",response);
    b = v.get(serverInstance3);
    b2 = new byte[b.readableBytes()];
    b.readBytes(b2);
    response = new String(b2);
    Assert.assertEquals("response_2_0",response);
    b = v.get(serverInstance4);
    b2 = new byte[b.readableBytes()];
    b.readBytes(b2);
    response = new String(b2);
    Assert.assertEquals("response_3_0",response);

    server1.shutdownGracefully();
    server2.shutdownGracefully();
    server3.shutdownGracefully();
    server4.shutdownGracefully();


  }

  public static class TestRequestHandlerFactory implements RequestHandlerFactory
  {
    public final int _numRequests;
    public final int _id;

    public TestRequestHandlerFactory(int id, int numRequests)
    {
      _id = id;
      _numRequests = numRequests;
    }
    @Override
    public RequestHandler createNewRequestHandler() {
      List<String> responses = new ArrayList<String>();
      for (int i =0 ; i < _numRequests; i++)
      {
        responses.add("response_" + _id + "_" + i);
      }
      return new TestRequestHandler(responses);
    }
  }

  public static class TestRequestHandler implements RequestHandler
  {
    private final List<String> _responses;
    private final List<String> _request;

    private final AtomicInteger _index = new AtomicInteger(-1);
    public TestRequestHandler(List<String> responses)
    {
      _responses = responses;
      _request = new ArrayList<String>();
    }

    @Override
    public byte[] processRequest(ByteBuf request) {
      byte[] dst = new byte[request.readableBytes()];
      request.getBytes(0, dst);
      _request.add(new String(dst));
      int index = _index.incrementAndGet();
      String res = _responses.get(index);
      return res.getBytes();
    }

    public List<String> getRequest() {
      return _request;
    }
  }

  public static class TestScatterGatherRequest implements ScatterGatherRequest
  {
    private final Map<PartitionGroup, List<ServerInstance>> _partitionServicesMap;
    private final Map<PartitionGroup, String> _responsesMap;

    public TestScatterGatherRequest(Map<PartitionGroup, List<ServerInstance>> partitionServicesMap,
        Map<PartitionGroup, String> responsesMap)
    {
      _partitionServicesMap = partitionServicesMap;
      _responsesMap = responsesMap;
    }
    @Override
    public Map<PartitionGroup, List<ServerInstance>> getPartitionServicesMap() {
      return _partitionServicesMap;
    }

    @Override
    public byte[] getRequestForService(ServerInstance service, PartitionGroup queryPartitions) {
      String s = _responsesMap.get(queryPartitions);
      return s.getBytes();
    }

    @Override
    public ReplicaSelection getReplicaSelection() {
      return new MyReplicaSelection();
    }

    @Override
    public ReplicaSelectionGranularity getReplicaSelectionGranularity() {
      return ReplicaSelectionGranularity.PARTITION_GROUP;
    }

    @Override
    public Object getHashKey() {
      return null;
    }

    @Override
    public int getNumSpeculativeRequests() {
      return 0;
    }

    @Override
    public BucketingSelection getPredefinedSelection() {
      return null;
    }
    @Override
    public String toString() {
      return "TestScatterGatherRequest [_partitionServicesMap=" + _partitionServicesMap + "]";
    }

  }

  public static class MyReplicaSelection implements ReplicaSelection
  {

    @Override
    public void reset(Partition p) {
    }

    @Override
    public void reset(PartitionGroup p) {
    }

    @Override
    public ServerInstance selectServer(Partition p, List<ServerInstance> orderedServers,
        BucketingSelection predefinedSelection, Object hashKey) {
      System.out.println("Partition :" + p + ", Ordered Servers :" + orderedServers);
      return orderedServers.get(0);
    }
  }
}
