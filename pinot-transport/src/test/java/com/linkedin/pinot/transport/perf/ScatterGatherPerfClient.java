package com.linkedin.pinot.transport.perf;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.MetricsHelper.TimerContext;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.config.ResourceRoutingConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;



public class ScatterGatherPerfClient implements Runnable {
  private static final String ROUTING_CFG_PREFIX = "pinot.broker.routing";
  private static Logger LOGGER = LoggerFactory.getLogger(ScatterGatherPerfClient.class);
  private static final String BROKER_CONFIG_OPT_NAME = "broker_conf";
  private static final String NUM_REQUESTS_OPT_NAME = "num_requests";
  private static final String REQUEST_SIZE_OPT_NAME = "request_size";
  private static final String RESOURCE_NAME_OPT_NAME = "resource_name";

  // RequestId Generator
  private static AtomicLong _requestIdGen = new AtomicLong(0);

  //Routing Config and Pool
  private final RoutingTableConfig _routingConfig;
  private ScatterGather _scatterGather;
  private KeyedPool<ServerInstance, NettyClientConnection> _pool;
  private ExecutorService _service;
  private EventLoopGroup _eventLoopGroup;
  private Timer _timer;
  private ScheduledExecutorService _timedExecutor;
  private byte[] _request;
  private final int _numRequests;
  private final String _resourceName;

  private int _numRequestsMeasured;
  private TimerContext _timerContext;
  
  // Input Reader
  private final BufferedReader _reader;

  // If true, the client main thread scatters the request and does not wait for the response. The response is read by another thread 
  private final boolean _asyncRequestSubmit;
  private final AsyncReader _readerThread;
  private final LinkedBlockingQueue<QueueEntry> _queue;
  
  /**
   * We will skip the first n requests for measured to only measure steady-state time
   */
  private final long _numRequestsToSkipForMeasurement = 5;
  private long _beginFirstRequestTime;
  private long _endLastResponseTime;
  
  private final Histogram _latencyHistogram =  
      MetricsHelper.newHistogram(null, new MetricName(ScatterGatherPerfClient.class, "latency"), false);;
  
  
  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
  }
  

  public ScatterGatherPerfClient(RoutingTableConfig config, int requestSize, String resourceName, boolean asyncRequestSubmit, int numRequests) {
    _routingConfig = config;
    _reader = new BufferedReader(new InputStreamReader(System.in));
    StringBuilder s1 = new StringBuilder();
    for (int i =0 ; i < requestSize; i++)
    {
      s1.append("a");
    }
    _request = s1.toString().getBytes();
    _resourceName = resourceName;
    _numRequests = numRequests;
    _asyncRequestSubmit = asyncRequestSubmit;
    _queue = new LinkedBlockingQueue<ScatterGatherPerfClient.QueueEntry>();
    _readerThread = new AsyncReader(_queue, _latencyHistogram);
    
    setup();
  }

  private void setup() {
    MetricsRegistry registry = new MetricsRegistry();
    _timedExecutor = new ScheduledThreadPoolExecutor(1);
    _service = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new LinkedBlockingDeque<Runnable>());
    _eventLoopGroup = new NioEventLoopGroup();
    _timer = new HashedWheelTimer();

    NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "client_");
    PooledNettyClientResourceManager rm =
        new PooledNettyClientResourceManager(_eventLoopGroup, _timer, clientMetrics);
    _pool =
        new KeyedPoolImpl<ServerInstance, NettyClientConnection>(1, 1, 300000, 1, rm, _timedExecutor, _service,
            registry);
    rm.setPool(_pool);
    _scatterGather = new ScatterGatherImpl(_pool);
    _readerThread.start();
  }

  
  public void run() {
    System.out.println("Client starting !!");
    try
    {
      List<ServerInstance> s1 = new ArrayList<ServerInstance>();
      ServerInstance s = new ServerInstance("localhost", 9099);
      s1.add(s);

      SimpleScatterGatherRequest req = null;
      TimerContext tc = null;

      for (int i = 0; i < _numRequests; i++) {
        do {
          req = getRequest();
        } while ((null == req));

        if ( i == _numRequestsToSkipForMeasurement)
        {
          tc = MetricsHelper.startTimer();
          _beginFirstRequestTime = System.currentTimeMillis();
        }

        if ( i >= _numRequestsToSkipForMeasurement)
        {
          _numRequestsMeasured++;
        }

        if ( ! _asyncRequestSubmit)
        {  
          sendRequestAndGetResponse(req);
          _endLastResponseTime = System.currentTimeMillis();
        } else {
          CompositeFuture<ServerInstance, ByteBuf> future = asyncSendRequestAndGetResponse(req);
          _queue.offer(new QueueEntry(false, i>=_numRequestsToSkipForMeasurement, System.currentTimeMillis(), future));
        }
        
        
        //System.out.println("Response is :" + r);
        //System.out.println("\n\n");
        req = null;
      }

      _queue.offer(new QueueEntry(true, false, System.currentTimeMillis(), null));

      if ( _asyncRequestSubmit )
        _readerThread.join();
      
      if ( null != tc)
      {
        tc.stop();
        _timerContext = tc;
        
        System.out.println("Num Requests :" + _numRequestsMeasured);
        System.out.println("Total time :" + tc.getLatencyMs() );
        System.out.println("Throughput (Requests/Second) :" + ((_numRequestsMeasured * 1.0 * 1000)/tc.getLatencyMs()));
        System.out.println("Latency :" + new LatencyMetric<Histogram>(_latencyHistogram));

      }
    } catch (Exception ex) {
      System.err.println("Client stopped abnormally ");
      ex.printStackTrace();
    }
    
    shutdown();
    System.out.println("Client done !!");
    
  }

  /**
   * Shutdown all resources
   */
  public void shutdown() {
    if (null != _pool) {
      LOGGER.info("Shutting down Pool !!");
      try {
        _pool.shutdown().get();
        LOGGER.info("Pool shut down!!");
      } catch (Exception ex) {
        LOGGER.error("Unable to shutdown pool", ex);
      }
    }

    if (null != _timedExecutor) {
      LOGGER.info("Shutting down scheduled executor !!");
      _timedExecutor.shutdown();
      LOGGER.info("Scheduled executor shut down !!");
    }

    if (null != _eventLoopGroup) {
      LOGGER.info("Shutting down event-loop group !!");
      _eventLoopGroup.shutdownGracefully();
      LOGGER.info("Event Loop group shut down !!");
    }

    if (null != _service) {
      LOGGER.info("Shutting down executor service !!");
      _service.shutdown();
      LOGGER.info("Executor Service shut down !!");
    }

    if ( null != _timer) {
      LOGGER.info("Shutting down timer !!");
      _timer.stop();
      LOGGER.info("Timer shut down !!");
    }
  }

  /**
   * Build a request from the JSON query and partition passed
   * @return
   * @throws IOException
   * @throws JSONException 
   */
  public SimpleScatterGatherRequest getRequest() throws IOException, JSONException {

    ResourceRoutingConfig cfg =
        _routingConfig.getResourceRoutingCfg().get(_resourceName);
    if (null == cfg) {
      System.out.println("Unable to find routing config for resource ("
          + _resourceName + ")");
      return null;
    }

    SimpleScatterGatherRequest request = new SimpleScatterGatherRequest(_request, cfg);
    return request;
  }

  /**
   * Helper to send request to server and get back response
   * @param request
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private String sendRequestAndGetResponse(SimpleScatterGatherRequest request) throws InterruptedException,
  ExecutionException, IOException, ClassNotFoundException {
    CompositeFuture<ServerInstance, ByteBuf> future = _scatterGather.scatterGather(request);
    ByteBuf b = future.getOne();
    String r = null;
    if (null != b) {
      byte[] b2 = new byte[b.readableBytes()];
      b.readBytes(b2);
      r = new String(b2);
    }
    return r;
  }

  private CompositeFuture<ServerInstance, ByteBuf> asyncSendRequestAndGetResponse(SimpleScatterGatherRequest request) throws InterruptedException {
    return _scatterGather.scatterGather(request);
  }
  
  private static class QueueEntry
  {
    private final boolean _last;
    private final boolean _measured;
    private final long _timeSentMs;
    private final CompositeFuture<ServerInstance, ByteBuf> future;
    
    public QueueEntry(boolean last,boolean measured, long timeSentMs, CompositeFuture<ServerInstance, ByteBuf> future) {
      super();
      _last = last;
      _measured = measured;
      _timeSentMs = timeSentMs;
      this.future = future;
    }
    
    
    public boolean isMeasured() {
      return _measured;
    }


    public boolean isLast() {
      return _last;
    }
    
    public CompositeFuture<ServerInstance, ByteBuf> getFuture() {
      return future;
    }

    public long getTimeSentMs() {
      return _timeSentMs;
    }
  }
  
  
  private class AsyncReader extends Thread
  {
    private final LinkedBlockingQueue<QueueEntry> _queue;
    private final Histogram _latencyHistogram;
    
    public AsyncReader(LinkedBlockingQueue<QueueEntry> queue, Histogram histogram)
    {
      _queue = queue;
      _latencyHistogram  = histogram;
    }
    
    public void run()
    {
      while ( true)
      {
        QueueEntry e = null;
        try {
          e = _queue.take();
        } catch (InterruptedException e2) {
          // TODO Auto-generated catch block
          e2.printStackTrace();
        }
        
        if ( e.isLast())
          break;
        
        ByteBuf b = null;
        try {
          b = e.getFuture().getOne();
          _endLastResponseTime = System.currentTimeMillis();
        } catch (InterruptedException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        } catch (ExecutionException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
        long timeDiff = System.currentTimeMillis() - e.getTimeSentMs();
        _latencyHistogram.update(timeDiff);
        
        String r = null;
        if (null != b) {
          byte[] b2 = new byte[b.readableBytes()];
          b.readBytes(b2);
          r = new String(b2);
        }
      }
    }
  }
  
  
  public Histogram getLatencyHistogram() {
    return _latencyHistogram;
  }

  public static class SimpleScatterGatherRequest implements ScatterGatherRequest {
    private final byte[] _brokerRequest;

    private final Map<SegmentIdSet, List<ServerInstance>> _pgToServersMap;

    public SimpleScatterGatherRequest(byte[] q, ResourceRoutingConfig routingConfig) {
      _brokerRequest = q;
      _pgToServersMap = routingConfig.buildRequestRoutingMap();
    }

    @Override
    public Map<SegmentIdSet, List<ServerInstance>> getSegmentsServicesMap() {
      return _pgToServersMap;
    }

    @Override
    public byte[] getRequestForService(ServerInstance service, SegmentIdSet queryPartitions) {
      return _brokerRequest;
    }

    @Override
    public ReplicaSelection getReplicaSelection() {
      return new FirstReplicaSelection();
    }

    @Override
    public ReplicaSelectionGranularity getReplicaSelectionGranularity() {
      return ReplicaSelectionGranularity.SEGMENT_ID_SET;
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
    public long getRequestTimeoutMS() {
      return 10000; //10 second timeout
    }

    @Override
    public long getRequestId() {
      return 0;
    }
  }

  /**
   * Selects the first replica in the list
   * @author bvaradar
   *
   */
  public static class FirstReplicaSelection extends ReplicaSelection {

    @Override
    public void reset(SegmentId p) {
    }

    @Override
    public void reset(SegmentIdSet p) {
    }

    @Override
    public ServerInstance selectServer(SegmentId p, List<ServerInstance> orderedServers, Object hashKey) {
      //System.out.println("Partition :" + p + ", Ordered Servers :" + orderedServers);
      return orderedServers.get(0);
    }
  }

  private static Options buildCommandLineOptions() {
    Options options = new Options();
    options.addOption(BROKER_CONFIG_OPT_NAME, true, "Broker Config file");
    options.addOption(RESOURCE_NAME_OPT_NAME, true, "Resource Name");
    options.addOption(REQUEST_SIZE_OPT_NAME, true, "Request Size");
    options.addOption(NUM_REQUESTS_OPT_NAME, true, "Num Requests");
    return options;
  }

  public static void main(String[] args) throws Exception {
    //Process Command Line to get config and port
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = buildCommandLineOptions();

    CommandLine cmd = cliParser.parse(cliOptions, args, true);

    if ((!cmd.hasOption(BROKER_CONFIG_OPT_NAME)) || 
        (!cmd.hasOption(REQUEST_SIZE_OPT_NAME)) ||
        (!cmd.hasOption(RESOURCE_NAME_OPT_NAME)) ||
        (!cmd.hasOption(RESOURCE_NAME_OPT_NAME))) {
      System.err.println("Missing required arguments !!");
      System.err.println(cliOptions);
      throw new RuntimeException("Missing required arguments !!");
    }

    String brokerConfigPath = cmd.getOptionValue(BROKER_CONFIG_OPT_NAME);
    int requestSize = Integer.parseInt(cmd.getOptionValue(REQUEST_SIZE_OPT_NAME));
    int numRequests = Integer.parseInt(cmd.getOptionValue(NUM_REQUESTS_OPT_NAME));
    String resourceName = cmd.getOptionValue(RESOURCE_NAME_OPT_NAME);

    // build  brokerConf
    PropertiesConfiguration brokerConf = new PropertiesConfiguration();
    brokerConf.setDelimiterParsingDisabled(false);
    brokerConf.load(brokerConfigPath);

    RoutingTableConfig config = new RoutingTableConfig();
    config.init(brokerConf.subset(ROUTING_CFG_PREFIX));
    ScatterGatherPerfClient client = new ScatterGatherPerfClient(config, requestSize, resourceName, false, numRequests);
    client.run();

    System.out.println("Shutting down !!");
    client.shutdown();
    System.out.println("Shut down complete !!");
  }

  public int getNumRequestsMeasured() {
    return _numRequestsMeasured;
  }

  public TimerContext getTimerContext() {
    return _timerContext;
  }

  public long getBeginFirstRequestTime() {
    return _beginFirstRequestTime;
  }

  public long getEndLastResponseTime() {
    return _endLastResponseTime;
  }
  
}
