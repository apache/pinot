package com.linkedin.pinot.server.starter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.Request;
import com.linkedin.pinot.query.response.InstanceResponse;
import com.linkedin.pinot.server.conf.BrokerRoutingConfig;
import com.linkedin.pinot.server.conf.ResourceRoutingConfig;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * 
 *
 * A broker which would connect to the servers configured in the (config file) and provide interactive console
 * for sending request
 * 
 * 
 * Usage:
 * 
 * The query file looks like this:
 * <json-query1>\n
 * <comma-seperated partitions for query1>\n
 * ......
 * ......
 * <json-queryn>\n
 * 
 * For example:
 * [bvaradar@bvaradar-ld server (master)]$ cat s_query1
 *{"source":"midas.testTable0","aggregations":[{"aggregationType":"sum","params":{"column":"met"}}]}
 *[bvaradar@bvaradar-ld server (master)]$
 * 
 * 
 * Command to run :
 *  [bvaradar@bvaradar-ld server (master)]$ cat s_query1 | mvn exec:java -Dexec.mainClass=com.linkedin.pinot.server.starter.InteractiveBroker -Dexec.args="--broker_conf  /home/bvaradar/workspace/pinot_os_new/pinot/server/src/test/resources/conf/pinot-broker.properties"
 * 
 * If you done pipe the query-file, you can run in interactive mode.
 * 
 * @author bvaradar
 *
 */
public class InteractiveBroker {

  /*
  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }
   */

  private static final String ROUTING_CFG_PREFIX ="pinot.broker.routing";
  private static Logger LOGGER = LoggerFactory.getLogger(InteractiveBroker.class);
  private static final String BROKER_CONFIG_OPT_NAME = "broker_conf";

  /**
   * Not really a daemon but this mode allows incorrect queries and exceptions to not shutdown the program.
   * Needed for running as a service
   */
  private static final String DAEMON_MODE_OPT_NAME = "daemon";

  // Broker Config File Path
  private static String _brokerConfigPath;

  /**
   * Not really a daemon but this mode allows incorrect queries and exceptions to not shutdown the program.
   * Needed for running as a service
   */
  private static boolean _isDaemonMode;

  // RequestId Generator
  private static AtomicLong _requestIdGen = new AtomicLong(0);

  //Routing Config and Pool
  private final BrokerRoutingConfig _routingConfig;
  private ScatterGather _scatterGather;
  private KeyedPool<ServerInstance, NettyClientConnection> _pool;
  private ExecutorService _service;
  private EventLoopGroup _eventLoopGroup;
  private ScheduledExecutorService _timedExecutor;
  // Input Reader
  private final BufferedReader _reader;

  public InteractiveBroker(BrokerRoutingConfig config)
  {
    _routingConfig = config;
    _reader = new BufferedReader(new InputStreamReader(System.in));
    setup();
  }

  private void setup()
  {
    MetricsRegistry registry = new MetricsRegistry();
    _timedExecutor = new ScheduledThreadPoolExecutor(1);
    _service = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new LinkedBlockingDeque<Runnable>());
    _eventLoopGroup = new NioEventLoopGroup();
    NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "client_");
    PooledNettyClientResourceManager rm = new PooledNettyClientResourceManager(_eventLoopGroup, new HashedWheelTimer(),clientMetrics);
    _pool = new KeyedPoolImpl<ServerInstance, NettyClientConnection>(1, 1, 300000, 1, rm, _timedExecutor, _service, registry);
    rm.setPool(_pool);
    _scatterGather = new ScatterGatherImpl(_pool);
  }

  public void runQueries() throws Exception
  {
    List<ServerInstance> s1 = new ArrayList<ServerInstance>();
    ServerInstance s = new ServerInstance("localhost", 9099);
    s1.add(s);

    SimpleScatterGatherRequest req = null;
    while ( true )
    {
      // If in daemon mode, no exit unless killed,
      do
      {
        req = getRequest();
      } while ( (null  == req) && (_isDaemonMode));

      if ( null == req ) {
        return;
      }

      InstanceResponse r = sendRequestAndGetResponse(req);
      System.out.println("Response is :" + r);
      System.out.println("\n\n");
      req = null;
    }
  }

  /**
   * Shutdown all resources
   */
  public void shutdown()
  {
    if (null != _pool) {
      LOGGER.info("Shutting down Pool !!");
      try
      {
        _pool.shutdown().get();
        LOGGER.info("Pool shut down!!");
      } catch (Exception ex) {
        LOGGER.error("Unable to shutdown pool", ex);
      }
    }

    if ( null != _timedExecutor) {
      _timedExecutor.shutdown();
    }

    if ( null != _eventLoopGroup) {
      _eventLoopGroup.shutdownGracefully();
    }

    if ( null != _service) {
      _service.shutdown();
    }

  }

  /**
   * Build a request from the JSON query and partition passed
   * @return
   * @throws IOException
   * @throws JSONException 
   */
  public SimpleScatterGatherRequest getRequest() throws IOException, JSONException
  {
    System.out.println("Please provide the Query (Type exit/quit to exit) !!");
    String queryStr = _reader.readLine();
    if((null == queryStr) || queryStr.toLowerCase().startsWith("exit") || queryStr.toLowerCase().startsWith("quit"))
    {
      return null;
    }
    JSONObject jsonObj = new JSONObject(queryStr);
    Query q = null;

    try
    {
      q = Query.fromJson(jsonObj);
    } catch (Exception ex) {
      System.out.println("Input query is wrong. Got exception :" + Arrays.toString(ex.getStackTrace()));
      return null;
    }

    ResourceRoutingConfig cfg = _routingConfig.getResourceRoutingCfg().get(q.getResourceName());
    if ( null == cfg )
    {
      System.out.println("Unable to find routing config for resource (" + q.getResourceName() + ")");
      return null;
    }

    SimpleScatterGatherRequest request = new SimpleScatterGatherRequest(q, cfg);
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
  private InstanceResponse sendRequestAndGetResponse(SimpleScatterGatherRequest request) throws InterruptedException, ExecutionException, IOException, ClassNotFoundException
  {
    CompositeFuture<ServerInstance, ByteBuf> future = _scatterGather.scatterGather(request);
    ByteBuf b = future.getOne();
    InstanceResponse r = null;
    if ( null != b )
    {
      byte[] b2 = new byte[b.readableBytes()];
      b.readBytes(b2);
      r = (InstanceResponse)deserialize(b2);
    }
    return r;
  }

  private static Options buildCommandLineOptions()
  {
    Options options = new Options();
    options.addOption(BROKER_CONFIG_OPT_NAME, true, "Broker Config file");
    options.addOption(DAEMON_MODE_OPT_NAME,false, "Daemon mode");
    return options;
  }

  private static void processCommandLineArgs(String[] cliArgs) throws ParseException
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = buildCommandLineOptions();

    CommandLine cmd = cliParser.parse(cliOptions, cliArgs, true);

    if ( !cmd.hasOption(BROKER_CONFIG_OPT_NAME))
    {
      System.err.println("Missing required arguments !!");
      System.err.println(cliOptions);
      throw new RuntimeException("Missing required arguments !!");
    }

    if ( cmd.hasOption(DAEMON_MODE_OPT_NAME))
    {
      System.out.println("Daemon mode enabled");
      _isDaemonMode = true;
    }

    _brokerConfigPath = cmd.getOptionValue(BROKER_CONFIG_OPT_NAME);
  }

  public static void main(String[] args) throws Exception
  {
    //Process Command Line to get config and port
    processCommandLineArgs(args);


    // build  brokerConf
    PropertiesConfiguration brokerConf = new PropertiesConfiguration();
    brokerConf.setDelimiterParsingDisabled(false);
    brokerConf.load(_brokerConfigPath);


    BrokerRoutingConfig config = new BrokerRoutingConfig(brokerConf.subset(ROUTING_CFG_PREFIX));
    InteractiveBroker broker = new InteractiveBroker(config);
    broker.runQueries();

    System.out.println("Shutting down !!");
    broker.shutdown();
    System.out.println("Shut down complete !!");

  }


  public static class SimpleScatterGatherRequest implements ScatterGatherRequest
  {
    private final Query _query;

    private final Map<SegmentIdSet, List<ServerInstance>> _pgToServersMap;

    public SimpleScatterGatherRequest(Query q, ResourceRoutingConfig routingConfig )
    {
      _query = q;
      _pgToServersMap = routingConfig.buildRequestRoutingMap();
    }

    @Override
    public Map<SegmentIdSet, List<ServerInstance>> getSegmentsServicesMap() {
      return _pgToServersMap;
    }

    @Override
    public byte[] getRequestForService(ServerInstance service, SegmentIdSet queryPartitions) {
      Request r = new Request();

      //Set query
      r.setQuery(_query);

      //Set Request Id
      r.setRequestId(_requestIdGen.incrementAndGet());

      //Serialize Request
      byte[] b = null;
      try
      {
        b = serialize(r);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      return b;
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
  public static class FirstReplicaSelection extends ReplicaSelection
  {

    @Override
    public void reset(SegmentId p) {
    }

    @Override
    public void reset(SegmentIdSet p) {
    }

    @Override
    public ServerInstance selectServer(SegmentId p, List<ServerInstance> orderedServers,
        Object hashKey) {
      //System.out.println("Partition :" + p + ", Ordered Servers :" + orderedServers);
      return orderedServers.get(0);
    }
  }

  public static byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    ObjectOutputStream o = new ObjectOutputStream(b);
    o.writeObject(obj);
    return b.toByteArray();
  }


  public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream b = new ByteArrayInputStream(bytes);
    ObjectInputStream o = new ObjectInputStream(b);
    return o.readObject();
  }
}
