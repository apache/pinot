package com.linkedin.pinot.server.starter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.index.query.FilterQuery;
import com.linkedin.pinot.query.request.AggregationInfo;
import com.linkedin.pinot.query.request.Query;
import com.linkedin.pinot.query.request.Request;
import com.linkedin.pinot.query.response.InstanceResponse;
import com.linkedin.pinot.server.conf.BrokerRoutingConfig;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.Partition;
import com.linkedin.pinot.transport.common.PartitionGroup;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.ServerInstance;
import com.linkedin.pinot.transport.metrics.NettyClientMetrics;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.PooledNettyClientResourceManager;
import com.linkedin.pinot.transport.pool.KeyedPoolImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGather;
import com.linkedin.pinot.transport.scattergather.ScatterGatherImpl;
import com.linkedin.pinot.transport.scattergather.ScatterGatherRequest;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * A broker which would connect to the servers configured in the (config file) and provide interactive console
 * for sending request
 */
public class InteractiveBroker {

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }

  private static final String ROUTING_CFG_PREFIX ="pinot.broker.routing";
  private static Logger LOGGER = LoggerFactory.getLogger(InteractiveBroker.class);
  private static final String BROKER_CONFIG_OPT_NAME = "broker_conf";

  // Broker Config File Path
  private static String _brokerConfigPath;

  // RequestId Generator
  private static AtomicLong _requestIdGen = new AtomicLong(0);

  //Routing Config
  private final BrokerRoutingConfig _routingConfig;
  private ScatterGather _scatterGather;

  public InteractiveBroker(BrokerRoutingConfig config)
  {
    _routingConfig = config;
    setup();
  }

  private void setup()
  {
    MetricsRegistry registry = new MetricsRegistry();
    ScheduledExecutorService timedExecutor = new ScheduledThreadPoolExecutor(1);
    ExecutorService service = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new LinkedBlockingDeque<Runnable>());
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "client_");
    PooledNettyClientResourceManager rm = new PooledNettyClientResourceManager(eventLoopGroup, clientMetrics);
    KeyedPoolImpl<ServerInstance, NettyClientConnection> pool = new KeyedPoolImpl<ServerInstance, NettyClientConnection>(1, 1, 300000, 1, rm, timedExecutor, service, registry);
    rm.setPool(pool);
    _scatterGather = new ScatterGatherImpl(pool);
  }

  public void issueQuery() throws Exception
  {
    Partition p1 = new Partition(0);
    PartitionGroup pg = new PartitionGroup();
    pg.addPartition(p1);

    List<ServerInstance> s1 = new ArrayList<ServerInstance>();
    ServerInstance s = new ServerInstance("localhost", 9099);
    s1.add(s);

    Query q = getMaxQuery();
    InstanceResponse r = sendRequestAndGetResponse(pg, s, q);
    System.out.println(r);
  }

  private static Query getMaxQuery() {
    Query query = new Query();
    query.setResourceName("midas");
    AggregationInfo aggregationInfo = getMaxAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static Query getMinQuery() {
    Query query = new Query();
    query.setResourceName("midas");
    AggregationInfo aggregationInfo = getMinAggregationInfo();
    List<AggregationInfo> aggregationsInfo = new ArrayList<AggregationInfo>();
    aggregationsInfo.add(aggregationInfo);
    query.setAggregationsInfo(aggregationsInfo);
    FilterQuery filterQuery = getFilterQuery();
    query.setFilterQuery(filterQuery);
    return query;
  }

  private static AggregationInfo getMaxAggregationInfo()
  {
    String type = "max";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static AggregationInfo getMinAggregationInfo()
  {
    String type = "min";
    Map<String, String> params = new HashMap<String, String>();
    params.put("column", "met");
    return new AggregationInfo(type, params);
  }

  private static FilterQuery getFilterQuery() {
    FilterQuery filterQuery = new FilterQuery();
    return filterQuery;
  }

  private InstanceResponse sendRequestAndGetResponse(PartitionGroup pg, ServerInstance s, Query q) throws InterruptedException, ExecutionException, IOException, ClassNotFoundException
  {
    SimpleScatterGatherRequest request = new SimpleScatterGatherRequest(q, pg, s);
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
    broker.issueQuery();
    System.out.println(config);
  }


  public static class SimpleScatterGatherRequest implements ScatterGatherRequest
  {
    private final Query _query;
    private final PartitionGroup _partitions;
    private final ServerInstance _server;
    private final Map<PartitionGroup, List<ServerInstance>> _pgToServersMap;

    public SimpleScatterGatherRequest(Query q, PartitionGroup pg, ServerInstance server )
    {
      _query = q;
      _partitions = pg;
      _server = server;
      List<ServerInstance> servers = new ArrayList<ServerInstance>();
      servers.add(_server);
      _pgToServersMap = new HashMap<PartitionGroup, List<ServerInstance>>();
      _pgToServersMap.put(_partitions, servers);
    }

    @Override
    public Map<PartitionGroup, List<ServerInstance>> getPartitionServicesMap() {
      return _pgToServersMap;
    }

    @Override
    public byte[] getRequestForService(ServerInstance service, PartitionGroup queryPartitions) {
      Request r = new Request();

      //Set query
      r.setQuery(_query);

      //Set Partitions
      Set<Partition> partitionSet = queryPartitions.getPartitions();
      List<Long> partitionIds = new ArrayList<Long>();
      for (Partition p : partitionSet)
      {
        partitionIds.add(p.getPartitionNumer());
      }
      r.setSearchPartitions(partitionIds);

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
  }

  /**
   * Selects the first replica in the list
   * @author bvaradar
   *
   */
  public static class FirstReplicaSelection implements ReplicaSelection
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
