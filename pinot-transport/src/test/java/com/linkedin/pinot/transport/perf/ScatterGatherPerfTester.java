package com.linkedin.pinot.transport.perf;

import io.netty.util.ResourceLeakDetector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

import com.linkedin.pinot.common.metrics.AggregatedHistogram;
import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.config.ResourceRoutingConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;
import com.yammer.metrics.core.Histogram;

public class ScatterGatherPerfTester {

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout("%d %p (%t) [%c] - %m%n"), "System.out"));
//          new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));

    org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
    //ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }
  
  public enum ExecutionMode
  {
    RUN_CLIENT,
    RUN_SERVER,
    RUN_BOTH
  };
  
  private final int _numClients;
  private final int _numServers;
  private final int _requestSize;
  private final int _responseSize;
  private final int _numRequests;
  private final int _startPortNum;
  private final String _resourceName;
  private final boolean _asyncRequestDispatch;
  private final ExecutionMode _mode;
  private final List<String> _remoteServerHosts; // Only applicable if mode == RUN_CLIENT
  private final int _maxActiveConnectionsPerClientServerPair;
  private final int _numResponseReaderThreads;
  private final long _responseLatencyAtServer;
  
  public ScatterGatherPerfTester(int numClients,
                                 int numServers, 
                                 int requestSize, 
                                 int responseSize, 
                                 int numRequests,
                                 int startPortNum,
                                 boolean asyncRequestDispatch,
                                 ExecutionMode mode,
                                 List<String> remoteServerHosts,
                                 int maxActiveConnectionsPerClientServerPair,
                                 int numResponseReaderThreads,
                                 long responseLatencyAtServer)
  {
    _numClients = numClients;
    _numServers = numServers;
    _requestSize = requestSize;
    _responseSize = responseSize;
    _numRequests = numRequests;
    _startPortNum = startPortNum;
    _resourceName = "testResource";
    _asyncRequestDispatch = asyncRequestDispatch;
    _mode = mode;
    _remoteServerHosts = remoteServerHosts;
    _maxActiveConnectionsPerClientServerPair = maxActiveConnectionsPerClientServerPair;
    _numResponseReaderThreads = numResponseReaderThreads;
    _responseLatencyAtServer = responseLatencyAtServer;
  }

  
  public List<ScatterGatherPerfServer> runServer() throws Exception
  {
    // Start the servers
    List<ScatterGatherPerfServer> servers = new ArrayList<ScatterGatherPerfServer>();
    int port = _startPortNum;
    for (int i = 0; i < _numServers; i++)
    {
      ScatterGatherPerfServer server = new ScatterGatherPerfServer(port++, _responseSize, _responseLatencyAtServer);
      servers.add(server);
      System.out.println("Starting the server with port : " + (port -1));
      server.run();
    }
    
    Thread.sleep(3000);    
    
    return servers;
  }
  
  public void run() throws Exception
  {

    List<ScatterGatherPerfServer> servers = null;
    
    // Run Servers when mode is RUN_SERVER or RUN_BOTH
    if (_mode != ExecutionMode.RUN_CLIENT)
    {
      servers = runServer();
    }
    
    if (_mode != ExecutionMode.RUN_SERVER)
    {
      int port = _startPortNum;
      // Setup Routing config for clients
      RoutingTableConfig config = new RoutingTableConfig();
      Map<String, ResourceRoutingConfig> cfg = config.getResourceRoutingCfg();
      ResourceRoutingConfig c = new ResourceRoutingConfig(null);
      Map<Integer, List<ServerInstance>> instanceMap = c.getNodeToInstancesMap();
      port = _startPortNum;
      
      int numUniqueServers = _remoteServerHosts.size();
            
      for (int i = 0; i < _numServers; i++) {
        List<ServerInstance> instances = new ArrayList<ServerInstance>();
        String server = null;
        if (_mode == ExecutionMode.RUN_BOTH)
          server = "localhost";
        else
          server = _remoteServerHosts.get(i%numUniqueServers);
        ServerInstance instance = new ServerInstance(server, port++);
        instances.add(instance);
        instanceMap.put(i, instances);
      }
      String server = null;
      if (_mode == ExecutionMode.RUN_BOTH)
        server = "localhost";
      else
        server = _remoteServerHosts.get(0);
      c.getDefaultServers().add(new ServerInstance(server, port - 1));
      cfg.put(_resourceName, c);

      System.out.println("Routing Config is :" + cfg);

      // Build Clients
      List<Thread> clientThreads = new ArrayList<Thread>();
      List<ScatterGatherPerfClient> clients = new ArrayList<ScatterGatherPerfClient>();
      AggregatedHistogram<Histogram> latencyHistogram = new AggregatedHistogram<Histogram>();
      for (int i = 0; i < _numClients; i++) {
        ScatterGatherPerfClient c2 =
            new ScatterGatherPerfClient(config, _requestSize, _resourceName, _asyncRequestDispatch, _numRequests, _maxActiveConnectionsPerClientServerPair, _numResponseReaderThreads);
        Thread t = new Thread(c2);
        clients.add(c2);
        latencyHistogram.add(c2.getLatencyHistogram());
        clientThreads.add(t);
      }

      System.out.println("Starting the clients !!");
      long startTimeMs = 0;
      // Start Clients
      for (Thread t2 : clientThreads)
        t2.start();

      System.out.println("Waiting for clients to finish");

      // Wait for clients to finish
      for (Thread t2 : clientThreads)
        t2.join();

      Thread.sleep(3000);

      System.out.println("Client threads done !!");

      int totalRequestsMeasured = 0;
      long beginRequestTime = Long.MAX_VALUE;
      long endResponseTime = Long.MIN_VALUE;
      for (ScatterGatherPerfClient c3 : clients) {
        int numRequestsMeasured = c3.getNumRequestsMeasured();
        totalRequestsMeasured += numRequestsMeasured;
        beginRequestTime = Math.min(beginRequestTime, c3.getBeginFirstRequestTime());
        endResponseTime = Math.max(endResponseTime, c3.getEndLastResponseTime());
        //System.out.println("2 Num Requests :" + numRequestsMeasured);
        //System.out.println("2 time :" + timeTakenMs );
        //System.out.println("2 Throughput (Requests/Second) :" + ((numRequestsMeasured* 1.0 * 1000)/timeTakenMs));
      }
      long totalTimeTakenMs = endResponseTime - beginRequestTime;
      System.out.println("Overall Total Num Requests :" + totalRequestsMeasured);
      System.out.println("Overall Total time :" + totalTimeTakenMs);
      System.out.println("Overall Throughput (Requests/Second) :"
          + ((totalRequestsMeasured * 1.0 * 1000) / totalTimeTakenMs));
      latencyHistogram.refresh();
      System.out.println("Latency :" + new LatencyMetric<AggregatedHistogram<Histogram>>(latencyHistogram));
    }
  
    if ( _mode == ExecutionMode.RUN_BOTH)
    {
      // Shutdown Servers
      for (ScatterGatherPerfServer s : servers)
      {
        s.shutdown();
      }
    }
  }
  
  
  public static void main(String[] args) throws Exception
  {
    List<String> servers = new ArrayList<String>();
    servers.add("dpatel-ld");
    servers.add("ychang-ld1");
    ScatterGatherPerfTester tester = new ScatterGatherPerfTester(1, // num Client Threads
                                                                 10, // Num Servers
                                                                 1000, // Request Size
                                                                 10000, // Response Size
                                                                 2, //  Num Requests
                                                                 9078, // Server start port
                                                                 true, // Async Request sending
                                                                 ExecutionMode.RUN_BOTH, // Execution mode
                                                                 servers, // Server Hosts. All servers need to run on the same port 
                                                                 10, // Number of Active Client connections per Client-Server pair
                                                                 3, // Number of Response Reader threads in client
                                                                 10); // 10 ms latency at server
    tester.run();
  }
  
}
