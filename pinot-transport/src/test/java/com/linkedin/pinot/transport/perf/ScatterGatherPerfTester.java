package com.linkedin.pinot.transport.perf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.metrics.AggregatedHistogram;
import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.config.ResourceRoutingConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;
import com.yammer.metrics.core.Histogram;

public class ScatterGatherPerfTester {

  private final int _numClients;
  private final int _numServers;
  private final int _requestSize;
  private final int _responseSize;
  private final int _numRequests;
  private final int _startPortNum;
  private final String _resourceName;
  private final boolean _asyncRequestDispatch;

  public ScatterGatherPerfTester(int numClients,
                                 int numServers, 
                                 int requestSize, 
                                 int responseSize, 
                                 int numRequests,
                                 int startPortNum,
                                 boolean asyncRequestDispatch)
  {
    _numClients = numClients;
    _numServers = numServers;
    _requestSize = requestSize;
    _responseSize = responseSize;
    _numRequests = numRequests;
    _startPortNum = startPortNum;
    _resourceName = "testResource";
    _asyncRequestDispatch = asyncRequestDispatch;
  }

  
  public void run() throws Exception
  {
    // Start the servers
    List<ScaterGatherPerfServer> servers = new ArrayList<ScaterGatherPerfServer>();
    int port = _startPortNum;
    for (int i = 0; i < _numServers; i++)
    {
      ScaterGatherPerfServer server = new ScaterGatherPerfServer(port++, _responseSize);
      servers.add(server);
      server.run();
    }
    
    Thread.sleep(3000);
    
    // Setup Routing config for clients
    RoutingTableConfig config = new RoutingTableConfig();
    Map<String, ResourceRoutingConfig> cfg = config.getResourceRoutingCfg();
    ResourceRoutingConfig c = new ResourceRoutingConfig(null);
    Map<Integer, List<ServerInstance>> instanceMap = c.getNodeToInstancesMap();
    port = _startPortNum;
    for(int i = 0; i < _numServers; i++)
    {
      List<ServerInstance> instances = new ArrayList<ServerInstance>();
      ServerInstance instance = new ServerInstance("localhost", port++);
      instances.add(instance);
      instanceMap.put(i, instances);
    }
    c.getDefaultServers().add(new ServerInstance("localhost", port-1));
    cfg.put(_resourceName, c);
    
    System.out.println("Routing Config is :" + cfg);
    
    // Build Clients
    List<Thread> clientThreads = new ArrayList<Thread>();
    List<ScatterGatherPerfClient> clients = new ArrayList<ScatterGatherPerfClient>();
    AggregatedHistogram<Histogram> latencyHistogram = new AggregatedHistogram<Histogram>();
    for (int i = 0 ; i <  _numClients; i++)
    {
      ScatterGatherPerfClient c2 = new ScatterGatherPerfClient(config, _requestSize, _resourceName, _asyncRequestDispatch, _numRequests);
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
    
    // Shutdown Servers
    for (ScaterGatherPerfServer s : servers)
    {
      s.shutdown();
    }
    
    int totalRequestsMeasured = 0;
    long beginRequestTime = Long.MAX_VALUE;
    long endResponseTime = Long.MIN_VALUE;
    for (ScatterGatherPerfClient c3 : clients)
    {
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
    System.out.println("Overall Total time :" + totalTimeTakenMs );
    System.out.println("Overall Throughput (Requests/Second) :" + ((totalRequestsMeasured * 1.0 * 1000)/totalTimeTakenMs));  
    latencyHistogram.refresh();
    System.out.println("Latency :" + new LatencyMetric<AggregatedHistogram<Histogram>>(latencyHistogram));
  }
  
  
  public static void main(String[] args) throws Exception
  {
    ScatterGatherPerfTester tester = new ScatterGatherPerfTester(1,5,1000,10000,100005,9078,true);
    tester.run();
  }
  
}
