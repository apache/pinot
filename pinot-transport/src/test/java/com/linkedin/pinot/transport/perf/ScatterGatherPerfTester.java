/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.perf;

import com.linkedin.pinot.common.metrics.AggregatedHistogram;
import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.transport.config.PerTableRoutingConfig;
import com.linkedin.pinot.transport.config.RoutingTableConfig;
import com.yammer.metrics.core.Histogram;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class ScatterGatherPerfTester {

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
      Map<String, PerTableRoutingConfig> cfg = config.getPerTableRoutingCfg();
      PerTableRoutingConfig c = new PerTableRoutingConfig(null);
      Map<Integer, List<String>> instanceMap = c.getNodeToInstancesMap();

      int numUniqueServers = _remoteServerHosts.size();

      for (int i = 0; i < _numServers; i++) {
        List<String> serverNames = new ArrayList<>();
        String host;
        if (_mode == ExecutionMode.RUN_BOTH) {
          host = "localhost";
        } else {
          host = _remoteServerHosts.get(i % numUniqueServers);
        }
        String serverName = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + host
            + ServerInstance.NAME_PORT_DELIMITER_FOR_INSTANCE_NAME + port++;
        serverNames.add(serverName);
        instanceMap.put(i, serverNames);
      }
      String host;
      if (_mode == ExecutionMode.RUN_BOTH) {
        host = "localhost";
      } else {
        host = _remoteServerHosts.get(0);
      }
      String serverName = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + host
          + ServerInstance.NAME_PORT_DELIMITER_FOR_INSTANCE_NAME + port++;
      c.getDefaultServers().add(serverName);
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

  private static final String EXECUTION_MODE = "exec_mode";
  private static final String NUM_CLIENTS = "num_clients";
  private static final String NUM_SERVERS = "num_servers";
  private static final String REQUEST_SIZE = "num_servers";
  private static final String RESPONSE_SIZE = "num_servers";
  private static final String NUM_REQUESTS = "num_servers";
  private static final String SERVER_START_PORT = "server_start_port";
  private static final String SYNC_REQUEST_DISPATCH = "is_sync_request_dispatch";
  private static final String SERVER_HOSTS = "server_hosts";
  private static final String CONN_POOL_SIZE_PER_PEER = "conn_pool_size";
  private static final String NUM_RESPONSE_READERS = "num_readers";
  private static final String RESPONSE_LATENCY = "response_induced_latency";

  private static Options buildCommandLineOptions() {
    Options options = new Options();
    options.addOption(EXECUTION_MODE, true, "Execution Mode. One of " + EnumSet.allOf(ExecutionMode.class));
    options.addOption(NUM_CLIENTS, true, "Number of Client instances. (Clients will not share connection-pool). Used only when execution mode is RUN_CLIENT or RUN_BOTH");
    options.addOption(NUM_SERVERS, true, "Number of server instances. Used only when execution mode is RUN_SERVER or RUN_BOTH");
    options.addOption(REQUEST_SIZE, true, "Request Size. Used only when execution mode is RUN_SERVER or RUN_BOTH");
    options.addOption(RESPONSE_SIZE, true, "Response Size. Used only when execution mode is RUN_SERVER or RUN_BOTH");
    options.addOption(NUM_REQUESTS, true, "Number of requests to be sent per Client instances. Used only when execution mode is RUN_CLIENT or RUN_BOTH");
    options.addOption(SERVER_START_PORT, true, "Start port for server. If execution_mode == RUN_SERVER or RUN_BOTH, then, N (controlled by num_servers) servers will be started with port numbers monotonically incremented from this value. If execution_mode == RUN_CLIENT, then N servers are assumed to be running remotely and this client connects to them");
    options.addOption(SYNC_REQUEST_DISPATCH, false, "Do we want to send requests synchronously (one by one requests and response per client). Set it to false to mimic production workflows");
    options.addOption(SERVER_HOSTS, true, "Comma seperated list of remote hosts where the servers are assumed to be running with same ports (assigned from start_port_num)");
    options.addOption(CONN_POOL_SIZE_PER_PEER, true, "Number of max active connections to be allowed");
    options.addOption(NUM_RESPONSE_READERS, true, "Number of reponse reader threads per Client instances. Used only when execution mode is RUN_CLIENT or RUN_BOTH");
    options.addOption(RESPONSE_LATENCY, true, "Induced Latency in server per request. Used only when execution mode is RUN_SERVER or RUN_BOTH");
    return options;
  }

  public static void main(String[] args) throws Exception
  {
    CommandLineParser cliParser = new GnuParser();

    Options cliOptions = buildCommandLineOptions();

    CommandLine cmd = cliParser.parse(cliOptions, args, true);

    if (!cmd.hasOption(EXECUTION_MODE)) {
      System.out.println("Missing required argument (" + EXECUTION_MODE + ")");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "", cliOptions );
      System.exit(-1);
    }

    ExecutionMode  mode = ExecutionMode.valueOf(cmd.getOptionValue(EXECUTION_MODE));

    int numClients = 1;
    int numServers = 1;
    int requestSize = 1000;
    int responseSize = 100000;
    int numRequests = 10000;
    int startPortNum = 9078;
    boolean isAsyncRequest = true;
    List<String> servers = new ArrayList<String>();
    int numActiveConnectionsPerPeer = 10;
    int numResponseReaders = 3;
    long serverInducedLatency = 10;

    if ( mode == ExecutionMode.RUN_CLIENT)
    {
      if (!cmd.hasOption(SERVER_HOSTS)) {
        System.out.println("Missing required argument (" + SERVER_HOSTS + ")");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "", cliOptions );
        System.exit(-1);
      }
    }

    if (cmd.hasOption(NUM_CLIENTS)) {
      numClients = Integer.parseInt(cmd.getOptionValue(NUM_CLIENTS));
    }

    if (cmd.hasOption(NUM_SERVERS)) {
      numServers = Integer.parseInt(cmd.getOptionValue(NUM_SERVERS));
    }
    if (cmd.hasOption(REQUEST_SIZE)) {
      requestSize = Integer.parseInt(cmd.getOptionValue(REQUEST_SIZE));
    }
    if (cmd.hasOption(RESPONSE_SIZE)) {
      responseSize = Integer.parseInt(cmd.getOptionValue(RESPONSE_SIZE));
    }
    if (cmd.hasOption(NUM_REQUESTS)) {
      numRequests = Integer.parseInt(cmd.getOptionValue(NUM_REQUESTS));
    }
    if (cmd.hasOption(SERVER_START_PORT)) {
      startPortNum = Integer.parseInt(cmd.getOptionValue(SERVER_START_PORT));
    }
    if (cmd.hasOption(SYNC_REQUEST_DISPATCH)) {
      isAsyncRequest = false;
    }
    if (cmd.hasOption(CONN_POOL_SIZE_PER_PEER)) {
      numActiveConnectionsPerPeer = Integer.parseInt(cmd.getOptionValue(CONN_POOL_SIZE_PER_PEER));
    }
    if (cmd.hasOption(NUM_RESPONSE_READERS)) {
      numResponseReaders = Integer.parseInt(cmd.getOptionValue(NUM_RESPONSE_READERS));
    }
    if (cmd.hasOption(RESPONSE_LATENCY)) {
      serverInducedLatency = Integer.parseInt(cmd.getOptionValue(RESPONSE_LATENCY));
    }

    if (cmd.hasOption(SERVER_HOSTS)) {
      servers = Arrays.asList(cmd.getOptionValue(SERVER_HOSTS).split(","));
    }

    ScatterGatherPerfTester tester = new ScatterGatherPerfTester(numClients, // num Client Threads
                                                                 numServers, // Num Servers
                                                                 requestSize, // Request Size
                                                                 responseSize, // Response Size
                                                                 numRequests, //  Num Requests
                                                                 startPortNum, // Server start port
                                                                 isAsyncRequest, // Async Request sending
                                                                 ExecutionMode.RUN_CLIENT, // Execution mode
                                                                 servers, // Server Hosts. All servers need to run on the same port
                                                                 numActiveConnectionsPerPeer, // Number of Active Client connections per Client-Server pair
                                                                 numResponseReaders, // Number of Response Reader threads in client
                                                                 serverInducedLatency); // 10 ms latency at server
    tester.run();
  }

}
