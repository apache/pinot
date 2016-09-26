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
package com.linkedin.pinot.server.starter;

import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.server.request.ScheduledRequestHandler;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.transport.netty.NettyServer;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer;


/**
*
* A Standalone Server which will run on the port configured in the properties file.
* and accept queries. All configurations needed to run the server is provided
* in the config file passed. No external cluster manager integration available (yet)
*
*/
public class FileBasedServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedServer.class);

  public static final String SERVER_PORT_OPT_NAME = "server_port";
  public static final String SERVER_CONFIG_OPT_NAME = "server_conf";

  private static int _serverPort;
  private static String _serverConfigPath;

  private static Options buildCommandLineOptions() {
    Options options = new Options();
    options.addOption(SERVER_PORT_OPT_NAME, true, "Server Port for accepting queries from broker");
    options.addOption(SERVER_CONFIG_OPT_NAME, true, "Server Config file");
    return options;
  }

  private static void processCommandLineArgs(String[] cliArgs) throws ParseException {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = buildCommandLineOptions();

    CommandLine cmd = cliParser.parse(cliOptions, cliArgs, true);

    if (!cmd.hasOption(SERVER_CONFIG_OPT_NAME) || !cmd.hasOption(SERVER_PORT_OPT_NAME)) {
      System.err.println("Missing required arguments !!");
      System.err.println(cliOptions);
      throw new RuntimeException("Missing required arguments !!");
    }

    _serverConfigPath = cmd.getOptionValue(SERVER_CONFIG_OPT_NAME);
    _serverPort = Integer.parseInt(cmd.getOptionValue(SERVER_PORT_OPT_NAME));
  }

  public static void main(String[] args) throws Exception {
    //Process Command Line to get config and port
    processCommandLineArgs(args);

    LOGGER.info("Trying to build server config");
    final MetricsRegistry metricsRegistry = new MetricsRegistry();
    final ServerBuilder serverBuilder = new ServerBuilder(new File(_serverConfigPath), metricsRegistry);

    LOGGER.info("Trying to build InstanceDataManager");
    final DataManager instanceDataManager = serverBuilder.buildInstanceDataManager();
    LOGGER.info("Trying to start InstanceDataManager");
    instanceDataManager.start();
    //    bootstrapSegments(instanceDataManager);

    LOGGER.info("Trying to build QueryExecutor");
    final QueryExecutor queryExecutor = serverBuilder.buildQueryExecutor(instanceDataManager);
    final QueryScheduler queryScheduler = serverBuilder.buildQueryScheduler(queryExecutor);
    LOGGER.info("Trying to build RequestHandlerFactory");
    RequestHandlerFactory simpleRequestHandlerFactory = new RequestHandlerFactory() {
      @Override
      public NettyServer.RequestHandler createNewRequestHandler() {
        return new ScheduledRequestHandler(queryScheduler, serverBuilder.getServerMetrics());
      }
    };
    LOGGER.info("Trying to build NettyServer");

    NettyServer nettyServer = new NettyTCPServer(_serverPort, simpleRequestHandlerFactory, null);
    Thread serverThread = new Thread(nettyServer);
    ShutdownHook shutdownHook = new ShutdownHook(nettyServer);
    serverThread.start();
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  public static class ShutdownHook extends Thread {
    private final NettyServer _server;

    public ShutdownHook(NettyServer server) {
      _server = server;
    }

    @Override
    public void run() {
      LOGGER.info("Running shutdown hook");
      if (_server != null) {
        _server.shutdownGracefully();
      }
      LOGGER.info("Shutdown completed !!");
    }
  }

}
