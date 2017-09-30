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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.metrics.AggregatedMetricsRegistry;
import com.linkedin.pinot.transport.netty.NettyServer;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;
import com.linkedin.pinot.transport.netty.NettyTCPServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * This class is used for benchmarking the Scatter-Gather Layer
 *
 *
 */
public class ScatterGatherPerfServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScatterGatherPerfServer.class);

  /*
  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
  }
  */

  public static final String RESPONSE_SIZE_OPT_NAME = "response_size";
  public static final String SERVER_PORT_OPT_NAME = "server_port";

  private final int _serverPort;
  private final int _responseSize;
  private final long _responseLatencyMs;


  private byte[] _bakedResponse;

  private NettyTCPServer _server;

  public ScatterGatherPerfServer(int serverPort, int responseSize, long responseLatencyMs)
  {
    _serverPort = serverPort;
    _responseSize = responseSize;
    _responseLatencyMs = responseLatencyMs;
  }

  public void run()
  {
    AggregatedMetricsRegistry metricsRegistry = new AggregatedMetricsRegistry();
    _bakedResponse = new byte[_responseSize];
    for (int i = 0 ; i < _responseSize; i++)
      _bakedResponse[i] = 'a';

    MyRequestHandler handler = new MyRequestHandler(new String(_bakedResponse), null, _responseLatencyMs);
    MyRequestHandlerFactory handlerFactory = new MyRequestHandlerFactory(handler);
    _server = new NettyTCPServer(_serverPort, handlerFactory, metricsRegistry);
    Thread serverThread = new Thread(_server, "ServerMain");
    ShutdownHook shutdownHook = new ShutdownHook(_server);
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    serverThread.start();
  }

  public void shutdown()
  {
    if (null != _server)
    {
      _server.shutdownGracefully();
      _server = null;
    }
  }

  private static Options buildCommandLineOptions() {
    Options options = new Options();
    options.addOption(SERVER_PORT_OPT_NAME, true, "Server Port for accepting queries from broker");
    options.addOption(RESPONSE_SIZE_OPT_NAME, true, "Response Size");
    return options;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{

    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = buildCommandLineOptions();

    CommandLine cmd = cliParser.parse(cliOptions, args, true);

    if (!cmd.hasOption(RESPONSE_SIZE_OPT_NAME) || !cmd.hasOption(SERVER_PORT_OPT_NAME)) {
      System.err.println("Missing required arguments !!");
      System.err.println(cliOptions);
      throw new RuntimeException("Missing required arguments !!");
    }

    int responseSize =  Integer.parseInt(cmd.getOptionValue(RESPONSE_SIZE_OPT_NAME));
    int serverPort = Integer.parseInt(cmd.getOptionValue(SERVER_PORT_OPT_NAME));

    ScatterGatherPerfServer server = new ScatterGatherPerfServer(serverPort, responseSize, 2); // 2ms latency
    server.run();
  }

  private static class MyRequestHandlerFactory implements RequestHandlerFactory {
    private final MyRequestHandler _requestHandler;

    public MyRequestHandlerFactory(MyRequestHandler requestHandler) {
      _requestHandler = requestHandler;
    }

    @Override
    public RequestHandler createNewRequestHandler() {
      return _requestHandler;
    }

  }

  private static class MyRequestHandler implements RequestHandler {
    private String _response;
    private String _request;
    private final CountDownLatch _responseHandlingLatch;
    private final long _responseLatencyMs;

    public MyRequestHandler(String response, CountDownLatch responseHandlingLatch, long responseLatencyMs) {
      _response = response;
      _responseHandlingLatch = responseHandlingLatch;
      _responseLatencyMs = responseLatencyMs;
    }

    @Override
    public ListenableFuture<byte[]> processRequest(ChannelHandlerContext channelHandlerContext, ByteBuf request) {
      byte[] b = new byte[request.readableBytes()];
      request.readBytes(b);
      if (null != _responseHandlingLatch) {
        try {
          _responseHandlingLatch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      _request = new String(b);

      if (_responseLatencyMs > 0)
      {
        try {
          Thread.sleep(_responseLatencyMs);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      //LOG.info("Server got the request (" + _request + ")");
      return Futures.immediateFuture(_response.getBytes());
    }

    public String getRequest() {
      return _request;
    }

    public String getResponse() {
      return _response;
    }

    public void setResponse(String response) {
      _response = response;
    }
  }

  public static class ShutdownHook extends Thread {
    private final NettyServer _server;

    public ShutdownHook(NettyServer server) {
      _server = server;
    }

    @Override
    public void run() {
      LOGGER.info("Running shutdown hook");
      if ((_server != null) && (!_server.isShutdownComplete())) {
        _server.shutdownGracefully();
      }
      LOGGER.info("Shutdown completed !!");
    }
  }
}
