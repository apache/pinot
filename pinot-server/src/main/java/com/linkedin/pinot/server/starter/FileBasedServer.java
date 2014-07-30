package com.linkedin.pinot.server.starter;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.SegmentMetadata;
import com.linkedin.pinot.query.executor.QueryExecutor;
import com.linkedin.pinot.query.utils.IndexSegmentUtils;
import com.linkedin.pinot.server.instance.InstanceDataManager;
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

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }

  private static Logger LOGGER = LoggerFactory.getLogger(FileBasedServer.class);

  public static final String SERVER_PORT_OPT_NAME = "server_port";
  public static final String SERVER_CONFIG_OPT_NAME = "server_conf";

  private static int _serverPort;
  private static String _serverConfigPath;

  private static Options buildCommandLineOptions()
  {
    Options options = new Options();
    options.addOption(SERVER_PORT_OPT_NAME, true, "Server Port for accepting queries from broker");
    options.addOption(SERVER_CONFIG_OPT_NAME, true, "Server Config file");
    return options;
  }

  private static void processCommandLineArgs(String[] cliArgs) throws ParseException
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = buildCommandLineOptions();

    CommandLine cmd = cliParser.parse(cliOptions, cliArgs, true);

    if ( !cmd.hasOption(SERVER_CONFIG_OPT_NAME) || !cmd.hasOption(SERVER_PORT_OPT_NAME))
    {
      System.err.println("Missing required arguments !!");
      System.err.println(cliOptions);
      throw new RuntimeException("Missing required arguments !!");
    }

    _serverConfigPath = cmd.getOptionValue(SERVER_CONFIG_OPT_NAME);
    _serverPort = Integer.parseInt(cmd.getOptionValue(SERVER_PORT_OPT_NAME));
  }

  public static void main(String[] args) throws Exception
  {
    //Process Command Line to get config and port
    processCommandLineArgs(args);

    LOGGER.info("Trying to build server config");
    ServerBuilder serverBuilder = new ServerBuilder(new File(_serverConfigPath));

    LOGGER.info("Trying to build InstanceDataManager");
    final InstanceDataManager instanceDataManager = serverBuilder.buildInstanceDataManager();
    LOGGER.info("Trying to start InstanceDataManager");
    instanceDataManager.start();
    bootstrapSegments(instanceDataManager);

    LOGGER.info("Trying to build QueryExecutor");
    final QueryExecutor queryExecutor = serverBuilder.buildQueryExecutor(instanceDataManager);

    LOGGER.info("Trying to build RequestHandlerFactory");
    RequestHandlerFactory simpleRequestHandlerFactory = serverBuilder.buildRequestHandlerFactory(queryExecutor);
    LOGGER.info("Trying to build NettyServer");

    NettyServer nettyServer = new NettyTCPServer(_serverPort, simpleRequestHandlerFactory, null);
    Thread serverThread = new Thread(nettyServer);
    ShutdownHook shutdownHook = new ShutdownHook(nettyServer);
    serverThread.start();
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  /**
* TODO: Lot of hard-codings here. Need to make it config
* @param instanceDataManager
*/
  private static void bootstrapSegments(InstanceDataManager instanceDataManager) {
    for (int i = 0; i < 2; ++i) {
      IndexSegment indexSegment = IndexSegmentUtils.getIndexSegmentWithAscendingOrderValues(20000001);
      SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
      // segmentMetadata.setResourceName("midas");
      // segmentMetadata.setTableName("testTable0");
// indexSegment.setSegmentMetadata(segmentMetadata);
// indexSegment.setSegmentName("index_" + i);
      instanceDataManager.getResourceDataManager("midas");
      instanceDataManager.getResourceDataManager("midas").getPartitionDataManager(0).addSegment(indexSegment);
    }
  }


  public static class ShutdownHook extends Thread
  {
    private final NettyServer _server;

    public ShutdownHook(NettyServer server)
    {
      _server = server;
    }

    @Override
    public void run()
    {
      LOGGER.info("Running shutdown hook");
      if ( _server != null )
      {
        _server.shutdownGracefully();
      }
      LOGGER.info("Shutdown completed !!");
    }
  }

}
