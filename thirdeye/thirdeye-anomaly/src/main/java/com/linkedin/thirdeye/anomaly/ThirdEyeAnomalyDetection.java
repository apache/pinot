package com.linkedin.thirdeye.anomaly;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionFactory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistoryImpl;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistoryNoOp;
import com.linkedin.thirdeye.anomaly.api.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.api.HandlerProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.task.AnomalyDetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.api.task.WorkListAnomalyDetectionTask;
import com.linkedin.thirdeye.anomaly.database.FunctionTable;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.driver.AnomalyDetectionDriver;
import com.linkedin.thirdeye.anomaly.driver.DimensionKeySeries;
import com.linkedin.thirdeye.anomaly.generic.GenericFunctionFactory;
import com.linkedin.thirdeye.anomaly.handler.AnomalyResultHandlerDatabase;
import com.linkedin.thirdeye.anomaly.rulebased.RuleBasedFunctionFactory;
import com.linkedin.thirdeye.anomaly.server.AnomalyManagementServer;
import com.linkedin.thirdeye.anomaly.util.ThirdEyeServerUtils;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.DefaultThirdEyeClientConfig;
import com.linkedin.thirdeye.client.FlowControlledDefaultThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;


/**
 * This class may be called by the standalone modes of operation or an external scheduling service.
 */
public class ThirdEyeAnomalyDetection implements Callable<Void> {

  private static final int DEFAULT_CACHE_EXPIRATION_MINUTES = 5;

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeAnomalyDetection.class);

  private final ThirdEyeAnomalyDetectionConfiguration config;

  private final TimeRange timeRange;

  /**
   * @param config
   *  Overall application config
   * @param timeRange
   *  Time range to run anomaly detection
   */
  public ThirdEyeAnomalyDetection(ThirdEyeAnomalyDetectionConfiguration config,
      TimeRange timeRange)
  {
    this.config = config;
    this.timeRange = timeRange;
  }

  /**
   * {@inheritDoc}
   * @see java.util.concurrent.Callable#call()
   */
  public Void call() throws Exception
  {
    DefaultThirdEyeClientConfig thirdEyeClientConfig = new DefaultThirdEyeClientConfig();
    thirdEyeClientConfig.setExpirationTime(DEFAULT_CACHE_EXPIRATION_MINUTES);
    thirdEyeClientConfig.setExpirationUnit(TimeUnit.MINUTES);
    thirdEyeClientConfig.setExpireAfterAccess(false);

    ThirdEyeClient thirdEyeClient = new FlowControlledDefaultThirdEyeClient(config.getThirdEyeServerHost(),
        config.getThirdEyeServerPort(), thirdEyeClientConfig, 1);

    AnomalyDetectionFunctionFactory functionFactory;
    switch (config.getMode()) {
      case RULEBASED:
      {
        functionFactory = new RuleBasedFunctionFactory();
        break;
      }
      case GENERIC:
      {
        functionFactory = new GenericFunctionFactory();
        break;
      }
      default:
      {
        functionFactory = null;
        break;
      }
    }

    List<WorkListAnomalyDetectionTask> tasks = buildTasks(thirdEyeClient, timeRange, functionFactory);

    ExecutorService taskExecutors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    for (WorkListAnomalyDetectionTask task : tasks) {
      taskExecutors.execute(task);
    }

    taskExecutors.shutdown();
    taskExecutors.awaitTermination(config.getMaxWaitToCompletion().getSize(),
        config.getMaxWaitToCompletion().getUnit());

    thirdEyeClient.close();
    return null;
  }

  /**
   * @param thirdEyeClient
   * @param timeRange
   * @param functionFactory
   * @return
   *  A list of tasks to run
   * @throws Exception
   */
  public List<WorkListAnomalyDetectionTask> buildTasks(ThirdEyeClient thirdEyeClient, TimeRange timeRange,
      AnomalyDetectionFunctionFactory functionFactory) throws Exception {

    List<WorkListAnomalyDetectionTask> tasks = new LinkedList<>();

    AnomalyDetectionDriverConfig driverConfig = config.getDriverConfig();

    List<? extends FunctionTableRow> rows = FunctionTable.selectActiveRows(config.getAnomalyDatabaseConfig(),
        functionFactory.getFunctionRowClass(), config.getCollectionName());

    // load star tree
    StarTreeConfig starTreeConfig = ThirdEyeServerUtils.getStarTreeConfig(config.getThirdEyeServerHost(),
        config.getThirdEyeServerPort(), config.getCollectionName());

    // explore the cube
    List<DimensionKeySeries> dimensionsToEvaluate = new AnomalyDetectionDriver(
        driverConfig, starTreeConfig, timeRange, thirdEyeClient).call();

    for (FunctionTableRow functionTableRow : rows) {
      // for testing purposes
      if (config.getFunctionIdToEvaluate() != null &&
          config.getFunctionIdToEvaluate() != functionTableRow.getFunctionId()) {
        continue;
      }

      try {
        // load the function
        AnomalyDetectionFunction function = functionFactory.getFunction(starTreeConfig,
            config.getAnomalyDatabaseConfig(), functionTableRow);

        // create task info
        AnomalyDetectionTaskInfo taskInfo = new AnomalyDetectionTaskInfo(functionTableRow.getFunctionName(),
            functionTableRow.getFunctionId(), functionTableRow.getFunctionDescription(), timeRange);

        // make a handler
        AnomalyResultHandler resultHandler = new AnomalyResultHandlerDatabase(config.getAnomalyDatabaseConfig());
        resultHandler.init(starTreeConfig, new HandlerProperties());

        // make the function history interface
        AnomalyDetectionFunctionHistory functionHistory;
        if (config.isProvideAnomalyHistory()) {
          functionHistory = new AnomalyDetectionFunctionHistoryImpl(starTreeConfig, config.getAnomalyDatabaseConfig(),
              functionTableRow.getFunctionId());
        } else {
          functionHistory = AnomalyDetectionFunctionHistoryNoOp.sharedInstance();
        }

        // make the task
        WorkListAnomalyDetectionTask task = new WorkListAnomalyDetectionTask(starTreeConfig, taskInfo, function,
            resultHandler, functionHistory, thirdEyeClient, dimensionsToEvaluate);

        tasks.add(task);
      } catch (Exception e) {
        LOGGER.error("could not create function for function_id={}", functionTableRow.getFunctionId(), e);
      }
    }

    return tasks;
  }

  private static final String OPT_POLLING_INTERVAL = "pollingInterval";
  private static final String OPT_TIME_RANGE = "timeRange";
  private static final String OPT_DETECTION_INTERVAL = "detectionInterval";
  private static final String OPT_HELP = "help";
  private static final String OPT_SETUP = "setup";
  private static final String OPT_SERVER = "server";

  /**
   * Main method for when running as a standalone process.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(Option.builder()
        .longOpt(OPT_SETUP)
        .desc("Setup thirdeye-anomaly detection")
        .hasArg(false).build());
    options.addOption(Option.builder("t")
        .argName("start end")
        .longOpt(OPT_TIME_RANGE)
        .desc("Run anomaly detection on this time range in milliseconds. If detection interval is also specified, "
            + "the application will run in simulated online mode.")
        .hasArgs().numberOfArgs(2).build());
    options.addOption(Option.builder("d")
        .argName("size-unit")
        .longOpt(OPT_DETECTION_INTERVAL)
        .desc("The frequency to run anomaly detection. default: 1-HOURS")
        .hasArg().build());
    options.addOption(Option.builder("p")
        .argName("minutes")
        .longOpt(OPT_POLLING_INTERVAL)
        .desc("The frequency that thirdeye-anomaly should poll thirdeye-server for new segments. default: 5")
        .hasArg().build());
    options.addOption(Option.builder("s")
        .argName("server-config.yml")
        .longOpt(OPT_SERVER)
        .desc("Run a function database management server.")
        .hasArg().build());
    options.addOption("h", OPT_HELP, false, "");

    CommandLineParser parser = new DefaultParser();
    final CommandLine cmd = parser.parse(options, args);

    /*
     * Help information
     */
    if (cmd.hasOption(OPT_HELP)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java -jar <this_jar> [OPTIONS] <config_1>.yml <config_2>.yml ...", options);
      return;
    }

    /*
     * Help the user configure anomaly detection
     */
    if (cmd.hasOption(OPT_SETUP)) {
      new ThirdEyeAnomalyDetectionSetup().setup();
      return;
    }

    /*
     * For options below, at least one config file must be given
     */
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    List<ThirdEyeAnomalyDetectionConfiguration> configs = new LinkedList<ThirdEyeAnomalyDetectionConfiguration>();

    args = cmd.getArgs();
    if (args.length == 0) {
      System.err.println("An anomaly-config.yml file is required. To create one, run with --setup.");
      return;
    } else {
      for (String fileName : args) {
        configs.add(mapper.readValue(new File(fileName), ThirdEyeAnomalyDetectionConfiguration.class));
      }
    }

    /*
     * Run a server connecting to the database in the config to manage functions
     */
    if (cmd.hasOption(OPT_SERVER)) {
      new AnomalyManagementServer(configs).run(new String[]{"server", cmd.getOptionValue(OPT_SERVER)});
      return;
    }

    for (final ThirdEyeAnomalyDetectionConfiguration config : configs)
    {
      new Thread(new Runnable() {
        @Override
        public void run() {
          // get detection interval
          String[] detectionIntervalArgs = cmd.getOptionValue(OPT_DETECTION_INTERVAL, "1-HOURS").split("-");
          TimeGranularity detectionInterval = new TimeGranularity(Integer.valueOf(detectionIntervalArgs[0]),
              TimeUnit.valueOf(detectionIntervalArgs[1]));

          /*
           * Run anomaly detection
           */
          if (cmd.hasOption(OPT_TIME_RANGE)) {
            // run with fixed time range
            String[] timeRangeArgs = cmd.getOptionValues(OPT_TIME_RANGE);
            TimeRange timeRange = new TimeRange(Long.valueOf(timeRangeArgs[0]), Long.valueOf(timeRangeArgs[1]));
            if (cmd.hasOption(OPT_DETECTION_INTERVAL)) {
              runWithOnlineSimulation(config, timeRange, detectionInterval);
            } else {
              runWithExplicitTimeRange(config, timeRange);
            }
          } else {
            // get polling delay
            long pollingMillis = TimeUnit.MINUTES.toMillis(
                Integer.valueOf(cmd.getOptionValue(OPT_POLLING_INTERVAL, "5")));
            try {
              runWithPolling(config, detectionInterval, pollingMillis);
            } catch (Exception e) {
              LOGGER.error("problem polling", e);
            }
          }
        }
      }).start();
    }
  }

  /**
   * Standalone mode where the service polls the third-eye server for data
   * @throws Exception
   */
  private static void runWithPolling(
      final ThirdEyeAnomalyDetectionConfiguration config,
      final TimeGranularity detectionInterval,
      final long pollingMillis) throws Exception {
    long detectionIntervalInMillis = TimeGranularityUtils.toMillis(detectionInterval);

    long latestTimeDataAvailable = ThirdEyeServerUtils.getLatestTime(config.getThirdEyeServerHost(),
        config.getThirdEyeServerPort(), config.getCollectionName());
    long prevTime = TimeGranularityUtils.truncateBy(latestTimeDataAvailable, detectionInterval);

    while (true)
    {
      try {
        latestTimeDataAvailable = ThirdEyeServerUtils.getLatestTime(config.getThirdEyeServerHost(),
            config.getThirdEyeServerPort(), config.getCollectionName());

        long nextTime = prevTime;
        while (nextTime + detectionIntervalInMillis <= latestTimeDataAvailable) {
          nextTime += detectionIntervalInMillis;
        }

        if (nextTime != prevTime) {;
          LOGGER.info("begin processing for {} to {}", prevTime, nextTime);
          final TimeRange taskTimeRange = new TimeRange(prevTime, nextTime);

          new Thread(new Runnable() {
            @Override
            public void run() {
              try {
                new ThirdEyeAnomalyDetection(config, taskTimeRange).call();
              } catch (Exception e) {
                LOGGER.error("uncaught exception", e);
              }
            }
          }).start();

          prevTime = nextTime;
        } else {
          LOGGER.info("no new data available, polling again at {} for {}",
              DateTime.now().plusMillis((int) pollingMillis), new DateTime(prevTime + detectionIntervalInMillis));
        }

      } catch (IOException e) {
        LOGGER.error("error in polling", e);
      } finally {
        Thread.sleep(pollingMillis);
      }
    }

  }

  /**
   * Standalone mode for debugging and quickly testing on historical data.
   */
  private static void runWithExplicitTimeRange(
      final ThirdEyeAnomalyDetectionConfiguration config,
      final TimeRange applicationRunTimeWindow) {
    long startTimeWindow = applicationRunTimeWindow.getStart();
    long endTimeWindow = applicationRunTimeWindow.getEnd();

    LOGGER.info("begin processing for {} to {}", startTimeWindow, endTimeWindow);
    try {
      new ThirdEyeAnomalyDetection(config, new TimeRange(startTimeWindow, endTimeWindow)).call();
    } catch (Exception e) {
      LOGGER.error("uncaught exception", e);
    }
  }

  /**
   * Standalone mode for simulating online detection on an explicit time range.
   */
  private static void runWithOnlineSimulation(
      final ThirdEyeAnomalyDetectionConfiguration config,
      final TimeRange applicationRunTimeWindow,
      final TimeGranularity detectionInterval) {
    long startTimeWindow = applicationRunTimeWindow.getStart();
    long endTimeWindow = applicationRunTimeWindow.getEnd();

    long detectionTimeWindowMillis = TimeGranularityUtils.toMillis(detectionInterval);

    long currTimeWindow = startTimeWindow;
    while (currTimeWindow < endTimeWindow) {
      LOGGER.info("begin processing for {} to {}", startTimeWindow, endTimeWindow);
      try {
        new ThirdEyeAnomalyDetection(config,
            new TimeRange(currTimeWindow, currTimeWindow + detectionTimeWindowMillis)).call();
      } catch (Exception e) {
        LOGGER.error("uncaught exception", e);
      }
      currTimeWindow += detectionTimeWindowMillis;
    }
  }

}
