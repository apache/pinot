package com.linkedin.thirdeye.anomaly;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionFactory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionHistory;
import com.linkedin.thirdeye.anomaly.api.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.api.HandlerProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.task.LocalDriverAnomalyDetectionTask;
import com.linkedin.thirdeye.anomaly.api.task.AnomalyDetectionTaskInfo;
import com.linkedin.thirdeye.anomaly.database.FunctionTable;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.generic.GenericFunctionFactory;
import com.linkedin.thirdeye.anomaly.handler.AnomalyResultHandlerDatabase;
import com.linkedin.thirdeye.anomaly.rulebased.RuleBasedFunctionFactory;
import com.linkedin.thirdeye.anomaly.util.ServerUtils;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.DefaultThirdEyeClientConfig;
import com.linkedin.thirdeye.client.FlowControlledDefaultThirdeyeClient;
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

    ThirdEyeClient thirdEyeClient = new FlowControlledDefaultThirdeyeClient(config.getThirdEyeServerHost(),
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

    List<LocalDriverAnomalyDetectionTask> tasks = buildTasks(thirdEyeClient, timeRange, functionFactory);

    ExecutorService taskExecutors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    for (LocalDriverAnomalyDetectionTask task : tasks) {
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
  public List<LocalDriverAnomalyDetectionTask> buildTasks(ThirdEyeClient thirdEyeClient, TimeRange timeRange,
      AnomalyDetectionFunctionFactory functionFactory) throws Exception {

    List<LocalDriverAnomalyDetectionTask> tasks = new LinkedList<LocalDriverAnomalyDetectionTask>();

    AnomalyDetectionDriverConfig driverConfig = config.getDriverConfig();

    List<? extends FunctionTableRow> rows = FunctionTable.selectRows(config.getAnomalyDatabaseConfig(),
        functionFactory.getFunctionRowClass(), config.getCollectionName());

    // load star tree
    StarTreeConfig starTreeConfig = ServerUtils.getStarTreeConfig(config.getThirdEyeServerHost(),
        config.getThirdEyeServerPort(), config.getCollectionName());

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
        AnomalyDetectionFunctionHistory functionHistory = new AnomalyDetectionFunctionHistory(starTreeConfig,
            config.getAnomalyDatabaseConfig(), functionTableRow.getFunctionId());

        // make the task
        LocalDriverAnomalyDetectionTask task = new LocalDriverAnomalyDetectionTask(starTreeConfig, driverConfig, taskInfo,
            function, resultHandler, functionHistory, thirdEyeClient);

        tasks.add(task);
      } catch (Exception e) {
        LOGGER.error("could not create function for function_id={}", functionTableRow.getFunctionId(), e);
      }
    }

    return tasks;
  }

  /**
   * Main method for when running as a standalone process.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // help information
    if (args.length != 1 || args[0].equalsIgnoreCase("--help")) {
      System.out.println("usage: java -jar <this_jar> <your_config.yml>");
      System.out.println("usage (for setup walkthrough): java -jar <this_jar> --setup");
      return;
    }

    // help the user configure anomaly detection
    if (args[0].equals("--setup")) {
      new ThirdEyeAnomalyDetectionSetup().setup();
      return;
    }

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    ThirdEyeAnomalyDetectionConfiguration config = mapper.readValue(new File(args[0]),
        ThirdEyeAnomalyDetectionConfiguration.class);
    if (config.getExplicitTimeRange() != null) {
      runWithExplicitTimeRange(config);
    } else {
      runWithPolling(config);
    }
  }

  /** the maximum rate at which to poll */
  private static long MILLIS_BEWEEEN_POLLS = TimeUnit.MINUTES.toMillis(5);

  /**
   * Standalone mode where the service polls the third-eye server for data
   * @throws Exception
   */
  private static void runWithPolling(final ThirdEyeAnomalyDetectionConfiguration config) throws Exception {
    long detectionIntervalInMillis = TimeGranularityUtils.toMillis(config.getDetectionInterval());

    long latestTimeDataAvailable = ServerUtils.getLatestTime(config.getThirdEyeServerHost(),
        config.getThirdEyeServerPort(), config.getCollectionName());
    long prevTime = TimeGranularityUtils.truncateBy(latestTimeDataAvailable, config.getDetectionInterval());

    while (true)
    {
      latestTimeDataAvailable = ServerUtils.getLatestTime(config.getThirdEyeServerHost(),
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
            DateTime.now().plusMillis((int) MILLIS_BEWEEEN_POLLS), new DateTime(prevTime + detectionIntervalInMillis));
      }

      Thread.sleep(MILLIS_BEWEEEN_POLLS);
    }

  }

  /**
   * Standalone mode for debugging and quickly testing on historical data.
   */
  public static void runWithExplicitTimeRange(final ThirdEyeAnomalyDetectionConfiguration config) {
    TimeRange applicationRunTimeWindow = config.getExplicitTimeRange();

    long startTimeWindow = applicationRunTimeWindow.getStart();
    startTimeWindow = TimeGranularityUtils.truncateBy(startTimeWindow, config.getDetectionInterval());
    long endTimeWindow = applicationRunTimeWindow.getEnd();

    LOGGER.info("begin processing for {} to {}", startTimeWindow, endTimeWindow);

    try {
      new ThirdEyeAnomalyDetection(config, new TimeRange(startTimeWindow, endTimeWindow)).call();
    } catch (Exception e) {
      LOGGER.error("uncaught exception", e);
    }
  }


}
