package com.linkedin.thirdeye.anomaly;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionFactory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionTask;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionTaskBuilder;
import com.linkedin.thirdeye.anomaly.generic.GenericFunctionFactory;
import com.linkedin.thirdeye.anomaly.rulebased.RuleBasedFunctionFactory;
import com.linkedin.thirdeye.anomaly.util.ThirdEyeMultiClient;
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.TimeRange;


/**
 * This class may be called by the standalone modes of operation or an external scheduling service.
 */
public class ThirdEyeAnomalyDetection implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeAnomalyDetection.class);

  private final ThirdEyeAnomalyDetectionConfiguration config;
  private final TimeRange timeRange;

  public ThirdEyeAnomalyDetection(ThirdEyeAnomalyDetectionConfiguration config, TimeRange timeRange) {
    this.config = config;
    this.timeRange = timeRange;
  }

  public Void call() throws Exception
  {
    List<AnomalyDetectionDriverConfig> collectionDriverConfigurations = config.getCollectionDriverConfigurations();
    AnomalyDatabaseConfig anomalyDatabase = config.getAnomalyDatabaseConfig();
    ThirdEyeMultiClient thirdEyeMultiClient = new ThirdEyeMultiClient(collectionDriverConfigurations);

    AnomalyDetectionFunctionFactory functionFactory;
    switch (config.getMode()) {
      case RuleBased:
      {
        functionFactory = new RuleBasedFunctionFactory();
        break;
      }
      case Generic:
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

    List<AnomalyDetectionTask> tasks = new AnomalyDetectionTaskBuilder(collectionDriverConfigurations, anomalyDatabase,
        thirdEyeMultiClient).buildTasks(timeRange, functionFactory);

    ExecutorService taskExecutors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    for (AnomalyDetectionTask task : tasks) {
      taskExecutors.execute(task);
    }

    taskExecutors.shutdown();
    taskExecutors.awaitTermination(config.getMaxWaitToCompletion().getSize(),
        config.getMaxWaitToCompletion().getUnit());

    // close all the clients
    thirdEyeMultiClient.close();

    return null;
  }

  /**
   * Main method for when running as a standalone process.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    ThirdEyeAnomalyDetectionConfiguration config = mapper.readValue(new File(args[0]),
        ThirdEyeAnomalyDetectionConfiguration.class);
    if (config.getExplicitTimeRange() != null) {
      runWithExplicitTimeRange(config);
    } else {
      runWithScheduler(config);
    }
  }

  /**
   * Standalone mode where anomaly detection is run at scheduled intervals
   */
  private static void runWithScheduler(final ThirdEyeAnomalyDetectionConfiguration config) {
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    final int detectionIntervalInMillis = (int) TimeGranularityUtils.toMillis(config.getDetectionInterval());
    final int detectionLagInMillis = (int) TimeGranularityUtils.toMillis(config.getDetectionLag());

    long appStartTime = DateTime.now(DateTimeZone.UTC).getMillis();
    long firstExecution = TimeGranularityUtils.truncateBy(DateTime.now(DateTimeZone.UTC).getMillis(),
        config.getDetectionInterval()) + detectionLagInMillis;
    while (firstExecution - detectionIntervalInMillis > appStartTime) {
      firstExecution -= detectionIntervalInMillis;
    }

    long initialDelay = firstExecution - DateTime.now(DateTimeZone.UTC).getMillis();

    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        long currentTime = DateTime.now(DateTimeZone.UTC).getMillis();
        currentTime -= detectionLagInMillis;

        currentTime = TimeGranularityUtils.truncateBy(currentTime, config.getDetectionInterval());

        /*
         * Compute the start and end times for the current batch
         */
        long taskEndTime = currentTime;
        long taskStartTime = currentTime - detectionIntervalInMillis;

        LOGGER.info("begin processing for {} to {}", taskStartTime, taskEndTime);

        try {
          new ThirdEyeAnomalyDetection(config, new TimeRange(taskStartTime, taskEndTime)).call();
        } catch (Exception e) {
          LOGGER.error("uncaught exception", e);
        }
      }

    }, initialDelay, detectionIntervalInMillis, TimeUnit.MILLISECONDS);

    LOGGER.info("scheduling tasks to run every {}", config.getDetectionInterval());
    LOGGER.info("first execution at {}", new DateTime(firstExecution, DateTimeZone.UTC));
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
