package com.linkedin.thirdeye.anomaly;

import java.io.File;
import java.util.List;
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
import com.linkedin.thirdeye.anomaly.util.TimeGranularityUtils;
import com.linkedin.thirdeye.api.TimeRange;


/**
 *
 */
public class ThirdEyeAnomalyDetection {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeAnomalyDetection.class);

  private final ExecutorService taskExecutors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  private final ThirdEyeAnomalyDetectionConfiguration config;

  public ThirdEyeAnomalyDetection(String filename) throws Exception {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    config = mapper.readValue(new File(filename), ThirdEyeAnomalyDetectionConfiguration.class);
  }

  /**
   * Run the application
   *
   * @throws Exception
   */
  public void run() throws Exception {
    if (config.getExplicitTimeRange() != null) {
      runWithExplicitTimeRange();
    } else {
      runWithScheduler();
    }
  }

  /**
   * Schedule at fixed intervals.
   */
  private void runWithScheduler() {
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

        loadAndRunTasks(config, new TimeRange(taskStartTime, taskEndTime));
      }

    }, initialDelay, detectionIntervalInMillis, TimeUnit.MILLISECONDS);

    LOGGER.info("scheduling tasks to run every {}", config.getDetectionInterval());
    LOGGER.info("first execution at {}", new DateTime(firstExecution, DateTimeZone.UTC));
  }

  /**
   * For debugging and quickly testing on historical data.
   * @throws InterruptedException
   */
  public void runWithExplicitTimeRange() throws InterruptedException {
    TimeRange applicationRunTimeWindow = config.getExplicitTimeRange();

    long startTimeWindow = applicationRunTimeWindow.getStart();
    startTimeWindow = TimeGranularityUtils.truncateBy(startTimeWindow, config.getDetectionInterval());
    long endTimeWindow = applicationRunTimeWindow.getEnd();

    LOGGER.info("begin processing for {} to {}", startTimeWindow, endTimeWindow);

    loadAndRunTasks(config, new TimeRange(startTimeWindow, endTimeWindow));

    taskExecutors.shutdown();
    taskExecutors.awaitTermination(1, TimeUnit.HOURS);
  }

  /**
   * Load and run anomaly detection tasks depending on the mode that is being run.
   */
  private void loadAndRunTasks(ThirdEyeAnomalyDetectionConfiguration thirdEyeAnomalyDetectionConfig,
      TimeRange timeRange) {

    List<AnomalyDetectionDriverConfig> collectionDriverConfigurations =
        thirdEyeAnomalyDetectionConfig.getCollectionDriverConfigurations();
    AnomalyDatabaseConfig anomalyDatabase = thirdEyeAnomalyDetectionConfig.getAnomalyDatabaseConfig();

    AnomalyDetectionFunctionFactory functionFactory;
    switch (thirdEyeAnomalyDetectionConfig.getMode()) {
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

    List<AnomalyDetectionTask> tasks = null;
    try {
      tasks = new AnomalyDetectionTaskBuilder(collectionDriverConfigurations, anomalyDatabase)
        .buildTasks(timeRange, functionFactory);
    } catch (Exception e) {
      LOGGER.error("failed to load tasks", e);
      System.exit(-1);
    }

    for (AnomalyDetectionTask task : tasks) {
      taskExecutors.execute(task);
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    new ThirdEyeAnomalyDetection(args[0]).run();
  }

}
