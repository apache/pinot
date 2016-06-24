package com.linkedin.thirdeye.detector.driver;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.hibernate.SessionFactory;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.lib.util.JobUtils;

public class AnomalyDetectionJobManager {
  public static final String MONITORING_WINDOW_KEY = "monitoringWindow";
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionJobManager.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

  private final Scheduler quartzScheduler;
  private final TimeSeriesHandler timeSeriesHandler;
  private final TimeSeriesResponseConverter timeSeriesResponseConverter;
  private final AnomalyFunctionSpecDAO specDAO;
  private final AnomalyFunctionRelationDAO relationDAO;
  private final AnomalyResultDAO resultDAO;
  private final SessionFactory sessionFactory;
  private final Object sync;
  private final Map<Long, String> scheduledJobKeys;
  private final MetricRegistry metricRegistry;
  private final AnomalyFunctionFactory anomalyFunctionFactory;
  private final FailureEmailConfiguration failureEmailConfig;
  private final QueryCache queryCache;

  private static final ObjectMapper reader = new ObjectMapper(new YAMLFactory());

  public AnomalyDetectionJobManager(Scheduler quartzScheduler, AnomalyFunctionSpecDAO specDAO,
      AnomalyFunctionRelationDAO relationDAO, AnomalyResultDAO resultDAO,
      SessionFactory sessionFactory, MetricRegistry metricRegistry,
      AnomalyFunctionFactory anomalyFunctionFactory, FailureEmailConfiguration failureEmailConfig) {

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

    timeSeriesHandler = new TimeSeriesHandler(queryCache);
    timeSeriesResponseConverter = TimeSeriesResponseConverter.getInstance();

    this.quartzScheduler = quartzScheduler;
    this.specDAO = specDAO;
    this.relationDAO = relationDAO;
    this.resultDAO = resultDAO;
    this.sessionFactory = sessionFactory;
    this.metricRegistry = metricRegistry;
    this.sync = new Object();
    this.scheduledJobKeys = new HashMap<>();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.failureEmailConfig = failureEmailConfig;
  }

  public List<Long> getActiveJobs() {
    synchronized (sync) {
      List<Long> jobs = new ArrayList<>(scheduledJobKeys.keySet());
      Collections.sort(jobs);
      return jobs;
    }
  }

  public void runAdHoc(Long id, String windowStartIsoString, String windowEndIsoString)
      throws Exception {
    synchronized (sync) {
      AnomalyFunctionSpec spec = specDAO.findById(id);
      if (spec == null) {
        throw new IllegalArgumentException("No function with id " + id);
      }
      AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

      String triggerKey = String.format("ad_hoc_anomaly_function_trigger_%d_%s-%s", spec.getId(),
          windowStartIsoString, windowEndIsoString);
      Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

      String jobKey = String.format("ad_hoc_anomaly_function_job_%d_%s-%s", spec.getId(),
          windowStartIsoString, windowEndIsoString);
      buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec, windowStartIsoString,
          windowEndIsoString);
    }
  }

  /**
   * @param jobKey
   * @param trigger
   * @param anomalyFunction
   * @param spec
   * @param windowStartIsoString
   * @param windowEndIsoString
   * @throws SchedulerException
   */
  private void buildAndScheduleJob(String jobKey, Trigger trigger, AnomalyFunction anomalyFunction,
      AnomalyFunctionSpec spec, String windowStartIsoString, String windowEndIsoString)
      throws SchedulerException {
    JobDetail job = JobBuilder.newJob(AnomalyDetectionJob.class).withIdentity(jobKey).build();

    job.getJobDataMap().put(AnomalyDetectionJob.FUNCTION, anomalyFunction);
    // job.getJobDataMap().put(AnomalyDetectionJob.CLIENT, thirdEyeClient);
    job.getJobDataMap().put(AnomalyDetectionJob.TIME_SERIES_HANDLER, timeSeriesHandler);
    job.getJobDataMap().put(AnomalyDetectionJob.TIME_SERIES_RESPONSE_CONVERTER,
        timeSeriesResponseConverter);
    job.getJobDataMap().put(AnomalyDetectionJob.WINDOW_START, windowStartIsoString);
    job.getJobDataMap().put(AnomalyDetectionJob.WINDOW_END, windowEndIsoString);
    job.getJobDataMap().put(AnomalyDetectionJob.RESULT_DAO, resultDAO);
    job.getJobDataMap().put(AnomalyDetectionJob.SESSION_FACTORY, sessionFactory);
    job.getJobDataMap().put(AnomalyDetectionJob.METRIC_REGISTRY, metricRegistry);
    job.getJobDataMap().put(AnomalyDetectionJob.RELATION_DAO, relationDAO);

    job.getJobDataMap().put(FailureEmailConfiguration.FAILURE_EMAIL_CONFIG_KEY, failureEmailConfig);

    quartzScheduler.scheduleJob(job, trigger);

    LOG.info(
        "buildAndScheduleJob anomalyFunction: {}, windowStartIsoString: {}, windowEndIsoString: {}",
        anomalyFunction, windowStartIsoString, windowEndIsoString);

    LOG.info("Started {}: {}", jobKey, spec);
  }

  public void start(Long id) throws Exception {
    synchronized (sync) {
      AnomalyFunctionSpec spec = specDAO.findById(id);
      if (spec == null) {
        throw new IllegalArgumentException("No function with id " + id);
      }
      AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

      String triggerKey = String.format("scheduled_anomaly_function_trigger_%d", spec.getId());
      CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
          .withSchedule(CronScheduleBuilder.cronSchedule(spec.getCron())).build();

      String jobKey = String.format("scheduled_anomaly_function_job_%d", spec.getId());
      scheduledJobKeys.put(id, jobKey);

      buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec, null, null); // use schedule time
                                                                               // to determine
                                                                               // start/end
    }
  }

  public void stop(Long id) throws Exception {
    synchronized (sync) {
      String jobKey = scheduledJobKeys.remove(id);
      if (jobKey == null) {
        throw new IllegalArgumentException("No scheduled job for function id " + id);
      }

      quartzScheduler.deleteJob(JobKey.jobKey(jobKey));

      LOG.info("Stopped {}", jobKey);
    }
  }

  /**
   * Available for testing, but anomalies need to be created with a valid anomaly function ID
   * (foreign key constraint).
   */
  public void runAdhocFile(String filePath, int existingFunctionId, String windowStartIsoString,
      String windowEndIsoString) throws Exception {
    synchronized (sync) {
      File file = new File(filePath);
      if (!file.exists() || file.isDirectory()) {
        throw new IllegalArgumentException("File does not exist or is a directory: " + file);
      }
      AnomalyFunctionSpec spec = reader.readValue(file, AnomalyFunctionSpec.class);
      spec.setId(existingFunctionId);
      runAdhocConfig(spec, windowStartIsoString, windowEndIsoString, filePath);
    }
  }

  public void runAdhocConfig(AnomalyFunctionSpec spec, String windowStartIsoString,
      String windowEndIsoString, String executionName) throws Exception, SchedulerException {
    AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

    String triggerKey = String.format("file-based_anomaly_function_trigger_%s", executionName);
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    String jobKey = String.format("file-based_anomaly_function_job_%s", executionName);
    buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec, windowStartIsoString,
        windowEndIsoString);
  }

  public void simulatePeriod(Long id, String startIsoString, String endIsoString) throws Exception {
    // TODO error checks
    AnomalyFunctionSpec spec = specDAO.findById(id);
    if (spec == null) {
      throw new IllegalArgumentException("No function with id " + id);
    }
    AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startIsoString);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endIsoString);

    long dataWindowMillis =
        new TimeGranularity(spec.getWindowSize(), spec.getWindowUnit()).toMillis();
    long monitorWindowMillis = getMonitoringWindowGranularity(spec).toMillis();
    int numBuckets = (int) Math
        .ceil((double) (endTime.getMillis() - startTime.getMillis()) / monitorWindowMillis);
    LOG.info("Found {} windows for function {} from {} to {}, with monitoring window of {} ms",
        numBuckets, id, startTime, endTime, monitorWindowMillis);
    DateTime currentMonitoringWindowStart;
    DateTime currentMonitoringWindowEnd = startTime.plus(monitorWindowMillis);

    String functionIDString = String.format("%s(%d)", spec.getFunctionName(), id);
    while (currentMonitoringWindowEnd.isBefore(endTime)
        || currentMonitoringWindowEnd.isEqual(endTime)) {
      currentMonitoringWindowStart = currentMonitoringWindowEnd.minus(dataWindowMillis);
      LOG.info("Running {} with monitoring window: {} to {}", id, currentMonitoringWindowStart,
          currentMonitoringWindowEnd);
      try {
        String triggerKey = String.format("simulate_period_anomaly_function_trigger_%d_%s-%s", id,
            currentMonitoringWindowStart, currentMonitoringWindowEnd);
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

        String jobKey = String.format("simulate_period_anomaly_function_job_%d_%s-%s", id,
            currentMonitoringWindowStart, currentMonitoringWindowEnd);
        buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec,
            currentMonitoringWindowStart.toString(), currentMonitoringWindowEnd.toString());
      } catch (Exception e) {
        LOG.error("Error encountered for {}, {}, {}:\n {}", functionIDString,
            currentMonitoringWindowStart, currentMonitoringWindowEnd, e);
      }
      currentMonitoringWindowEnd = currentMonitoringWindowEnd.plus(monitorWindowMillis);
    }
    if (!currentMonitoringWindowEnd.equals(endTime)) {
      LOG.error("Time windows did not line up with monitoring window!: {}, {}",
          currentMonitoringWindowEnd, endTime);
    }
  }

  private TimeGranularity getMonitoringWindowGranularity(AnomalyFunctionSpec spec) {
    // TODO fully implement the monitoring window property within AFS
    Properties props = JobUtils.decodeCompactedProperties(spec.getProperties());
    // if specified in props, adjust start as needed.
    int monitoringWindowSize = Integer.parseInt(props.getProperty(MONITORING_WINDOW_KEY, "-1"));
    if (monitoringWindowSize > 0) {
      return new TimeGranularity(monitoringWindowSize * spec.getBucketSize(), spec.getBucketUnit());
    } else {
      // otherwise use full window
      return new TimeGranularity(spec.getWindowSize(), spec.getWindowUnit());
    }
  }

}
