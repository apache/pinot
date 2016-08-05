package com.linkedin.thirdeye.detector.driver;

import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class AnomalyDetectionJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionJobManager.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final Scheduler quartzScheduler;
  private final TimeSeriesHandler timeSeriesHandler;
  private final TimeSeriesResponseConverter timeSeriesResponseConverter;
  private final AnomalyFunctionDAO specDAO;
  private final AnomalyFunctionRelationDAO relationDAO;
  private final AnomalyResultDAO resultDAO;
  private final Object sync;
  private final Map<Long, String> scheduledJobKeys;
  private final MetricRegistry metricRegistry;
  private final AnomalyFunctionFactory anomalyFunctionFactory;
  private final FailureEmailConfiguration failureEmailConfig;
  private QueryCache queryCache;

  private static final ObjectMapper reader = new ObjectMapper(new YAMLFactory());

  public AnomalyDetectionJobManager(Scheduler quartzScheduler, AnomalyFunctionDAO specDAO,
      AnomalyFunctionRelationDAO relationDAO, AnomalyResultDAO resultDAO,
      MetricRegistry metricRegistry, AnomalyFunctionFactory anomalyFunctionFactory,
      FailureEmailConfiguration failureEmailConfig) {

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

    timeSeriesHandler = new TimeSeriesHandler(queryCache);
    timeSeriesResponseConverter = TimeSeriesResponseConverter.getInstance();

    this.quartzScheduler = quartzScheduler;
    this.specDAO = specDAO;
    this.relationDAO = relationDAO;
    this.resultDAO = resultDAO;
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

      String triggerKey = String.format("ad_hoc_anomaly_function_trigger_%d", spec.getId());
      Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

      String jobKey = String.format("ad_hoc_anomaly_function_job_%d", spec.getId());
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
    job.getJobDataMap().put(AnomalyDetectionJob.TIME_SERIES_HANDLER, timeSeriesHandler);
    job.getJobDataMap().put(AnomalyDetectionJob.TIME_SERIES_RESPONSE_CONVERTER,
        timeSeriesResponseConverter);
    job.getJobDataMap().put(AnomalyDetectionJob.WINDOW_START, windowStartIsoString);
    job.getJobDataMap().put(AnomalyDetectionJob.WINDOW_END, windowEndIsoString);
    job.getJobDataMap().put(AnomalyDetectionJob.RESULT_DAO, resultDAO);
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
      CronTrigger trigger =
          TriggerBuilder.newTrigger().withIdentity(triggerKey)
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
  public void runAdhocFile(String filePath, long existingFunctionId, String windowStartIsoString,
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
      String windowEndIsoString, String executionName) throws Exception {
    AnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(spec);

    String triggerKey = String.format("file-based_anomaly_function_trigger_%s", executionName);
    Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startNow().build();

    String jobKey = String.format("file-based_anomaly_function_job_%s", executionName);
    buildAndScheduleJob(jobKey, trigger, anomalyFunction, spec, windowStartIsoString,
        windowEndIsoString);
  }

}
