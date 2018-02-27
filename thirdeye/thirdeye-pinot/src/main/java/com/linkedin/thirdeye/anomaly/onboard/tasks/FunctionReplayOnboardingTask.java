package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.dashboard.resources.DetectionJobResource;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This task performs replay on anomaly functions
 */
public class FunctionReplayOnboardingTask extends BaseDetectionOnboardTask {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionReplayOnboardingTask.class);
  public static final String TASK_NAME = "FunctionReplay";

  public static final String ANOMALY_FUNCTION_CONFIG = DefaultDetectionOnboardJob.ANOMALY_FUNCTION_CONFIG;
  public static final String ALERT_FILTER_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY;
  public static final String ALERT_FILTER_AUTOTUNE_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY;
  public static final String BACKFILL_PERIOD = DefaultDetectionOnboardJob.PERIOD;
  public static final String BACKFILL_START = DefaultDetectionOnboardJob.START;
  public static final String BACKFILL_END = DefaultDetectionOnboardJob.END;
  public static final String BACKFILL_FORCE = DefaultDetectionOnboardJob.FORCE;
  public static final String BACKFILL_SPEEDUP = DefaultDetectionOnboardJob.SPEEDUP;
  public static final String BACKFILL_REMOVE_ANOMALY_IN_WINDOW = DefaultDetectionOnboardJob.REMOVE_ANOMALY_IN_WINDOW;

  public static final String DEFAULT_BACKFILL_PERIOD = "P30D";
  public static final Boolean DEFAULT_BACKFILL_FORCE = true;
  public static final Boolean DEFAULT_BACKFILL_SPEEDUP = false;
  public static final Boolean DEFAULT_BACKFILL_REMOVE_ANOMALY_IN_WINDOW = false;

  private DetectionJobScheduler detectionJobScheduler;

  public FunctionReplayOnboardingTask() {
    super(TASK_NAME);
  }

  /**
   * Executes the task. To fail this task, throw exceptions. The job executor will catch the exception and store
   * it in the message in the execution status of this task.
   */
  @Override
  public void run() {

    try {
      Response response = initDetectionJob();
      Map<Long, Long> functionIdToJobIdMap = (Map<Long, Long>) response.getEntity();
      for (long jobId : functionIdToJobIdMap.values()) {
        JobStatus jobStatus = detectionJobScheduler.waitForJobDone(jobId);
        if (JobStatus.FAILED.equals(jobStatus) || JobStatus.TIMEOUT.equals(jobStatus)) {
          throw new InterruptedException("Get Job Status: " + jobStatus);
        }
      }

    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public Response initDetectionJob() throws Exception{
    Configuration taskConfiguration = taskContext.getConfiguration();
    DetectionOnboardExecutionContext executionContext = taskContext.getExecutionContext();

    Preconditions.checkNotNull(executionContext.getExecutionResult(ALERT_FILTER_FACTORY));
    Preconditions.checkNotNull(executionContext.getExecutionResult(ALERT_FILTER_AUTOTUNE_FACTORY));

    AlertFilterFactory alertFilterFactory = (AlertFilterFactory) executionContext.getExecutionResult(ALERT_FILTER_FACTORY);
    AlertFilterAutotuneFactory alertFilterAutotuneFactory = (AlertFilterAutotuneFactory)
        executionContext.getExecutionResult(ALERT_FILTER_AUTOTUNE_FACTORY);

    Preconditions.checkNotNull(alertFilterFactory);
    Preconditions.checkNotNull(alertFilterAutotuneFactory);

    detectionJobScheduler = new DetectionJobScheduler();
    DetectionJobResource detectionJobResource = new DetectionJobResource(detectionJobScheduler,
        alertFilterFactory, alertFilterAutotuneFactory);
    AnomalyFunctionDTO anomalyFunction = (AnomalyFunctionDTO) executionContext.getExecutionResult(ANOMALY_FUNCTION_CONFIG);
    long functionId = anomalyFunction.getId();
    Period backfillPeriod = Period.parse(taskConfiguration.getString(BACKFILL_PERIOD, DEFAULT_BACKFILL_PERIOD));
    DateTime start = DateTime.parse(taskConfiguration.getString(BACKFILL_START, DateTime.now().minus(backfillPeriod).toString()));
    DateTime end = DateTime.parse(taskConfiguration.getString(BACKFILL_END, DateTime.now().toString()));
    executionContext.setExecutionResult(BACKFILL_START, start);
    executionContext.setExecutionResult(BACKFILL_END, end);

    Response response = null;
    try {
      LOG.info("Running replay task for {} from {} to {}", anomalyFunction, start, end);
      response = detectionJobResource.generateAnomaliesInRange(functionId, start.toString(), end.toString(),
          Boolean.toString(taskConfiguration.getBoolean(BACKFILL_FORCE, DEFAULT_BACKFILL_FORCE)),
          taskConfiguration.getBoolean(BACKFILL_SPEEDUP, DEFAULT_BACKFILL_SPEEDUP),
          taskConfiguration.getBoolean(BACKFILL_REMOVE_ANOMALY_IN_WINDOW, DEFAULT_BACKFILL_REMOVE_ANOMALY_IN_WINDOW));
    } catch (Exception e){
      throw new IllegalStateException(String.format("Unable to create detection job for %d from %s to %s\n%s",
          functionId, start.toString(), end.toString(), ExceptionUtils.getStackTrace(e)));
    }
    return response;
  }
}
