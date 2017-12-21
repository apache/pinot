package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.dashboard.resources.DetectionJobResource;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This task takes the output of replay result and tune the alert filter for the given detected anomalies
 */
public class AlertFilterAutoTuneOnboardingTask extends BaseDetectionOnboardTask {
  private static final Logger LOG = LoggerFactory.getLogger(AlertFilterAutoTuneOnboardingTask.class);

  public static final String TASK_NAME = "AlertFilterAutotune";

  public static final String ALERT_FILTER_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY;
  public static final String ALERT_FILTER_AUTOTUNE_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY;
  public static final String ANOMALY_FUNCTION = DefaultDetectionOnboardJob.ANOMALY_FUNCTION;
  public static final String BACKFILL_PERIOD = DefaultDetectionOnboardJob.PERIOD;
  public static final String BACKFILL_START = DefaultDetectionOnboardJob.START;
  public static final String BACKFILL_END = DefaultDetectionOnboardJob.END;
  public static final String AUTOTUNE_PATTERN = DefaultDetectionOnboardJob.AUTOTUNE_PATTERN;
  public static final String AUTOTUNE_SENSITIVITY_LEVEL = DefaultDetectionOnboardJob.AUTOTUNE_SENSITIVITY_LEVEL;

  public static final String DEFAULT_AUTOTUNE_PATTERN = "Up,Down";
  public static final String DEFAULT_AUTOTUNE_SENSITIVITY_LEVEL = "MEDIUM";

  public static final String DEFAULT_BACKFILL_PERIOD = FunctionReplayOnboardingTask.DEFAULT_BACKFILL_PERIOD;

  public AlertFilterAutoTuneOnboardingTask() {
    super(TASK_NAME);
  }

  /**
   * Executes the task. To fail this task, throw exceptions. The job executor will catch the exception and store
   * it in the message in the execution status of this task.
   */
  @Override
  public void run() {
    Configuration taskConfiguration = taskContext.getConfiguration();
    DetectionOnboardExecutionContext executionContext = taskContext.getExecutionContext();

    Preconditions.checkNotNull(executionContext.getExecutionResult(ALERT_FILTER_FACTORY));
    Preconditions.checkNotNull(executionContext.getExecutionResult(ALERT_FILTER_AUTOTUNE_FACTORY));

    AlertFilterFactory alertFilterFactory = (AlertFilterFactory) executionContext.getExecutionResult(ALERT_FILTER_FACTORY);
    AlertFilterAutotuneFactory alertFilterAutotuneFactory = (AlertFilterAutotuneFactory)
        executionContext.getExecutionResult(ALERT_FILTER_AUTOTUNE_FACTORY);

    Preconditions.checkNotNull(alertFilterFactory);
    Preconditions.checkNotNull(alertFilterAutotuneFactory);

    DetectionJobResource detectionJobResource = new DetectionJobResource(new DetectionJobScheduler(),
        alertFilterFactory, alertFilterAutotuneFactory);
    MergedAnomalyResultManager mergedAnomalyResultDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

    AnomalyFunctionDTO anomalyFunctionSpec = (AnomalyFunctionDTO) executionContext.getExecutionResult(ANOMALY_FUNCTION);
    long functionId = anomalyFunctionSpec.getId();
    Period backfillPeriod = Period.parse(taskConfiguration.getString(BACKFILL_PERIOD, DEFAULT_BACKFILL_PERIOD));
    DateTime start = DateTime.parse(taskConfiguration.getString(BACKFILL_START, DateTime.now().toString()));
    DateTime end = DateTime.parse(taskConfiguration.getString(BACKFILL_END, DateTime.now().minus(backfillPeriod).toString()));

    int numReplayedAnomalies = mergedAnomalyResultDAO
        .findByStartTimeInRangeAndFunctionId(start.getMillis(), end.getMillis(), functionId, false)
        .size();

    Response initialAutotuneResponse = detectionJobResource.
        initiateAlertFilterAutoTune(functionId, start.toString(), end.toString(), "AUTOTUNE",
            taskConfiguration.getString(AUTOTUNE_PATTERN, DEFAULT_AUTOTUNE_PATTERN),
            taskConfiguration.getString(AUTOTUNE_SENSITIVITY_LEVEL, DEFAULT_AUTOTUNE_SENSITIVITY_LEVEL),
            "", "");

    if (initialAutotuneResponse.getEntity() != null) {
      detectionJobResource.updateAlertFilterToFunctionSpecByAutoTuneId(
          Long.valueOf(initialAutotuneResponse.getEntity().toString()));
      LOG.info("Initial alert filter applied");
    } else {
      LOG.info("AutoTune doesn't applied");
    }
  }
}
