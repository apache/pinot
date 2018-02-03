package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.dashboard.resources.DetectionJobResource;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import java.io.IOException;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.dataframe.StringSeries.*;


/**
 * This task takes the output of replay result and tune the alert filter for the given detected anomalies
 */
public class AlertFilterAutoTuneOnboardingTask extends BaseDetectionOnboardTask {
  private static final Logger LOG = LoggerFactory.getLogger(AlertFilterAutoTuneOnboardingTask.class);
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String TASK_NAME = "AlertFilterAutotune";

  public static final String ALERT_FILTER_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY;
  public static final String ALERT_FILTER_AUTOTUNE_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY;
  public static final String ANOMALY_FUNCTION_CONFIG = DefaultDetectionOnboardJob.ANOMALY_FUNCTION_CONFIG;
  public static final String BACKFILL_PERIOD = DefaultDetectionOnboardJob.PERIOD;
  public static final String BACKFILL_START = DefaultDetectionOnboardJob.START;
  public static final String BACKFILL_END = DefaultDetectionOnboardJob.END;
  public static final String AUTOTUNE_PATTERN = DefaultDetectionOnboardJob.AUTOTUNE_PATTERN;
  public static final String AUTOTUNE_TYPE = DefaultDetectionOnboardJob.AUTOTUNE_TYPE;
  public static final String AUTOTUNE_PATTERN_ONLY = DefaultDetectionOnboardJob.AUTOTUNE_PATTERN_ONLY;
  public static final String AUTOTUNE_FEATURES = DefaultDetectionOnboardJob.AUTOTUNE_FEATURES;
  public static final String AUTOTUNE_MTTD = DefaultDetectionOnboardJob.AUTOTUNE_MTTD;
  public static final String HOLIDAY_STARTS = DefaultDetectionOnboardJob.HOLIDAY_STARTS;
  public static final String HOLIDAY_ENDS = DefaultDetectionOnboardJob.HOLIDAY_ENDS;

  public static final String DEFAULT_AUTOTUNE_PATTERN = "UP,DOWN";
  public static final String DEFAULT_AUTOTUNE_TYPE = "AUTOTUNE";

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

    AlertFilterFactory alertFilterFactory =
        (AlertFilterFactory) executionContext.getExecutionResult(ALERT_FILTER_FACTORY);
    AlertFilterAutotuneFactory alertFilterAutotuneFactory =
        (AlertFilterAutotuneFactory) executionContext.getExecutionResult(ALERT_FILTER_AUTOTUNE_FACTORY);

    Preconditions.checkNotNull(alertFilterFactory);
    Preconditions.checkNotNull(alertFilterAutotuneFactory);

    DetectionJobResource detectionJobResource =
        new DetectionJobResource(new DetectionJobScheduler(), alertFilterFactory, alertFilterAutotuneFactory);

    AnomalyFunctionDTO anomalyFunctionSpec =
        (AnomalyFunctionDTO) executionContext.getExecutionResult(ANOMALY_FUNCTION_CONFIG);
    long functionId = anomalyFunctionSpec.getId();
    DateTime start = ((DateTime) executionContext.getExecutionResult(BACKFILL_START)).minusDays(1);
    DateTime end = ((DateTime) executionContext.getExecutionResult(BACKFILL_END)).plusDays(1);

    Response autotuneResponse = detectionJobResource.
        tuneAlertFilter(Long.toString(functionId), start.toString(), end.toString(),
            taskConfiguration.getString(AUTOTUNE_TYPE, DEFAULT_AUTOTUNE_TYPE),
            taskConfiguration.getString(HOLIDAY_STARTS, ""), taskConfiguration.getString(HOLIDAY_ENDS, ""),
            taskConfiguration.getString(AUTOTUNE_FEATURES), taskConfiguration.getString(AUTOTUNE_MTTD),
            taskConfiguration.getString(AUTOTUNE_PATTERN, DEFAULT_AUTOTUNE_PATTERN),
            taskConfiguration.getString(AUTOTUNE_PATTERN_ONLY,
                Boolean.toString(Strings.isNullOrEmpty(taskConfiguration.getString(AUTOTUNE_MTTD)))));

    if (autotuneResponse.getEntity() != null) {
      List<Long> autotuneIds;
      try {
        autotuneIds = OBJECT_MAPPER.readValue(autotuneResponse.getEntity().toString(), List.class);
      } catch (IOException e) {
        throw new IllegalStateException("Unable to parse autotune response: " + autotuneResponse.getEntity().toString(),
            e);
      }
      for (int i = 0; i < autotuneIds.size(); i++) {
        detectionJobResource.updateAlertFilterToFunctionSpecByAutoTuneId(((Number) autotuneIds.get(i)).longValue());
      }
      LOG.info("Initial alert filter applied");
    } else {
      LOG.info("AutoTune doesn't applied");
    }
  }
}
