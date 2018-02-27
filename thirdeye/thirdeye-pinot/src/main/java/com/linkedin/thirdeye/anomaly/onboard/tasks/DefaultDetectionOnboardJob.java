package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardJob;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnBoardJobRunner;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.utils.PropertyCheckUtils;
import com.linkedin.thirdeye.dashboard.resources.DetectionJobResource;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;


/**
 * This job is for self-serve onboarding. During self-serve, a list of tasks needs to be done. Using a wrapper may block
 * users on browser, and it is hard for them to check onboarding status. This job is sent to back-end scheduler and enable
 * the status-check functionality.
 */
public class DefaultDetectionOnboardJob extends BaseDetectionOnboardJob {
  public static final String ABORT_ON_FAILURE = DetectionOnBoardJobRunner.ABORT_ON_FAILURE;

  public static final String FUNCTION_FACTORY_CONFIG_PATH = "functionFactoryConfigPath";
  public static final String ALERT_FILTER_FACTORY_CONFIG_PATH = "alertFilterFactoryConfigPath";
  public static final String ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH = "alertFilterAutotuneFactoryConfigPath";
  public static final String FUNCTION_FACTORY = "functionFactory";
  public static final String ALERT_FILTER_FACTORY = "alertFilterFactory";
  public static final String ALERT_FILTER_AUTOTUNE_FACTORY = "alertFilterAutotuneFactory";
  public static final String NOTIFY_IF_FAILS = "notifyIfFails";
  public static final String FUNCTION_NAME = "functionName";
  public static final String COLLECTION_NAME = "collection";
  public static final String METRIC_NAME = "metric";
  public static final String EXPLORE_DIMENSION = "exploreDimensions";
  public static final String FILTERS = "filters";
  public static final String METRIC_FUNCTION = "metricFunction";
  public static final String FUNCTION_TYPE = "functionType";
  public static final String WINDOW_SIZE = "windowSize";
  public static final String WINDOW_UNIT = "windowUnit";
  public static final String WINDOW_DELAY = "windowDelay";
  public static final String WINDOW_DELAY_UNIT = "windowDelayUnit";
  public static final String DATA_GRANULARITY = "dataGranularity";
  public static final String FUNCTION_PROPERTIES = "properties";
  public static final String FUNCTION_IS_ACTIVE = "isActive";
  public static final String CRON_EXPRESSION = "cron";
  public static final String REQUIRE_DATA_COMPLETENESS = "requireDataCompleteness";
  public static final String ALERT_ID = "alertId";
  public static final String ALERT_NAME = "alertName";
  public static final String ALERT_CRON = "alertCron";
  public static final String ALERT_FROM = "alertSender";
  public static final String ALERT_TO = "alertRecipients";
  public static final String ALERT_APPLICATION = "application";
  public static final String ANOMALY_FUNCTION_CONFIG = "anomalyFuncitonConfig";
  public static final String ALERT_CONFIG = "alertConfig";
  public static final String AUTOTUNE_PATTERN = DetectionJobResource.AUTOTUNE_PATTERN_KEY;
  public static final String AUTOTUNE_TYPE = "autoTuneType";
  public static final String AUTOTUNE_PATTERN_ONLY = DetectionJobResource.AUTOTUNE_PATTERN_ONLY;
  public static final String AUTOTUNE_FEATURES = DetectionJobResource.AUTOTUNE_FEATURE_KEY;
  public static final String AUTOTUNE_MTTD = DetectionJobResource.AUTOTUNE_MTTD_KEY;
  public static final String HOLIDAY_STARTS = "holidayStarts";
  public static final String HOLIDAY_ENDS = "holidayEnds";
  public static final String REMOVE_ANOMALY_IN_WINDOW = "removeAnomaliesInWindow";
  public static final String PERIOD = "period";
  public static final String START = "start";
  public static final String END = "end";
  public static final String FORCE = "force";
  public static final String SPEEDUP = "speedup";
  public static final String SMTP_HOST = "smtpHost";
  public static final String SMTP_PORT = "smtpPort";
  public static final String THIRDEYE_DASHBOARD_HOST = "thirdeyeDashboardHost";
  public static final String DEFAULT_ALERT_SENDER_ADDRESS = "alertSender";
  public static final String DEFAULT_ALERT_RECEIVER_ADDRESS = "alertReceiver";
  public static final String PHANTON_JS_PATH = "phantonJsPath";
  public static final String ROOT_DIR = "rootDir";

  protected AnomalyFunctionFactory anomalyFunctionFactory;
  protected AlertFilterFactory alertFilterFactory;

  public static final String MISSING_PARAMETER_ERROR_MESSAGE_TEMPLATE = "Require parameter field: %s";
  public static final Boolean DEFAULT_NOTIFY_IF_FAILS = Boolean.TRUE;

  public DefaultDetectionOnboardJob(String jobName, Map<String, String> properties) {
    super(jobName, properties);
  }

  /**
   * Return a task configuration with task name as its prefix, e.g. task1.property1
   * Note that, if a property is used by multiple tasks, the property key is reused for each task,
   * e.g. task1.property1, task2.property2, and so on
   * @return a task configuration with task name as the property key prefix
   */
  @Override
  public Configuration getTaskConfiguration() {
    PropertyCheckUtils.checkNotNull(this.properties,
        Arrays.asList(FUNCTION_FACTORY_CONFIG_PATH, ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH,
            ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH,
            FUNCTION_NAME,
            COLLECTION_NAME,
            METRIC_NAME,
            SMTP_HOST,
            SMTP_PORT,
            DEFAULT_ALERT_RECEIVER_ADDRESS,
            DEFAULT_ALERT_SENDER_ADDRESS,
            THIRDEYE_DASHBOARD_HOST,
            PHANTON_JS_PATH,
            ROOT_DIR));
    Preconditions.checkArgument(properties.containsKey(ALERT_ID) || properties.containsKey(ALERT_NAME),
        String.format(MISSING_PARAMETER_ERROR_MESSAGE_TEMPLATE, ALERT_ID + " OR " + ALERT_NAME));
    if (!properties.containsKey(ALERT_ID)) {
      Preconditions.checkNotNull(properties.get(ALERT_TO),
          String.format(MISSING_PARAMETER_ERROR_MESSAGE_TEMPLATE, ALERT_TO));
    }

    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put (SMTP_HOST, this.properties.get(SMTP_HOST));
    taskConfigs.put (SMTP_PORT, this.properties.get(SMTP_PORT));
    taskConfigs.put (DEFAULT_ALERT_SENDER_ADDRESS, this.properties.get(DEFAULT_ALERT_SENDER_ADDRESS));
    taskConfigs.put (DEFAULT_ALERT_RECEIVER_ADDRESS, this.properties.get(DEFAULT_ALERT_RECEIVER_ADDRESS));

    String taskPrefix = DataPreparationOnboardingTask.TASK_NAME + ".";
    taskConfigs.put(taskPrefix + ABORT_ON_FAILURE, Boolean.TRUE.toString());
    taskConfigs.put(taskPrefix + FUNCTION_FACTORY_CONFIG_PATH, this.properties.get(FUNCTION_FACTORY_CONFIG_PATH));
    taskConfigs.put(taskPrefix + ALERT_FILTER_FACTORY_CONFIG_PATH, this.properties.get(ALERT_FILTER_FACTORY_CONFIG_PATH));
    taskConfigs.put(taskPrefix + ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH, this.properties.get(
        ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH));

    taskPrefix = FunctionCreationOnboardingTask.TASK_NAME + ".";
    taskConfigs.put(taskPrefix + ABORT_ON_FAILURE, Boolean.TRUE.toString());
    taskConfigs.put(taskPrefix + FUNCTION_NAME, this.properties.get(FUNCTION_NAME));
    taskConfigs.put(taskPrefix + COLLECTION_NAME, this.properties.get(COLLECTION_NAME));
    taskConfigs.put(taskPrefix + METRIC_NAME, this.properties.get(METRIC_NAME));
    if (this.properties.containsKey(EXPLORE_DIMENSION)) {
      taskConfigs.put(taskPrefix + EXPLORE_DIMENSION, this.properties.get(EXPLORE_DIMENSION));
    }
    if (this.properties.containsKey(FILTERS)) {
      taskConfigs.put(taskPrefix + FILTERS, this.properties.get(FILTERS));
    }
    if (this.properties.containsKey(METRIC_FUNCTION)) {
      taskConfigs.put(taskPrefix + METRIC_FUNCTION, this.properties.get(METRIC_FUNCTION));
    }
    if (this.properties.containsKey(WINDOW_SIZE)) {
      taskConfigs.put(taskPrefix + WINDOW_SIZE, this.properties.get(WINDOW_SIZE));
    }
    if (this.properties.containsKey(WINDOW_UNIT)) {
      taskConfigs.put(taskPrefix + WINDOW_UNIT, this.properties.get(WINDOW_UNIT));
    }
    if (this.properties.containsKey(WINDOW_DELAY)) {
      taskConfigs.put(taskPrefix + WINDOW_DELAY, this.properties.get(WINDOW_DELAY));
    }
    if (this.properties.containsKey(WINDOW_DELAY_UNIT)) {
      taskConfigs.put(taskPrefix + WINDOW_DELAY_UNIT, this.properties.get(WINDOW_DELAY_UNIT));
    }
    if (this.properties.containsKey(DATA_GRANULARITY)) {
      taskConfigs.put(taskPrefix + DATA_GRANULARITY, this.properties.get(DATA_GRANULARITY));
    }
    if (this.properties.containsKey(FUNCTION_PROPERTIES)) {
      taskConfigs.put(taskPrefix + FUNCTION_PROPERTIES, this.properties.get(FUNCTION_PROPERTIES));
    }
    if (this.properties.containsKey(FUNCTION_IS_ACTIVE)) {
      taskConfigs.put(taskPrefix + FUNCTION_IS_ACTIVE, this.properties.get(FUNCTION_IS_ACTIVE));
    }
    if (this.properties.containsKey(AUTOTUNE_PATTERN)) {
      taskConfigs.put(taskPrefix + AUTOTUNE_PATTERN, this.properties.get(AUTOTUNE_PATTERN));
    }
    if (this.properties.containsKey(AUTOTUNE_TYPE)) {
      taskConfigs.put(taskPrefix + AUTOTUNE_TYPE, this.properties.get(AUTOTUNE_TYPE));
    }
    if (this.properties.containsKey(AUTOTUNE_FEATURES)) {
      taskConfigs.put(taskPrefix + AUTOTUNE_FEATURES, this.properties.get(AUTOTUNE_FEATURES));
    }
    if (this.properties.containsKey(AUTOTUNE_MTTD)) {
      taskConfigs.put(taskPrefix + AUTOTUNE_MTTD, this.properties.get(AUTOTUNE_MTTD));
    }
    taskConfigs.put(taskPrefix + CRON_EXPRESSION, this.properties.get(CRON_EXPRESSION));
    if (this.properties.containsKey(ALERT_ID)) {
      taskConfigs.put(taskPrefix + ALERT_ID, this.properties.get(ALERT_ID));
    }
    if (this.properties.containsKey(ALERT_NAME)) {
      taskConfigs.put(taskPrefix + ALERT_NAME, this.properties.get(ALERT_NAME));
    }
    if (this.properties.containsKey(ALERT_CRON)) {
      taskConfigs.put(taskPrefix + ALERT_CRON, this.properties.get(ALERT_CRON));
    }
    if (this.properties.containsKey(ALERT_FROM)) {
      taskConfigs.put(taskPrefix + ALERT_FROM, this.properties.get(ALERT_FROM));
    }
    if (this.properties.containsKey(ALERT_TO)) {
      taskConfigs.put(taskPrefix + ALERT_TO, this.properties.get(ALERT_TO));
    }
    if (this.properties.containsKey(ALERT_APPLICATION)) {
      taskConfigs.put(taskPrefix + ALERT_APPLICATION, this.properties.get(ALERT_APPLICATION));
    }
    if (this.properties.containsKey(DEFAULT_ALERT_RECEIVER_ADDRESS)) {
      taskConfigs.put (taskPrefix + DEFAULT_ALERT_RECEIVER_ADDRESS, this.properties.get(DEFAULT_ALERT_RECEIVER_ADDRESS));
    }

    taskPrefix = FunctionReplayOnboardingTask.TASK_NAME + ".";
    taskConfigs.put(taskPrefix + ABORT_ON_FAILURE, Boolean.FALSE.toString());
    if (this.properties.containsKey(PERIOD)) {
      taskConfigs.put (taskPrefix + PERIOD, this.properties.get(PERIOD));
    }
    if (this.properties.containsKey(START)) {
      taskConfigs.put (taskPrefix + START, this.properties.get(START));
    }
    if (this.properties.containsKey(END)) {
      taskConfigs.put (taskPrefix + END, this.properties.get(END));
    }
    if (this.properties.containsKey(FORCE)) {
      taskConfigs.put (taskPrefix + FORCE, this.properties.get(FORCE));
    }
    if (this.properties.containsKey(SPEEDUP)) {
      taskConfigs.put (taskPrefix + SPEEDUP, this.properties.get(SPEEDUP));
    }
    if (this.properties.containsKey(REMOVE_ANOMALY_IN_WINDOW)) {
      taskConfigs.put (taskPrefix + REMOVE_ANOMALY_IN_WINDOW, this.properties.get(REMOVE_ANOMALY_IN_WINDOW));
    }

    taskPrefix = AlertFilterAutoTuneOnboardingTask.TASK_NAME + ".";
    taskConfigs.put(taskPrefix + ABORT_ON_FAILURE, Boolean.FALSE.toString());
    if (this.properties.containsKey(PERIOD)) {
      taskConfigs.put (taskPrefix + PERIOD, this.properties.get(PERIOD));
    }
    if (this.properties.containsKey(START)) {
      taskConfigs.put (taskPrefix + START, this.properties.get(START));
    }
    if (this.properties.containsKey(END)) {
      taskConfigs.put (taskPrefix + END, this.properties.get(END));
    }
    if (this.properties.containsKey(AUTOTUNE_PATTERN)) {
      taskConfigs.put (taskPrefix + AUTOTUNE_PATTERN, this.properties.get(AUTOTUNE_PATTERN));
    }
    if (this.properties.containsKey(AUTOTUNE_TYPE)) {
      taskConfigs.put (taskPrefix + AUTOTUNE_TYPE, this.properties.get(AUTOTUNE_TYPE));
    }
    if (this.properties.containsKey(AUTOTUNE_FEATURES)) {
      taskConfigs.put (taskPrefix + AUTOTUNE_FEATURES, this.properties.get(AUTOTUNE_FEATURES));
    }
    if (this.properties.containsKey(AUTOTUNE_MTTD)) {
      taskConfigs.put (taskPrefix + AUTOTUNE_MTTD, this.properties.get(AUTOTUNE_MTTD));
    }
    if (this.properties.containsKey(HOLIDAY_STARTS)) {
      taskConfigs.put (taskPrefix + HOLIDAY_STARTS, this.properties.get(HOLIDAY_STARTS));
    }
    if (this.properties.containsKey(HOLIDAY_ENDS)) {
      taskConfigs.put (taskPrefix + HOLIDAY_ENDS, this.properties.get(HOLIDAY_ENDS));
    }

    taskPrefix = NotificationOnboardingTask.TASK_NAME + ".";
    taskConfigs.put(taskPrefix + ABORT_ON_FAILURE, Boolean.TRUE.toString());
    if (this.properties.containsKey(START)) {
      taskConfigs.put (taskPrefix + START, this.properties.get(START));
    }
    if (this.properties.containsKey(END)) {
      taskConfigs.put (taskPrefix + END, this.properties.get(END));
    }
    taskConfigs.put (taskPrefix + SMTP_HOST, this.properties.get(SMTP_HOST));
    taskConfigs.put (taskPrefix + SMTP_PORT, this.properties.get(SMTP_PORT));
    taskConfigs.put (taskPrefix + THIRDEYE_DASHBOARD_HOST, this.properties.get(THIRDEYE_DASHBOARD_HOST));
    taskConfigs.put (taskPrefix + DEFAULT_ALERT_SENDER_ADDRESS, this.properties.get(DEFAULT_ALERT_SENDER_ADDRESS));
    taskConfigs.put (taskPrefix + DEFAULT_ALERT_RECEIVER_ADDRESS, this.properties.get(DEFAULT_ALERT_RECEIVER_ADDRESS));
    taskConfigs.put (taskPrefix + PHANTON_JS_PATH, this.properties.get(PHANTON_JS_PATH));
    taskConfigs.put (taskPrefix + ROOT_DIR, this.properties.get(ROOT_DIR));

    /*
    Set the DelimiterParsingDisabled to be true to avoid configuration automatically parse property with comma to list
     */
    MapConfiguration mapConfiguration =  new MapConfiguration(taskConfigs);
    mapConfiguration.setDelimiterParsingDisabled(true);
    return mapConfiguration;
  }

  /**
   * Return a list of tasks will be run in the job
   * @return a list of tasks
   */
  @Override
  public List<DetectionOnboardTask> getTasks() {
    List<DetectionOnboardTask> detectionOnboardTasks = new ArrayList<>();
    detectionOnboardTasks.add(new DataPreparationOnboardingTask());
    detectionOnboardTasks.add(new FunctionCreationOnboardingTask());
    detectionOnboardTasks.add(new FunctionReplayOnboardingTask());
    detectionOnboardTasks.add(new AlertFilterAutoTuneOnboardingTask());
    detectionOnboardTasks.add(new NotificationOnboardingTask());
    return detectionOnboardTasks;
  }

  public static String getAbortOnFailure() {
    return ABORT_ON_FAILURE;
  }
}
