package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTaskContext;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.EmailConfig;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.Arrays;
import java.util.NoSuchElementException;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This task runs the function creation and assign function id to an existing or new alert config
 * Three steps are included:
 *  - create a new anomaly function
 *  - assign the function id to an existing or new alert config
 */
public class FunctionCreationOnboardingTask extends BaseDetectionOnboardTask {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionCreationOnboardingTask.class);

  public static final String TASK_NAME = "FunctionAlertCreation";

  public static final String FUNCTION_FACTORY = DefaultDetectionOnboardJob.FUNCTION_FACTORY;
  public static final String ALERT_FILTER_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY;
  public static final String FUNCTION_NAME = DefaultDetectionOnboardJob.FUNCTION_NAME;
  public static final String COLLECTION_NAME = DefaultDetectionOnboardJob.COLLECTION_NAME;
  public static final String METRIC_NAME = DefaultDetectionOnboardJob.METRIC_NAME;
  public static final String EXPLORE_DIMENSION = DefaultDetectionOnboardJob.EXPLORE_DIMENSION;
  public static final String FILTERS = DefaultDetectionOnboardJob.FILTERS;
  public static final String METRIC_FUNCTION = DefaultDetectionOnboardJob.METRIC_FUNCTION;
  public static final String WINDOW_SIZE = DefaultDetectionOnboardJob.WINDOW_SIZE;
  public static final String WINDOW_UNIT = DefaultDetectionOnboardJob.WINDOW_UNIT;
  public static final String WINDOW_DELAY = DefaultDetectionOnboardJob.WINDOW_DELAY;
  public static final String WINDOW_DELAY_UNIT = DefaultDetectionOnboardJob.WINDOW_DELAY_UNIT;
  public static final String DATA_GRANULARITY = DefaultDetectionOnboardJob.DATA_GRANULARITY;
  public static final String PROPERTIES = DefaultDetectionOnboardJob.FUNCTION_PROPERTIES;
  public static final String IS_ACTIVE = DefaultDetectionOnboardJob.FUNCTION_IS_ACTIVE;
  public static final String CRON_EXPRESSION = DefaultDetectionOnboardJob.CRON_EXPRESSION;
  public static final String ALERT_ID = DefaultDetectionOnboardJob.ALERT_ID;
  public static final String ALERT_NAME = DefaultDetectionOnboardJob.ALERT_NAME;
  public static final String ALERT_CRON = DefaultDetectionOnboardJob.ALERT_CRON;
  public static final String ALERT_FROM = DefaultDetectionOnboardJob.ALERT_FROM;
  public static final String ALERT_TO = DefaultDetectionOnboardJob.ALERT_TO;
  public static final String ALERT_APPLICATION = DefaultDetectionOnboardJob.ALERT_APPLICATION;
  public static final String DEFAULT_ALERT_RECEIVER = DefaultDetectionOnboardJob.DEFAULT_ALERT_RECEIVER;

  public static final String DEFAULT_CRON_EXPRESSION = "0 0 0 * * ?"; // Everyday
  public static final Boolean DEFAULT_IS_ACTIVE = true;
  public static final String DEFAULT_METRIC_FUNCTION = "SUM";
  public static final String DEFAULT_WINDOW_DELAY = "0";
  public static final String DEFAULT_ALERT_CRON = "0 0/5 * 1/1 * ? *"; // Every 5 min

  private AnomalyFunctionManager anoomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private DatasetConfigManager datasetConfigDAO;

  public FunctionCreationOnboardingTask() {
    super(TASK_NAME);
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getAlertConfigDAO();
    this.anoomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    this.datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
  }

  /**
   * Executes the task. To fail this task, throw exceptions. The job executor will catch the exception and store
   * it in the message in the execution status of this task.
   */
  @Override
  public void run() {
    DetectionOnboardTaskContext taskContext = getTaskContext();
    DetectionOnboardExecutionContext executionContext = taskContext.getExecutionContext();
    Configuration configuration = taskContext.getConfiguration();

    Preconditions.checkNotNull(executionContext.getExecutionResult(FUNCTION_FACTORY));
    Preconditions.checkNotNull(executionContext.getExecutionResult(ALERT_FILTER_FACTORY));

    AnomalyFunctionFactory anomalyFunctionFactory = (AnomalyFunctionFactory)
        executionContext.getExecutionResult(FUNCTION_FACTORY);
    AlertFilterFactory alertFilterFactory = (AlertFilterFactory) executionContext.getExecutionResult(ALERT_FILTER_FACTORY);

    Preconditions.checkNotNull(anomalyFunctionFactory);
    Preconditions.checkNotNull(alertFilterFactory);

    // Assert if null
    Preconditions.checkNotNull(taskContext);
    Preconditions.checkNotNull(configuration.getString(FUNCTION_NAME));
    Preconditions.checkNotNull(configuration.getString(COLLECTION_NAME));
    Preconditions.checkNotNull(configuration.getString(METRIC_NAME));
    Preconditions.checkNotNull(configuration.getString(WINDOW_SIZE));
    Preconditions.checkNotNull(configuration.getString(WINDOW_UNIT));
    Preconditions.checkArgument(configuration.containsKey(ALERT_ID) || configuration.containsKey(ALERT_NAME));
    if (!configuration.containsKey(ALERT_ID)) {
      Preconditions.checkNotNull(configuration.getString(ALERT_TO));
    }

    // update datasetConfig
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(configuration.getString(COLLECTION_NAME));
    if (datasetConfig == null) {
      throw new NoSuchElementException("Cannot find collection: " + configuration.getString(COLLECTION_NAME));
    }
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeGranularity dataGranularity = timeSpec.getDataGranularity();
    try{
      dataGranularity = TimeGranularity.fromString(configuration.getString(DATA_GRANULARITY));
    } catch (Exception e) {
      LOG.warn("Unable to parse user input data granularity: {}; use default from dataset config", configuration.getString(DATA_GRANULARITY));
    }

    // create function
    AnomalyResource anomalyResource = new AnomalyResource(anomalyFunctionFactory, alertFilterFactory);
    AnomalyFunctionDTO anomalyFunction = null;
    try {
      Response response = anomalyResource.createAnomalyFunction(configuration.getString(COLLECTION_NAME),
          configuration.getString(FUNCTION_NAME), configuration.getString(METRIC_NAME),
          configuration.getString(METRIC_FUNCTION, DEFAULT_METRIC_FUNCTION),
          getFunctionTypeByTimeGranularity(dataGranularity), configuration.getString(WINDOW_SIZE), configuration.getString(WINDOW_UNIT),
          configuration.getString(WINDOW_DELAY, DEFAULT_WINDOW_DELAY),
          configuration.getString(CRON_EXPRESSION, DEFAULT_CRON_EXPRESSION), configuration.getString(WINDOW_DELAY_UNIT),
          configuration.getString(EXPLORE_DIMENSION), configuration.getString(FILTERS),
          configuration.getString(DATA_GRANULARITY), configuration.getString(PROPERTIES),
          configuration.getBoolean(IS_ACTIVE, DEFAULT_IS_ACTIVE));
      if (response.getStatusInfo().equals(Response.Status.OK)) {
        long functionId = Long.valueOf(response.getEntity().toString());
        anomalyFunction = anoomalyFunctionDAO.findById(functionId);
        executionContext.setExecutionResult(DefaultDetectionOnboardJob.ANOMALY_FUNCTION, anomalyFunction);
      } else {
        throw new UnsupportedOperationException("Get Exception from Anomaly Function Creation End-Point");
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

    // create alert config
    AlertConfigDTO alertConfig = null;
    if (configuration.containsKey(ALERT_ID)) {
      alertConfig = alertConfigDAO.findById(configuration.getLong(ALERT_ID));
      EmailConfig emailConfig = alertConfig.getEmailConfig();
      emailConfig.getFunctionIds().add(anomalyFunction.getId());
      alertConfigDAO.update(alertConfig);
    } else {
      alertConfig = new AlertConfigDTO();
      EmailConfig emailConfig = new EmailConfig();
      emailConfig.setFunctionIds(Arrays.asList(anomalyFunction.getId()));
      alertConfig.setEmailConfig(emailConfig);
      alertConfig.setName(configuration.getString(ALERT_NAME));
      String thirdeyeDefaultEmail = configuration.getString(configuration.getString(DEFAULT_ALERT_RECEIVER));
      alertConfig.setFromAddress(configuration.getString(ALERT_FROM, thirdeyeDefaultEmail));
      String alertRecipients = thirdeyeDefaultEmail;
      if (configuration.containsKey(ALERT_TO)) {
        alertRecipients = alertRecipients + "," + configuration.getString(ALERT_TO);
      }
      alertConfig.setApplication(configuration.getString(ALERT_APPLICATION));
      alertConfig.setRecipients(alertRecipients);
      alertConfig.setCronExpression(configuration.getString(ALERT_CRON, DEFAULT_ALERT_CRON));
      alertConfigDAO.save(alertConfig);
    }
    executionContext.setExecutionResult(DefaultDetectionOnboardJob.ALERT_CONFIG, alertConfig);
  }

  protected String getFunctionTypeByTimeGranularity(TimeGranularity timeGranularity) {
    switch (timeGranularity.getUnit()) {
      case MINUTES:
        return "CONFIDENCE_INTERVAL_SIGN_TEST";
      case HOURS:
        return "REGRESSION_GAUSSIAN_SCAN";
      case DAYS:
        return "SPLINE_REGRESSION_VANILLA";
        default:
          return "WEEK_OVER_WEEK_RULE";
    }
  }
}
