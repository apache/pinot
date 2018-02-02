package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTaskContext;
import com.linkedin.thirdeye.anomaly.utils.EmailUtils;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.EmailConfig;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.Response;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
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

  public static final String ANOMALY_FUNCTION_CONFIG = DefaultDetectionOnboardJob.ANOMALY_FUNCTION_CONFIG;
  public static final String ALERT_CONFIG = DefaultDetectionOnboardJob.ALERT_CONFIG;
  public static final String FUNCTION_FACTORY = DefaultDetectionOnboardJob.FUNCTION_FACTORY;
  public static final String ALERT_FILTER_FACTORY = DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY;
  public static final String FUNCTION_NAME = DefaultDetectionOnboardJob.FUNCTION_NAME;
  public static final String COLLECTION_NAME = DefaultDetectionOnboardJob.COLLECTION_NAME;
  public static final String METRIC_NAME = DefaultDetectionOnboardJob.METRIC_NAME;
  public static final String EXPLORE_DIMENSION = DefaultDetectionOnboardJob.EXPLORE_DIMENSION;
  public static final String FILTERS = DefaultDetectionOnboardJob.FILTERS;
  public static final String FUNCTION_TYPE = DefaultDetectionOnboardJob.FUNCTION_TYPE;
  public static final String METRIC_FUNCTION = DefaultDetectionOnboardJob.METRIC_FUNCTION;
  public static final String WINDOW_SIZE = DefaultDetectionOnboardJob.WINDOW_SIZE;
  public static final String WINDOW_UNIT = DefaultDetectionOnboardJob.WINDOW_UNIT;
  public static final String WINDOW_DELAY = DefaultDetectionOnboardJob.WINDOW_DELAY;
  public static final String WINDOW_DELAY_UNIT = DefaultDetectionOnboardJob.WINDOW_DELAY_UNIT;
  public static final String DATA_GRANULARITY = DefaultDetectionOnboardJob.DATA_GRANULARITY;
  public static final String PROPERTIES = DefaultDetectionOnboardJob.FUNCTION_PROPERTIES;
  public static final String IS_ACTIVE = DefaultDetectionOnboardJob.FUNCTION_IS_ACTIVE;
  public static final String CRON_EXPRESSION = DefaultDetectionOnboardJob.CRON_EXPRESSION;
  public static final String ALERT_FILTER_PATTERN = DefaultDetectionOnboardJob.AUTOTUNE_PATTERN;
  public static final String ALERT_FILTER_TYPE = DefaultDetectionOnboardJob.AUTOTUNE_TYPE;
  public static final String ALERT_FILTER_FEATURES = DefaultDetectionOnboardJob.AUTOTUNE_FEATURES;
  public static final String ALERT_FILTER_MTTD = DefaultDetectionOnboardJob.AUTOTUNE_MTTD;
  public static final String ALERT_ID = DefaultDetectionOnboardJob.ALERT_ID;
  public static final String ALERT_NAME = DefaultDetectionOnboardJob.ALERT_NAME;
  public static final String ALERT_CRON = DefaultDetectionOnboardJob.ALERT_CRON;
  public static final String ALERT_FROM = DefaultDetectionOnboardJob.ALERT_FROM;
  public static final String ALERT_TO = DefaultDetectionOnboardJob.ALERT_TO;
  public static final String ALERT_APPLICATION = DefaultDetectionOnboardJob.ALERT_APPLICATION;
  public static final String DEFAULT_ALERT_RECEIVER = DefaultDetectionOnboardJob.DEFAULT_ALERT_RECEIVER_ADDRESS;

  public static final Boolean DEFAULT_IS_ACTIVE = true;
  public static final String DEFAULT_WINDOW_DELAY = "0";
  public static final String DEFAULT_ALERT_CRON = "0 0/5 * 1/1 * ? *"; // Every 5 min
  public static final String DEFAULT_ALERT_FILTER_PATTERN = AlertFilterAutoTuneOnboardingTask.DEFAULT_AUTOTUNE_PATTERN;
  public static final String DEFAULT_ALERT_FILTER_TYPE = "AUTOTUNE";

  private AnomalyFunctionManager anoomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;

  public FunctionCreationOnboardingTask() {
    super(TASK_NAME);
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getAlertConfigDAO();
    this.anoomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    this.datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    this.metricConfigDAO = daoRegistry.getMetricConfigDAO();
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
    Preconditions.checkArgument(configuration.containsKey(ALERT_ID) || configuration.containsKey(ALERT_NAME));
    if (!configuration.containsKey(ALERT_ID)) {
      Preconditions.checkNotNull(configuration.getString(ALERT_TO));
    }

    // check if duplicate name exists
    AnomalyFunctionDTO duplicateFunction = anoomalyFunctionDAO.findWhereNameEquals(configuration.getString(FUNCTION_NAME));
    if (duplicateFunction != null) {
      throw new IllegalArgumentException("Duplicate function name " + configuration.getString(FUNCTION_NAME)
          + " is found");
    }

    if (StringUtils.isNotBlank(configuration.getString(ALERT_NAME))) {
      AlertConfigDTO duplicateAlert = alertConfigDAO.findWhereNameEquals(configuration.getString(ALERT_NAME));
      if (duplicateAlert != null) {
        throw new IllegalArgumentException("Duplicate alert name " + configuration.getString(ALERT_NAME)
            + " is found");
      }
    }

    // update datasetConfig
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(configuration.getString(COLLECTION_NAME));
    if (datasetConfig == null) {
      throw new NoSuchElementException("Cannot find collection: " + configuration.getString(COLLECTION_NAME));
    }
    TimeSpec timeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
    TimeGranularity dataGranularity = timeSpec.getDataGranularity();
    if (configuration.containsKey(DATA_GRANULARITY)) {
      TimeGranularity userAssignedDataGranularity = null;
      try {
        userAssignedDataGranularity = TimeGranularity.fromString(configuration.getString(DATA_GRANULARITY));
      } catch (Exception e) {
        LOG.error("Unable to parse user input data granularity: {}",
            configuration.getString(DATA_GRANULARITY));
        throw new IllegalArgumentException("Unsupported time granularity: " + configuration.getString(DATA_GRANULARITY));
      }
      dataGranularity = userAssignedDataGranularity;
    }

    // use the aggregate function in MetricConfig as default function
    MetricConfigDTO metricConfig = metricConfigDAO.findByMetricAndDataset(configuration.getString(METRIC_NAME), configuration.getString(COLLECTION_NAME));
    String defaultMetricFunction = metricConfig.getDefaultAggFunction().name();

    // create function
    AnomalyResource anomalyResource = new AnomalyResource(anomalyFunctionFactory, alertFilterFactory);
    AnomalyFunctionDTO anomalyFunction = null;
    try {
      AnomalyFunctionDTO defaultFunctionSpec = getDefaultFunctionSpecByTimeGranularity(dataGranularity);

      // Merge user properties with default properties; the user assigned property can override default property
      Properties userAssignedFunctionProperties = com.linkedin.thirdeye.datalayer.util.StringUtils
          .decodeCompactedProperties(configuration.getString(PROPERTIES, ""));
      Properties defaultFunctionProperties = com.linkedin.thirdeye.datalayer.util.StringUtils
          .decodeCompactedProperties(defaultFunctionSpec.getProperties());
      for (Map.Entry propertyEntry : userAssignedFunctionProperties.entrySet()) {
        defaultFunctionProperties.setProperty((String) propertyEntry.getKey(), (String) propertyEntry.getValue());
      }

      Response response = anomalyResource.createAnomalyFunction(configuration.getString(COLLECTION_NAME),
          configuration.getString(FUNCTION_NAME), configuration.getString(METRIC_NAME),
          configuration.getString(METRIC_FUNCTION, defaultMetricFunction),
          configuration.getString(FUNCTION_TYPE, defaultFunctionSpec.getType()),
          configuration.getString(WINDOW_SIZE, Integer.toString(defaultFunctionSpec.getWindowSize())),
          configuration.getString(WINDOW_UNIT, defaultFunctionSpec.getWindowUnit().name()),
          configuration.getString(WINDOW_DELAY, DEFAULT_WINDOW_DELAY),
          configuration.getString(CRON_EXPRESSION, defaultFunctionSpec.getCron()), configuration.getString(WINDOW_DELAY_UNIT),
          configuration.getString(EXPLORE_DIMENSION), configuration.getString(FILTERS),
          configuration.getString(DATA_GRANULARITY),
          com.linkedin.thirdeye.datalayer.util.StringUtils.encodeCompactedProperties(defaultFunctionProperties),
          configuration.getBoolean(IS_ACTIVE, DEFAULT_IS_ACTIVE));
      if (Response.Status.OK.equals(response.getStatusInfo())) {
        long functionId = Long.valueOf(response.getEntity().toString());
        anomalyFunction = anoomalyFunctionDAO.findById(functionId);
        executionContext.setExecutionResult(ANOMALY_FUNCTION_CONFIG, anomalyFunction);
      } else {
        throw new UnsupportedOperationException("Get Exception from Anomaly Function Creation End-Point");
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }

    // Assign Default Alert Filter
    Map<String, String> alertFilter = new HashMap<>();
    alertFilter.put(ALERT_FILTER_PATTERN, configuration.getString(ALERT_FILTER_PATTERN, DEFAULT_ALERT_FILTER_PATTERN));
    alertFilter.put(ALERT_FILTER_TYPE, configuration.getString(ALERT_FILTER_TYPE, DEFAULT_ALERT_FILTER_TYPE));
    if (configuration.containsKey(ALERT_FILTER_FEATURES)) {
      alertFilter.put(ALERT_FILTER_FEATURES, configuration.getString(ALERT_FILTER_FEATURES));
    }
    if (configuration.containsKey(ALERT_FILTER_MTTD)) {
      alertFilter.put(ALERT_FILTER_MTTD, configuration.getString(ALERT_FILTER_MTTD));
    }
    anomalyFunction.setAlertFilter(alertFilter);
    this.anoomalyFunctionDAO.update(anomalyFunction);

    // create alert config
    AlertConfigDTO alertConfig = null;
    if (configuration.containsKey(ALERT_ID)) {
      alertConfig = alertConfigDAO.findById(configuration.getLong(ALERT_ID));
      EmailConfig emailConfig = alertConfig.getEmailConfig();
      emailConfig.getFunctionIds().add(anomalyFunction.getId());

      // Add recipients to existing alert group
      String recipients = configuration.getString(ALERT_TO);
      if (StringUtils.isNotBlank(recipients)) {
        if (StringUtils.isNotBlank(alertConfig.getRecipients())) {
          recipients = recipients + "," + alertConfig.getRecipients();
        }
        recipients = EmailUtils.getValidEmailAddresses(recipients);
        alertConfig.setRecipients(recipients);
      }
      alertConfigDAO.update(alertConfig);
    } else {
      alertConfig = new AlertConfigDTO();
      EmailConfig emailConfig = new EmailConfig();
      emailConfig.setFunctionIds(Arrays.asList(anomalyFunction.getId()));
      alertConfig.setEmailConfig(emailConfig);
      alertConfig.setName(configuration.getString(ALERT_NAME));
      String thirdeyeDefaultEmail = configuration.getString(DEFAULT_ALERT_RECEIVER);
      alertConfig.setFromAddress(configuration.getString(ALERT_FROM, thirdeyeDefaultEmail));
      String alertRecipients = thirdeyeDefaultEmail;
      if (configuration.containsKey(ALERT_TO)) {
        alertRecipients = alertRecipients + "," + configuration.getString(ALERT_TO);
      }
      alertRecipients = EmailUtils.getValidEmailAddresses(alertRecipients);
      alertConfig.setApplication(configuration.getString(ALERT_APPLICATION));
      alertConfig.setRecipients(alertRecipients);
      alertConfig.setCronExpression(configuration.getString(ALERT_CRON, DEFAULT_ALERT_CRON));
      alertConfigDAO.save(alertConfig);
    }
    executionContext.setExecutionResult(ALERT_CONFIG, alertConfig);
  }

  protected AnomalyFunctionDTO getDefaultFunctionSpecByTimeGranularity(TimeGranularity timeGranularity) {
    AnomalyFunctionDTO anomalyFunctionSpec = new AnomalyFunctionDTO();
    switch (timeGranularity.getUnit()) {
      case MINUTES:
        anomalyFunctionSpec.setType("CONFIDENCE_INTERVAL_SIGN_TEST");
        anomalyFunctionSpec.setCron("0 0 0 * * ?");
        anomalyFunctionSpec.setWindowSize(6);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionSpec.setProperties("");
        break;
      case HOURS:
        anomalyFunctionSpec.setType("REGRESSION_GAUSSIAN_SCAN");
        anomalyFunctionSpec.setCron("0 0 14 1/1 * ? *");
        anomalyFunctionSpec.setWindowSize(84);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionSpec.setProperties("");
        break;
      case DAYS:
        anomalyFunctionSpec.setType("SPLINE_REGRESSION_VANILLA");
        anomalyFunctionSpec.setCron("0 0 14 1/1 * ? *");
        anomalyFunctionSpec.setWindowSize(1);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.DAYS);
        anomalyFunctionSpec.setProperties("");
        break;
      default:
        anomalyFunctionSpec.setType("WEEK_OVER_WEEK_RULE");
        anomalyFunctionSpec.setCron("0 0 0 * * ?");
        anomalyFunctionSpec.setWindowSize(6);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionSpec.setProperties("");
        break;
    }
    return anomalyFunctionSpec;
  }
}
