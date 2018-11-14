/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.anomaly.onboard.framework.BaseDetectionOnboardTask;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardExecutionContext;
import com.linkedin.thirdeye.anomaly.onboard.framework.DetectionOnboardTaskContext;
import com.linkedin.thirdeye.anomaly.onboard.utils.FunctionCreationUtils;
import com.linkedin.thirdeye.anomaly.utils.EmailUtils;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
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
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  public static final String REQUIRE_DATA_COMPLETENESS = DefaultDetectionOnboardJob.REQUIRE_DATA_COMPLETENESS;
  public static final String ALERT_FILTER_PATTERN = DefaultDetectionOnboardJob.AUTOTUNE_PATTERN;
  public static final String ALERT_FILTER_TYPE = DefaultDetectionOnboardJob.AUTOTUNE_TYPE;
  public static final String ALERT_FILTER_FEATURES = DefaultDetectionOnboardJob.AUTOTUNE_FEATURES;
  public static final String ALERT_FILTER_MTTD = DefaultDetectionOnboardJob.AUTOTUNE_MTTD;
  public static final String ALERT_ID = DefaultDetectionOnboardJob.ALERT_ID;
  public static final String ALERT_NAME = DefaultDetectionOnboardJob.ALERT_NAME;
  public static final String ALERT_CRON = DefaultDetectionOnboardJob.ALERT_CRON;
  public static final String ALERT_FROM = DefaultDetectionOnboardJob.ALERT_FROM;
  public static final String ALERT_TO = DefaultDetectionOnboardJob.ALERT_TO;
  public static final String ALERT_CC = DefaultDetectionOnboardJob.ALERT_CC;
  public static final String ALERT_BCC = DefaultDetectionOnboardJob.ALERT_BCC;
  public static final String ALERT_APPLICATION = DefaultDetectionOnboardJob.ALERT_APPLICATION;
  public static final String DEFAULT_ALERT_SENDER = DefaultDetectionOnboardJob.DEFAULT_ALERT_SENDER_ADDRESS;
  public static final String DEFAULT_ALERT_RECEIVER = DefaultDetectionOnboardJob.DEFAULT_ALERT_RECEIVER_ADDRESS;

  public static final Boolean DEFAULT_IS_ACTIVE = true;
  public static final Integer DEFAULT_WINDOW_DELAY = 0;
  public static final String DEFAULT_ALERT_CRON = "0 0/5 * * * ? *"; // Every 5 min
  public static final String DEFAULT_ALERT_FILTER_PATTERN = AlertFilterAutoTuneOnboardingTask.DEFAULT_AUTOTUNE_PATTERN;
  public static final String DEFAULT_ALERT_FILTER_TYPE = "AUTOTUNE";
  public static final String DEFAULT_URL_DECODER = "UTF-8";

  private AnomalyFunctionManager anomalyFunctionDAO;
  private AlertConfigManager alertConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;

  public FunctionCreationOnboardingTask() {
    super(TASK_NAME);
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getAlertConfigDAO();
    this.anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
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

    // Get pre-created function name
    // TODO Once pipeline refactor is done, this logic should be changed to be new function creation
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findWhereNameEquals(configuration.getString(FUNCTION_NAME));
    if (anomalyFunction == null) {
      throw new IllegalArgumentException(String.format("No function with name %s is found in the system",
          configuration.getString(FUNCTION_NAME)));
    }

    // check if duplicate name exists
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

      anomalyFunction.setMetricId(metricConfig.getId());
      anomalyFunction.setMetric(metricConfig.getName());
      anomalyFunction.setTopicMetric(metricConfig.getName());
      anomalyFunction.setMetrics(Arrays.asList(metricConfig.getName()));
      anomalyFunction.setCollection(datasetConfig.getDataset());
      anomalyFunction.setCron(configuration.getString(CRON_EXPRESSION, defaultFunctionSpec.getCron()));
      anomalyFunction.setMetricFunction(MetricAggFunction.valueOf(
          configuration.getString(METRIC_FUNCTION, defaultMetricFunction)));
      String filters = configuration.getString(FILTERS);
      if (!org.apache.commons.lang3.StringUtils.isBlank(filters)) {
        filters = URLDecoder.decode(filters, DEFAULT_URL_DECODER);
        String filterString = ThirdEyeUtils.getSortedFiltersFromJson(filters);
        anomalyFunction.setFilters(filterString);
      }
      if (org.apache.commons.lang3.StringUtils.isNotEmpty(configuration.getString(EXPLORE_DIMENSION))) {
        anomalyFunction.setExploreDimensions(FunctionCreationUtils.getDimensions(datasetConfig, configuration.getString(EXPLORE_DIMENSION)));
      }

      anomalyFunction.setWindowSize(configuration.getInt(WINDOW_SIZE, defaultFunctionSpec.getWindowSize()));
      anomalyFunction.setWindowUnit(TimeUnit.valueOf(configuration
          .getString(WINDOW_UNIT, defaultFunctionSpec.getWindowUnit().name())));
      anomalyFunction.setWindowDelay(configuration.getInt(WINDOW_DELAY, DEFAULT_WINDOW_DELAY));
      anomalyFunction.setWindowDelayUnit(TimeUnit.valueOf(
          configuration.getString(WINDOW_DELAY_UNIT, dataGranularity.getUnit().toString())));
      anomalyFunction.setType(configuration.getString(FUNCTION_TYPE, defaultFunctionSpec.getType()));
      anomalyFunction.setProperties(com.linkedin.thirdeye.datalayer.util.StringUtils.
          encodeCompactedProperties(defaultFunctionProperties));
      if (defaultFunctionSpec.getFrequency() != null) {
        anomalyFunction.setFrequency(defaultFunctionSpec.getFrequency());
      }
      anomalyFunction.setBucketSize(dataGranularity.getSize());
      anomalyFunction.setBucketUnit(dataGranularity.getUnit());
      anomalyFunction.setIsActive(configuration.getBoolean(IS_ACTIVE, DEFAULT_IS_ACTIVE));
      anomalyFunction.setRequiresCompletenessCheck(configuration.getBoolean(REQUIRE_DATA_COMPLETENESS,
          defaultFunctionSpec.isRequiresCompletenessCheck()));

      anomalyFunctionDAO.update(anomalyFunction);

      executionContext.setExecutionResult(ANOMALY_FUNCTION_CONFIG, anomalyFunction);
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
    this.anomalyFunctionDAO.update(anomalyFunction);

    // create alert config
    AlertConfigDTO alertConfig = null;
    if (configuration.containsKey(ALERT_ID)) {
      alertConfig = alertConfigDAO.findById(configuration.getLong(ALERT_ID));
      EmailConfig emailConfig = alertConfig.getEmailConfig();
      if (emailConfig == null) {
        EmailConfig emailConf = new EmailConfig();
        emailConf.setFunctionIds(Arrays.asList(anomalyFunction.getId()));
      } else {
        emailConfig.getFunctionIds().add(anomalyFunction.getId());
      }
      alertConfig.setEmailConfig(emailConfig);

      // Add recipients to existing alert group
      DetectionAlertFilterRecipients recipients = alertConfig.getReceiverAddresses();
      Set<String> toAddr = EmailUtils.getValidEmailAddresses(configuration.getString(ALERT_TO));
      if (recipients.getTo() == null) {
        recipients.setTo(toAddr);
      } else {
        recipients.getTo().addAll(toAddr);
      }
      Set<String> ccAddr = EmailUtils.getValidEmailAddresses(configuration.getString(ALERT_CC));
      if (recipients.getCc() == null) {
        recipients.setCc(ccAddr);
      } else {
        recipients.getCc().addAll(ccAddr);
      }
      Set<String> bccAddr = EmailUtils.getValidEmailAddresses(configuration.getString(ALERT_BCC));
      if (recipients.getBcc() == null) {
        recipients.setBcc(bccAddr);
      } else {
        recipients.getBcc().addAll(bccAddr);
      }

      alertConfigDAO.update(alertConfig);
    } else {
      alertConfig = new AlertConfigDTO();
      EmailConfig emailConfig = new EmailConfig();
      emailConfig.setFunctionIds(Arrays.asList(anomalyFunction.getId()));
      alertConfig.setEmailConfig(emailConfig);
      alertConfig.setName(configuration.getString(ALERT_NAME));

      Set<String> toAddresses = EmailUtils.getValidEmailAddresses(configuration.getString(ALERT_TO));
      Set<String> ccAddresses = EmailUtils.getValidEmailAddresses(configuration.getString(ALERT_CC));
      ccAddresses.addAll(EmailUtils.getValidEmailAddresses(configuration.getString(DEFAULT_ALERT_RECEIVER)));
      Set<String> bccAddresses = EmailUtils.getValidEmailAddresses(configuration.getString(ALERT_BCC));

      Set<String> defaultSender = EmailUtils.getValidEmailAddresses(configuration.getString(DEFAULT_ALERT_SENDER));
      if (defaultSender.isEmpty()) {
        throw new IllegalArgumentException("No sender configured for the emails. Please set " + DEFAULT_ALERT_SENDER);
      }
      alertConfig.setFromAddress(configuration.getString(ALERT_FROM, defaultSender.iterator().next()));

      alertConfig.setReceiverAddresses(new DetectionAlertFilterRecipients(toAddresses, ccAddresses, bccAddresses));
      alertConfig.setApplication(configuration.getString(ALERT_APPLICATION));
      alertConfig.setCronExpression(configuration.getString(ALERT_CRON, DEFAULT_ALERT_CRON));
      alertConfigDAO.save(alertConfig);
    }
    executionContext.setExecutionResult(ALERT_CONFIG, alertConfig);
  }

  protected AnomalyFunctionDTO getDefaultFunctionSpecByTimeGranularity(TimeGranularity timeGranularity) {
    AnomalyFunctionDTO anomalyFunctionSpec = new AnomalyFunctionDTO();
    switch (timeGranularity.getUnit()) {
      case MINUTES:
        anomalyFunctionSpec.setType("SIGN_TEST_WRAPPER");
        anomalyFunctionSpec.setCron("0 0/15 * * * ? *");
        anomalyFunctionSpec.setWindowSize(6);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionSpec.setFrequency(new TimeGranularity(15, TimeUnit.MINUTES));
        anomalyFunctionSpec.setProperties("variables.seasonalPeriod=P7D;module.training=nonparametric.SeasonalSlidingWindowTrainingModule;variables.slidingWindowWidth=8;variables.pattern=UP,DOWN;variables.anomalyRemovalThreshold=0.6,-0.6;module.data=SeasonalDataModule;variables.signTestStepSize=1;variables.pValueThreshold=0.05;function=ConfigurableAnomalyDetectionFunction;module.testingPreprocessors=DummyPreprocessModule;variables.seasonalCount=3;variables.signTestWindowSize=24;module.detection=SignTestDetectionModule;variables.decayRate=0.5;variables.confidenceLevel=0.99;module.trainingPreprocessors=AnomalyRemovalByWeight;variables.trainPadding=PT20M,PT25M");
        anomalyFunctionSpec.setRequiresCompletenessCheck(false);
        break;
      case HOURS:
        anomalyFunctionSpec.setType("REGRESSION_GAUSSIAN_SCAN_WRAPPER");
        anomalyFunctionSpec.setCron("0 0 * * * ? *");
        anomalyFunctionSpec.setWindowSize(24);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionSpec.setProperties("variables.isMajor=false;downgrade.variables.seasonalities=;function=SelfRecoverableAnomalyDetectionFunction;variables.pValueThreshold=0.01;variables.continuumOffset=P60D;module.detection=GaussianScanDetectionModule;variables.anomalyRemovalThreshold=1.0,-1.0;workflow=RegressionWorkflow;module.training=parametric.NullBasisRegressionTrainingModule;variables.seasonalities=HOURLY_SEASONALITY,DAILY_SEASONALITY;module.data=ContinuumDataModule;variables.scanNumSimulations=500;variables.scanTargetNumAnomalies=1;module.trainingPreprocessors=AnomalyRemovalByWeight;variables.scanMaxWindowSize=48;variables.scanMinWindowSize=1;variables.scanStepSize=1;module.testingPreprocessors=DummyPreprocessModule;variables.scanUseBootStrap=true");
        anomalyFunctionSpec.setRequiresCompletenessCheck(false);
        break;
      case DAYS:
        anomalyFunctionSpec.setType("SPLINE_REGRESSION_WRAPPER");
        anomalyFunctionSpec.setCron("0 0 14 * * ? *");
        anomalyFunctionSpec.setWindowSize(1);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.DAYS);
        anomalyFunctionSpec.setProperties("variables.continuumOffset=P90D;module.training=parametric.GenericSplineTrainingModule;variables.numberOfKnots=0;variables.degree=3;variables.predictionMode=TRENDING;variables.anomalyRemovalThreshold=0.6,-0.6;module.data=ContinuumDataModule;variables.pValueThreshold=0.025;function=SelfRecoverableAnomalyDetectionFunction;variables.seasonalities=DAILY_SEASONALITY;module.detection=ConfidenceIntervalDetectionModule;module.testingPreprocessors=DummyPreprocessModule;workflow=RegressionWorkflow;variables.recentPeriod=P14D;module.trainingPreprocessors=AnomalyRemovalByWeight;variables.r2Cutoff=0.9;downgrade.variables.seasonalities=");
        anomalyFunctionSpec.setRequiresCompletenessCheck(true);
        break;
      default:
        anomalyFunctionSpec.setType("WEEK_OVER_WEEK_RULE");
        anomalyFunctionSpec.setCron("0 0 0 * * ?");
        anomalyFunctionSpec.setWindowSize(6);
        anomalyFunctionSpec.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionSpec.setProperties("");
        anomalyFunctionSpec.setRequiresCompletenessCheck(false);
        break;
    }
    return anomalyFunctionSpec;
  }
}
