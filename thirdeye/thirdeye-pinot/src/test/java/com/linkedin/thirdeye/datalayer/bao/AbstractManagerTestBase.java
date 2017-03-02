package com.linkedin.thirdeye.datalayer.bao;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.TestUtils;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.IngraphDashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;

public abstract class AbstractManagerTestBase {
  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected RawAnomalyResultManager rawAnomalyResultDAO;
  protected JobManager jobDAO;
  protected TaskManager taskDAO;
  protected EmailConfigurationManager emailConfigurationDAO;
  protected MergedAnomalyResultManager mergedAnomalyResultDAO;
  protected DatasetConfigManager datasetConfigDAO;
  protected MetricConfigManager metricConfigDAO;
  protected DashboardConfigManager dashboardConfigDAO;
  protected IngraphDashboardConfigManager ingraphDashboardConfigDAO;
  protected IngraphMetricConfigManager ingraphMetricConfigDAO;
  protected OverrideConfigManager overrideConfigDAO;
  protected AlertConfigManager alertConfigDAO;
  protected DataCompletenessConfigManager dataCompletenessConfigDAO;
  protected EventManager eventManager;

  protected TestUtils testUtils;
  protected DAORegistry daoRegistry;

  protected void init() {
    testUtils = TestUtils.setupDAO();
    daoRegistry = testUtils.getTestDaoRegistry();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    rawAnomalyResultDAO = daoRegistry.getRawAnomalyResultDAO();
    jobDAO = daoRegistry.getJobDAO();
    taskDAO = daoRegistry.getTaskDAO();
    emailConfigurationDAO = daoRegistry.getEmailConfigurationDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    metricConfigDAO = daoRegistry.getMetricConfigDAO();
    dashboardConfigDAO = daoRegistry.getDashboardConfigDAO();
    ingraphDashboardConfigDAO = daoRegistry.getIngraphDashboardConfigDAO();
    ingraphMetricConfigDAO = daoRegistry.getIngraphMetricConfigDAO();
    overrideConfigDAO = daoRegistry.getOverrideConfigDAO();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
    dataCompletenessConfigDAO = daoRegistry.getDataCompletenessConfigDAO();
    eventManager = daoRegistry.getEventDAO();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
  }

  protected void cleanup() {
    TestUtils.teardownDAO(testUtils);
  }

  protected AnomalyFunctionDTO getTestFunctionSpec(String metricName, String collection) {
    AnomalyFunctionDTO functionSpec = new AnomalyFunctionDTO();
    functionSpec.setFunctionName("integration test function 1");
    functionSpec.setType("WEEK_OVER_WEEK_RULE");
    functionSpec.setTopicMetric(metricName);
    functionSpec.setMetrics(Arrays.asList(metricName));
    functionSpec.setCollection(collection);
    functionSpec.setMetricFunction(MetricAggFunction.SUM);
    functionSpec.setCron("0/10 * * * * ?");
    functionSpec.setBucketSize(1);
    functionSpec.setBucketUnit(TimeUnit.HOURS);
    functionSpec.setWindowDelay(3);
    functionSpec.setWindowDelayUnit(TimeUnit.HOURS);
    functionSpec.setWindowSize(1);
    functionSpec.setWindowUnit(TimeUnit.DAYS);
    functionSpec.setProperties("baseline=w/w;changeThreshold=0.001");
    functionSpec.setIsActive(true);
    return functionSpec;
  }

  protected AlertConfigDTO getTestAlertConfiguration(String name) {
    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setName(name);
    alertConfigDTO.setActive(true);
    alertConfigDTO.setFromAddress("te@linkedin.com");
    alertConfigDTO.setRecipients("anomaly@linedin.com");
    alertConfigDTO.setCronExpression("0/10 * * * * ?");
    AlertConfigBean.EmailConfig emailConfig = new AlertConfigBean.EmailConfig();
    emailConfig.setAnomalyWatermark(0l);
    alertConfigDTO.setEmailConfig(emailConfig);
    AlertConfigBean.ReportConfigCollection
        reportConfigCollection = new AlertConfigBean.ReportConfigCollection();
    reportConfigCollection.setEnabled(true);
    alertConfigDTO.setReportConfigCollection(reportConfigCollection);
    return alertConfigDTO;
  }

  protected EmailConfigurationDTO getTestEmailConfiguration(String metricName, String collection) {
    EmailConfigurationDTO emailConfiguration = new EmailConfigurationDTO();
    emailConfiguration.setCollection(collection);
    emailConfiguration.setActive(true);
    emailConfiguration.setCron("0/10 * * * * ?");
    emailConfiguration.setFromAddress("thirdeye@linkedin.com");
    emailConfiguration.setMetric(metricName);
    emailConfiguration.setSendZeroAnomalyEmail(true);
    emailConfiguration.setToAddresses("anomaly@linkedin.com");
    emailConfiguration.setWindowDelay(2);
    emailConfiguration.setWindowSize(10);
    emailConfiguration.setWindowUnit(TimeUnit.HOURS);
    emailConfiguration.setWindowDelayUnit(TimeUnit.HOURS);
    return emailConfiguration;
  }

  protected RawAnomalyResultDTO getAnomalyResult() {
    RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
    anomalyResult.setScore(1.1);
    anomalyResult.setStartTime(System.currentTimeMillis());
    anomalyResult.setEndTime(System.currentTimeMillis());
    anomalyResult.setWeight(10.1);
    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("dimensionName", "dimensionValue");
    anomalyResult.setDimensions(dimensionMap);
    anomalyResult.setCreationTimeUtc(System.currentTimeMillis());
    return anomalyResult;
  }

  JobDTO getTestJobSpec() {
    JobDTO jobSpec = new JobDTO();
    jobSpec.setJobName("Test_Anomaly_Job");
    jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setWindowStartTime(new DateTime().minusHours(20).getMillis());
    jobSpec.setWindowEndTime(new DateTime().minusHours(10).getMillis());
    return jobSpec;
  }

  protected DatasetConfigDTO getTestDatasetConfig(String collection) {
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(collection);
    datasetConfigDTO.setDimensions(Lists.newArrayList("country", "browser", "environment"));
    datasetConfigDTO.setTimeColumn("time");
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeUnit(TimeUnit.HOURS);
    datasetConfigDTO.setActive(true);
    datasetConfigDTO.setRequiresCompletenessCheck(false);
    return datasetConfigDTO;
  }

  protected MetricConfigDTO getTestMetricConfig(String collection, String metric, Long id) {
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    if (id != null) {
      metricConfigDTO.setId(id);
    }
    metricConfigDTO.setDataset(collection);
    metricConfigDTO.setDatatype(MetricType.LONG);
    metricConfigDTO.setName(metric);
    metricConfigDTO.setAlias(ThirdEyeUtils.constructMetricAlias(collection, metric));
    return metricConfigDTO;
  }

  protected IngraphMetricConfigDTO getTestIngraphMetricConfig(String rrd, String metric, String dashboard) {
    IngraphMetricConfigDTO ingraphMetricConfigDTO = new IngraphMetricConfigDTO();
    ingraphMetricConfigDTO.setRrdName(rrd);
    ingraphMetricConfigDTO.setMetricName(metric);
    ingraphMetricConfigDTO.setDashboardName(dashboard);
    ingraphMetricConfigDTO.setContainer("test");
    ingraphMetricConfigDTO.setMetricDataType("test");
    ingraphMetricConfigDTO.setMetricSourceType("test");
    return ingraphMetricConfigDTO;
  }

  protected IngraphDashboardConfigDTO getTestIngraphDashboardConfig(String name) {
    IngraphDashboardConfigDTO ingraphDashboardConfigDTO = new IngraphDashboardConfigDTO();
    ingraphDashboardConfigDTO.setName(name);
    ingraphDashboardConfigDTO.setFabricGroup("test");
    ingraphDashboardConfigDTO.setFetchIntervalPeriod(3600_000);
    ingraphDashboardConfigDTO.setMergeNumAvroRecords(100);
    ingraphDashboardConfigDTO.setGranularitySize(5);
    ingraphDashboardConfigDTO.setGranularityUnit(TimeUnit.MINUTES);
    ingraphDashboardConfigDTO.setBootstrap(true);
    DateTime now = new DateTime();
    ingraphDashboardConfigDTO.setBootstrapStartTime(now.getMillis());
    ingraphDashboardConfigDTO.setBootstrapEndTime(now.minusDays(30).getMillis());
    return ingraphDashboardConfigDTO;
  }

  protected OverrideConfigDTO getTestOverrideConfigForTimeSeries(DateTime now) {
    OverrideConfigDTO overrideConfigDTO = new OverrideConfigDTO();
    overrideConfigDTO.setStartTime(now.minusHours(8).getMillis());
    overrideConfigDTO.setEndTime(now.plusHours(8).getMillis());
    overrideConfigDTO.setTargetEntity(OverrideConfigHelper.ENTITY_TIME_SERIES);
    overrideConfigDTO.setActive(true);

    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(ScalingFactor.SCALING_FACTOR, "1.2");
    overrideConfigDTO.setOverrideProperties(overrideProperties);

    Map<String, List<String>> overrideTarget = new HashMap<>();
    overrideTarget.put(OverrideConfigHelper.TARGET_COLLECTION, Arrays.asList("collection1",
        "collection2"));
    overrideTarget.put(OverrideConfigHelper.EXCLUDED_COLLECTION, Arrays.asList("collection3"));
    overrideConfigDTO.setTargetLevel(overrideTarget);

    return overrideConfigDTO;
  }

  protected DataCompletenessConfigDTO getTestDataCompletenessConfig(String dataset, long dateToCheckInMS,
      String dateToCheckInSDF, boolean dataComplete) {
    DataCompletenessConfigDTO dataCompletenessConfigDTO = new DataCompletenessConfigDTO();
    dataCompletenessConfigDTO.setDataset(dataset);
    dataCompletenessConfigDTO.setDateToCheckInMS(dateToCheckInMS);
    dataCompletenessConfigDTO.setDateToCheckInSDF(dateToCheckInSDF);
    dataCompletenessConfigDTO.setDataComplete(dataComplete);
    dataCompletenessConfigDTO.setCountStar(2000);
    dataCompletenessConfigDTO.setPercentComplete(79);
    dataCompletenessConfigDTO.setNumAttempts(3);
    return dataCompletenessConfigDTO;
  }
}
