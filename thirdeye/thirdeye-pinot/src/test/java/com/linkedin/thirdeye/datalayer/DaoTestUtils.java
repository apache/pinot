package com.linkedin.thirdeye.datalayer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.alert.commons.AnomalyFeedConfig;
import com.linkedin.thirdeye.alert.commons.AnomalyFetcherConfig;
import com.linkedin.thirdeye.alert.commons.AnomalyNotifiedStatus;
import com.linkedin.thirdeye.alert.commons.AnomalySource;
import com.linkedin.thirdeye.anomaly.job.JobConstants;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.ConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RootcauseSessionDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;


public class DaoTestUtils {
  public static AnomalyFunctionDTO getTestFunctionSpec(String metricName, String collection) {
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
    functionSpec.setRequiresCompletenessCheck(false);
    return functionSpec;
  }

  public static AnomalyFunctionDTO getTestFunctionAlphaBetaAlertFilterSpec(String metricName, String collection){
    AnomalyFunctionDTO functionSpec = getTestFunctionSpec(metricName, collection);
    Map<String, String> alphaBetaAlertFilter = new HashMap<>();
    alphaBetaAlertFilter.put("type", "alpha_beta");
    alphaBetaAlertFilter.put(AlphaBetaAlertFilter.ALPHA, "1");
    alphaBetaAlertFilter.put(AlphaBetaAlertFilter.BETA, "1");
    alphaBetaAlertFilter.put(AlphaBetaAlertFilter.THRESHOLD, "0.5");
    functionSpec.setAlertFilter(alphaBetaAlertFilter);
    return functionSpec;
  }

  public static AlertConfigDTO getTestAlertConfiguration(String name) {
    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setName(name);
    alertConfigDTO.setActive(true);
    alertConfigDTO.setFromAddress("te@linkedin.com");
    alertConfigDTO.setRecipients("anomaly@linedin.com");
    alertConfigDTO.setCronExpression("0/10 * * * * ?");
    AlertConfigBean.EmailConfig emailConfig = new AlertConfigBean.EmailConfig();
    emailConfig.setAnomalyWatermark(0l);
    alertConfigDTO.setEmailConfig(emailConfig);
    AlertConfigBean.ReportConfigCollection reportConfigCollection =
        new AlertConfigBean.ReportConfigCollection();
    reportConfigCollection.setEnabled(true);
    alertConfigDTO.setReportConfigCollection(reportConfigCollection);
    return alertConfigDTO;
  }

  public static ClassificationConfigDTO getTestGroupingConfiguration(List<Long> mainFunctionIdList) {
    ClassificationConfigDTO configDTO = new ClassificationConfigDTO();
    configDTO.setName("classificationJob");
    configDTO.setMainFunctionIdList(mainFunctionIdList);
    configDTO.setAuxFunctionIdList(mainFunctionIdList);
    configDTO.setActive(true);
    return configDTO;
  }

  public static RawAnomalyResultDTO getAnomalyResult() {
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

  public static JobDTO getTestJobSpec() {
    JobDTO jobSpec = new JobDTO();
    jobSpec.setJobName("Test_Anomaly_Job");
    jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
    jobSpec.setTaskType(TaskConstants.TaskType.ANOMALY_DETECTION);
    jobSpec.setScheduleStartTime(System.currentTimeMillis());
    jobSpec.setWindowStartTime(new DateTime().minusHours(20).getMillis());
    jobSpec.setWindowEndTime(new DateTime().minusHours(10).getMillis());
    jobSpec.setConfigId(100);
    return jobSpec;
  }

  public static DatasetConfigDTO getTestDatasetConfig(String collection) {
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

  public static MetricConfigDTO getTestMetricConfig(String collection, String metric, Long id) {
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

  public static OverrideConfigDTO getTestOverrideConfigForTimeSeries(DateTime now) {
    OverrideConfigDTO overrideConfigDTO = new OverrideConfigDTO();
    overrideConfigDTO.setStartTime(now.minusHours(8).getMillis());
    overrideConfigDTO.setEndTime(now.plusHours(8).getMillis());
    overrideConfigDTO.setTargetEntity(OverrideConfigHelper.ENTITY_TIME_SERIES);
    overrideConfigDTO.setActive(true);

    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(ScalingFactor.SCALING_FACTOR, "1.2");
    overrideConfigDTO.setOverrideProperties(overrideProperties);

    Map<String, List<String>> overrideTarget = new HashMap<>();
    overrideTarget
        .put(OverrideConfigHelper.TARGET_COLLECTION, Arrays.asList("collection1", "collection2"));
    overrideTarget.put(OverrideConfigHelper.EXCLUDED_COLLECTION, Arrays.asList("collection3"));
    overrideConfigDTO.setTargetLevel(overrideTarget);

    return overrideConfigDTO;
  }

  public static DataCompletenessConfigDTO getTestDataCompletenessConfig(String dataset,
      long dateToCheckInMS, String dateToCheckInSDF, boolean dataComplete) {
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

  public static DetectionStatusDTO getTestDetectionStatus(String dataset, long dateToCheckInMS,
      String dateToCheckInSDF, boolean detectionRun, long functionId) {
    DetectionStatusDTO detectionStatusDTO = new DetectionStatusDTO();
    detectionStatusDTO.setDataset(dataset);
    detectionStatusDTO.setFunctionId(functionId);
    detectionStatusDTO.setDateToCheckInMS(dateToCheckInMS);
    detectionStatusDTO.setDateToCheckInSDF(dateToCheckInSDF);
    detectionStatusDTO.setDetectionRun(detectionRun);
    return detectionStatusDTO;
  }

  public static AutotuneConfigDTO getTestAutotuneConfig(long functionId, long start, long end) {
    AutotuneConfigDTO autotuneConfigDTO = new AutotuneConfigDTO();
    autotuneConfigDTO.setFunctionId(functionId);
    autotuneConfigDTO.setStartTime(start);
    autotuneConfigDTO.setEndTime(end);
    autotuneConfigDTO.setPerformanceEvaluationMethod(PerformanceEvaluationMethod.ANOMALY_PERCENTAGE);
    autotuneConfigDTO.setLastUpdateTimestamp(DateTime.now().getMillis());
    Map<String, String> config = new HashMap<>();
    config.put("ConfigKey", "ConfigValue");
    autotuneConfigDTO.setConfiguration(config);
    Map<String, Double> performance = new HashMap<>();
    performance.put(autotuneConfigDTO.getPerformanceEvaluationMethod().name(), 0.5);
    autotuneConfigDTO.setPerformance(performance);
    return autotuneConfigDTO;
  }

  public static ClassificationConfigDTO getTestClassificationConfig(String name, List<Long> mainFunctionIdList,
      List<Long> functionIds) {
    ClassificationConfigDTO classificationConfigDTO = new ClassificationConfigDTO();
    classificationConfigDTO.setName(name);
    classificationConfigDTO.setMainFunctionIdList(mainFunctionIdList);
    classificationConfigDTO.setAuxFunctionIdList(functionIds);
    classificationConfigDTO.setActive(true);
    return classificationConfigDTO;
  }

  public static EntityToEntityMappingDTO getTestEntityToEntityMapping(String fromURN, String toURN, String mappingType) {
    EntityToEntityMappingDTO dto = new EntityToEntityMappingDTO();
    dto.setFromURN(fromURN);
    dto.setToURN(toURN);
    dto.setMappingType(mappingType);
    dto.setScore(1);
    return dto;
  }

  public static OnboardDatasetMetricDTO getTestOnboardConfig(String datasetName, String metricName, String dataSource) {
    OnboardDatasetMetricDTO dto = new OnboardDatasetMetricDTO();
    dto.setDatasetName(datasetName);
    dto.setMetricName(metricName);
    dto.setDataSource(dataSource);
    return dto;
  }

  public static ConfigDTO getTestConfig(String namespace, String name, Object value) {
    ConfigDTO dto = new ConfigDTO();
    dto.setNamespace(namespace);
    dto.setName(name);
    dto.setValue(value);
    return dto;
  }

  public static AlertSnapshotDTO getTestAlertSnapshot(){
    AlertSnapshotDTO alertSnapshot = new AlertSnapshotDTO();
    alertSnapshot.setLastNotifyTime(0);
    Multimap<String, AnomalyNotifiedStatus> snapshot = HashMultimap.create();
    snapshot.put("test::{dimension=[test]}", new AnomalyNotifiedStatus(0,-0.1));
    snapshot.put("test::{dimension=[test]}", new AnomalyNotifiedStatus(2,-0.2));
    snapshot.put("test::{dimension=[test2]}", new AnomalyNotifiedStatus(4,-0.4));
    alertSnapshot.setSnapshot(snapshot);
    return alertSnapshot;
  }

  public static AnomalyFeedConfig getTestAnomalyFeedConfig() {
    AnomalyFeedConfig anomalyFeedConfig = new AnomalyFeedConfig();
    anomalyFeedConfig.setAnomalyFeedType("UnionAnomalyFeed");
    anomalyFeedConfig.setAnomalySourceType(AnomalySource.METRIC);
    anomalyFeedConfig.setAnomalySource("test");
    anomalyFeedConfig.setAlertSnapshotId(1l);

    AnomalyFetcherConfig anomalyFetcherConfig = getTestAnomalyFetcherConfig();
    List<AnomalyFetcherConfig> fetcherConfigs = new ArrayList<>();
    fetcherConfigs.add(anomalyFetcherConfig);
    anomalyFeedConfig.setAnomalyFetcherConfigs(fetcherConfigs);

    Map<String, String> alertFilterConfig = new HashMap<>();
    alertFilterConfig.put(AlphaBetaAlertFilter.TYPE, "DUMMY");
    alertFilterConfig.put("properties", "");
    List<Map<String, String>> filterConfigs = new ArrayList<>();
    filterConfigs.add(alertFilterConfig);
    anomalyFeedConfig.setAlertFilterConfigs(filterConfigs);

    return anomalyFeedConfig;
  }

  public static AnomalyFetcherConfig getTestAnomalyFetcherConfig(){
    AnomalyFetcherConfig anomalyFetcherConfig = new AnomalyFetcherConfig();
    anomalyFetcherConfig.setType("UnnotifiedAnomalyFetcher");
    anomalyFetcherConfig.setProperties("");
    anomalyFetcherConfig.setAnomalySourceType(AnomalySource.METRIC);
    anomalyFetcherConfig.setAnomalySource("test");
    return anomalyFetcherConfig;
  }

  public static MergedAnomalyResultDTO getTestMergedAnomalyResult(long startTime, long endTime, String collection,
      String metric, double weight, long functionId, long createdTime) {
    // Add mock anomalies
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(startTime);
    anomaly.setEndTime(endTime);
    anomaly.setCollection(collection);
    anomaly.setMetric(metric);
    anomaly.setWeight(weight);
    anomaly.setFunctionId(functionId);
    anomaly.setCreatedTime(createdTime);

    return anomaly;
  }

  public static RootcauseSessionDTO getTestRootcauseSessionResult(long start, long end, long created, long updated,
      String name, String owner, String text, String granularity, String compareMode, Long previousId, Long anomalyId) {
    RootcauseSessionDTO session = new RootcauseSessionDTO();
    session.setAnomalyRangeStart(start);
    session.setAnomalyRangeEnd(end);
    session.setAnalysisRangeStart(start - 100);
    session.setAnalysisRangeEnd(end + 100);
    session.setName(name);
    session.setOwner(owner);
    session.setText(text);
    session.setPreviousId(previousId);
    session.setAnomalyId(anomalyId);
    session.setCreated(created);
    session.setUpdated(updated);
    session.setGranularity(granularity);
    session.setCompareMode(compareMode);
    return session;
  }
}
