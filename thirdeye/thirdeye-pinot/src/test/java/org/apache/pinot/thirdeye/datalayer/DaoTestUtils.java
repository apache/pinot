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

package org.apache.pinot.thirdeye.datalayer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.alert.commons.AnomalyFeedConfig;
import org.apache.pinot.thirdeye.alert.commons.AnomalyFetcherConfig;
import org.apache.pinot.thirdeye.alert.commons.AnomalyNotifiedStatus;
import org.apache.pinot.thirdeye.alert.commons.AnomalySource;
import org.apache.pinot.thirdeye.anomaly.job.JobConstants;
import org.apache.pinot.thirdeye.anomaly.override.OverrideConfigHelper;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.anomaly.utils.EmailUtils;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AlertSnapshotDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ClassificationConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionStatusDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EntityToEntityMappingDTO;
import org.apache.pinot.thirdeye.datalayer.dto.JobDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.OnboardDatasetMetricDTO;
import org.apache.pinot.thirdeye.datalayer.dto.OverrideConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.RootcauseSessionDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator;
import org.apache.pinot.thirdeye.detection.yaml.translator.SubscriptionConfigTranslator;
import org.apache.pinot.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import org.apache.pinot.thirdeye.detector.metric.transfer.ScalingFactor;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;

import static org.apache.pinot.thirdeye.detection.alert.StatefulDetectionAlertFilter.*;


public class DaoTestUtils {

  public static DetectionConfigDTO getTestDetectionConfig(DataProvider provider, String detectionConfigFile) throws
                                                                                                             IOException {
    String yamlConfig = IOUtils.toString(DaoTestUtils.class.getResourceAsStream(detectionConfigFile));

    // Translate
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yamlConfig, provider);
    DetectionConfigDTO detectionConfig = translator.translate();

    Map<String, Object> properties = detectionConfig.getProperties();
    properties.put("timezone", "UTC");
    detectionConfig.setProperties(properties);
    detectionConfig.setCron("0/10 * * * * ?");

    DetectionConfigValidator validator = new DetectionConfigValidator(provider);
    validator.validateConfig(detectionConfig);

    detectionConfig.setLastTimestamp(DateTime.now().minusDays(2).getMillis());
    return detectionConfig;
  }

  public static DetectionAlertConfigDTO getTestDetectionAlertConfig(DetectionConfigManager detectionConfigManager, String alertConfigFile) throws IOException {

    String yamlConfig = IOUtils.toString(DaoTestUtils.class.getResourceAsStream(alertConfigFile));

    DetectionAlertConfigDTO alertConfig = new SubscriptionConfigTranslator(
        detectionConfigManager, yamlConfig).translate();
    alertConfig.setCronExpression("0/10 * * * * ?");
    return alertConfig;
  }

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
    functionSpec.setProperties("baseline=w/w;changeThreshold=0.001;min=100;max=900");
    functionSpec.setIsActive(true);
    functionSpec.setRequiresCompletenessCheck(false);
    functionSpec.setSecondaryAnomalyFunctionsType(Arrays.asList("MIN_MAX_THRESHOLD"));
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

  public static DetectionAlertConfigDTO getTestNotificationConfig(String name) {
    DetectionAlertConfigDTO notificationConfigDTO = new DetectionAlertConfigDTO();
    notificationConfigDTO.setName(name);
    notificationConfigDTO.setActive(true);
    notificationConfigDTO.setApplication("test");
    notificationConfigDTO.setFrom("te@linkedin.com");
    notificationConfigDTO.setCronExpression("0/10 * * * * ?");

    Map<String, Object> properties = new HashMap<>();
    Map<String, Set<String>> recipients = new HashMap<>();
    recipients.put(PROP_TO, Collections.singleton("anomaly-to@linedin.com"));
    recipients.put(PROP_CC, Collections.singleton("anomaly-cc@linedin.com"));
    recipients.put(PROP_BCC, Collections.singleton("anomaly-bcc@linedin.com"));
    properties.put(PROP_RECIPIENTS, recipients);
    notificationConfigDTO.setProperties(properties);

    Map<Long, Long> vectorClocks = new HashMap<>();
    notificationConfigDTO.setVectorClocks(vectorClocks);

    return notificationConfigDTO;
  }

  public static AlertConfigDTO getTestAlertConfiguration(String name) {
    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setName(name);
    alertConfigDTO.setActive(true);
    alertConfigDTO.setApplication("test");
    alertConfigDTO.setFromAddress("te@linkedin.com");

    DetectionAlertFilterRecipients recipients = new DetectionAlertFilterRecipients(
        EmailUtils.getValidEmailAddresses("anomaly-to@linedin.com"),
        EmailUtils.getValidEmailAddresses("anomaly-cc@linedin.com"),
        EmailUtils.getValidEmailAddresses("anomaly-bcc@linedin.com"));
    alertConfigDTO.setReceiverAddresses(recipients);
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

  public static JobDTO getTestJobSpec() {
    JobDTO jobSpec = new JobDTO();
    jobSpec.setJobName("Test_Anomaly_Job");
    jobSpec.setStatus(JobConstants.JobStatus.SCHEDULED);
    jobSpec.setTaskType(TaskConstants.TaskType.DETECTION);
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
    datasetConfigDTO.setDataSource(PinotThirdEyeDataSource.class.getSimpleName());
    datasetConfigDTO.setLastRefreshTime(System.currentTimeMillis());
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
    anomaly.setDetectionConfigId(functionId);
    anomaly.setCreatedTime(createdTime);

    return anomaly;
  }

  public static MergedAnomalyResultDTO getTestGroupedAnomalyResult(long startTime, long endTime, long createdTime, long id) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(startTime);
    anomaly.setEndTime(endTime);
    anomaly.setCollection(null);
    anomaly.setMetric(null);
    anomaly.setCreatedTime(createdTime);
    anomaly.setDetectionConfigId(id);

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
