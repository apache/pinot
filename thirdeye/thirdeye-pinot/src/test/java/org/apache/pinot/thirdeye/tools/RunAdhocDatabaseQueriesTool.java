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

package org.apache.pinot.thirdeye.tools;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.ClassificationConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionStatusManager;
import org.apache.pinot.thirdeye.datalayer.bao.JobManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.OverrideConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.RawAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ApplicationManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.ClassificationConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionAlertConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.DetectionStatusManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.TaskManagerImpl;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ClassificationConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionStatusDTO;
import org.apache.pinot.thirdeye.datalayer.dto.JobDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.OverrideConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datalayer.util.DaoProviderUtil;

import org.apache.pinot.thirdeye.datalayer.util.Predicate;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.Set;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DefaultDataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Run adhoc queries to db
 */
public class RunAdhocDatabaseQueriesTool {

  private static final Logger LOG = LoggerFactory.getLogger(RunAdhocDatabaseQueriesTool.class);

  private DetectionConfigManager detectionConfigDAO;
  private DetectionAlertConfigManager detectionAlertConfigDAO;
  private EventManagerImpl eventDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawResultDAO;
  private MergedAnomalyResultManager mergedResultDAO;
  private MetricConfigManager metricConfigDAO;
  private OverrideConfigManager overrideConfigDAO;
  private JobManager jobDAO;
  private TaskManager taskDAO;
  private DataCompletenessConfigManager dataCompletenessConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private DetectionStatusManager detectionStatusDAO;
  private AlertConfigManager alertConfigDAO;
  private ClassificationConfigManager classificationConfigDAO;
  private ApplicationManager applicationDAO;

  private static final DateTimeZone TIMEZONE = DateTimeZone.forID("America/Los_Angeles");
  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("YYYY-MM-dd");
  private static final String ENTITY_STATS_TEMPLATE = "#children = {}, properties = {}";
  private static final String ENTITY_TIME_TEMPLATE = "[create {}, start {}, end {}]";

  public RunAdhocDatabaseQueriesTool(File persistenceFile)
      throws Exception {
    init(persistenceFile);
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    detectionConfigDAO = DaoProviderUtil.getInstance(DetectionConfigManagerImpl.class);
    detectionAlertConfigDAO = DaoProviderUtil.getInstance(DetectionAlertConfigManagerImpl.class);
    eventDAO = DaoProviderUtil.getInstance(EventManagerImpl.class);
    anomalyFunctionDAO = DaoProviderUtil.getInstance(AnomalyFunctionManagerImpl.class);
    rawResultDAO = DaoProviderUtil.getInstance(RawAnomalyResultManagerImpl.class);
    mergedResultDAO = DaoProviderUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    metricConfigDAO = DaoProviderUtil.getInstance(MetricConfigManagerImpl.class);
    overrideConfigDAO = DaoProviderUtil.getInstance(OverrideConfigManagerImpl.class);
    jobDAO = DaoProviderUtil.getInstance(JobManagerImpl.class);
    taskDAO = DaoProviderUtil.getInstance(TaskManagerImpl.class);
    dataCompletenessConfigDAO = DaoProviderUtil.getInstance(DataCompletenessConfigManagerImpl.class);
    datasetConfigDAO = DaoProviderUtil.getInstance(DatasetConfigManagerImpl.class);
    detectionStatusDAO = DaoProviderUtil.getInstance(DetectionStatusManagerImpl.class);
    alertConfigDAO = DaoProviderUtil.getInstance(AlertConfigManagerImpl.class);
    classificationConfigDAO = DaoProviderUtil.getInstance(ClassificationConfigManagerImpl.class);
    applicationDAO = DaoProviderUtil.getInstance(ApplicationManagerImpl.class);
  }

  private void toggleAnomalyFunction(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    anomalyFunction.setActive(true);
    anomalyFunctionDAO.update(anomalyFunction);
  }

  /**
   * Removes the specified anomaly function and its anomalies and alert configs.
   *
   * @param id the id of the anomaly function.
   */
  private void deleteAnomalyFunction(long id) {
    // Disable anomaly function first
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    anomalyFunction.setActive(false);
    anomalyFunctionDAO.save(anomalyFunction);

    // Remove anomaly function from alerts
    List<AlertConfigDTO> alerts = alertConfigDAO.findByFunctionId(id);
    LOG.info("Updating {} alert configs that depends on function {}.", alerts.size(), id);
    int deleteCounter = 0;
    for (AlertConfigDTO alert : alerts) {
      AlertConfigBean.EmailConfig emailConfig = alert.getEmailConfig();
      List<Long> functionIds = emailConfig.getFunctionIds();
      functionIds.remove(Long.valueOf(id));
      if (functionIds.size() == 0) {
        LOG.info("Removing alert configs {}.", alert.getId());
        alertConfigDAO.delete(alert);
        deleteCounter++;
      } else {
        alertConfigDAO.save(alert);
      }
    }
    if (deleteCounter != 0) {
      LOG.info("Removed {} alert configs that depends on function {}.", deleteCounter, id);
    }

    Predicate predicate = Predicate.EQ("functionId", id);
    // Delete merged anomaly that is associated with the specified anomaly function
    LOG.info("Deleting {} merged anomalies of function {}.", mergedResultDAO.findIdsByPredicate(predicate).size(), id);
    List<MergedAnomalyResultDTO> mergedAnomalyResults = mergedResultDAO.findByFunctionId(id);
    for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
      // Delete feedback of this merged anomaly
      if (mergedAnomalyResult.getFeedback() != null) {
        AnomalyFeedbackDTO feedback = (AnomalyFeedbackDTO) mergedAnomalyResult.getFeedback();
        mergedResultDAO.deleteById(feedback.getId());
      }
    }
    mergedResultDAO.deleteByPredicate(predicate);
    assert (mergedResultDAO.findIdsByPredicate(predicate).size() == 0);
    // Delete raw anomaly that is associated with the specified anoamly function
    LOG.info("Deleted {} raw anomalies of function {}.", rawResultDAO.deleteByPredicate(predicate), id);
    assert (rawResultDAO.findIdsByPredicate(predicate).size() == 0);
    // Delete detection status
    LOG.info("Deleted {} detection status of funtion {}.", detectionStatusDAO.deleteByPredicate(predicate), id);
    assert (detectionStatusDAO.findIdsByPredicate(predicate).size() == 0);
    // Delete the specified anomaly function
    anomalyFunctionDAO.deleteById(id);
    LOG.info("Deleted function {}.", id);
  }

  /**
   * Clean up anomaly functions whose name like the given string.
   *
   * @param charSequence the given string.
   * @param excludingSet the id of the anomaly function to be excluded from the clean up.
   */
  private void deleteAnomalyFunctioWhereNameLike(String charSequence, Set<Long> excludingSet) {
    if (excludingSet == null) {
      excludingSet = Collections.emptySet();
    }
    int count = 0;
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findWhereNameLike(charSequence);
    LOG.info("Found {} functions whose name like {}.", anomalyFunctions.size(), charSequence);
    List<Long> deletedFunctions = new ArrayList<>();
    for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
      long functionId = anomalyFunction.getId();
      if (!excludingSet.contains(functionId)) {
        LOG.info("Deleting function {} name={}", functionId, anomalyFunction.getFunctionName());
        deleteAnomalyFunction(functionId);
        deletedFunctions.add(functionId);
        ++count;
      } else {
        LOG.info("Excluding function {} name={}", functionId, anomalyFunction.getFunctionName());
      }
    }
    LOG.info("Deleted {} functions: {}", count, deletedFunctions);
  }

  /**
   * Delete the specified dataset. The anomaly functions, raw anomalies, merged anomalies, data completeness
   * entries, detection status entries, and metrics of the dataset will be deleted too.
   *
   * @param datasetName the dataset to be deleted.
   */
  private void deleteDataset(String datasetName) {
    DatasetConfigDTO dataset = datasetConfigDAO.findByDataset(datasetName);
    if (dataset == null) {
      return;
    }
    LOG.info("Deleting dataset {}, name={}.", dataset.getId(), datasetName);
    // Disable anomaly functions
    List<AnomalyFunctionDTO> functions = anomalyFunctionDAO.findAllByCollection(datasetName);
    for (AnomalyFunctionDTO function : functions) {
      function.setActive(false);
      anomalyFunctionDAO.save(function);
    }
    Predicate predicate = Predicate.EQ("dataset", datasetName);
    // Delete datacompleteness entries
    LOG.info("Deleted {} data completeness.", dataCompletenessConfigDAO.deleteByPredicate(predicate));
    assert (dataCompletenessConfigDAO.findIdsByPredicate(predicate).size() == 0);
    // Delete detection status
    LOG.info("Deleted {} detection status.", detectionStatusDAO.deleteByPredicate(predicate));
    assert (detectionStatusDAO.findIdsByPredicate(predicate).size() == 0);
    // Delete the anomaly functions, raw and merged anomalies, feedbacks that are associate with the specified dataset
    LOG.info("Deleting {} functions.", functions.size());
    for (AnomalyFunctionDTO function : functions) {
      deleteAnomalyFunction(function.getId());
    }
    // Delete metrics of the specified dataset
    LOG.info("Deleted {} metrics.", metricConfigDAO.deleteByPredicate(predicate));
    assert (metricConfigDAO.findIdsByPredicate(predicate).size() == 0);
    // Delete dataset
    datasetConfigDAO.deleteById(dataset.getId());
    LOG.info("Deleted dataset {}, name={}.", dataset.getId(), datasetName);
  }

  private void updateField(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    //anomalyFunction.setCronExpression("0/10 * * * * ?");
    anomalyFunction.setActive(true);
    anomalyFunctionDAO.update(anomalyFunction);
  }

  private void customFunction() {
    List<AnomalyFunctionDTO> anomalyFunctionDTOs = anomalyFunctionDAO.findAll();
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctionDTOs) {
      anomalyFunctionDTO.setActive(false);
      anomalyFunctionDAO.update(anomalyFunctionDTO);
    }
  }

  private void updateNotified() {
    List<MergedAnomalyResultDTO> mergedResults = mergedResultDAO.findAll();
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setNotified(true);
      mergedResultDAO.update(mergedResult);
    }
  }


  private void setAlertFilterForFunctionInCollection(String collection, List<String> metricList,
      Map<String, Map<String, String>> metricRuleMap, Map<String, String> defaultAlertFilter) {
    List<AnomalyFunctionDTO> anomalyFunctionDTOs =
        anomalyFunctionDAO.findAllByCollection(collection);
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctionDTOs) {
      String topicMetricName = anomalyFunctionDTO.getTopicMetric();
      if (metricList.contains(topicMetricName)) {
        Map<String, String> alertFilter = defaultAlertFilter;
        if (metricRuleMap.containsKey(topicMetricName)) {
          alertFilter = metricRuleMap.get(topicMetricName);
        }
        anomalyFunctionDTO.setAlertFilter(alertFilter);
        anomalyFunctionDAO.update(anomalyFunctionDTO);
        LOG.info("Add alert filter {} to function {} (dataset: {}, topic metric: {})", alertFilter,
            anomalyFunctionDTO.getId(), collection, topicMetricName);
      }
    }
  }

  private Long createOverrideConfig(OverrideConfigDTO overrideConfigDTO) {
    // Check if there exist duplicate override config
    List<OverrideConfigDTO> existingOverrideConfigDTOs = overrideConfigDAO
        .findAllConflictByTargetType(overrideConfigDTO.getTargetEntity(),
            overrideConfigDTO.getStartTime(), overrideConfigDTO.getEndTime());

    for (OverrideConfigDTO existingOverrideConfig : existingOverrideConfigDTOs) {
      if (existingOverrideConfig.equals(overrideConfigDTO)) {
        LOG.warn("Exists a duplicate override config: {}", existingOverrideConfig.toString());
        return null;
      }
    }

    return overrideConfigDAO.save(overrideConfigDTO);
  }

  private void updateOverrideConfig(long id, OverrideConfigDTO overrideConfigDTO) {
    OverrideConfigDTO overrideConfigToUpdated = overrideConfigDAO.findById(id);
    if (overrideConfigToUpdated == null) {
      LOG.warn("Failed to update config {}", id);
    } else {
      overrideConfigToUpdated.setStartTime(overrideConfigDTO.getStartTime());
      overrideConfigToUpdated.setEndTime(overrideConfigDTO.getEndTime());
      overrideConfigToUpdated.setTargetLevel(overrideConfigDTO.getTargetLevel());
      overrideConfigToUpdated.setTargetEntity(overrideConfigDTO.getTargetEntity());
      overrideConfigToUpdated.setOverrideProperties(overrideConfigDTO.getOverrideProperties());
      overrideConfigToUpdated.setActive(overrideConfigDTO.getActive());
      overrideConfigDAO.update(overrideConfigToUpdated);
      LOG.info("Updated config {}" + id);
    }
  }


  private void deleteAllDataCompleteness() {
    for (DataCompletenessConfigDTO dto : dataCompletenessConfigDAO.findAll()) {
      dataCompletenessConfigDAO.delete(dto);
    }
  }

  private void disableAnomalyFunctions(String dataset) {
    List<AnomalyFunctionDTO> anomalyFunctionDTOs = anomalyFunctionDAO.findAllByCollection(dataset);
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctionDTOs) {
    anomalyFunctionDTO.setActive(false);
    anomalyFunctionDAO.update(anomalyFunctionDTO);
    }
  }

  private void addRequiresCompletenessCheck(List<String> datasets) {
    for (String dataset : datasets) {
      DatasetConfigDTO dto = datasetConfigDAO.findByDataset(dataset);
      dto.setActive(false);
      datasetConfigDAO.update(dto);
    }
  }

  private void updateDetectionRun(String dataset) {
    for (DetectionStatusDTO dto : detectionStatusDAO.findAll()) {
      if (dto.getDataset().equals(dataset)) {
        dto.setDetectionRun(false);
        detectionStatusDAO.update(dto);
      }
    }
  }

  private void enableDataCompleteness(String dataset) {
    List<DataCompletenessConfigDTO> dtos = dataCompletenessConfigDAO.findAllByDataset(dataset);
    for (DataCompletenessConfigDTO dto : dtos) {
      dto.setDataComplete(true);
      dataCompletenessConfigDAO.update(dto);
    }
  }

  private void disableAlertConfigs() {
    List<AlertConfigDTO> alertConfigs = alertConfigDAO.findAll();
    for (AlertConfigDTO alertConfigDTO : alertConfigs) {
      alertConfigDTO.setActive(false);
      alertConfigDAO.save(alertConfigDTO);
    }
  }

  private void createClassificationConfig(String name, List<Long> mainFunctionIdList, List<Long> functionIdList,
      boolean active) {
    ClassificationConfigDTO configDTO = new ClassificationConfigDTO();
    configDTO.setName(name);
    configDTO.setMainFunctionIdList(mainFunctionIdList);
    configDTO.setAuxFunctionIdList(functionIdList);
    configDTO.setActive(active);
    System.out.println(configDTO);
    classificationConfigDAO.save(configDTO);
  }

  private void updateJobIndex() {
    List<JobDTO> jobDTOs = jobDAO.findAll();
    for (JobDTO jobDTO : jobDTOs) {
      String name = jobDTO.getJobName();
      if (!name.contains("-")) {
        continue;
      }
      String[] names = name.split("-");
      try {
        long anomalyFunctionId = Long.parseLong(names[0]);
        jobDTO.setConfigId(anomalyFunctionId);
        jobDTO.setTaskType(TaskConstants.TaskType.DETECTION);
        jobDTO.setLastModified(new Timestamp(System.currentTimeMillis()));
        jobDAO.save(jobDTO);
      } catch (Exception e) {
        continue;
      }
    }
  }

  private void deactivateDataset(String dataset) {
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    for (MetricConfigDTO metricConfig : metricConfigs) {
      metricConfig.setActive(false);
      metricConfigDAO.update(metricConfig);
    }
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    datasetConfig.setActive(false);
    datasetConfigDAO.update(datasetConfig);
  }

  private void unsetMergedAnomalyNotifiedField(String dataset, long duration) {
    long windowEnd = System.currentTimeMillis();
    long windowStart = windowEnd - duration;

    List<MergedAnomalyResultDTO> mergedAnomalyResults =
        mergedResultDAO.findByCollectionTime(dataset, windowStart, windowEnd);
    LOG.info("{} anomalies to update for dataset {}", mergedAnomalyResults.size(), dataset);
    for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
      if (mergedAnomalyResult.isNotified()) {
        LOG.info((" Updating anomaly: {}"), mergedAnomalyResult.getId());
        mergedAnomalyResult.setNotified(false);
        mergedResultDAO.update(mergedAnomalyResult);
      }
    }
  }

  private void disableAllActiveDetections() {
    disableAllActiveDetections(null);
  }

  /**
   * Disable all detections and activate the exclusion list
   */
  private void disableAllActiveDetections(Collection<Long> excludeIds){
    List<DetectionConfigDTO> detections = detectionConfigDAO.findAll();
    for (DetectionConfigDTO detection : detections) {
      // Activate the whitelisted detections
      if (excludeIds.contains(detection.getId())) {
        detection.setActive(true);
        detectionConfigDAO.update(detection);
      } else {
        // Disable other configs
        if (detection.isActive()) {
          detection.setActive(false);
          detectionConfigDAO.update(detection);
        }
      }
    }
  }

  /**
   * Disable all subscription groups and activate the exclusion list
   */
  private void disableAllActiveSubsGroups(Collection<Long> excludeIds){
    List<DetectionAlertConfigDTO> subsConfigs = detectionAlertConfigDAO.findAll();
    for (DetectionAlertConfigDTO subsConfig : subsConfigs) {
      // Activate the whitelisted configs
      if (excludeIds.contains(subsConfig.getId())) {
        subsConfig.setActive(true);
        detectionAlertConfigDAO.update(subsConfig);
      } else {
        // Disable other configs
        if (subsConfig.isActive()) {
          subsConfig.setActive(false);
          detectionAlertConfigDAO.update(subsConfig);
        }
      }
    }
  }

  /**
   * Generates a report of the status and owner of all the un-subscribed anomaly functions
   */
  private void unsubscribedDetections(){
    List<ApplicationDTO> apps = applicationDAO.findAll();

    List<AnomalyFunctionDTO> allAnomalyFuncs = anomalyFunctionDAO.findAll();
    LOG.info("Total number of funcs: " + allAnomalyFuncs.size());

    Set<Long> allAnomalyFuncsIds = new HashSet<>();
    Set<Long> subscribedFuncsIds = new HashSet<>();
    for (AnomalyFunctionDTO anom : allAnomalyFuncs) {
      allAnomalyFuncsIds.add(anom.getId());
    }

    List<AnomalyFunctionDTO> subscribedFuncs = new ArrayList<>();
    for (ApplicationDTO app : apps) {
      List<AlertConfigDTO> alertConfigDTOS = alertConfigDAO.findByPredicate(Predicate.EQ("application", app.getApplication()));
      if (alertConfigDTOS != null) {
        for (AlertConfigDTO alertDTO : alertConfigDTOS) {
          if (alertDTO.getEmailConfig() != null) {
            if (alertDTO.getEmailConfig().getFunctionIds() != null) {
              for (long id : alertDTO.getEmailConfig().getFunctionIds()) {
                AnomalyFunctionDTO func = anomalyFunctionDAO.findById(id);
                if (func != null) {
                  subscribedFuncs.add(func);
                }
              }
            }
          }
        }
      }
    }

    for (AnomalyFunctionDTO anom : subscribedFuncs) {
      subscribedFuncsIds.add(anom.getId());
    }

    allAnomalyFuncsIds.removeAll(subscribedFuncsIds);

    for (long id : allAnomalyFuncsIds) {
      AnomalyFunctionDTO func = anomalyFunctionDAO.findById(id);
      System.out.println(String.format("%s\t%s\t%s\t%s\t%s", func.getId(), func.getFunctionName(), func.getIsActive(), func.getCreatedBy(), func.getUpdatedBy()));

    }
  }

  /**
   * Generates a report of the status, owner and recipients of all the subscription groups by application
   */
  private void notificationOwners(){
    List<ApplicationDTO> apps = applicationDAO.findAll();
    for (ApplicationDTO app : apps) {
      List<AlertConfigDTO> alertConfigDTOS = alertConfigDAO.findByPredicate(Predicate.EQ("application", app.getApplication()));
      for (AlertConfigDTO alertDTO : alertConfigDTOS) {
        String recipients = fetchRecipients(alertDTO.getReceiverAddresses());
        System.out.println(String.format("%s\t%s\t%s\t%s\t%s", app.getApplication(), alertDTO.getName(), alertDTO.isActive(), alertDTO.getCreatedBy(), recipients));
      }
    }
  }

  /**
   * Generates a report of the status, owner and recipients of all the subscribed anomaly functions by application
   */
  private void detectionOwners(){
    List<ApplicationDTO> apps = applicationDAO.findAll();

    for (ApplicationDTO app : apps) {
      List<AlertConfigDTO> alertConfigDTOS = alertConfigDAO.findByPredicate(Predicate.EQ("application", app.getApplication()));
      if (alertConfigDTOS != null) {
        for (AlertConfigDTO alertDTO : alertConfigDTOS) {
          if (alertDTO.getEmailConfig() != null) {
            if (alertDTO.getEmailConfig().getFunctionIds() != null) {
              for (long id : alertDTO.getEmailConfig().getFunctionIds()) {
                AnomalyFunctionDTO func = anomalyFunctionDAO.findById(id);
                if (func != null) {
                  String recipients = fetchRecipients(alertDTO.getReceiverAddresses());
                  System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s", app.getApplication(), func.getId(), func.getFunctionName(),
                      func.getIsActive(), func.getCreatedBy(), String.join(",", recipients)));
                }
              }
            }
          }
        }
      }
    }
  }

  private String fetchRecipients(DetectionAlertFilterRecipients receivers) {
    String recipients = String.join(", ", receivers.getTo()).trim();
    while (recipients.startsWith(",")) {
      recipients = recipients.substring(1, recipients.length());
    }
    while (recipients.endsWith(",")) {
      recipients = recipients.substring(0, recipients.length() - 1);
    }
    return recipients;
  }

  private void cleanup(){
    List<DetectionConfigDTO> detectionConfigDTOS = detectionConfigDAO.findAll();

    for (DetectionConfigDTO detFunction : detectionConfigDTOS) {
      // Delete all the anomalies generated by the functions
      List<AnomalyFunctionDTO> anomalies = anomalyFunctionDAO.findByPredicate(Predicate.EQ("baseId", detFunction.getId()));
      for (AnomalyFunctionDTO anomaly : anomalies) {
        anomalyFunctionDAO.delete(anomaly);
      }
      // Delete all the functions
      detectionConfigDAO.delete(detFunction);
    }

    List<DetectionAlertConfigDTO> detectionAlertConfigDTOS = detectionAlertConfigDAO.findAll();
    for (DetectionAlertConfigDTO alert : detectionAlertConfigDTOS) {
      if (alert.getId() == 89049435 || alert.getId() == 89235978) {
        continue;
      }

      detectionAlertConfigDAO.delete(alert);
    }
  }

  /**
   * Replayed anomalies are flagged accordingly and such anomalies are excluded from the email report.
   * This method removes the replay flag to test an email report from replayed results.
   */
  private void removeReplayFlagFromAnomalies(long detectionConfigId) {
    List<MergedAnomalyResultDTO> anomalies = mergedResultDAO.findByDetectionConfigAndIdGreaterThan(detectionConfigId,0l);
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomaly.setAnomalyResultSource(AnomalyResultSource.DEFAULT_ANOMALY_DETECTION);
      mergedResultDAO.save(anomaly);
    }
  }

  /**
   * Migrate all the existing subscription group watermarks from using end time to create time
   */
  private void migrateSubscriptionWatermarks() {
    List<DetectionAlertConfigDTO> subscriptions = detectionAlertConfigDAO.findAll();
    for (DetectionAlertConfigDTO subscription : subscriptions) {
      Map<Long, Long> updatedVectorClocks = new HashMap<>();
      Map<Long, Long> vectorClocks = subscription.getVectorClocks();
      if (vectorClocks != null && vectorClocks.size() != 0) {
        for (Map.Entry<Long, Long> clockEntry : vectorClocks.entrySet()) {
          DetectionConfigDTO detectionConfigDTO = detectionConfigDAO.findById(clockEntry.getKey());
          if (detectionConfigDTO == null) {
            // skip the detection; doesn't exist
            continue;
          }

          // find all recent anomalies (last 14 days) with end time <= watermark;
          long lookback = TimeUnit.DAYS.toMillis(14);
          Predicate predicate = Predicate.AND(
              Predicate.LE("endTime", clockEntry.getValue()),
              Predicate.GT("endTime", clockEntry.getValue() - lookback),
              Predicate.EQ("detectionConfigId", detectionConfigDTO.getId()));
          List<MergedAnomalyResultDTO> anomalies = mergedResultDAO.findByPredicate(predicate);

          // find the max create time among anomalies
          long createTimeMax = 0;
          for (MergedAnomalyResultDTO anomaly : anomalies) {
            createTimeMax = Math.max(createTimeMax, anomaly.getCreatedTime());
          }

          // update the watermark
          if (createTimeMax > 0) {
            updatedVectorClocks.put(clockEntry.getKey(), createTimeMax);
          } else {
            // If there are no anomalies or the anomaly create time is not available, then we will leave
            // it as the original watermark.
            updatedVectorClocks.put(clockEntry.getKey(), clockEntry.getValue());
          }
        }

        subscription.setVectorClocks(updatedVectorClocks);
        long updatedRows = detectionAlertConfigDAO.update(subscription);
        if (updatedRows > 0) {
          LOG.info(subscription.getId() + " - successfully updated watermarks");
        } else {
          LOG.info(subscription.getId() + " - failed to update watermarks");
        }
      }
    }
  }

  /**
   * Revert changes made in {@link RunAdhocDatabaseQueriesTool#migrateSubscriptionWatermarks()}
   */
  private void rollbackMigrateSubscriptionWatermarks() {
    List<DetectionAlertConfigDTO> subscriptions = detectionAlertConfigDAO.findAll();

    for (DetectionAlertConfigDTO subscription : subscriptions) {
      Map<Long, Long> updatedVectorClocks = new HashMap<>();
      Map<Long, Long> vectorClocks = subscription.getVectorClocks();
      if (vectorClocks != null && vectorClocks.size() != 0) {
        for (Map.Entry<Long, Long> clockEntry : vectorClocks.entrySet()) {
          DetectionConfigDTO detectionConfigDTO = detectionConfigDAO.findById(clockEntry.getKey());
          if (detectionConfigDTO == null) {
            // skip the detection; doesn't exist
            continue;
          }

          // find all recent anomalies (last 14 days) with end time <= watermark;
          long lookback = TimeUnit.DAYS.toMillis(14);
          Predicate predicate = Predicate.AND(Predicate.LE("createTime", clockEntry.getValue()),
              Predicate.GT("createTime", clockEntry.getValue() - lookback),
              Predicate.EQ("detectionConfigId", detectionConfigDTO.getId()));
          List<MergedAnomalyResultDTO> anomalies = mergedResultDAO.findByPredicate(predicate);

          // find the max create time
          long endTime = 0;
          for (MergedAnomalyResultDTO anomaly : anomalies) {
            endTime = Math.max(endTime, anomaly.getEndTime());
          }

          // update the watermark
          if (endTime > 0) {
            updatedVectorClocks.put(clockEntry.getKey(), endTime);
          } else {
            updatedVectorClocks.put(clockEntry.getKey(), clockEntry.getValue());
          }
        }

        subscription.setVectorClocks(updatedVectorClocks);
        long updatedRows = detectionAlertConfigDAO.update(subscription);
        if (updatedRows > 0) {
          LOG.info(subscription.getId() + " - successfully updated watermarks");
        } else {
          LOG.info(subscription.getId() + " - failed to update watermarks");
        }
      }
    }
  }

  private void printEntityAnomalyDetails(MergedAnomalyResultDTO anomaly, String indent, int index) {
    LOG.info("");
    LOG.info("Exploring Entity Anomaly {} with id {}", index, anomaly.getId());
    LOG.info(ENTITY_STATS_TEMPLATE, anomaly.getChildren().size(), anomaly.getProperties());
    LOG.info(ENTITY_TIME_TEMPLATE,
        new DateTime(anomaly.getCreatedTime(), TIMEZONE),
        DATE_FORMAT.print(new DateTime(anomaly.getStartTime(), TIMEZONE)),
        DATE_FORMAT.print(new DateTime(anomaly.getEndTime(), TIMEZONE)));
  }

  /**
   * Visualizes the entity anomalies by printing them
   *
   * Eg: dq.printEntityAnomalyTrees(158750221, 0, System.currentTimeMillis())
   *
   * @param detectionId The detection id whose anomalies need to be printed
   * @param start The start time of the anomaly slice
   * @param end The end time of the anomaly slice
   */
  private void printEntityAnomalyTrees(long detectionId, long start, long end) {
    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricConfigDAO, datasetConfigDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricConfigDAO, datasetConfigDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
    DefaultDataProvider provider =
        new DefaultDataProvider(metricConfigDAO, datasetConfigDAO, eventDAO, mergedResultDAO,
            DAORegistry.getInstance().getEvaluationManager(), timeseriesLoader, aggregationLoader,
            new DetectionPipelineLoader(), TimeSeriesCacheBuilder.getInstance(), AnomaliesCacheBuilder.getInstance());

    AnomalySlice anomalySlice = new AnomalySlice();
    anomalySlice = anomalySlice.withDetectionId(detectionId).withStart(start).withEnd(end);
    Multimap<AnomalySlice, MergedAnomalyResultDTO>
        sliceToAnomaliesMap = provider.fetchAnomalies(Collections.singletonList(anomalySlice));

    LOG.info("Total number of entity anomalies = " + sliceToAnomaliesMap.values().size());

    int i = 1;
    for (MergedAnomalyResultDTO parentAnomaly : sliceToAnomaliesMap.values()) {
      printEntityAnomalyDetails(parentAnomaly, "", i);
      int j = 1;
      for (MergedAnomalyResultDTO child : parentAnomaly.getChildren()) {
        printEntityAnomalyDetails(parentAnomaly, "\t", j);
        int k = 1;
        for (MergedAnomalyResultDTO grandchild : child.getChildren()) {
          printEntityAnomalyDetails(grandchild, "\t\t", k);
          k++;
        }
        j++;
      }
      i++;
    }
  }

  public static void main(String[] args) throws Exception {
    File persistenceFile = new File("/Users/akrai/persistence-local.yml");
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    RunAdhocDatabaseQueriesTool dq = new RunAdhocDatabaseQueriesTool(persistenceFile);
    dq.disableAllActiveDetections(Collections.singleton(160640739L));
    LOG.info("DONE");
  }
}
