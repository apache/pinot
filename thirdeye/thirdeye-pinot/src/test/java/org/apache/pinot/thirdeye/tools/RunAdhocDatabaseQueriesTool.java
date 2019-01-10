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

package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.anomaly.task.TaskConstants;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.ClassificationConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionStatusManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.ClassificationConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DetectionStatusManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run adhoc queries to db
 */
public class RunAdhocDatabaseQueriesTool {

  private static final Logger LOG = LoggerFactory.getLogger(RunAdhocDatabaseQueriesTool.class);

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

  public RunAdhocDatabaseQueriesTool(File persistenceFile)
      throws Exception {
    init(persistenceFile);
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
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
        jobDTO.setTaskType(TaskConstants.TaskType.ANOMALY_DETECTION);
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

  private void disableAllActiveFunction() {
    disableAllActiveFunction(null);
  }

  private void disableAllActiveFunction(Collection<Long> exception){
    List<AnomalyFunctionDTO> functionSpecs = anomalyFunctionDAO.findAllActiveFunctions();
    for (AnomalyFunctionDTO functionSpec : functionSpecs) {
      if (functionSpec.getIsActive() && (CollectionUtils.isEmpty(exception) || !exception
          .contains(functionSpec.getId()))) {
        functionSpec.setActive(false);
        anomalyFunctionDAO.update(functionSpec);
      }
    }
  }

  public static void main(String[] args) throws Exception {

    File persistenceFile = new File(args[0]);
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    RunAdhocDatabaseQueriesTool dq = new RunAdhocDatabaseQueriesTool(persistenceFile);

  }

}
