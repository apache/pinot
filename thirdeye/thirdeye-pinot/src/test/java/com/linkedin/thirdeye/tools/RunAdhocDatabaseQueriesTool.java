package com.linkedin.thirdeye.tools;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.bao.OverrideConfigManager;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.autoload.pinot.metrics.ConfigGenerator;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionStatusManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DetectionStatusManagerImpl;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean.EmailConfig;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

/**
 * Run adhoc queries to db
 */
public class RunAdhocDatabaseQueriesTool {

  private static final Logger LOG = LoggerFactory.getLogger(RunAdhocDatabaseQueriesTool.class);

  private AnomalyFunctionManager anomalyFunctionDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private RawAnomalyResultManager rawResultDAO;
  private MergedAnomalyResultManager mergedResultDAO;
  private MetricConfigManager metricConfigDAO;
  private DashboardConfigManager dashboardConfigDAO;
  private OverrideConfigManager overrideConfigDAO;
  private JobManager jobDAO;
  private TaskManager taskDAO;
  private DataCompletenessConfigManager dataCompletenessConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private DetectionStatusManager detectionStatusDAO;
  private AlertConfigManager alertConfigDAO;

  public RunAdhocDatabaseQueriesTool(File persistenceFile)
      throws Exception {
    init(persistenceFile);
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    emailConfigurationDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl.class);
    rawResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    mergedResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
    metricConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl.class);
    dashboardConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.DashboardConfigManagerImpl.class);
    overrideConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.OverrideConfigManagerImpl.class);
    jobDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl.class);
    taskDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl.class);
    dataCompletenessConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.DataCompletenessConfigManagerImpl.class);
    datasetConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl.class);
    detectionStatusDAO = DaoProviderUtil.getInstance(DetectionStatusManagerImpl.class);
    alertConfigDAO = DaoProviderUtil.getInstance(AlertConfigManagerImpl.class);
  }

  private void toggleAnomalyFunction(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    anomalyFunction.setActive(true);
    anomalyFunctionDAO.update(anomalyFunction);
  }

  private void updateFields() {
    List<EmailConfigurationDTO> emailConfigs = emailConfigurationDAO.findAll();
    for (EmailConfigurationDTO emailConfig : emailConfigs) {
      LOG.info(emailConfig.getId() + " " + emailConfig.getToAddresses());
    }
  }

  private void updateField(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    //anomalyFunction.setCron("0/10 * * * * ?");
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

  private void updateEmailConfigs() {
    List<EmailConfigurationDTO> emailConfigs = emailConfigurationDAO.findByCollection("login_additive");
    for (EmailConfigurationDTO emailConfig : emailConfigs) {
      emailConfig.setToAddresses("thirdeye-dev@linkedin.com,zilin@linkedin.com,ehuang@linkedin.com,login-alerts@linkedin.com");
      emailConfigurationDAO.update(emailConfig);
    }
  }

  private void createDashboard(String dataset) {

    String dashboardName = ThirdEyeUtils.getDefaultDashboardName(dataset);
    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.findByName(dashboardName);
    dashboardConfig.setMetricIds(ConfigGenerator.getMetricIdsFromMetricConfigs(metricConfigDAO.findByDataset(dataset)));
    dashboardConfigDAO.update(dashboardConfig);
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
      dto.setRequiresCompletenessCheck(true);
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

  private void playWithAlertCOnfigs() {
    List<EmailConfigurationDTO> emailConfigs = emailConfigurationDAO.findAll();
    Multimap<String, EmailConfigurationDTO> datasetToEmailConfig = ArrayListMultimap.create();
    for (EmailConfigurationDTO emailConfig : emailConfigs) {
      if (emailConfig.isActive() && !emailConfig.getFunctionIds().isEmpty()) {
        datasetToEmailConfig.put(emailConfig.getCollection(), emailConfig);
      }
    }
    for (String dataset : datasetToEmailConfig.keySet()) {
      List<EmailConfigurationDTO> emailConfigsList = Lists.newArrayList(datasetToEmailConfig.get(dataset));

      String name = "Beta " + dataset + " Alert Config";
      String cron = emailConfigsList.get(0).getCron();
      boolean active = true;
      long watermark = 0L;
      Set<Long> functionIds = new HashSet<>();
      for (EmailConfigurationDTO config : emailConfigsList) {
        functionIds.addAll(config.getFunctionIds());
      }
      EmailConfig emailConfig = new EmailConfig();
      emailConfig.setAnomalyWatermark(watermark);
      emailConfig.setFunctionIds(Lists.newArrayList(functionIds));
      String recipients = "thirdeyeproductteam@linkedin.com";
      String fromAddress = "thirdeye-dev@linkedin.com";

      AlertConfigDTO alertConfig = new AlertConfigDTO();
      alertConfig.setName(name);
      alertConfig.setCronExpression(cron);
      alertConfig.setActive(active);
      alertConfig.setEmailConfig(emailConfig);
      alertConfig.setRecipients(recipients);
      alertConfig.setFromAddress(fromAddress);
      System.out.println(alertConfig);
      alertConfigDAO.save(alertConfig);

    }
  }

  public static void main(String[] args) throws Exception {

    File persistenceFile = new File(args[0]);
    if (!persistenceFile.exists()) {
      System.err.println("Missing file:" + persistenceFile);
      System.exit(1);
    }
    RunAdhocDatabaseQueriesTool dq = new RunAdhocDatabaseQueriesTool(persistenceFile);
    dq.playWithAlertCOnfigs();
  }

}
