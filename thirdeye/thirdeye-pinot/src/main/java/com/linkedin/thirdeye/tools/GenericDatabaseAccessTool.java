package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.autoload.pinot.metrics.ConfigGenerator;
import com.linkedin.thirdeye.datalayer.bao.*;
import com.linkedin.thirdeye.datalayer.dto.*;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Generic utility class for Database Access
 * Table included:
 *   AnomalyFunction, EmailConfiguration, RawAnomalyResult, MergedAnomalyResult
 *   DashboardConfig, OverrideConfig
 */

public class GenericDatabaseAccessTool {

  private static final Logger LOG = LoggerFactory.getLogger(GenericDatabaseAccessTool.class);

  protected AnomalyFunctionManager anomalyFunctionDAO;
  protected EmailConfigurationManager emailConfigurationDAO;
  protected RawAnomalyResultManager rawResultDAO;
  protected MergedAnomalyResultManager mergedResultDAO;
  protected MetricConfigManager metricConfigDAO;
  protected DashboardConfigManager dashboardConfigDAO;
  protected OverrideConfigManager overrideConfigDAO;

  /**
   * initialize the access tool with the thirdeye database configuration file
   * @throws Exception
   */
  public GenericDatabaseAccessTool(File persistenceFile)
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
  }

  protected void toggleAnomalyFunctionById(Long id) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(id);
    anomalyFunction.setActive(true);
    anomalyFunctionDAO.update(anomalyFunction);
  }

  protected void updateEmailConfigByCollection(String collection, String emailAddresses) {
    List<EmailConfigurationDTO> emailConfigs = emailConfigurationDAO.findByCollection(collection);
    for (EmailConfigurationDTO emailConfig : emailConfigs) {
      emailConfig.setToAddresses(emailAddresses);
      emailConfigurationDAO.update(emailConfig);
    }
  }

  protected void createDashboard(String dataset) {

    String dashboardName = ThirdEyeUtils.getDefaultDashboardName(dataset);
    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.findByName(dashboardName);
    dashboardConfig.setMetricIds(ConfigGenerator.getMetricIdsFromMetricConfigs(metricConfigDAO.findByDataset(dataset)));
    dashboardConfigDAO.update(dashboardConfig);
  }

  protected void setAlertFilterForFunctionInCollection(String collection, List<String> metricList,
                                                     Map<String, Map<String, String>> metricRuleMap,
                                                     Map<String, String> defaultAlertFilter) {
    List<AnomalyFunctionDTO> anomalyFunctionDTOs =
        anomalyFunctionDAO.findAllByCollection(collection);
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctionDTOs) {
      String metricName = anomalyFunctionDTO.getMetric();
      if (metricList.contains(metricName)) {
        Map<String, String> alertFilter = defaultAlertFilter;
        if (metricRuleMap.containsKey(metricName)) {
          alertFilter = metricRuleMap.get(metricName);
        }
        anomalyFunctionDTO.setAlertFilter(alertFilter);
        anomalyFunctionDAO.update(anomalyFunctionDTO);
        LOG.info("Add alert filter {} to function {} (dataset: {}, metric: {})", alertFilter,
            anomalyFunctionDTO.getId(), collection, metricName);
      }
    }
  }

  protected Long createOverrideConfig(OverrideConfigDTO overrideConfigDTO) {
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

  protected void updateOverrideConfig(long id, OverrideConfigDTO overrideConfigDTO) {
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

}
