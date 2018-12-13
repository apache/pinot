package com.linkedin.thirdeye.detection.validators;

import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.ApplicationManager;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.Yaml;


public abstract class ConfigValidator {

  protected static final Yaml YAML = new Yaml();

  final AlertConfigManager alertDAO;
  final ApplicationManager applicationDAO;

  ConfigValidator() {
    this.alertDAO = DAORegistry.getInstance().getAlertConfigDAO();
    this.applicationDAO = DAORegistry.getInstance().getApplicationDAO();
  }

  /**
   * Perform basic validations on a yaml file like verifying if
   * the yaml exists and is parsable
   */
  @SuppressWarnings("unchecked")
  public boolean validateYAMLConfig(String yamlConfig, Map<String, String> responseMessage) {
    // Check if YAML is empty or not
    if (StringUtils.isEmpty(yamlConfig)) {
      responseMessage.put("message", "The config file cannot be blank.");
      responseMessage.put("more-info", "Payload in the request is empty");
      return false;
    }

    // Check if the YAML is parsable
    try {
      Map<String, Object> yamlConfigMap = (Map<String, Object>) YAML.load(yamlConfig);
    } catch (Exception e) {
      responseMessage.put("message", "There was an error parsing the yaml file. Check for syntax issues.");
      responseMessage.put("more-info", "Error parsing YAML" + e);
      return false;
    }

    return true;
  }

}
