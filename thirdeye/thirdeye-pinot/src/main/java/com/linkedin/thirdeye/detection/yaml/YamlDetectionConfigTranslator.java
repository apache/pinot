package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.DataProvider;
import java.util.Map;
import org.apache.commons.collections.MapUtils;


/**
 * The YAML config translator converts the yaml config into a detection config.
 * Calls training module for each stage.
 */
public abstract class YamlDetectionConfigTranslator {
  private static final String PROP_NAME = "detectionName";
  private static final String PROP_CRON = "cron";

  private static final String CRON_SCHEDULE_DEFAULT = "0 0 14 * * ? *";

  protected Map<String, Object> yamlConfig;
  protected long startTime;
  protected long endTime;
  protected DataProvider dataProvider;

  public YamlDetectionConfigTranslator(Map<String, Object> yamlConfig, DataProvider provider) {
    this.yamlConfig = yamlConfig;
    this.dataProvider = provider;
  }

  public YamlDetectionConfigTranslator withTrainingWindow(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    return this;
  }

  /**
   * Convert Yaml configurations into detection properties. Can be customized and override by different detection flow.
   * @return properties of the detection pipeline
   */
  abstract YamlTranslationResult translateYaml();

  /**
   * Fill in common fields of detection config. Properties of the pipeline is filled by the subclass.
   */
  DetectionConfigDTO generateDetectionConfig() {
    validateYAML(yamlConfig);

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(MapUtils.getString(yamlConfig, PROP_NAME));
    config.setCron(MapUtils.getString(yamlConfig, PROP_CRON, CRON_SCHEDULE_DEFAULT));
    config.setLastTimestamp(System.currentTimeMillis());
    config.setActive(true);
    YamlTranslationResult translationResult = translateYaml();
    Preconditions.checkArgument(!translationResult.getProperties().isEmpty(), "Empty detection property");
    config.setProperties(translationResult.getProperties());
    config.setComponents(translationResult.getComponents());
    return config;
  }

  /**
   * Check the yaml configuration is semantically valid. Throws an IllegalArgumentException if not.
   * @param yamlConfig yamlConfiguration to be checked
   */
  protected void validateYAML(Map<String, Object> yamlConfig) {
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_NAME), "Property missing " + PROP_NAME);
  }
}
