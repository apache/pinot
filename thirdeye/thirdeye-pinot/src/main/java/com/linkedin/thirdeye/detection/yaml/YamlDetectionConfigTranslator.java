package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The YAML config translator converts the yaml config into a detection config.
 * Calls training module for each stage.
 */
public abstract class YamlDetectionConfigTranslator {
  protected static final Logger LOG = LoggerFactory.getLogger(YamlDetectionConfigTranslator.class);
  private static final String PROP_NAME = "detectionName";


  protected Map<String, Object> yamlConfig;
  protected long startTime;
  protected long endTime;
  protected DataProvider dataProvider;
  protected DetectionConfigDTO existingConfig;
  protected Map<String, Object> existingComponentSpecs;

  public YamlDetectionConfigTranslator(Map<String, Object> yamlConfig, DataProvider provider) {
    this.yamlConfig = yamlConfig;
    this.dataProvider = provider;
    this.existingComponentSpecs = new HashMap<>();
  }

  public YamlDetectionConfigTranslator withTrainingWindow(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    return this;
  }

  public YamlDetectionConfigTranslator withExistingDetectionConfig(DetectionConfigDTO existingDTO) {
    this.existingConfig = existingDTO;
    this.existingComponentSpecs = existingDTO.getComponentSpecs();
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
    config.setLastTimestamp(System.currentTimeMillis());
    config.setActive(true);
    YamlTranslationResult translationResult = translateYaml();
    Preconditions.checkArgument(!translationResult.getProperties().isEmpty(), "Empty detection property");
    config.setProperties(translationResult.getProperties());
    config.setComponentSpecs(translationResult.getComponents());
    config.setCron(translationResult.getCron());
    if (existingConfig != null) {
      config.setId(existingConfig.getId());
      config.setLastTimestamp(existingConfig.getLastTimestamp());
    }

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
