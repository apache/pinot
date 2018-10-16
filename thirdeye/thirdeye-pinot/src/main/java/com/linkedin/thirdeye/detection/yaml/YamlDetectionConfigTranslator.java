package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.detection.ConfigUtils.*;


/**
 * The YAML config translator converts the yaml config into a detection config
 */
public abstract class YamlDetectionConfigTranslator {
  private static final String PROP_NAME = "name";
  private static final String PROP_CRON = "cron";

  private static final String CRON_SCHEDULE_DEFAULT = "0 0 14 * * ? *";

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  MetricConfigManager metricDAO;

  public YamlDetectionConfigTranslator() {
    this.metricDAO = DAO_REGISTRY.getMetricConfigDAO();
  }

  /**
   * Convert Yaml configurations into detection properties. Can be customized and override by different detection flow.
   * @param yamlConfig yaml configuration of a detection pipeline flow type
   * @return properties of the detection pipeline
   */
  abstract Map<String, Object> buildDetectionProperties(Map<String, Object> yamlConfig);

  /**
   * Fill in common fields of detection config. Properties of the pipeline is filled by the subclass.
   */
  DetectionConfigDTO generateDetectionConfig(Map<String, Object> yamlConfig) {
    validateYAML(yamlConfig);

    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName(MapUtils.getString(yamlConfig, PROP_NAME));
    config.setCron(MapUtils.getString(yamlConfig, PROP_CRON, CRON_SCHEDULE_DEFAULT));
    config.setLastTimestamp(System.currentTimeMillis());
    config.setActive(true);
    Map<String, Object> properties = buildDetectionProperties(yamlConfig);
    Preconditions.checkArgument(!properties.isEmpty(), "Empty detection property");
    config.setProperties(properties);
    return config;
  }

  protected void validateYAML(Map<String, Object> yamlConfig) {
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_NAME), "Property missing " + PROP_NAME);
  }
}
