package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.yaml.snakeyaml.Yaml;


/**
 * The translator converts the yaml config into a detection config
 */
public abstract class YamlDetectionConfigTranslator {
  private static final String PROP_NAME = "name";
  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";
  private static final String PROP_CRON = "cron";
  private static final String PROP_FILTERS = "filters";

  private static final String CRON_SCHEDULE_DEFAULT = "0 0 14 * * ? *";

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private MetricConfigManager metricDAO;

  public YamlDetectionConfigTranslator() {
    this.metricDAO = DAO_REGISTRY.getMetricConfigDAO();
  }

  /**
   * Convert Yaml configurations into detection properties. Can be customized and override by different detection flow.
   */
  abstract Map<String, Object> buildDetectionProperties(String metricUrn, Map<String, Object> yamlConfig);

  /**
   * Fill in common fields of detection config. Properties of the pipeline is filled by different detection flow translator.
   */
  DetectionConfigDTO generateDetectionConfig(Map<String, Object> yamlConfig) {
    DetectionConfigDTO config = new DetectionConfigDTO();

    Preconditions.checkArgument(yamlConfig.containsKey(PROP_NAME), "Property missing " + PROP_NAME);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_METRIC), "Property missing " + PROP_METRIC);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_DATASET), "Property missing " + PROP_DATASET);

    config.setName(MapUtils.getString(yamlConfig, PROP_NAME));
    config.setCron(MapUtils.getString(yamlConfig, PROP_CRON, CRON_SCHEDULE_DEFAULT));
    config.setLastTimestamp(System.currentTimeMillis());
    config.setActive(true);

    Map<String, Collection<String>> filterMaps = MapUtils.getMap(yamlConfig, PROP_FILTERS);

    Multimap<String, String> filters = ArrayListMultimap.create();
    if (filterMaps != null) {
      for (Map.Entry<String, Collection<String>> entry : filterMaps.entrySet()) {
        filters.putAll(entry.getKey(), entry.getValue());
      }
    }
    MetricConfigDTO metricConfig = this.metricDAO.findByMetricAndDataset(MapUtils.getString(yamlConfig, PROP_METRIC),
        MapUtils.getString(yamlConfig, PROP_DATASET));
    Preconditions.checkNotNull(metricConfig, "Metric not found");

    MetricEntity me = MetricEntity.fromMetric(1.0, metricConfig.getId(), filters);
    String metricUrn = me.getUrn();

    config.setProperties(buildDetectionProperties(metricUrn, yamlConfig));
    return config;
  }
}
