package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
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


public class YamlDetectionConfigTranslator {
  private static final String PROP_NAME = "name";
  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";
  private static final String PROP_CRON = "cron";
  private static final String PROP_FILTERS = "filters";
  private static final String PROP_DIMENSION_EXPLORATION = "dimensionExploration";

  private static final String PROP_ALGORITHM_DETECTION = "algorithmDetection";
  private static final String PROP_RULE_DETECTION = "ruleDetection";
  private static final String PROP_RULE_FILTER = "ruleFilter";
  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_ID = "id";

  private static final String CRON_SCHEDULE_DEFAULT = "0 0 14 * * ? *";
  private static final Yaml YAML_READER = new Yaml();
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final YamlTranslatorInfoMap YAML_TRANSLATOR_INFO_MAP = new YamlTranslatorInfoMap();

  private MetricConfigManager metricDAO;

  public YamlDetectionConfigTranslator() {
    this.metricDAO = DAO_REGISTRY.getMetricConfigDAO();
  }

  DetectionConfigDTO convertYamlToDetectionConfig(String yaml) {
    Map<String, Object> yamlConfig = (Map<String, Object>) this.YAML_READER.load(yaml);
    DetectionConfigDTO config = new DetectionConfigDTO();

    Preconditions.checkArgument(yamlConfig.containsKey(PROP_NAME), "Property missing " + PROP_NAME);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_METRIC), "Property missing " + PROP_METRIC);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_DATASET), "Property missing " + PROP_DATASET);

    config.setName(MapUtils.getString(yamlConfig, PROP_NAME));
    config.setCron(MapUtils.getString(yamlConfig, PROP_CRON, CRON_SCHEDULE_DEFAULT));

    Map<String, Collection<String>> filterMaps = MapUtils.getMap(yamlConfig, PROP_FILTERS);
    Multimap<String, String> multimap = ArrayListMultimap.create();
    if (filterMaps != null) {
      for (Map.Entry<String, Collection<String>> entry : filterMaps.entrySet()) {
        multimap.putAll(entry.getKey(), entry.getValue());
      }
    }
    MetricConfigDTO metricConfig = this.metricDAO.findByMetricAndDataset(MapUtils.getString(yamlConfig, PROP_METRIC),
        MapUtils.getString(yamlConfig, PROP_DATASET));
    MetricEntity me = MetricEntity.fromMetric(1.0, metricConfig.getId(), multimap);
    String metricUrn = me.getUrn();

    config.setProperties(buildDetectionProperties(metricUrn, yamlConfig));
    return config;
  }

  Map<String, Object> buildDetectionProperties(String metricUrn, Map<String, Object> yamlConfig) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, MergeWrapper.class.getName());
    properties.put(PROP_NESTED, new ArrayList<>());
    Map<String, Object> dimensionExplorationYamlConfigs = MapUtils.getMap(yamlConfig, PROP_DIMENSION_EXPLORATION);

//    if (yamlConfig.containsKey(PROP_ALGORITHM_DETECTION)) {
//      Map<String, Object> algorithmDetectionProps = new HashMap<>();
//      Map<String, Object> algorithmDetectionYamlConfigs = MapUtils.getMap(yamlConfig, PROP_ALGORITHM_DETECTION);
//      LegacyAnomalyFunctionTranslator translator = new LegacyAnomalyFunctionTranslator();
//      ConfigUtils.getList(properties.get(PROP_NESTED)).add(algorithmDetectionProps);
//    }


    if (yamlConfig.containsKey(PROP_RULE_DETECTION)) {
      Map<String, Object> ruleDetectionYamlConfigs = MapUtils.getMap(yamlConfig, PROP_RULE_DETECTION);
      Preconditions.checkArgument(ruleDetectionYamlConfigs.containsKey(PROP_TYPE),
          PROP_RULE_DETECTION + " property missing " + PROP_ID);

      Map<String, Object> ruleDetectionProps = new HashMap<>();
      ruleDetectionProps.put(PROP_CLASS_NAME, DimensionWrapper.class.getName());
      ruleDetectionProps.put("metricUrn", metricUrn);
      if (dimensionExplorationYamlConfigs != null) {
        for (Map.Entry<String, Object> entry : dimensionExplorationYamlConfigs.entrySet()) {
          ruleDetectionProps.put(entry.getKey(), entry.getValue());
        }
      }
      Map<String, Object> nestedRuleProperties = new HashMap<>();
      fillInProperties(nestedRuleProperties, ruleDetectionYamlConfigs);
      ruleDetectionProps.put(PROP_NESTED, Collections.singletonList(nestedRuleProperties));
      ((List<Object>) properties.get(PROP_NESTED)).add(ruleDetectionProps);
    }

    if (yamlConfig.containsKey(PROP_RULE_FILTER)) {
      Map<String, Object> ruleFilterProperties = new HashMap<>();
      Map<String, Object> ruleFilterYamlConfigs = MapUtils.getMap(yamlConfig, PROP_RULE_FILTER);
      Preconditions.checkArgument(ruleFilterYamlConfigs.containsKey(PROP_TYPE),
          PROP_RULE_FILTER + " property missing " + PROP_TYPE);
      fillInProperties(ruleFilterProperties, ruleFilterYamlConfigs);
      ruleFilterProperties.put(PROP_NESTED, Collections.singletonList(properties));
      properties = ruleFilterProperties;
    }
    return properties;
  }

  private void fillInProperties(Map<String, Object> properties, Map<String, Object> ruleFilterYamlConfigs) {
    for (Map.Entry<String, Object> entry : ruleFilterYamlConfigs.entrySet()) {
      if (entry.getKey().equals(PROP_TYPE)) {
        properties.put(PROP_CLASS_NAME, YAML_TRANSLATOR_INFO_MAP.get(MapUtils.getString(ruleFilterYamlConfigs, PROP_TYPE)));
      } else {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
