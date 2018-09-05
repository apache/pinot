package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

/**
 * The YAML translator of composite pipeline. Convert YAML file into detection pipeline properties
 * The pipeline has the following structure. (Grouper not implemented yet)
 *
 *         +-----------+
 *         | Dimension |
 *         |Exploration|
 *         +-----+-----+
 *               |
 *         +-----v-----+
 *         |Dimension  |
 *         |Filter     |
 *       +-------------+--+
 *       |                |
 *       |                |
 * +-----v----+     +-----v----+
 * | Rule     |     | Algorithm|
 * | detection|     | Detection|
 * +-------------+  +-----+----+
 *               |        |
 *               |  +-----v----+
 *               |  |Algorithm |
 *               |  |Merger    |
 *               |  +-----+----+
 *               |        |
 *               |        ^
 *               |  +-----+----+
 *               |  |Algorithm |
 *               |  |Filter    |
 *               |  +-----+----+
 *               +--------+
 *               |
 *         +-----v-----+
 *         | Merger    |
 *         |           |
 *         +-----+-----+
 *               |
 *               |
 *         +-----v-----+
 *         |  Grouper* |
 *         |           |
 *         +-----------+
 *
 */
public class CompositePipelineConfigTranslator extends YamlDetectionConfigTranslator {
  private static final String PROP_DIMENSION_EXPLORATION = "dimensionExploration";

  private static final String PROP_ALGORITHM_DETECTION = "algorithmDetection";
  private static final String PROP_RULE_DETECTION = "ruleDetection";
  private static final String PROP_RULE_FILTER = "ruleFilter";
  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_ID = "id";
  private static final YamlTranslatorInfoMap YAML_TRANSLATOR_INFO_MAP = new YamlTranslatorInfoMap();

  @Override
  Map<String, Object> buildDetectionProperties(String metricUrn, Map<String, Object> yamlConfig) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, MergeWrapper.class.getName());
    properties.put(PROP_NESTED, new ArrayList<>());
    Map<String, Object> dimensionExplorationYamlConfigs = MapUtils.getMap(yamlConfig, PROP_DIMENSION_EXPLORATION);
//
//    if (yamlConfig.containsKey(PROP_ALGORITHM_DETECTION)) {
//      Map<String, Object> algorithmDetectionProps = new HashMap<>();
//      Map<String, Object> algorithmDetectionYamlConfigs = MapUtils.getMap(yamlConfig, PROP_ALGORITHM_DETECTION);
//      LegacyAnomalyFunctionTranslator translator = new LegacyAnomalyFunctionTranslator();
//      ConfigUtils.getList(properties.get(PROP_NESTED)).add(algorithmDetectionProps);
//    }


    if (yamlConfig.containsKey(PROP_RULE_DETECTION)) {
      Map<String, Object> ruleDetectionYamlConfigs = MapUtils.getMap(yamlConfig, PROP_RULE_DETECTION);
      Preconditions.checkArgument(ruleDetectionYamlConfigs.containsKey(PROP_TYPE), PROP_RULE_DETECTION + " property missing " + PROP_ID);

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
          PROP_RULE_FILTER + " property missing: " + PROP_TYPE);
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
