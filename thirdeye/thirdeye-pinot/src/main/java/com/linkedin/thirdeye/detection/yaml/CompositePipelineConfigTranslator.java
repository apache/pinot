package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyAlertFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.LegacyMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.MergeWrapper;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;


/**
 * The YAML translator of composite pipeline. Convert YAML file into detection pipeline properties
 * Conceptually, the pipeline has the following structure. (Grouper not implemented yet)
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
 *               |  |  Merger  |
 *               |  +-----+----+
 *               |        |
 *               |        v
 *               |  +-----+----+
 *               |  |Algorithm |
 *               |  |Filter    |
 *               |  +-----+----+
 *               +--------+
 *               |
 *         +-----v-----+
 *         |   Merger  |
 *         |           |
 *         +-----+-----+
 *               |
 *               |
 *         +-----v-----+
 *         |  Grouper* |
 *         |           |
 *         +-----------+
 *
 *
 * This translator translates a yaml that describes the above detection flow
 * to the flowing wrapper structure to be execute in the detection pipeline.
 * +-----------------------------------------+
 * |  Rule Filter                            |
 * |  +--------------------------------------+
 * |  |   Merger                            ||
 * |  | +---------------------------------+ ||
 * |  | |   Dimension Exploration & Filter| ||
 * |  | |  +----------------------------+ | ||
 * |  | |  |  Rule detection            | | ||
 * |  | |  +----------------------------+ | ||
 * |  | +---------------------------------+ ||
 * |  | +---------------------------------+ ||
 * |  | |  Algorithm Alert Filter         | ||
 * |  | | +----Legacy-Merger------------+ | ||
 * |  | | | +--Dim-Explore-&-Filter-----| | ||
 * |  | | | | Algorithm detection      || | ||
 * |  | | | +---------------------------| | ||
 * |  | | +-----------------------------| | ||
 * |  | +---------------------------------+ ||
 * |  +--------------------------------------|
 * |  |--------------------------------------|
 * +-----------------------------------------+
 *
 */
public class CompositePipelineConfigTranslator extends YamlDetectionConfigTranslator {
  private static final String PROP_DIMENSION_EXPLORATION = "dimensionExploration";

  private static final String PROP_ALGORITHM_DETECTION = "algorithmDetection";
  private static final String PROP_ALGORITHM_FILTER = "algorithmFilter";
  private static final String PROP_RULE_DETECTION = "ruleDetection";
  private static final String PROP_RULE_FILTER = "ruleFilter";
  private static final String PROP_DIMENSION_FILTER = "dimensionFilter";
  private static final String PROP_FILTERS = "filters";
  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";

  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";
  private static final String PROP_SPEC = "specs";
  private static final String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static final String PROP_ALERT_FILTER = "alertFilter";
  private static final String PROP_METRIC_URN = "metricUrn";

  private static final YamlTranslatorInfoMap YAML_TRANSLATOR_INFO_MAP = new YamlTranslatorInfoMap();

  @Override
  Map<String, Object> buildDetectionProperties(Map<String, Object> yamlConfig) {
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_METRIC), "Property missing " + PROP_METRIC);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_DATASET), "Property missing " + PROP_DATASET);
    String metricUrn = generateMetricUrn(yamlConfig);

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, MergeWrapper.class.getName());
    properties.put(PROP_NESTED, new ArrayList<>());

    // algorithm detection pipeline
    if (yamlConfig.containsKey(PROP_ALGORITHM_DETECTION)) {
      Map<String, Object> algorithmDetectionSpecs = new HashMap<>();
      Map<String, Object> algorithmDetectionYamlConfigs = MapUtils.getMap(yamlConfig, PROP_ALGORITHM_DETECTION);
      Preconditions.checkArgument(algorithmDetectionYamlConfigs.containsKey(PROP_TYPE),
          PROP_ALGORITHM_DETECTION + " property missing " + PROP_TYPE);
      algorithmDetectionSpecs.putAll(algorithmDetectionYamlConfigs);

      Map<String, Object> algorithmDetectionProperties = new HashMap<>();
      if (yamlConfig.containsKey(PROP_ALGORITHM_FILTER)) {
        Map<String, Object> algorithmFilterYamlConfigs = MapUtils.getMap(yamlConfig, PROP_ALGORITHM_FILTER);
        Preconditions.checkArgument(algorithmFilterYamlConfigs.containsKey(PROP_TYPE),
            PROP_ALGORITHM_FILTER + " property missing " + PROP_TYPE);

        algorithmDetectionSpecs.put(PROP_ALERT_FILTER, algorithmFilterYamlConfigs);
        algorithmDetectionProperties.put(PROP_LEGACY_ALERT_FILTER_CLASS_NAME, YAML_TRANSLATOR_INFO_MAP.get(MapUtils.getString(algorithmFilterYamlConfigs, PROP_TYPE)));
        algorithmDetectionProperties.put(PROP_CLASS_NAME, LegacyAlertFilterWrapper.class.getName());
      } else {
        algorithmDetectionProperties.put(PROP_CLASS_NAME, LegacyMergeWrapper.class.getName());
      }
      algorithmDetectionProperties.put(PROP_ANOMALY_FUNCTION_CLASS, YAML_TRANSLATOR_INFO_MAP.get(MapUtils.getString(algorithmDetectionYamlConfigs, PROP_TYPE)));
      algorithmDetectionProperties.put(PROP_SPEC, algorithmDetectionSpecs);

      ((List<Object>) properties.get(PROP_NESTED)).add(algorithmDetectionProperties);
    }

    // rule detection pipeline
    if (yamlConfig.containsKey(PROP_RULE_DETECTION)) {
      Map<String, Object> ruleDetectionYamlConfigs = MapUtils.getMap(yamlConfig, PROP_RULE_DETECTION);
      Preconditions.checkArgument(ruleDetectionYamlConfigs.containsKey(PROP_TYPE),
          PROP_RULE_DETECTION + " property missing " + PROP_TYPE);

      Map<String, Object> ruleDetectionProps = new HashMap<>();
      ruleDetectionProps.put(PROP_CLASS_NAME, DimensionWrapper.class.getName());
      ruleDetectionProps.put(PROP_METRIC_URN, metricUrn);
      if (yamlConfig.containsKey(PROP_DIMENSION_EXPLORATION)) {
        fillInProperties(ruleDetectionProps, MapUtils.getMap(yamlConfig, PROP_DIMENSION_EXPLORATION));
      }
      if (yamlConfig.containsKey(PROP_DIMENSION_FILTER)) {
        fillInProperties(ruleDetectionProps, MapUtils.getMap(yamlConfig, PROP_DIMENSION_FILTER));
      }
      Map<String, Object> nestedRuleProperties = new HashMap<>();
      fillInProperties(nestedRuleProperties, ruleDetectionYamlConfigs);
      ruleDetectionProps.put(PROP_NESTED, Collections.singletonList(nestedRuleProperties));
      ((List<Object>) properties.get(PROP_NESTED)).add(ruleDetectionProps);
    }

    // rule filter
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

  private void fillInProperties(Map<String, Object> properties, Map<String, Object> yamlConfigs) {
    for (Map.Entry<String, Object> entry : yamlConfigs.entrySet()) {
      if (entry.getKey().equals(PROP_TYPE)) {
        properties.put(PROP_CLASS_NAME, YAML_TRANSLATOR_INFO_MAP.get(MapUtils.getString(yamlConfigs, PROP_TYPE)));
      } else {
        properties.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private String generateMetricUrn(Map<String, Object> yamlConfig) {
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
    return me.getUrn();
  }
}
