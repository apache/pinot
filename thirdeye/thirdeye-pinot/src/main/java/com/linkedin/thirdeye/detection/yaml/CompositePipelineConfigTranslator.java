package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.algorithm.BaselineFillingMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.ChildKeepingMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.stage.AnomalyDetectionStageWrapper;
import com.linkedin.thirdeye.detection.algorithm.stage.AnomalyFilterStageWrapper;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.detection.ConfigUtils.*;


/**
 * The YAML translator of composite pipeline. Convert YAML file into detection pipeline properties.
 * This pipeline supports multiple detection algorithm running in OR relationship.
 * Supports running multiple filter rule running in AND relationship.
 * Conceptually, the pipeline has the following structure. (Grouper not implemented yet).
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
 *       |                |
 * +-----v----+     +-----v----+
 * |Rule      |     |Algorithm |
 * |  Merger  |     |  Merger  |
 * +-----+----+     +-----+----+
 *       |                |
 *       v                v
 * +-----+----+     +-----+----+
 * |Rule      |     |Algorithm |
 * |Filter    |     |Filter    |
 * +-----+----+     +-----+----+
 *       +-------+--------+
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
 * |  | +--Dimension Exploration & Filter-+ ||
 * |  | |        Filter                   | ||
 * |  | |  +--------Merger--------------+ | ||
 * |  | |  |  Rule detection            | | ||
 * |  | |  +----------------------------+ | ||
 * |  | +---------------------------------+ ||
 * |  | +---------------------------------+ ||
 * |  | |  Algorithm Alert Filter         | ||
 * |  | | +-----------Merger------------+ | ||
 * |  | | | +---------------------------| | ||
 * |  | | | | Algorithm detection       | | ||
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

  private static final String PROP_DETECTION = "detection";
  private static final String PROP_FILTER = "filter";
  private static final String PROP_FILTERS = "filters";
  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";
  private static final String PROP_STAGE_CLASSNAME = "stageClassName";
  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_SPEC = "specs";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_ANOMALY_DETECTION = "anomalyDetection";
  private static final String PROP_NESTED = "nested";

  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();

  @Override
  Map<String, Object> buildDetectionProperties(Map<String, Object> yamlConfig) {
    String metricUrn = buildMetricUrn(yamlConfig);

    List<Map<String, Object>> detectionYamls = getList(yamlConfig.get(PROP_ANOMALY_DETECTION));
    List<Map<String, Object>> nestedPipelines = new ArrayList<>();
    for (Map<String, Object> detectionYaml : detectionYamls) {
      List<Map<String, Object>> filterYamls = ConfigUtils.getList(detectionYaml.get(PROP_FILTER));
      Map<String, Object> detectionProperties = buildWrapperProperties(BaselineFillingMergeWrapper.class.getName(), buildListOfDetectionCoreProperties(
          ConfigUtils.<Map<String, Object>>getList(detectionYaml.get(PROP_DETECTION))));
      if (filterYamls == null || filterYamls.isEmpty()) {
        nestedPipelines.add(detectionProperties);
      } else {
        for (Map<String, Object> filterProperties : filterYamls) {
          nestedPipelines.addAll(buildStageWrapperProperties(AnomalyFilterStageWrapper.class.getName(),
              filterProperties, Collections.singletonList(detectionProperties)));
        }
      }
    }
    Map<String, Object> dimensionWrapperProperties = new HashMap<>();
    dimensionWrapperProperties.putAll(MapUtils.getMap(yamlConfig, PROP_DIMENSION_EXPLORATION));
    dimensionWrapperProperties.put(PROP_METRIC_URN, metricUrn);
    return buildWrapperProperties(ChildKeepingMergeWrapper.class.getName(), Collections.singletonList(
        buildWrapperProperties(DimensionWrapper.class.getName(), nestedPipelines, dimensionWrapperProperties)));
  }

  private Collection<Map<String, Object>> buildStageWrapperProperties(String wrapperClassName,
      Map<String, Object> yamlConfig, List<Map<String, Object>> nestedProperties) {
    if (yamlConfig == null || yamlConfig.isEmpty()) {
      return nestedProperties;
    }
    Map<String, Object> wrapperProperties = buildWrapperProperties(wrapperClassName, nestedProperties);
    if (wrapperProperties.isEmpty()) {
      return Collections.emptyList();
    }
    fillStageSpecs(wrapperProperties, yamlConfig);
    return Collections.singletonList(wrapperProperties);
  }

  private Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties) {
    return buildWrapperProperties(wrapperClassName, nestedProperties, Collections.<String, Object>emptyMap());
  }

  private Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties, Map<String, Object> defaultProperties) {
    Map<String, Object> properties = new HashMap<>();
    List<Map<String, Object>> wrapperNestedProperties = new ArrayList<>();
    for (Map<String, Object> nested : nestedProperties) {
      if (nested != null && !nested.isEmpty()) {
        wrapperNestedProperties.add(nested);
      }
    }
    if (wrapperNestedProperties.isEmpty()) {
      return properties;
    }
    properties.put(PROP_CLASS_NAME, wrapperClassName);
    properties.put(PROP_NESTED, wrapperNestedProperties);
    properties.putAll(defaultProperties);
    return properties;
  }

  private List<Map<String, Object>> buildListOfDetectionCoreProperties(List<Map<String, Object>> yamlConfigs) {
    List<Map<String, Object>> properties = new ArrayList<>();
    for (Map<String, Object> yamlConfig : yamlConfigs) {
      properties.add(buildDetectionCoreProperties(AnomalyDetectionStageWrapper.class.getName(), yamlConfig));
    }
    return properties;
  }

  private Map<String, Object> buildDetectionCoreProperties(String wrapperClassName, Map<String, Object> yamlConfig) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, wrapperClassName);
    fillStageSpecs(properties, yamlConfig);
    return properties;
  }

  private void fillStageSpecs(Map<String, Object> properties, Map<String, Object> detectionCoreYamlConfigs) {
    Map<String, Object> specs = new HashMap<>();
    for (Map.Entry<String, Object> entry : detectionCoreYamlConfigs.entrySet()) {
      if (entry.getKey().equals(PROP_TYPE)) {
        properties.put(PROP_STAGE_CLASSNAME,
            DETECTION_REGISTRY.lookup(MapUtils.getString(detectionCoreYamlConfigs, PROP_TYPE)));
      } else {
        specs.put(entry.getKey(), entry.getValue());
      }
    }
    properties.put(PROP_SPEC, specs);
  }

  private String buildMetricUrn(Map<String, Object> yamlConfig) {
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

  @Override
  protected void validateYAML(Map<String, Object> yamlConfig) {
    super.validateYAML(yamlConfig);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_METRIC), "Property missing " + PROP_METRIC);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_DATASET), "Property missing " + PROP_DATASET);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_ANOMALY_DETECTION), "Property missing " + PROP_ANOMALY_DETECTION);

    List<Map<String, Object>> detectionYamls = getList(yamlConfig.get(PROP_ANOMALY_DETECTION));
    for (int i = 0; i < detectionYamls.size(); i++) {
      Map<String, Object> detectionYaml = detectionYamls.get(i);
      Preconditions.checkArgument(detectionYaml.containsKey(PROP_DETECTION), "In rule No." + (i+1) + ", detection stage property missing. ");
      List<Map<String, Object>> detectionStageYamls = ConfigUtils.getList(detectionYaml.get(PROP_DETECTION));
      for (Map<String, Object> detectionStageYaml : detectionStageYamls) {
        Preconditions.checkArgument(detectionStageYaml.containsKey(PROP_TYPE), "In rule No." + (i+1) + ", detection stage type missing. ");
      }
      if (detectionYaml.containsKey(PROP_FILTER)) {
        List<Map<String, Object>> filterStageYamls = ConfigUtils.getList(MapUtils.getMap(detectionYaml, PROP_FILTER));
        for (Map<String, Object> filterStageYaml : filterStageYamls) {
          Preconditions.checkArgument(filterStageYaml.containsKey(PROP_TYPE), "In rule No." + (i+1) + ", filter stage type missing. ");
        }
      }
    }
  }
}
