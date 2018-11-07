package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.wrapper.BaselineFillingMergeWrapper;
import com.linkedin.thirdeye.detection.wrapper.ChildKeepingMergeWrapper;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.algorithm.stage.AnomalyDetectionStageWrapper;
import com.linkedin.thirdeye.detection.algorithm.stage.AnomalyFilterStageWrapper;
import com.linkedin.thirdeye.detection.annotation.DetectionRegistry;
import com.linkedin.thirdeye.detection.tune.StageTrainingModule;
import com.linkedin.thirdeye.detection.tune.TrainingModuleLoader;
import com.linkedin.thirdeye.detection.tune.TrainingResult;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.detection.ConfigUtils.*;
import static com.linkedin.thirdeye.detection.tune.StaticStageTrainingModule.*;


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
 * |Filters   |     |Filters   |
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
 * |  Merger                                 |
 * |  +--------------------------------------+
 * |  |   Dimension Exploration & Filter    ||
 * |  | +---------------------------------+ ||
 * |  | |        Filters                  | ||
 * |  | |  +--------Merger--------------+ | ||
 * |  | |  |  Rule detections           | | ||
 * |  | |  +----------------------------+ | ||
 * |  | +---------------------------------+ ||
 * |  | +---------------------------------+ ||
 * |  | |              Filters            | ||
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
  private static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";

  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  private static final TrainingModuleLoader TRAINING_MODULE_LOADER = new TrainingModuleLoader();
  private final Map<String, Object> components = new HashMap<>();

  public CompositePipelineConfigTranslator(Map<String, Object> yamlConfig, DataProvider provider) {
    super(yamlConfig, provider);
  }

  @Override
  YamlTranslationResult translateYaml() {
    String metricUrn = buildMetricUrn(yamlConfig);

    List<Map<String, Object>> anomalyDetectionYamls = getList(yamlConfig.get(PROP_ANOMALY_DETECTION));
    List<Map<String, Object>> nestedPipelines = new ArrayList<>();
    for (Map<String, Object> anomalyDetectionYaml : anomalyDetectionYamls) {
      List<Map<String, Object>> filterYamls = ConfigUtils.getList(anomalyDetectionYaml.get(PROP_FILTER));
      List<Map<String, Object>> detectionYamls = ConfigUtils.getList(anomalyDetectionYaml.get(PROP_DETECTION));
      List<Map<String, Object>> detectionProperties = buildListOfMergeWrapperProperties(detectionYamls);
      if (filterYamls == null || filterYamls.isEmpty()) {
        nestedPipelines.addAll(detectionProperties);
      } else {
        List<Map<String, Object>> filterNestedProperties = detectionProperties;
        for (Map<String, Object> filterProperties : filterYamls) {
          filterNestedProperties = buildStageWrapperProperties(AnomalyFilterStageWrapper.class.getName(),
              filterProperties, filterNestedProperties);
        }
        nestedPipelines.addAll(filterNestedProperties);
      }
    }
    Map<String, Object> dimensionWrapperProperties = new HashMap<>();
    dimensionWrapperProperties.putAll(MapUtils.getMap(yamlConfig, PROP_DIMENSION_EXPLORATION));
    dimensionWrapperProperties.put(PROP_METRIC_URN, metricUrn);
    Map<String, Object> properties = buildWrapperProperties(ChildKeepingMergeWrapper.class.getName(), Collections.singletonList(
        buildWrapperProperties(DimensionWrapper.class.getName(), nestedPipelines, dimensionWrapperProperties)));
    return new YamlTranslationResult().withProperties(properties).withComponents(this.components);
  }

  private List<Map<String, Object>>  buildListOfMergeWrapperProperties(List<Map<String, Object>> yamlConfigs) {
    List<Map<String, Object>> properties = new ArrayList<>();
    for (Map<String, Object> yamlConfig : yamlConfigs) {
      properties.add(buildMergeWrapperProperties(yamlConfig));
    }
    return properties;
  }

  private Map<String, Object> buildMergeWrapperProperties(Map<String, Object> yamlConfig) {
    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, AnomalyDetectionStageWrapper.class.getName());

    String type = MapUtils.getString(yamlConfig, PROP_TYPE);
    String stageClassName = DETECTION_REGISTRY.lookup(type);
    nestedProperties.put(PROP_STAGE_CLASSNAME, stageClassName);

    Map<String, Object> properties = new HashMap<>();
    TrainingResult result = trainStage(stageClassName, yamlConfig);
    String componentKey = makeComponentKey(type);
    nestedProperties.put(PROP_SPEC, result.getStageSpecs());
    properties.put(PROP_CLASS_NAME, BaselineFillingMergeWrapper.class.getName());
    properties.put(PROP_NESTED, Collections.singletonList(nestedProperties));
    return properties;
  }

  private String makeComponentKey(String type) {
    return "$REF_" + type;
  }

  private List<Map<String, Object>> buildStageWrapperProperties(String wrapperClassName,
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

  private void fillStageSpecs(Map<String, Object> properties, Map<String, Object> yamlConfig) {
    String stageClassName = DETECTION_REGISTRY.lookup(MapUtils.getString(yamlConfig, PROP_TYPE));
    properties.put(PROP_STAGE_CLASSNAME, stageClassName);
    TrainingResult result = trainStage(stageClassName, yamlConfig);
    properties.put(PROP_SPEC, result.getStageSpecs());
  }

  private Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties) {
    return buildWrapperProperties(wrapperClassName, nestedProperties, Collections.emptyMap());
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

  private TrainingResult trainStage(String stageClassName, Map<String, Object> yamlConfig) {
    StageTrainingModule trainingModule = TRAINING_MODULE_LOADER.from(DETECTION_REGISTRY.lookupTrainingModule(stageClassName));
    trainingModule.init(ImmutableMap.of(PROP_YAML_CONFIG, yamlConfig), this.startTime, this.endTime);
    return trainingModule.fit(this.dataProvider);
  }

  private String buildMetricUrn(Map<String, Object> yamlConfig) {
    Map<String, Collection<String>> filterMaps = MapUtils.getMap(yamlConfig, PROP_FILTERS);

    Multimap<String, String> filters = ArrayListMultimap.create();
    if (filterMaps != null) {
      for (Map.Entry<String, Collection<String>> entry : filterMaps.entrySet()) {
        filters.putAll(entry.getKey(), entry.getValue());
      }
    }
    MetricConfigDTO metricConfig = this.dataProvider.fetchMetric(MapUtils.getString(yamlConfig, PROP_METRIC),
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
