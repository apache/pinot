package com.linkedin.thirdeye.detection.algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;


/**
 * The Legacy dimension wrapper. Do dimension exploration for existing anomaly functions.
 */
public class LegacyDimensionWrapper extends DimensionWrapper {
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_SPEC = "specs";
  private static final String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final BaseAnomalyFunction anomalyFunction;
  private final Map<String, Object> anomalyFunctionSpecs;
  private final String anomalyFunctionClassName;

  /**
   * Instantiates a new Legacy dimension wrapper.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   * @throws Exception the exception
   */
  public LegacyDimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) throws Exception {
    super(provider, augmentConfig(config), startTime, endTime);

    this.anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);
    this.anomalyFunctionSpecs = MapUtils.getMap(config.getProperties(), PROP_SPEC);
    this.anomalyFunction = (BaseAnomalyFunction) Class.forName(this.anomalyFunctionClassName).newInstance();

    String specs = OBJECT_MAPPER.writeValueAsString(this.anomalyFunctionSpecs);
    this.anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));
    if (this.anomalyFunction.getSpec().getExploreDimensions() != null) {
      this.dimensions.add(this.anomalyFunction.getSpec().getExploreDimensions());
    }

    if (this.nestedProperties.isEmpty()) {
      this.nestedProperties.add(Collections.singletonMap(PROP_CLASS_NAME, (Object) LegacyAnomalyFunctionAlgorithm.class.getName()));
    }
  }

  @Override
  protected DetectionPipelineResult runNested(MetricEntity metric, Map<String, Object> template) throws Exception {
    Map<String, Object> properties = new HashMap<>(template);

    properties.put(this.nestedMetricUrnKey, metric.getUrn());
    if (!properties.containsKey(PROP_SPEC)) {
      properties.put(PROP_SPEC, this.anomalyFunctionSpecs);
    }
    if (!properties.containsKey(PROP_ANOMALY_FUNCTION_CLASS)) {
      properties.put(PROP_ANOMALY_FUNCTION_CLASS, this.anomalyFunctionClassName);
    }
    DetectionConfigDTO nestedConfig = new DetectionConfigDTO();
    nestedConfig.setId(this.config.getId());
    nestedConfig.setName(this.config.getName());
    nestedConfig.setProperties(properties);

    DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

    return pipeline.run();
  }

  private static DetectionConfigDTO augmentConfig(DetectionConfigDTO config) {
    config.setProperties(augmentProperties(config.getProperties()));
    return config;
  }

  private static Map<String, Object> augmentProperties(Map<String, Object> properties) {
    Map<String, Object> spec = ConfigUtils.getMap(properties.get(PROP_SPEC));

    if (!properties.containsKey(PROP_METRIC_URN)) {
      long metricId = MapUtils.getLongValue(spec, "metricId");
      String filterString = MapUtils.getString(spec, "filters");

      Multimap<String, String> filters = HashMultimap.create();
      for (String item : filterString.split(";")) {
        String[] parts = item.split("=", 2);
        filters.put(parts[0], parts[1]);
      }

      properties.put(PROP_METRIC_URN,MetricEntity.fromMetric(1.0, metricId, filters).getUrn());
    }
    
    return properties;
  }
}
