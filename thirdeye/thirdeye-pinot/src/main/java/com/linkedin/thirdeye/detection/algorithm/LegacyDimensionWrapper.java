package com.linkedin.thirdeye.detection.algorithm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;


/**
 * The Legacy dimension wrapper. Do dimension exploration for existing anomaly functions.
 */
public class LegacyDimensionWrapper extends DimensionWrapper {
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
  public LegacyDimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);

    this.anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);
    this.anomalyFunctionSpecs = MapUtils.getMap(config.getProperties(), PROP_SPEC);
    this.anomalyFunction = (BaseAnomalyFunction) Class.forName(this.anomalyFunctionClassName).newInstance();

    String specs = OBJECT_MAPPER.writeValueAsString(this.anomalyFunctionSpecs);
    this.anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));
    if (this.anomalyFunction.getSpec().getExploreDimensions() != null) {
      this.dimensions.add(this.anomalyFunction.getSpec().getExploreDimensions());
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
}
