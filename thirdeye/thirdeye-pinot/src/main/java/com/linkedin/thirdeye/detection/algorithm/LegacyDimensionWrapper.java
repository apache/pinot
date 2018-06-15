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


public class LegacyDimensionWrapper extends DimensionWrapper {
  private static String PROP_SPEC = "specs";
  private static String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private BaseAnomalyFunction anomalyFunction;
  private Map<String, Object> anomalyFunctionSpecs;
  private String anomalyFunctionClassName;

  public LegacyDimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);
    anomalyFunctionClassName = MapUtils.getString(config.getProperties(), PROP_ANOMALY_FUNCTION_CLASS);

    anomalyFunctionSpecs = MapUtils.getMap(config.getProperties(), PROP_SPEC);
    anomalyFunction = (BaseAnomalyFunction) Class.forName(anomalyFunctionClassName).newInstance();
    String specs = OBJECT_MAPPER.writeValueAsString(anomalyFunctionSpecs);
    anomalyFunction.init(OBJECT_MAPPER.readValue(specs, AnomalyFunctionDTO.class));
    if (anomalyFunction.getSpec().getExploreDimensions() != null) {
      this.dimensions.add(anomalyFunction.getSpec().getExploreDimensions());
    }
  }


  @Override
  protected DetectionPipelineResult runNested(MetricEntity metric, Map<String, Object> template) throws Exception {


    Map<String, Object> properties = new HashMap<>(template);

    properties.put(this.nestedMetricUrnKey, metric.getUrn());
    properties.put(PROP_ANOMALY_FUNCTION_CLASS, anomalyFunctionClassName);
    properties.put(PROP_SPEC, anomalyFunctionSpecs);

    DetectionConfigDTO nestedConfig = new DetectionConfigDTO();
    nestedConfig.setId(this.config.getId());
    nestedConfig.setName(this.config.getName());
    nestedConfig.setProperties(properties);

    DetectionPipeline pipeline = this.provider.loadPipeline(nestedConfig, this.startTime, this.endTime);

    return pipeline.run();
  }
}
