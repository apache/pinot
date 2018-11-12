package com.linkedin.thirdeye.detection.yaml;

import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.annotation.DetectionRegistry;
import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Loads the detection config translator fo a pipeline type
 */
public class YamlDetectionTranslatorLoader {
  private static final String PROP_PIPELINE_TYPE= "pipelineType";
  private static DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();

  public YamlDetectionConfigTranslator from(Map<String, Object> yamlConfig, DataProvider provider) throws Exception {
    String className = DETECTION_REGISTRY.lookupYamlConverter(yamlConfig.get(PROP_PIPELINE_TYPE).toString().toUpperCase());
    Constructor<?> constructor = Class.forName(className).getConstructor(Map.class, DataProvider.class);
    return (YamlDetectionConfigTranslator) constructor.newInstance(yamlConfig, provider);
  }

}
