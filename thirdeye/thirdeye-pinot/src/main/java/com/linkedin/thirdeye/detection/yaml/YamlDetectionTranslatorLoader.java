package com.linkedin.thirdeye.detection.yaml;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Loads the detection config translator fo a pipeline type
 */
public class YamlDetectionTranslatorLoader {
  private static final String PROP_PIPELINE_TYPE= "pipelineType";

  private static final Map<String, String> PIPELINE_TYPE_REGISTRY = ImmutableMap.<String, String>builder()
      .put("COMPOSITE", CompositePipelineConfigTranslator.class.getName())
      .build();

  public YamlDetectionConfigTranslator from(Map<String, Object> yamlConfig, DataProvider provider) throws Exception {
    String className = PIPELINE_TYPE_REGISTRY.get(yamlConfig.get(PROP_PIPELINE_TYPE).toString().toUpperCase());
    Constructor<?> constructor = Class.forName(className).getConstructor(Map.class, DataProvider.class);
    return (YamlDetectionConfigTranslator) constructor.newInstance(yamlConfig, provider);
  }

}
