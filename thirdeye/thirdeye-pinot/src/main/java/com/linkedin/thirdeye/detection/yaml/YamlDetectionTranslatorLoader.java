package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Loads the detection config translator fo a pipeline type
 */
public class YamlDetectionTranslatorLoader {
  private static final String PROP_PIPELINE_TYPE= "pipelineType";

  private static final Map<String, String> PIPELINE_TYPE_REGISTRY = ImmutableMap.<String, String>builder()
      .put("COMPOSITE", CompositePipelineConfigTranslator.class.getName())
      .build();

  public YamlDetectionConfigTranslator from(Map<String, Object> yamlConfig) throws Exception {
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_PIPELINE_TYPE), "pipeline type not found, abort.");
    String className = this.PIPELINE_TYPE_REGISTRY.get(yamlConfig.get(PROP_PIPELINE_TYPE).toString().toUpperCase());
    return (YamlDetectionConfigTranslator) Class.forName(className).newInstance();
  }

}
