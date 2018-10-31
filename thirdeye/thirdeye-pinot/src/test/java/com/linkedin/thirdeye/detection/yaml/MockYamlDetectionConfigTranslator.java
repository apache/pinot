package com.linkedin.thirdeye.detection.yaml;

import java.util.HashMap;
import java.util.Map;


public class MockYamlDetectionConfigTranslator extends YamlDetectionConfigTranslator {

  @Override
  Map<String, Object> buildDetectionProperties(Map<String, Object> yamlConfig) {
    Map<String, Object> result = new HashMap<>();
    result.put("yamlConfigs", yamlConfig);
    return result;
  }
}
