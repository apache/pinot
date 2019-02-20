package org.apache.pinot.thirdeye.detection.yaml;

import org.apache.pinot.thirdeye.detection.DataProvider;
import java.util.HashMap;
import java.util.Map;


public class MockYamlDetectionConfigTranslator extends YamlDetectionConfigTranslator {
  public MockYamlDetectionConfigTranslator(Map<String, Object> yamlConfig, DataProvider provider) {
    super(yamlConfig, provider);
  }

  @Override
  YamlTranslationResult translateYaml() {
    Map<String, Object> result = new HashMap<>();
    result.put("yamlConfigs", yamlConfig);
    return new YamlTranslationResult().withProperties(result);
  }
}
