package com.linkedin.thirdeye.detection.yaml;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.detection.algorithm.BaselineAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.BaselineRuleFilterWrapper;
import com.linkedin.thirdeye.detection.algorithm.MovingWindowAlgorithm;
import java.util.Map;


public class YamlTranslatorInfoMap {
  private static final Map<String, String> YAML_INFO_MAP = ImmutableMap.<String, String>builder()
      .put("BASELINE", String.valueOf(BaselineAlgorithm.class.getName()))
      .put("BUSINESS_RULE_FILTER", BaselineRuleFilterWrapper.class.getName())
      .put("MOVING_WINDOW", MovingWindowAlgorithm.class.getName())
      .build();

  public String get(String key) {
    return YAML_INFO_MAP.get(key);
  }
}
