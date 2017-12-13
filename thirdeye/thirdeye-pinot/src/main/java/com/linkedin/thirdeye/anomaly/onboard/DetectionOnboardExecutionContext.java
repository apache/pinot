package com.linkedin.thirdeye.anomaly.onboard;

import java.util.HashMap;
import java.util.Map;

public class DetectionOnboardExecutionContext {
  private Map<String, Object> executionResults = new HashMap<>();

  public Object setExecutionResult(String key, Object value) {
    return this.executionResults.put(key, value);
  }

  public Object getExcutionResult(String key) {
    return this.executionResults.get(key);
  }
}
