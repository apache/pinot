package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class DetectionOnboardTaskContext {
  private boolean abortAtFailure = true;
  private ImmutableMap<String, String> properties = ImmutableMap.of();

  public boolean isAbortAtFailure() {
    return abortAtFailure;
  }

  public void setAbortAtFailure(boolean abortAtFailure) {
    this.abortAtFailure = abortAtFailure;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    Preconditions.checkNotNull(properties);
    this.properties = ImmutableMap.copyOf(properties);
  }
}
