package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import java.util.Collections;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

public class DetectionOnboardTaskContext {
  private Configuration configuration = new MapConfiguration(Collections.emptyMap());
  private DetectionOnboardExecutionContext executionContext = new DetectionOnboardExecutionContext();

  public DetectionOnboardTaskContext() {
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    Preconditions.checkNotNull(configuration);
    this.configuration = configuration;
  }

  public DetectionOnboardExecutionContext getExecutionContext() {
    return executionContext;
  }

  public void setExecutionContext(DetectionOnboardExecutionContext executionContext) {
    Preconditions.checkNotNull(executionContext);
    this.executionContext = executionContext;
  }
}
