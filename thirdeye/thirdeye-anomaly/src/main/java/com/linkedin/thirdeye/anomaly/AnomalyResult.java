package com.linkedin.thirdeye.anomaly;

import java.util.Properties;

public class AnomalyResult
{
  private final boolean isAnomaly;
  private final Properties properties;

  public AnomalyResult(boolean isAnomaly)
  {
    this(isAnomaly, null);
  }

  public AnomalyResult(boolean isAnomaly, Properties properties)
  {
    this.isAnomaly = isAnomaly;
    this.properties = properties;
  }

  public boolean isAnomaly()
  {
    return isAnomaly;
  }

  public Properties getProperties()
  {
    return properties;
  }
}
