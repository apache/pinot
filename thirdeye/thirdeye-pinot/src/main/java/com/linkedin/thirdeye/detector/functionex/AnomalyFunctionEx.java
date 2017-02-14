package com.linkedin.thirdeye.detector.functionex;

public abstract class AnomalyFunctionEx {

  AnomalyFunctionExContext context;

  public void setContext(AnomalyFunctionExContext context) {
    this.context = context;
  }

  protected String getConfig(String key) {
    return context.getConfig().get(key);
  }

  protected String getConfig(String key, String defaultValue) {
    if(!context.getConfig().containsKey(key))
      return defaultValue;
    return context.getConfig().get(key);
  }

  protected double getConfigDouble(String key) {
    return Double.parseDouble(getConfig(key));
  }

  protected double getConfigDouble(String key, double defaultValue) {
    if(!context.getConfig().containsKey(key))
      return defaultValue;
    return Double.parseDouble(getConfig(key));
  }

  protected int getConfigInt(String key) {
    return Integer.parseInt(getConfig(key));
  }

  protected int getConfigInt(String key, int defaultValue) {
    if(!context.getConfig().containsKey(key))
      return defaultValue;
    return Integer.parseInt(getConfig(key));
  }

  protected long getConfigLong(String key) {
    return Long.parseLong(getConfig(key));
  }

  protected long getConfigLong(String key, long defaultValue) {
    if(!context.getConfig().containsKey(key))
      return defaultValue;
    return Long.parseLong(getConfig(key));
  }

  protected boolean getConfigBoolean(String key) {
    return Boolean.parseBoolean(getConfig(key));
  }

  protected boolean getConfigBoolean(String key, boolean defaultValue) {
    if(!context.getConfig().containsKey(key))
      return defaultValue;
    return Boolean.parseBoolean(getConfig(key));
  }

  protected <R, Q> R queryDataSource(String dataSource, Q query) throws Exception {
    if(!hasDataSource(dataSource))
      throw new IllegalArgumentException(String.format("DataSource '%s' not available", dataSource));
    return (R) context.getDataSources().get(dataSource).query(query, context);
  }

  protected boolean hasDataSource(String dataSource) {
    return context.getDataSources().containsKey(dataSource);
  }

  public abstract AnomalyFunctionExResult apply() throws Exception;

}
