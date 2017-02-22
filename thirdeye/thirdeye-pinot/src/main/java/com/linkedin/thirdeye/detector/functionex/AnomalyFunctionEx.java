package com.linkedin.thirdeye.detector.functionex;

public abstract class AnomalyFunctionEx {

  // TODO allow StrSubstitutor of jakarta commons lang

  AnomalyFunctionExContext context;

  public void setContext(AnomalyFunctionExContext context) {
    this.context = context;
  }

  public AnomalyFunctionExContext getContext() {
    return context;
  }

  protected String getConfig(String key) {
    return context.getConfig().get(key);
  }

  protected String getConfig(String key, String defaultValue) {
    if(!context.getConfig().containsKey(key))
      return defaultValue;
    return context.getConfig().get(key);
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
