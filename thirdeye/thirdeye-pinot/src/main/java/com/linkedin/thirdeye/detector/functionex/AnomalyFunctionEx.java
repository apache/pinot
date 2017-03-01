package com.linkedin.thirdeye.detector.functionex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AnomalyFunctionEx {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionEx.class);

  // TODO allow StrSubstitutor of jakarta commons lang

  AnomalyFunctionExContext context;

  public void setContext(AnomalyFunctionExContext context) {
    this.context = context;
  }

  public AnomalyFunctionExContext getContext() {
    return context;
  }

  protected String getConfig(String key) {
    if(!context.getConfig().containsKey(key))
      throw new IllegalStateException(String.format("Config or default for '%s' required", key));
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
    LOG.info("Querying '{}': {}", dataSource, query);
    return (R) context.getDataSources().get(dataSource).query(query, context);
  }

  protected boolean hasDataSource(String dataSource) {
    return context.getDataSources().containsKey(dataSource);
  }

  public abstract AnomalyFunctionExResult apply() throws Exception;

}
