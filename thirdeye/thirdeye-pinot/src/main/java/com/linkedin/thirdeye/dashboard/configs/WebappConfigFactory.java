package com.linkedin.thirdeye.dashboard.configs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.CollectionSchema;

public class WebappConfigFactory {

  private static final Logger LOG = LoggerFactory.getLogger(WebappConfigFactory.class);

  public enum WebappConfigType {
    COLLECTION_CONFIG,
    COLLECTION_SCHEMA,
    DASHBOARD_CONFIG
  }

  public static AbstractConfig getConfigFromConfigTypeAndJson(WebappConfigType configType, String payload) {
    AbstractConfig config = null;
    try {
      switch (configType) {
        case COLLECTION_CONFIG:
          config = AbstractConfig.fromJSON(payload, CollectionConfig.class);
          break;
        case COLLECTION_SCHEMA:
          config = AbstractConfig.fromJSON(payload, CollectionSchema.class);
          break;
        case DASHBOARD_CONFIG:
          config = AbstractConfig.fromJSON(payload, DashboardConfig.class);
          break;
        default:
          throw new UnsupportedOperationException("Invalid config type " + configType);
      }
    } catch (Exception e) {
      LOG.error("Invalid payload {} for configType {}", payload, configType, e);
    }
    return config;
  }

}
