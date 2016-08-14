package com.linkedin.thirdeye.dashboard.configs;

import com.linkedin.thirdeye.api.CollectionSchema;

public class WebappConfigClassFactory {

  public enum WebappConfigType {
    CollectionConfig,
    CollectionSchema,
    DashboardConfig
  }

  public static String getSimpleNameFromConfigType(WebappConfigType configType) {
    String simpleName = null;
    switch (configType) {
      case CollectionConfig:
        simpleName = CollectionConfig.class.getSimpleName();
        break;
      case CollectionSchema:
        simpleName = CollectionSchema.class.getSimpleName();
        break;
      case DashboardConfig:
        simpleName = DashboardConfig.class.getSimpleName();
        break;
      default:
        throw new UnsupportedOperationException("Invalid config type " + configType);

    }
    return simpleName;
  }

  public static Class<? extends AbstractConfig> getClassFromConfigType(WebappConfigType configType) {
    Class<? extends AbstractConfig> configClass = null;
    switch (configType) {
      case CollectionConfig:
        configClass = CollectionConfig.class;
        break;
      case CollectionSchema:
        configClass = CollectionSchema.class;
        break;
      case DashboardConfig:
        configClass = DashboardConfig.class;
        break;
      default:
        throw new UnsupportedOperationException("Invalid config type " + configType);
    }
    return configClass;
  }

}
