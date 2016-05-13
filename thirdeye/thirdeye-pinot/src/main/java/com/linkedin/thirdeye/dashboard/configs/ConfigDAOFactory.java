package com.linkedin.thirdeye.dashboard.configs;

import com.linkedin.thirdeye.api.CollectionSchema;

public interface ConfigDAOFactory {

  AbstractConfigDAO<CollectionConfig> getCollectionConfigDAO();

  AbstractConfigDAO<DashboardConfig> getDashboardConfigDAO();

  AbstractConfigDAO<WidgetConfig> getWidgetConfigDAO();

  AbstractConfigDAO<CollectionSchema> getCollectionSchemaDAO();

}
