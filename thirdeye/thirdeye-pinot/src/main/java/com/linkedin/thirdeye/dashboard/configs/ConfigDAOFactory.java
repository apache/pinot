package com.linkedin.thirdeye.dashboard.configs;

public interface ConfigDAOFactory {

  AbstractConfigDAO<CollectionConfig> getCollectionConfigDAO();
  
  AbstractConfigDAO<DashboardConfig> getDashboardConfigDAO();
  
  AbstractConfigDAO<WidgetConfig> getWidgetConfigDAO();
  
}
