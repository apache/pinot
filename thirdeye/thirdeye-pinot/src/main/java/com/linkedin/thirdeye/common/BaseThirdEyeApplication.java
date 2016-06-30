package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardApplication;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.FileBasedConfigDAOFactory;
import com.linkedin.thirdeye.dashboard.configs.WidgetConfig;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionRelation;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyJobSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.AnomalyTaskSpec;
import com.linkedin.thirdeye.detector.api.ContextualEvent;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;
import com.linkedin.thirdeye.detector.api.EmailFunctionDependency;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.db.ContextualEventDAO;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.detector.db.EmailFunctionDependencyDAO;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {

  protected final HibernateBundle<ThirdEyeConfiguration> hibernateBundle =
      new HibernateBundle<ThirdEyeConfiguration>(AnomalyFunctionSpec.class,
          AnomalyFunctionRelation.class, AnomalyResult.class, ContextualEvent.class,
          EmailConfiguration.class, EmailFunctionDependency.class, AnomalyJobSpec.class,
          AnomalyTaskSpec.class) {
        @Override
        public DataSourceFactory getDataSourceFactory(ThirdEyeConfiguration config) {
          return config.getDatabase();
        }
      };
  protected AnomalyFunctionSpecDAO anomalyFunctionSpecDAO;
  protected AnomalyResultDAO anomalyResultDAO;
  protected ContextualEventDAO contextualEventDAO;
  protected EmailConfigurationDAO emailConfigurationDAO;
  protected AnomalyFunctionRelationDAO anomalyFunctionRelationDAO;
  protected EmailFunctionDependencyDAO emailFunctionDependencyDAO;
  protected AnomalyJobSpecDAO anomalyJobSpecDAO;
  protected AnomalyTaskSpecDAO anomalyTaskSpecDAO;

  public void initDetectorRelatedDAO() {
    anomalyFunctionSpecDAO = new AnomalyFunctionSpecDAO(hibernateBundle.getSessionFactory());
    anomalyResultDAO = new AnomalyResultDAO(hibernateBundle.getSessionFactory());
    contextualEventDAO = new ContextualEventDAO(hibernateBundle.getSessionFactory());
    emailConfigurationDAO = new EmailConfigurationDAO(hibernateBundle.getSessionFactory());
    anomalyFunctionRelationDAO =
        new AnomalyFunctionRelationDAO(hibernateBundle.getSessionFactory());
    emailFunctionDependencyDAO =
        new EmailFunctionDependencyDAO(hibernateBundle.getSessionFactory());
    anomalyJobSpecDAO = new AnomalyJobSpecDAO(hibernateBundle.getSessionFactory());
    anomalyTaskSpecDAO = new AnomalyTaskSpecDAO(hibernateBundle.getSessionFactory());
  }

  // TODO below two methods depend on webapp configs
  public static AbstractConfigDAO<CollectionSchema> getCollectionSchemaDAO(
      ThirdEyeConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<CollectionSchema> configDAO = configDAOFactory.getCollectionSchemaDAO();
    return configDAO;
  }

  public static AbstractConfigDAO<CollectionConfig> getCollectionConfigDAO(
      ThirdEyeConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<CollectionConfig> configDAO = configDAOFactory.getCollectionConfigDAO();
    return configDAO;
  }

  public static AbstractConfigDAO<DashboardConfig> getDashboardConfigDAO(
      ThirdEyeConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<DashboardConfig> configDAO = configDAOFactory.getDashboardConfigDAO();
    return configDAO;
  }

  public static AbstractConfigDAO<WidgetConfig> getWidgetConfigDAO(ThirdEyeConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<WidgetConfig> configDAO = configDAOFactory.getWidgetConfigDAO();
    return configDAO;
  }

  private static String getWebappConfigDir(ThirdEyeConfiguration config) {
    String configRootDir = config.getRootDir();
    String webappConfigDir = configRootDir + ThirdEyeDashboardApplication.WEBAPP_CONFIG;
    return webappConfigDir;
  }

}
