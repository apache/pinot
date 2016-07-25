package com.linkedin.thirdeye.common;

import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardApplication;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.FileBasedConfigDAOFactory;
import com.linkedin.thirdeye.dashboard.configs.WidgetConfig;
import com.linkedin.thirdeye.db.dao.AnomalyFunctionDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.dao.EmailConfigurationDAO;
import com.linkedin.thirdeye.db.entity.AnomalyFeedback;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionRelation;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyJobSpec;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import com.linkedin.thirdeye.db.entity.AnomalyTaskSpec;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import com.linkedin.thirdeye.db.entity.EmailFunctionDependency;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyJobSpecDAO;
import com.linkedin.thirdeye.detector.db.dao.AnomalyTaskSpecDAO;
import com.linkedin.thirdeye.detector.db.EmailFunctionDependencyDAO;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import java.io.File;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {

  protected final HibernateBundle<ThirdEyeConfiguration> hibernateBundle =
      new HibernateBundle<ThirdEyeConfiguration>(AnomalyFunctionSpec.class,
          AnomalyFunctionRelation.class, AnomalyResult.class, EmailConfiguration.class,
          EmailFunctionDependency.class, AnomalyJobSpec.class, AnomalyTaskSpec.class,
          AnomalyFeedback.class) {
        @Override
        public DataSourceFactory getDataSourceFactory(ThirdEyeConfiguration config) {
          return config.getDatabase();
        }
      };
  protected AnomalyFunctionDAO anomalyFunctionSpecDAO;
  protected AnomalyResultDAO anomalyResultDAO;
  protected EmailConfigurationDAO emailConfigurationDAO;

  protected AnomalyFunctionRelationDAO anomalyFunctionRelationDAO;
  protected EmailFunctionDependencyDAO emailFunctionDependencyDAO;
  protected AnomalyJobSpecDAO anomalyJobSpecDAO;
  protected AnomalyTaskSpecDAO anomalyTaskSpecDAO;

  public void initDetectorRelatedDAO() {
    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence.yml";
    PersistenceUtil.init(new File(persistenceConfig));
    anomalyFunctionSpecDAO = PersistenceUtil.getInstance(AnomalyFunctionDAO.class);
    anomalyResultDAO = PersistenceUtil.getInstance(AnomalyResultDAO.class);
    emailConfigurationDAO = PersistenceUtil.getInstance(EmailConfigurationDAO.class);

    // TODO: change these to new DAO
    anomalyFunctionRelationDAO = new AnomalyFunctionRelationDAO(hibernateBundle.getSessionFactory());
    emailFunctionDependencyDAO = new EmailFunctionDependencyDAO(hibernateBundle.getSessionFactory());
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
