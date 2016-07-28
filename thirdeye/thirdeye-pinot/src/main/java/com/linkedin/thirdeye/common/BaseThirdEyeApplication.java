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
import com.linkedin.thirdeye.db.dao.AnomalyJobDAO;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.dao.AnomalyTaskDAO;
import com.linkedin.thirdeye.db.dao.EmailConfigurationDAO;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionRelation;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass());

  protected HibernateBundle<ThirdEyeConfiguration> hibernateBundle =
      new HibernateBundle<ThirdEyeConfiguration>(AnomalyFunctionSpec.class, AnomalyFunctionRelation.class,
          EmailConfiguration.class) {
        @Override
        public DataSourceFactory getDataSourceFactory(ThirdEyeConfiguration config) {
          return config.getDatabase();
        }
      };

  protected AnomalyFunctionDAO anomalyFunctionDAO;
  protected AnomalyResultDAO anomalyResultDAO;
  protected EmailConfigurationDAO emailConfigurationDAO;
  protected AnomalyJobDAO anomalyJobDAO;
  protected AnomalyTaskDAO anomalyTaskDAO;

  //TODO : remove this
  protected AnomalyFunctionRelationDAO anomalyFunctionRelationDAO;

  public void initDashboardRelatedDAO() {
    String persistenceConfig = System.getProperty("dw.rootDir") + "/persistence.yml";
    LOG.info("Loading persistence config from [{}]", persistenceConfig);
    PersistenceUtil.init(new File(persistenceConfig));
    anomalyFunctionDAO = PersistenceUtil.getInstance(AnomalyFunctionDAO.class);
    anomalyResultDAO = PersistenceUtil.getInstance(AnomalyResultDAO.class);
    emailConfigurationDAO = PersistenceUtil.getInstance(EmailConfigurationDAO.class);
  }

  public void initDetectorRelatedDAO() {
    initDashboardRelatedDAO();

    // TODO: change these to new DAO and remove hibernateBundle
    anomalyFunctionRelationDAO =
        new AnomalyFunctionRelationDAO(hibernateBundle.getSessionFactory());

    anomalyJobDAO = PersistenceUtil.getInstance(AnomalyJobDAO.class);
    anomalyTaskDAO = PersistenceUtil.getInstance(AnomalyTaskDAO.class);
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
