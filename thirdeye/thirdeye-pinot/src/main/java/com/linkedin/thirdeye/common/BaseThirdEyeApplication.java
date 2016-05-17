package com.linkedin.thirdeye.common;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.ClientProtocolException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.client.PinotQuery;
import com.linkedin.thirdeye.client.ThirdeyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.CollectionMaxDataTimeCacheLoader;
import com.linkedin.thirdeye.client.cache.CollectionSchemaCacheLoader;
import com.linkedin.thirdeye.client.cache.ResultSetGroupCacheLoader;
import com.linkedin.thirdeye.client.cache.SchemaCacheLoader;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardApplication;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.FileBasedConfigDAOFactory;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionRelation;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.ContextualEvent;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.ContextualEventDAO;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;

public abstract class BaseThirdEyeApplication<T extends Configuration> extends Application<T> {
  protected final HibernateBundle<ThirdEyeConfiguration> hibernateBundle =
      new HibernateBundle<ThirdEyeConfiguration>(AnomalyFunctionSpec.class,
          AnomalyFunctionRelation.class, AnomalyResult.class, ContextualEvent.class,
          EmailConfiguration.class) {
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

  public void initDetectorRelatedDAO() {
    anomalyFunctionSpecDAO = new AnomalyFunctionSpecDAO(hibernateBundle.getSessionFactory());
    anomalyResultDAO = new AnomalyResultDAO(hibernateBundle.getSessionFactory());
    contextualEventDAO = new ContextualEventDAO(hibernateBundle.getSessionFactory());
    emailConfigurationDAO = new EmailConfigurationDAO(hibernateBundle.getSessionFactory());
    anomalyFunctionRelationDAO =
        new AnomalyFunctionRelationDAO(hibernateBundle.getSessionFactory());
  }

  public void initCacheLoaders(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionSchema> collectionSchemaDAO)
      throws ClientProtocolException, IOException {

    ThirdeyeCacheRegistry cacheRegistry = ThirdeyeCacheRegistry.getInstance();

    // TODO: have a limit on key size and maximum weight
    LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache =
        CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
            .build(new ResultSetGroupCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerResultSetGroupCache(resultSetGroupCache);

    LoadingCache<String, Schema> schemaCache =
        CacheBuilder.newBuilder().build(new SchemaCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerSchemaCache(schemaCache);

    LoadingCache<String, CollectionSchema> collectionSchemaCache = CacheBuilder.newBuilder()
        .build(new CollectionSchemaCacheLoader(pinotThirdeyeClientConfig, collectionSchemaDAO));
    cacheRegistry.registerCollectionSchemaCache(collectionSchemaCache);

    LoadingCache<String, Long> collectionMaxDataTimeCache = CacheBuilder.newBuilder()
        .build(new CollectionMaxDataTimeCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerCollectionMaxDataTimeCache(collectionMaxDataTimeCache);
  }

  // TODO below two methods depend on webapp configs
  protected AbstractConfigDAO<CollectionSchema> getCollectionSchemaDAO(
      ThirdEyeConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<CollectionSchema> configDAO = configDAOFactory.getCollectionSchemaDAO();
    return configDAO;
  }

  protected String getWebappConfigDir(ThirdEyeConfiguration config) {
    String configRootDir = config.getRootDir();
    String webappConfigDir = configRootDir + ThirdEyeDashboardApplication.WEBAPP_CONFIG;
    return webappConfigDir;
  }

}
