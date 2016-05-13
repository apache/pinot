package com.linkedin.thirdeye.dashboard;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.Connection;
import com.linkedin.pinot.client.ConnectionFactory;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.client.PinotQuery;
import com.linkedin.thirdeye.client.QueryCache;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdeyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.CollectionMaxDataTimeCacheLoader;
import com.linkedin.thirdeye.client.cache.CollectionSchemaCacheLoader;
import com.linkedin.thirdeye.client.cache.ResultSetGroupCacheLoader;
import com.linkedin.thirdeye.client.cache.SchemaCacheLoader;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientFactory;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.FileBasedConfigDAOFactory;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.detector.resources.AnomalyResultResource;

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboardApplication.class);

  private static final String WEBAPP_CONFIG = "/webapp-config";

  private LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache;
  private LoadingCache<String, Schema> schemaCache;
  private LoadingCache<String, CollectionSchema> collectionSchemaCache;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;
  private ThirdeyeCacheRegistry cacheRegistry;

  public ThirdEyeDashboardApplication() {

  }

  @Override
  public String getName() {
    return "Thirdeye Dashboard";
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new HelperBundle());
    bootstrap.addBundle(hibernateBundle);
    bootstrap.addBundle(new AssetsBundle("/assets", "/assets"));
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/lib", "/assets/lib", null, "lib"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/data", "/assets/data", null, "data"));

  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment env) throws Exception {
    super.initDetectorRelatedDAO();

    // pinotThirdeyeClient configs
    PinotThirdEyeClientConfig pinotThirdeyeClientConfig =
        PinotThirdEyeClientConfig.createThirdEyeClientConfig(config);

    // Thirdeye client
    ThirdEyeClient thirdEyeClient = PinotThirdEyeClient.fromClientConfig(pinotThirdeyeClientConfig);

    AbstractConfigDAO<DashboardConfig> dashbaordConfigDAO = getDashboardConfigDAO(config);
    AbstractConfigDAO<CollectionConfig> collectionConfigDAO = getCollectionConfigDAO(config);
    AbstractConfigDAO<CollectionSchema> collectionSchemaDAO = getCollectionSchemaDAO(config);

    // initialize caches
    initCacheLoaders(pinotThirdeyeClientConfig, collectionConfigDAO, collectionSchemaDAO);

    ExecutorService queryExecutor = env.lifecycle().executorService("query_executor").build();
    QueryCache queryCache = createQueryCache(thirdEyeClient, queryExecutor);
    env.jersey().register(new DashboardResource(queryCache, dashbaordConfigDAO));
    env.jersey().register(new AnomalyResultResource(anomalyResultDAO));
  }

  private void initCacheLoaders(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionConfig> collectionConfigDAO,
      AbstractConfigDAO<CollectionSchema> collectionSchemaDAO) {
    cacheRegistry = ThirdeyeCacheRegistry.getInstance();
    // TODO: have a limit on key size and maximum weight
    resultSetGroupCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
        .build(new ResultSetGroupCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerResultSetGroupCache(resultSetGroupCache);

    schemaCache = CacheBuilder.newBuilder().build(new SchemaCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerSchemaCache(schemaCache);

    // TODO: add a end point to refresh/expire any cache on demand
    collectionSchemaCache =
        CacheBuilder.newBuilder().build(new CollectionSchemaCacheLoader(pinotThirdeyeClientConfig,
            collectionConfigDAO, collectionSchemaDAO));
    cacheRegistry.registerCollectionSchemaCache(collectionSchemaCache);

    collectionMaxDataTimeCache = CacheBuilder.newBuilder()
        .build(new CollectionMaxDataTimeCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerCollectionMaxDataTimeCache(collectionMaxDataTimeCache);
  }

  private QueryCache createQueryCache(ThirdEyeClient thirdEyeClient,
      ExecutorService queryExecutor) {
    QueryCache queryCache = new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10));
    return queryCache;
  }

  private String getWebappConfigDir(ThirdEyeDashboardConfiguration config) {
    String configRootDir = config.getRootDir();
    String webappConfigDir = configRootDir + WEBAPP_CONFIG;
    return webappConfigDir;
  }

  private AbstractConfigDAO<DashboardConfig> getDashboardConfigDAO(
      ThirdEyeDashboardConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<DashboardConfig> configDAO = configDAOFactory.getDashboardConfigDAO();
    return configDAO;
  }

  private AbstractConfigDAO<CollectionConfig> getCollectionConfigDAO(
      ThirdEyeDashboardConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<CollectionConfig> configDAO = configDAOFactory.getCollectionConfigDAO();
    return configDAO;
  }

  private AbstractConfigDAO<CollectionSchema> getCollectionSchemaDAO(
      ThirdEyeDashboardConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<CollectionSchema> configDAO = configDAOFactory.getCollectionSchemaDAO();
    return configDAO;
  }

  public static void main(String[] args) throws Exception {
    String thirdEyeConfigDir = args[0];
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String dashboardApplicationConfigFile = thirdEyeConfigDir + "/" + "dashboard.yml";
    new ThirdEyeDashboardApplication().run(new String[] {
        "server", dashboardApplicationConfigFile
    });
  }
}
