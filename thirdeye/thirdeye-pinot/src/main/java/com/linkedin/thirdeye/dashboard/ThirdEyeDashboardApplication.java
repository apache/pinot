package com.linkedin.thirdeye.dashboard;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.client.QueryCache;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdeyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.CollectionsCache;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.FileBasedConfigDAOFactory;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.detector.resources.AnomalyResultResource;

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboardApplication.class);

  public static final String WEBAPP_CONFIG = "/webapp-config";

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

    // PinotThirdeyeClient config
    PinotThirdEyeClientConfig pinotThirdeyeClientConfig =
        PinotThirdEyeClientConfig.createThirdEyeClientConfig(config);

    // ThirdEye client
    ThirdEyeClient thirdEyeClient = PinotThirdEyeClient.fromClientConfig(pinotThirdeyeClientConfig);

    AbstractConfigDAO<DashboardConfig> dashbaordConfigDAO = getDashboardConfigDAO(config);
    AbstractConfigDAO<CollectionSchema> collectionSchemaDAO = getCollectionSchemaDAO(config);

    // initialize caches
    try {
      initCacheLoaders(pinotThirdeyeClientConfig, collectionSchemaDAO, config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    ExecutorService queryExecutor = env.lifecycle().executorService("query_executor").build();
    QueryCache queryCache = createQueryCache(thirdEyeClient, queryExecutor);
    env.jersey().register(new DashboardResource(queryCache, dashbaordConfigDAO));
    env.jersey().register(new AnomalyResultResource(anomalyResultDAO));
    env.jersey().register(new CacheResource());
  }

  private void initCacheLoaders(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionSchema> collectionSchemaDAO,
      ThirdEyeDashboardConfiguration config) throws ClientProtocolException, IOException {
    super.initCacheLoaders(pinotThirdeyeClientConfig, collectionSchemaDAO);
    // Add collections cache in addition to defaults
    ThirdeyeCacheRegistry cacheRegistry = ThirdeyeCacheRegistry.getInstance();
    CollectionsCache collectionsCache = new CollectionsCache(pinotThirdeyeClientConfig, config);
    cacheRegistry.registerCollectionsCache(collectionsCache);
  }

  private QueryCache createQueryCache(ThirdEyeClient thirdEyeClient,
      ExecutorService queryExecutor) {
    QueryCache queryCache = new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10));
    return queryCache;
  }

  private AbstractConfigDAO<DashboardConfig> getDashboardConfigDAO(
      ThirdEyeDashboardConfiguration config) {
    FileBasedConfigDAOFactory configDAOFactory =
        new FileBasedConfigDAOFactory(getWebappConfigDir(config));
    AbstractConfigDAO<DashboardConfig> configDAO = configDAOFactory.getDashboardConfigDAO();
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
