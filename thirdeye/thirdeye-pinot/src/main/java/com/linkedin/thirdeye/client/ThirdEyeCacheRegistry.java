package com.linkedin.thirdeye.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.client.cache.CollectionConfigCacheLoader;
import com.linkedin.thirdeye.client.cache.CollectionMaxDataTimeCacheLoader;
import com.linkedin.thirdeye.client.cache.CollectionSchemaCacheLoader;
import com.linkedin.thirdeye.client.cache.CollectionsCache;
import com.linkedin.thirdeye.client.cache.DashboardsCacheLoader;
import com.linkedin.thirdeye.client.cache.DimensionFiltersCacheLoader;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.cache.ResultSetGroupCacheLoader;
import com.linkedin.thirdeye.client.cache.SchemaCacheLoader;
import com.linkedin.thirdeye.client.pinot.PinotQuery;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;

public class ThirdEyeCacheRegistry {

  private LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache;
  private LoadingCache<String, Schema> schemaCache;
  private LoadingCache<String, CollectionSchema> collectionSchemaCache;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;
  private LoadingCache<String, CollectionConfig> collectionConfigCache;
  private LoadingCache<String, String> dashboardsCache;
  private LoadingCache<String, String> dimensionFiltersCache;
  private CollectionsCache collectionsCache;
  private QueryCache queryCache;

  private static AbstractConfigDAO<CollectionSchema> collectionSchemaDAO;
  private static AbstractConfigDAO<CollectionConfig> collectionConfigDAO;
  private static AbstractConfigDAO<DashboardConfig> dashboardConfigDAO;
  private static PinotThirdEyeClientConfig pinotThirdeyeClientConfig;
  private static ThirdEyeClient thirdEyeClient;

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeCacheRegistry.class);

  private static class Holder {
    static final ThirdEyeCacheRegistry INSTANCE = new ThirdEyeCacheRegistry();
  }

  public static ThirdEyeCacheRegistry getInstance() {
    return Holder.INSTANCE;
  }

  private static void init(ThirdEyeConfiguration config) {
    try {

      pinotThirdeyeClientConfig = PinotThirdEyeClientConfig.createThirdEyeClientConfig(config);
      thirdEyeClient = PinotThirdEyeClient.fromClientConfig(pinotThirdeyeClientConfig);

      dashboardConfigDAO = BaseThirdEyeApplication.getDashboardConfigDAO(config);
      collectionSchemaDAO = BaseThirdEyeApplication.getCollectionSchemaDAO(config);
      collectionConfigDAO = BaseThirdEyeApplication.getCollectionConfigDAO(config);

    } catch (Exception e) {
     LOGGER.info("Caught exception while initializing caches", e);
    }

  }

  private static void init(ThirdEyeConfiguration config, PinotThirdEyeClientConfig pinotThirdEyeClientConfig) {
    try {

      pinotThirdeyeClientConfig = pinotThirdEyeClientConfig;
      thirdEyeClient = PinotThirdEyeClient.fromClientConfig(pinotThirdeyeClientConfig);

      dashboardConfigDAO = BaseThirdEyeApplication.getDashboardConfigDAO(config);
      collectionSchemaDAO = BaseThirdEyeApplication.getCollectionSchemaDAO(config);
      collectionConfigDAO = BaseThirdEyeApplication.getCollectionConfigDAO(config);

    } catch (Exception e) {
     LOGGER.info("Caught exception while initializing caches", e);
    }

  }

  /**
   * Initializes detector caches
   * @param config
   */
  public static void initializeDetectorCaches(ThirdEyeConfiguration config) {
    init(config);
    initCommonCacheLoaders();
  }

  /**
   * Initializes webapp caches
   * @param config
   */
  public static void initializeWebappCaches(ThirdEyeConfiguration config) {

    init(config);
    initCommonCacheLoaders();

    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();

    LoadingCache<String, String> dimensionFiltersCache = CacheBuilder.newBuilder()
        .build(new DimensionFiltersCacheLoader(cacheRegistry.getQueryCache()));
    cacheRegistry.registerDimensionFiltersCache(dimensionFiltersCache);

    LoadingCache<String, String> dashboardsCache = CacheBuilder.newBuilder()
        .build(new DashboardsCacheLoader(dashboardConfigDAO));
    cacheRegistry.registerDashboardsCache(dashboardsCache);

    CollectionsCache collectionsCache = new CollectionsCache(pinotThirdeyeClientConfig, config);
    cacheRegistry.registerCollectionsCache(collectionsCache);

    initPeriodicCacheRefresh();
  }

  /**
   * Initializes detector caches with custom PinotThirdEyeClientConfig : ONLY FOR TESTING
   * @param config
   */
  @Deprecated
  public static void initializeDetectorCaches(ThirdEyeConfiguration config, PinotThirdEyeClientConfig pinotThirdEyeClientConfig) {
    init(config, pinotThirdEyeClientConfig);
    initCommonCacheLoaders();
  }

  /**
   * Initialize web app caches with custom PinotThirdEyeClientConfig: ONLY FOR TESTING
   * @param config
   * @param pinotThirdeyeClientConfig
   */
  @Deprecated
  public static void initializeWebappCaches(ThirdEyeConfiguration config, PinotThirdEyeClientConfig pinotThirdeyeClientConfig) {

    init(config, pinotThirdeyeClientConfig);
    initCommonCacheLoaders();

    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();

    LoadingCache<String, String> dimensionFiltersCache = CacheBuilder.newBuilder()
        .build(new DimensionFiltersCacheLoader(cacheRegistry.getQueryCache()));
    cacheRegistry.registerDimensionFiltersCache(dimensionFiltersCache);

    LoadingCache<String, String> dashboardsCache = CacheBuilder.newBuilder()
        .build(new DashboardsCacheLoader(dashboardConfigDAO));
    cacheRegistry.registerDashboardsCache(dashboardsCache);

    CollectionsCache collectionsCache = new CollectionsCache(pinotThirdeyeClientConfig, config);
    cacheRegistry.registerCollectionsCache(collectionsCache);

    initPeriodicCacheRefresh();
  }


  private static void initCommonCacheLoaders() {

    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();

    // ResultSetGroup Cache
    // TODO: have a limit on key size and maximum weight
    LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache =
        CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS)
            .build(new ResultSetGroupCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerResultSetGroupCache(resultSetGroupCache);

    // Schema Cache
    LoadingCache<String, Schema> schemaCache =
        CacheBuilder.newBuilder().build(new SchemaCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerSchemaCache(schemaCache);

    // Collection Schema Cache
    LoadingCache<String, CollectionSchema> collectionSchemaCache = CacheBuilder.newBuilder()
        .build(new CollectionSchemaCacheLoader(pinotThirdeyeClientConfig, collectionSchemaDAO));
    cacheRegistry.registerCollectionSchemaCache(collectionSchemaCache);

    // CollectionMaxDataTime Cache
    LoadingCache<String, Long> collectionMaxDataTimeCache = CacheBuilder.newBuilder()
        .build(new CollectionMaxDataTimeCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerCollectionMaxDataTimeCache(collectionMaxDataTimeCache);

    // CollectionConfig Cache
    LoadingCache<String, CollectionConfig> collectionConfigCache = CacheBuilder.newBuilder()
        .build(new CollectionConfigCacheLoader(pinotThirdeyeClientConfig, collectionConfigDAO));
    cacheRegistry.registerCollectionConfigCache(collectionConfigCache);

    // Query Cache
    QueryCache queryCache = new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10));
    cacheRegistry.registerQueryCache(queryCache);

  }

  private static void initPeriodicCacheRefresh() {

    final CacheResource cacheResource = new CacheResource();
    // manually refreshing on startup, and setting delay
    // as weeklyService starts before hourlyService finishes,
    // causing NPE in reading collectionsCache
    cacheResource.refreshCollections();
    cacheResource.refreshMaxDataTimeCache();
    cacheResource.refreshDimensionFiltersCache();
    cacheResource.refreshDashboardsCache();

    ScheduledExecutorService hourlyService = Executors.newSingleThreadScheduledExecutor();
    hourlyService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        try {
          cacheResource.refreshCollections();
          cacheResource.refreshMaxDataTimeCache();
        } catch (Exception e) {
          LOGGER.error("Exception while loading collections", e);
        }
      }
    }, 1, 1, TimeUnit.HOURS);

    ScheduledExecutorService weeklyService = Executors.newSingleThreadScheduledExecutor();
    weeklyService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        try {
          cacheResource.refreshDimensionFiltersCache();
        } catch (Exception e) {
          LOGGER.error("Exception while loading filter caches", e);
        }
      }
    }, 7, 7, TimeUnit.DAYS);
  }

  public LoadingCache<PinotQuery, ResultSetGroup> getResultSetGroupCache() {
    return resultSetGroupCache;
  }

  public void registerResultSetGroupCache(LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache) {
    this.resultSetGroupCache = resultSetGroupCache;
  }

  public LoadingCache<String, Schema> getSchemaCache() {
    return schemaCache;
  }

  public void registerSchemaCache(LoadingCache<String, Schema> schemaCache) {
    this.schemaCache = schemaCache;
  }

  public LoadingCache<String, CollectionSchema> getCollectionSchemaCache() {
    return collectionSchemaCache;
  }

  public void registerCollectionSchemaCache(LoadingCache<String, CollectionSchema> collectionSchemaCache) {
    this.collectionSchemaCache = collectionSchemaCache;
  }

  public LoadingCache<String, Long> getCollectionMaxDataTimeCache() {
    return collectionMaxDataTimeCache;
  }

  public void registerCollectionMaxDataTimeCache(LoadingCache<String, Long> collectionMaxDataTimeCache) {
    this.collectionMaxDataTimeCache = collectionMaxDataTimeCache;
  }

  public CollectionsCache getCollectionsCache() {
    return collectionsCache;
  }

  public void registerCollectionsCache(CollectionsCache collectionsCache) {
    this.collectionsCache = collectionsCache;
  }

  public LoadingCache<String, CollectionConfig> getCollectionConfigCache() {
    return collectionConfigCache;
  }

  public void registerCollectionConfigCache(LoadingCache<String, CollectionConfig> collectionConfigCache) {
    this.collectionConfigCache = collectionConfigCache;
  }

  public LoadingCache<String, String> getDimensionFiltersCache() {
    return dimensionFiltersCache;
  }

  public void registerDimensionFiltersCache(LoadingCache<String, String> dimensionFiltersCache) {
    this.dimensionFiltersCache = dimensionFiltersCache;
  }

  public LoadingCache<String, String> getDashboardsCache() {
    return dashboardsCache;
  }

  public void registerDashboardsCache(LoadingCache<String, String> dashboardsCache) {
    this.dashboardsCache = dashboardsCache;
  }

  public QueryCache getQueryCache() {
    return queryCache;
  }

  public void registerQueryCache(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

}
