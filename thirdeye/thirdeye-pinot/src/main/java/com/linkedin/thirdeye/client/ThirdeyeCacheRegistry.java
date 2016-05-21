package com.linkedin.thirdeye.client;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.ClientProtocolException;
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
import com.linkedin.thirdeye.client.cache.ResultSetGroupCacheLoader;
import com.linkedin.thirdeye.client.cache.SchemaCacheLoader;
import com.linkedin.thirdeye.client.pinot.PinotQuery;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;

public class ThirdeyeCacheRegistry {

  private LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache;
  private LoadingCache<String, Schema> schemaCache;
  private LoadingCache<String, CollectionSchema> collectionSchemaCache;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;
  private LoadingCache<String, CollectionConfig> collectionConfigCache;
  private CollectionsCache collectionsCache;

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdeyeCacheRegistry.class);

  private static class Holder {
    static final ThirdeyeCacheRegistry INSTANCE = new ThirdeyeCacheRegistry();
  }

  public static ThirdeyeCacheRegistry getInstance() {
    return Holder.INSTANCE;
  }

  /**
   * Initialize required caches for thirdeye-detector
   * @param pinotThirdeyeClientConfig
   * @param collectionSchemaDAO
   * @param collectionConfigDAO
   * @throws ClientProtocolException
   * @throws IOException
   */
  public static void initializeDetectorCaches(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionSchema> collectionSchemaDAO,
      AbstractConfigDAO<CollectionConfig> collectionConfigDAO)
      throws ClientProtocolException, IOException {

    initCacheLoaders(pinotThirdeyeClientConfig, collectionSchemaDAO, collectionConfigDAO);
  }

  /**
   * Initializes required caches for thirdeye-webapp
   * @param pinotThirdeyeClientConfig
   * @param collectionSchemaDAO
   * @param collectionConfigDAO
   * @param config
   * @throws ClientProtocolException
   * @throws IOException
   */
  public static void initializeWebappCaches(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionSchema> collectionSchemaDAO,
      AbstractConfigDAO<CollectionConfig> collectionConfigDAO,
      ThirdEyeDashboardConfiguration config) throws ClientProtocolException, IOException {

    initCacheLoaders(pinotThirdeyeClientConfig, collectionSchemaDAO, collectionConfigDAO);

    ThirdeyeCacheRegistry cacheRegistry = ThirdeyeCacheRegistry.getInstance();
    CollectionsCache collectionsCache = new CollectionsCache(pinotThirdeyeClientConfig, config);
    cacheRegistry.registerCollectionsCache(collectionsCache);

    initPeriodicCacheRefresh();
  }

  private static void initCacheLoaders(PinotThirdEyeClientConfig pinotThirdeyeClientConfig,
      AbstractConfigDAO<CollectionSchema> collectionSchemaDAO,
      AbstractConfigDAO<CollectionConfig> collectionConfigDAO) {
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

    LoadingCache<String, CollectionConfig> collectionConfigCache = CacheBuilder.newBuilder()
        .build(new CollectionConfigCacheLoader(pinotThirdeyeClientConfig, collectionConfigDAO));
    cacheRegistry.registerCollectionConfigCache(collectionConfigCache);

  }

  private static void initPeriodicCacheRefresh() {

    final CacheResource cacheResource = new CacheResource();
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    service.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        try {
          cacheResource.refreshCollections();
          cacheResource.refreshMaxDataTimeCache();
        } catch (Exception e) {
          LOGGER.error("Exception while loading collections", e);
        }
      }
    }, 0, 1, TimeUnit.HOURS);
  }

  public LoadingCache<PinotQuery, ResultSetGroup> getResultSetGroupCache() {
    return resultSetGroupCache;
  }

  public void registerResultSetGroupCache(
      LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache) {
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

  public void registerCollectionSchemaCache(
      LoadingCache<String, CollectionSchema> collectionSchemaCache) {
    this.collectionSchemaCache = collectionSchemaCache;
  }

  public LoadingCache<String, Long> getCollectionMaxDataTimeCache() {
    return collectionMaxDataTimeCache;
  }

  public void registerCollectionMaxDataTimeCache(
      LoadingCache<String, Long> collectionMaxDataTimeCache) {
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

  public void registerCollectionConfigCache(
      LoadingCache<String, CollectionConfig> collectionConfigCache) {
    this.collectionConfigCache = collectionConfigCache;
  }

}
