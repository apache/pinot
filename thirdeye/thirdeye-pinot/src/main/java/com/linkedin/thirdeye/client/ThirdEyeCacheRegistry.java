package com.linkedin.thirdeye.client;

import com.google.common.cache.Weigher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.thirdeye.client.cache.CollectionMaxDataTimeCacheLoader;
import com.linkedin.thirdeye.client.cache.CollectionsCache;
import com.linkedin.thirdeye.client.cache.DashboardConfigCacheLoader;
import com.linkedin.thirdeye.client.cache.DashboardsCacheLoader;
import com.linkedin.thirdeye.client.cache.DatasetConfigCacheLoader;
import com.linkedin.thirdeye.client.cache.DimensionFiltersCacheLoader;
import com.linkedin.thirdeye.client.cache.MetricConfigCacheLoader;
import com.linkedin.thirdeye.client.cache.MetricDataset;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.cache.ResultSetGroupCacheLoader;
import com.linkedin.thirdeye.client.pinot.PinotQuery;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;

public class ThirdEyeCacheRegistry {

  private LoadingCache<String, DatasetConfigDTO> datasetConfigCache;
  private LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache;
  private LoadingCache<String, List<DashboardConfigDTO>> dashboardConfigsCache;
  private LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;
  private LoadingCache<String, String> dashboardsCache;
  private LoadingCache<String, String> dimensionFiltersCache;
  private CollectionsCache collectionsCache;
  private QueryCache queryCache;

  private static DatasetConfigManager datasetConfigDAO;
  private static MetricConfigManager metricConfigDAO;
  private static DashboardConfigManager dashboardConfigDAO;
  private static Map<String, ThirdEyeClient> thirdEyeClientMap;
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeCacheRegistry.class);

  private static ThirdEyeConfiguration thirdeyeConfig = null;
  private static ClientConfig clientConfig = null;

  // TODO: make default cache size configurable
  private static final int DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE = 50;
  private static final int DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 100;
  private static final int DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 8192;

  private static class Holder {
    static final ThirdEyeCacheRegistry INSTANCE = new ThirdEyeCacheRegistry();
  }

  public static ThirdEyeCacheRegistry getInstance() {
    return Holder.INSTANCE;
  }


  /**
   * Initializes webapp caches
   * @param config
   */
  public static void initializeCaches(ThirdEyeConfiguration config) {

    thirdeyeConfig = config;
    init();
    initCaches();
    initPeriodicCacheRefresh();
  }

  private static void init() {
    try {
      String clientConfigPath = thirdeyeConfig.getClientsPath();
      clientConfig = ClientConfigLoader.fromClientConfigPath(clientConfigPath);
      if (clientConfig == null) {
        throw new IllegalStateException("Could not create client config from path " + clientConfigPath);
      }
      thirdEyeClientMap = ClientConfigLoader.getClientMap(clientConfig);
      datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
      metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
      dashboardConfigDAO = DAO_REGISTRY.getDashboardConfigDAO();

    } catch (Exception e) {
     LOGGER.info("Caught exception while initializing caches", e);
    }
  }

  private static void initCaches() {
    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();

    RemovalListener<PinotQuery, ResultSetGroup> listener = new RemovalListener<PinotQuery, ResultSetGroup>() {
      @Override
      public void onRemoval(RemovalNotification<PinotQuery, ResultSetGroup> notification) {
        LOGGER.info("Expired {}", notification.getKey().getPql());
      }
    };

    // ResultSetGroup Cache. The size of this cache is limited by the total number of buckets in all ResultSetGroup.
    // We estimate that 1 bucket (including overhead) consumes 1KB and this cache is allowed to use up to 50% of max
    // heap space.
    PinotThirdEyeClientConfig pinotThirdeyeClientConfig = PinotThirdEyeClientConfig.createThirdeyeClientConfig(clientConfig);
    long maxBucketNumber = getApproximateMaxBucketNumber(DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE);
    LoadingCache<PinotQuery, ResultSetGroup> resultSetGroupCache = CacheBuilder.newBuilder()
        .removalListener(listener)
        .expireAfterAccess(1, TimeUnit.HOURS)
        .maximumWeight(maxBucketNumber)
        .weigher(new Weigher<PinotQuery, ResultSetGroup>() {
          @Override public int weigh(PinotQuery pinotQuery, ResultSetGroup resultSetGroup) {
            int resultSetCount = resultSetGroup.getResultSetCount();
            int weight = 0;
            for (int idx = 0; idx < resultSetCount; ++idx) {
              com.linkedin.pinot.client.ResultSet resultSet = resultSetGroup.getResultSet(idx);
              weight += (resultSet.getColumnCount() * resultSet.getRowCount());
            }
            return weight;
          }
        })
        .build(new ResultSetGroupCacheLoader(pinotThirdeyeClientConfig));
    cacheRegistry.registerResultSetGroupCache(resultSetGroupCache);
    LOGGER.info("Max bucket number for ResultSetGroup cache is set to {}", maxBucketNumber);

    // CollectionMaxDataTime Cache
    LoadingCache<String, Long> collectionMaxDataTimeCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(5, TimeUnit.MINUTES)
        .build(new CollectionMaxDataTimeCacheLoader(resultSetGroupCache, datasetConfigDAO));
    cacheRegistry.registerCollectionMaxDataTimeCache(collectionMaxDataTimeCache);

    // Query Cache
    QueryCache queryCache = new QueryCache(thirdEyeClientMap, Executors.newFixedThreadPool(10));
    cacheRegistry.registerQueryCache(queryCache);

    // Dimension Filter cache
    LoadingCache<String, String> dimensionFiltersCache = CacheBuilder.newBuilder()
        .build(new DimensionFiltersCacheLoader(cacheRegistry.getQueryCache()));
    cacheRegistry.registerDimensionFiltersCache(dimensionFiltersCache);

    // Dashboards cache
    LoadingCache<String, String> dashboardsCache = CacheBuilder.newBuilder()
        .build(new DashboardsCacheLoader(dashboardConfigDAO));
    cacheRegistry.registerDashboardsCache(dashboardsCache);

    // Collections cache
    CollectionsCache collectionsCache = new CollectionsCache(datasetConfigDAO, thirdeyeConfig);
    cacheRegistry.registerCollectionsCache(collectionsCache);

    // DatasetConfig cache
    LoadingCache<String, DatasetConfigDTO> datasetConfigCache = CacheBuilder.newBuilder()
        .build(new DatasetConfigCacheLoader(datasetConfigDAO));
    cacheRegistry.registerDatasetConfigCache(datasetConfigCache);

    // MetricConfig cache
    LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache = CacheBuilder.newBuilder()
        .build(new MetricConfigCacheLoader(metricConfigDAO));
    cacheRegistry.registerMetricConfigCache(metricConfigCache);

    // DashboardConfigs cache
    LoadingCache<String, List<DashboardConfigDTO>> dashboardConfigsCache = CacheBuilder.newBuilder()
        .build(new DashboardConfigCacheLoader(dashboardConfigDAO));
    cacheRegistry.registerDashboardConfigsCache(dashboardConfigsCache);
  }

  private static void initPeriodicCacheRefresh() {

    final CacheResource cacheResource = new CacheResource();
    // manually refreshing on startup, and setting delay
    // as weeklyService starts before hourlyService finishes,
    // causing NPE in reading collectionsCache

    // Start initial cache loading asynchronously to reduce application start time
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override public void run() {
        cacheResource.refreshCollections();
        cacheResource.refreshDatasetConfigCache();
        cacheResource.refreshDashoardConfigsCache();
        cacheResource.refreshDashboardsCache();
        cacheResource.refreshMetricConfigCache();
        cacheResource.refreshMaxDataTimeCache();
        cacheResource.refreshDimensionFiltersCache();
      }
    });

    ScheduledExecutorService minuteService = Executors.newSingleThreadScheduledExecutor();
    minuteService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          cacheResource.refreshMaxDataTimeCache();
        } catch (Exception e) {
          LOGGER.error("Exception while loading collections", e);
        }
      }
    }, 30, 30, TimeUnit.MINUTES);

    ScheduledExecutorService hourlyService = Executors.newSingleThreadScheduledExecutor();
    hourlyService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          cacheResource.refreshCollections();
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

  public LoadingCache<String, DatasetConfigDTO> getDatasetConfigCache() {
    return datasetConfigCache;
  }

  public void registerDatasetConfigCache(LoadingCache<String, DatasetConfigDTO> datasetConfigCache) {
    this.datasetConfigCache = datasetConfigCache;
  }

  public LoadingCache<MetricDataset, MetricConfigDTO> getMetricConfigCache() {
    return metricConfigCache;
  }

  public void registerMetricConfigCache(LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache) {
    this.metricConfigCache = metricConfigCache;
  }

  public LoadingCache<String, List<DashboardConfigDTO>> getDashboardConfigsCache() {
    return dashboardConfigsCache;
  }

  public void registerDashboardConfigsCache(LoadingCache<String, List<DashboardConfigDTO>> dashboardConfigsCache) {
    this.dashboardConfigsCache = dashboardConfigsCache;
  }

  /**
   * Returns the suggested max weight for LoadingCache according to the given percentage of max heap space.
   *
   * The approximate weight is calculated by following rules:
   * 1. We estimate that a bucket, including its overhead, occupies 1 KB.
   * 2. Cache size (in bytes) = System's maxMemory * percentage
   * 3. We also bound the cache size between DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB and
   *    DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB if max heap size is unavailable.
   * 4. Weight (number of buckets) = cache size / 1KB.
   *
   * @param percentage the percentage of JVM max heap space
   * @return the suggested max weight for LoadingCache
   */
  private static long getApproximateMaxBucketNumber(int percentage) {
    long jvmMaxMemoryInBytes = Runtime.getRuntime().maxMemory();
    if (jvmMaxMemoryInBytes == Long.MAX_VALUE) { // Check upper bound
      jvmMaxMemoryInBytes = DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * 1048576L; // MB to Bytes
    } else { // Check lower bound
      long lowerBoundInBytes = DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * 1048576L; // MB to Bytes
      if (jvmMaxMemoryInBytes < lowerBoundInBytes) {
        jvmMaxMemoryInBytes = lowerBoundInBytes;
      }
    }
    return (jvmMaxMemoryInBytes / 102400) * percentage;
  }

}
