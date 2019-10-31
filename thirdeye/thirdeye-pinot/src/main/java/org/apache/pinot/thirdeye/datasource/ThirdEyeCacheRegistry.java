/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datasource;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.cache.DatasetConfigCacheLoader;
import org.apache.pinot.thirdeye.datasource.cache.DatasetListCache;
import org.apache.pinot.thirdeye.datasource.cache.DatasetMaxDataTimeCacheLoader;
import org.apache.pinot.thirdeye.datasource.cache.DimensionFiltersCacheLoader;
import org.apache.pinot.thirdeye.datasource.cache.MetricConfigCacheLoader;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.detection.cache.CouchbaseCacheDAO;
import org.apache.pinot.thirdeye.detection.cache.DefaultTimeSeriesCache;
import org.apache.pinot.thirdeye.detection.cache.TimeSeriesCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdEyeCacheRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeCacheRegistry.class);
  private static final ThirdEyeCacheRegistry INSTANCE = new ThirdEyeCacheRegistry();

  // DAO to ThirdEye's data and meta-data storage.
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  // TODO: Rename QueryCache to a name like DataSourceCache.
  private QueryCache queryCache;
  private TimeSeriesCache timeSeriesCache = null;

  // Meta-data caches
  private LoadingCache<String, DatasetConfigDTO> datasetConfigCache;
  private LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache;
  private LoadingCache<String, Long> datasetMaxDataTimeCache;
  private LoadingCache<String, String> dimensionFiltersCache;
  private DatasetListCache datasetsCache;

  public static ThirdEyeCacheRegistry getInstance() {
    return INSTANCE;
  }

  /**
   * Initializes data sources and caches.
   *
   * @param thirdeyeConfig ThirdEye's configurations.
   */
  public static void initializeCaches(ThirdEyeConfiguration thirdeyeConfig) throws Exception {
    initDataSources(thirdeyeConfig);
    initMetaDataCaches();
    initCentralizedCache();
  }

  /**
   * Initializes the adaptor to data sources such as Pinot, MySQL, etc.
   */
  public static void initDataSources(ThirdEyeConfiguration thirdeyeConfig) {
    try {
      // Initialize adaptors to time series databases.
      URL dataSourcesUrl = thirdeyeConfig.getDataSourcesAsUrl();
      DataSources dataSources = DataSourcesLoader.fromDataSourcesUrl(dataSourcesUrl);
      if (dataSources == null) {
        throw new IllegalStateException("Could not create data sources from path " + dataSourcesUrl);
      }
      // Query Cache
      Map<String, ThirdEyeDataSource> thirdEyeDataSourcesMap = DataSourcesLoader.getDataSourceMap(dataSources);
      QueryCache queryCache = new QueryCache(thirdEyeDataSourcesMap, Executors.newCachedThreadPool());
      ThirdEyeCacheRegistry.getInstance().registerQueryCache(queryCache);
    } catch (Exception e) {
     LOGGER.info("Caught exception while initializing caches", e);
    }
  }

  public static void initCentralizedCache() {
    try {
      // I couldn't figure out how to register the CouchbaseCacheDAO in the DAO registry, so just going to
      // hardcode it in for now...

      if (INSTANCE.getTimeSeriesCache() == null) {
        TimeSeriesCache timeSeriesCache = new DefaultTimeSeriesCache(DAO_REGISTRY.getMetricConfigDAO(), DAO_REGISTRY.getDatasetConfigDAO(),
            ThirdEyeCacheRegistry.getInstance().getQueryCache(), new CouchbaseCacheDAO());

        ThirdEyeCacheRegistry.getInstance().registerTimeSeriesCache(timeSeriesCache);
      }
    } catch (Exception e) {
      LOGGER.info("Caught exception while initializing caches", e);
    }
  }

  /**
   * Initialize the cache for meta data. This method has to be invoked after data sources are connected.
   */
  public static void initMetaDataCaches() {
    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();
    QueryCache queryCache = cacheRegistry.getQueryCache();
    Preconditions.checkNotNull(queryCache,
        "Data sources are not initialzed. Please invoke initDataSources() before this method.");

    // DatasetConfig cache
    // TODO deprecate. read from database directly
    LoadingCache<String, DatasetConfigDTO> datasetConfigCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(new DatasetConfigCacheLoader(DAO_REGISTRY.getDatasetConfigDAO()));
    cacheRegistry.registerDatasetConfigCache(datasetConfigCache);

    // MetricConfig cache
    // TODO deprecate. read from database directly
    LoadingCache<MetricDataset, MetricConfigDTO> metricConfigCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.MINUTES)
        .build(new MetricConfigCacheLoader(DAO_REGISTRY.getMetricConfigDAO()));
    cacheRegistry.registerMetricConfigCache(metricConfigCache);

    // DatasetMaxDataTime Cache
    LoadingCache<String, Long> datasetMaxDataTimeCache = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new DatasetMaxDataTimeCacheLoader(queryCache, DAO_REGISTRY.getDatasetConfigDAO()));
    cacheRegistry.registerDatasetMaxDataTimeCache(datasetMaxDataTimeCache);

    // Dimension Filter cache
    LoadingCache<String, String> dimensionFiltersCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build(new DimensionFiltersCacheLoader(queryCache, DAO_REGISTRY.getDatasetConfigDAO()));
    cacheRegistry.registerDimensionFiltersCache(dimensionFiltersCache);

    // Dataset list
    DatasetListCache datasetListCache = new DatasetListCache(DAO_REGISTRY.getDatasetConfigDAO(), TimeUnit.HOURS.toMillis(1));
    cacheRegistry.registerDatasetsCache(datasetListCache);
  }

  public LoadingCache<String, Long> getDatasetMaxDataTimeCache() {
    return datasetMaxDataTimeCache;
  }

  public void registerDatasetMaxDataTimeCache(LoadingCache<String, Long> datasetMaxDataTimeCache) {
    this.datasetMaxDataTimeCache = datasetMaxDataTimeCache;
  }

  public DatasetListCache getDatasetsCache() {
    return datasetsCache;
  }

  public void registerDatasetsCache(DatasetListCache collectionsCache) {
    this.datasetsCache = collectionsCache;
  }

  public LoadingCache<String, String> getDimensionFiltersCache() {
    return dimensionFiltersCache;
  }

  public void registerDimensionFiltersCache(LoadingCache<String, String> dimensionFiltersCache) {
    this.dimensionFiltersCache = dimensionFiltersCache;
  }

  public QueryCache getQueryCache() {
    return queryCache;
  }

  public void registerQueryCache(QueryCache queryCache) {
    this.queryCache = queryCache;
  }

  public TimeSeriesCache getTimeSeriesCache() { return timeSeriesCache; }

  public void registerTimeSeriesCache(TimeSeriesCache timeSeriesCache) { this.timeSeriesCache = timeSeriesCache; }

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
}
