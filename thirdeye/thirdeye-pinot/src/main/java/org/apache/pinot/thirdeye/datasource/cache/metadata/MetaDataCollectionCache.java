/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.datasource.cache.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The cache for a meta-data collection.
 * This cache will hold a collection of meta-data and periodically reloads it using the cache loader.
 *
 * It's designed to cache the original or derived meta-data collection for ThirdEye Dashboard server, for example, a
 * list of all applications, subscription groups, and detection configs. Loading a collection of metadata has been
 * a pain point for ThirdEye. It requires data base to scan the index table and then do a look up for each config in the
 * generic json table, which is slow. This cache makes such loading asynchronous and returns user the cached result
 * when opening page.
 *
 * @param <T> the type of the value to be cached.
 */
public class MetaDataCollectionCache<T> {
  private static final String META_DATA_KEY = "metaData";
  protected static final Logger LOG = LoggerFactory.getLogger(MetaDataCollectionCache.class);
  private final LoadingCache<String, T> loadingCache;

  /**
   * Create a Metadata collection cache
   * @param cacheLoader the cache loader for the meta data collection
   * @param reloadPeriod the period of the reloading frequency
   * @param reloadUnit the unit for the reload period
   */
  public MetaDataCollectionCache(MetadataCacheLoader<T> cacheLoader, int reloadPeriod, TimeUnit reloadUnit) {
    this.loadingCache = CacheBuilder.newBuilder()
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .build(CacheLoader.from(cacheLoader::loadMetaData));

    ScheduledExecutorService cacheRefreshExecutor = Executors.newSingleThreadScheduledExecutor();
    // refresh the cached value every 3 minutes asynchronously. This will also load the data into cache when the
    // application starts.
    cacheRefreshExecutor.scheduleAtFixedRate(() -> this.loadingCache.refresh(META_DATA_KEY), 0, reloadPeriod,
        reloadUnit);
  }

  /**
   * Create a Metadata collection cache.
   * @param cacheLoader the cache loader for the meta data collection
   */
  public MetaDataCollectionCache(MetadataCacheLoader<T> cacheLoader) {
    this(cacheLoader, 3, TimeUnit.MINUTES);
  }

  /**
   * get the value hold by the cache
   * @return the cached value
   */
  public T get() {
    return this.loadingCache.getUnchecked(META_DATA_KEY);
  }
}
