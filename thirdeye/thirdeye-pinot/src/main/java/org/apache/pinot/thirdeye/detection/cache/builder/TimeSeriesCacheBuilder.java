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

package org.apache.pinot.thirdeye.detection.cache.builder;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.cache.CacheConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  A fetcher for fetching time-series from cache/datasource.
 *  The cache holds time-series information per Metric Slices
 */
public class TimeSeriesCacheBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesCacheBuilder.class);

  private static final long TIMEOUT = 60000;

  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final LoadingCache<MetricSlice, DataFrame> cache;

  private DefaultTimeSeriesLoader timeseriesLoader;

  private static TimeSeriesCacheBuilder INSTANCE;

  private TimeSeriesCacheBuilder() {
    this.cache = initCache();
  }

  synchronized public static TimeSeriesCacheBuilder getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new TimeSeriesCacheBuilder();
    }

    INSTANCE.setTimeseriesLoader(new DefaultTimeSeriesLoader(DAORegistry.getInstance().getMetricConfigDAO(),
        DAORegistry.getInstance().getDatasetConfigDAO(), ThirdEyeCacheRegistry.getInstance().getQueryCache(),
        ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache()));
    return INSTANCE;
  }

  private void setTimeseriesLoader(DefaultTimeSeriesLoader defaultTimeSeriesLoader) {
    this.timeseriesLoader = defaultTimeSeriesLoader;
  }

  private LoadingCache<MetricSlice, DataFrame> initCache() {
    // don't use more than one third of memory for detection time series
    long cacheSize = Runtime.getRuntime().freeMemory() / 3;
    LOG.info("initializing detection timeseries cache with {} bytes", cacheSize);
    return CacheBuilder.newBuilder()
        .maximumWeight(cacheSize)
        // Estimate that most detection tasks will complete within 15 minutes
        .expireAfterWrite(15, TimeUnit.MINUTES)
        .weigher((Weigher<MetricSlice, DataFrame>) (slice, dataFrame) -> dataFrame.size() * (Long.BYTES + Double.BYTES))
        .build(new CacheLoader<MetricSlice, DataFrame>() {
          // load single slice
          @Override
          public DataFrame load(MetricSlice slice) {
            return loadTimeseries(Collections.singleton(slice)).get(slice);
          }

          // buck loading time series slice in parallel
          @Override
          public Map<MetricSlice, DataFrame> loadAll(Iterable<? extends MetricSlice> slices) {
            return loadTimeseries(Lists.newArrayList(slices));
          }
        });
  }

  public Map<MetricSlice, DataFrame> fetchSlices(Collection<MetricSlice> slices) throws ExecutionException {
    if (CacheConfig.getInstance().useInMemoryCache()) {
      return this.cache.getAll(slices);
    } else {
      return loadTimeseries(slices);
    }
  }

  /**
   * Loads time-series data for the given slices. Fetch order:
   * a. Check if the time-series is already available in the cache and return
   * b. If cache-miss, load the information from data source and return
   */
  private Map<MetricSlice, DataFrame> loadTimeseries(Collection<MetricSlice> slices) {
    Map<MetricSlice, DataFrame> output = new HashMap<>();

    try {
      long ts = System.currentTimeMillis();

      // if the time series slice is already in cache, return directly
      if (CacheConfig.getInstance().useInMemoryCache()) {
        for (MetricSlice slice : slices) {
          for (Map.Entry<MetricSlice, DataFrame> entry : this.cache.asMap().entrySet()) {
            // current slice potentially contained in cache
            if (entry.getKey().containSlice(slice)) {
              DataFrame df = entry.getValue()
                  .filter(entry.getValue().getLongs(DataFrame.COL_TIME).between(slice.getStart(), slice.getEnd()))
                  .dropNull(DataFrame.COL_TIME);
              // double check if it is cache hit
              if (df.getLongs(DataFrame.COL_TIME).size() > 0) {
                output.put(slice, df);
                break;
              }
            }
          }
        }
      }

      // if not in cache, fetch from data source
      Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();
      for (final MetricSlice slice : slices) {
        if (!output.containsKey(slice)) {
          futures.put(slice, this.executor.submit(() -> TimeSeriesCacheBuilder.this.timeseriesLoader.load(slice)));
        }
      }
      //LOG.info("Fetching {} slices of timeseries, {} cache hit, {} cache miss", slices.size(), output.size(), futures.size());
      final long deadline = System.currentTimeMillis() + TIMEOUT;
      for (MetricSlice slice : slices) {
        if (!output.containsKey(slice)) {
          output.put(slice, futures.get(slice).get(DetectionUtils.makeTimeout(deadline), TimeUnit.MILLISECONDS));
        }
      }
      LOG.info("Fetching {} slices used {} milliseconds", slices.size(), System.currentTimeMillis() - ts);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return output;
  }

  public void cleanCache() {
    if (this.cache != null) {
      this.cache.cleanUp();
    }
  }
}