/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource.cache;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;

public class DatasetMaxDataTimeCacheLoader extends CacheLoader<String, Long> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetMaxDataTimeCacheLoader.class);

  private final QueryCache queryCache;
  private DatasetConfigManager datasetConfigDAO;

  private final ExecutorService reloadExecutor = Executors.newSingleThreadExecutor();

  public DatasetMaxDataTimeCacheLoader(QueryCache queryCache, DatasetConfigManager datasetConfigDAO) {
    this.queryCache = queryCache;
    this.datasetConfigDAO = datasetConfigDAO;
  }

  /**
   * Fetches the max date time in millis for this dataset from the right data source
   * {@inheritDoc}
   * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
   */
  @Override
  public Long load(String dataset) throws Exception {
    LOGGER.debug("Loading maxDataTime cache {}", dataset);
    long maxTime = 0;
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    String dataSourceName = datasetConfig.getDataSource();
    try {
      ThirdEyeDataSource dataSource = queryCache.getDataSource(dataSourceName);
      if (dataSource == null) {
       LOGGER.warn("dataSource [{}] found null in the query cache", dataSourceName);
      } else {
        maxTime = dataSource.getMaxDataTime(dataset);
      }
    } catch (Exception e) {
      LOGGER.error("Exception in getting max date time for {} from data source {}", dataset, dataSourceName, e);
    }
    if (maxTime <= 0) {
      maxTime = System.currentTimeMillis();
    }
    return maxTime;
  }

  @Override
  public ListenableFuture<Long> reload(final String dataset, Long preMaxDataTime) {
    ListenableFutureTask<Long> reloadTask = ListenableFutureTask.create(new Callable<Long>() {
      @Override public Long call() throws Exception {
        return DatasetMaxDataTimeCacheLoader.this.load(dataset);
      }
    });
    reloadExecutor.execute(reloadTask);
    LOGGER.info("Passively refreshing max data time of collection: {}", dataset);
    return reloadTask;
  }


}
