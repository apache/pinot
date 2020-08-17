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

package org.apache.pinot.thirdeye.datasource.loader;

import com.google.inject.Inject;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.detection.cache.CacheConfig;
import org.apache.pinot.thirdeye.detection.cache.TimeSeriesCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTimeSeriesLoader implements TimeSeriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTimeSeriesLoader.class);

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache queryCache;
  private final TimeSeriesCache timeSeriesCache;

  @Inject
  public DefaultTimeSeriesLoader(MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO,
      QueryCache queryCache,
      TimeSeriesCache cache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.queryCache = queryCache;
    this.timeSeriesCache = cache;
  }

  /**
   * Default implementation using metricDAO, datasetDAO, and TimeSeriesCache
   *
   * @param slice metric slice to fetch
   * @return DataFrame with timestamps and metric values
   * @throws Exception
   */
  @Override
  public DataFrame load(MetricSlice slice) throws Exception {
    LOG.info("Loading time series for '{}'", slice);

    TimeSeriesRequestContainer rc = DataFrameUtils.makeTimeSeriesRequestAligned(slice, "ref", this.metricDAO, this.datasetDAO);
    ThirdEyeResponse response;
    if (CacheConfig.getInstance().useCentralizedCache()) {
      response = this.timeSeriesCache.fetchTimeSeries(rc.getRequest());
    } else {
      response = this.queryCache.getQueryResult(rc.getRequest());
    }

    return DataFrameUtils.evaluateResponse(response, rc);
  }
}
