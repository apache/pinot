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

import com.google.common.cache.LoadingCache;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.StringSeries;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.RequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultAggregationLoader implements AggregationLoader {
  private static Logger LOG = LoggerFactory.getLogger(DefaultAggregationLoader.class);

  private static final long TIMEOUT = 600000;

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache cache;
  private final LoadingCache<String, Long> maxTimeCache;

  public DefaultAggregationLoader(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache cache, LoadingCache<String, Long> maxTimeCache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
    this.maxTimeCache = maxTimeCache;
  }

  @Override
  public DataFrame loadBreakdown(MetricSlice slice, int limit) throws Exception {
    final long metricId = slice.getMetricId();

    // fetch meta data
    MetricConfigDTO metric = this.metricDAO.findById(metricId);
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", metric.getDataset()));
    }

    List<String> dimensions = new ArrayList<>(dataset.getDimensions());
    dimensions.removeAll(slice.getFilters().keySet());
    dimensions.remove(dataset.getTimeColumn());

    LOG.info("De-Aggregating '{}' for dimensions '{}'", slice, dimensions);

    DataFrame dfAll = DataFrame
        .builder(COL_DIMENSION_NAME + ":STRING", COL_DIMENSION_VALUE + ":STRING", COL_VALUE + ":DOUBLE").build()
        .setIndex(COL_DIMENSION_NAME, COL_DIMENSION_VALUE);

    Map<String, RequestContainer> requests = new HashMap<>();
    Map<String, Future<ThirdEyeResponse>> responses = new HashMap<>();

    // submit requests
    for (String dimension : dimensions) {
      RequestContainer rc = DataFrameUtils.makeAggregateRequest(slice, Collections.singletonList(dimension), limit, "ref", this.metricDAO, this.datasetDAO);
      Future<ThirdEyeResponse> res = this.cache.getQueryResultAsync(rc.getRequest());

      requests.put(dimension, rc);
      responses.put(dimension, res);
    }

    // collect responses
    final long deadline = System.currentTimeMillis() + TIMEOUT;

    List<DataFrame> results = new ArrayList<>();
    for (String dimension : dimensions) {
      RequestContainer rc = requests.get(dimension);
      ThirdEyeResponse res = responses.get(dimension).get(makeTimeout(deadline), TimeUnit.MILLISECONDS);
      DataFrame dfRaw = DataFrameUtils.evaluateResponse(res, rc);
      DataFrame dfResult = new DataFrame()
          .addSeries(COL_DIMENSION_NAME, StringSeries.fillValues(dfRaw.size(), dimension))
          .addSeries(COL_DIMENSION_VALUE, dfRaw.get(dimension))
          .addSeries(COL_VALUE, dfRaw.get(COL_VALUE));
      results.add(dfResult);
    }

    return dfAll.append(results);
  }

  @Override
  public DataFrame loadAggregate(MetricSlice slice, List<String> dimensions, int limit) throws Exception {
    final long metricId = slice.getMetricId();

    // fetch meta data
    MetricConfigDTO metric = this.metricDAO.findById(metricId);
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", metricId));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", metric.getDataset()));
    }

    LOG.info("Aggregating '{}'", slice);

    List<String> cols = new ArrayList<>();
    for (String dimName : dimensions) {
      cols.add(dimName + ":STRING");
    }
    cols.add(COL_VALUE + ":DOUBLE");

    DataFrame dfEmpty = DataFrame.builder(cols).build().setIndex(dimensions);

    final long maxTime = this.maxTimeCache.get(dataset.getDataset());
    if (slice.getStart() > maxTime) {
      return dfEmpty;
    }

    RequestContainer rc = DataFrameUtils.makeAggregateRequest(slice, new ArrayList<>(dimensions), limit, "ref", this.metricDAO, this.datasetDAO);
    ThirdEyeResponse res = this.cache.getQueryResult(rc.getRequest());
    return DataFrameUtils.evaluateResponse(res, rc);
  }

  private static long makeTimeout(long deadline) {
    return Math.max(deadline - System.currentTimeMillis(), 0);
  }
}
