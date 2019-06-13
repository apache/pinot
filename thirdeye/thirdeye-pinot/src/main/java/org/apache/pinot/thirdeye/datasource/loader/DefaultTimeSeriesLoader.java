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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTimeSeriesLoader implements TimeSeriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTimeSeriesLoader.class);

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache cache;

  public DefaultTimeSeriesLoader(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, QueryCache cache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
  }

  /**
   * Default implementation using metricDAO, datasetDAO, and QueryCache
   *
   * @param slice metric slice to fetch
   * @return DataFrame with timestamps and metric values
   * @throws Exception
   */
  @Override
  public DataFrame load(MetricSlice slice) throws Exception {
    LOG.info("Loading time series for '{}'", slice);
    MetricConfigDTO metric = retriveMetric(slice.getMetricId());
    TimeSeriesRequestContainer rc =
        DataFrameUtils.makeTimeSeriesRequestAligned(slice, "ref", metric, retriveDataset(metric));
    ThirdEyeResponse response = this.cache.getQueryResult(rc.getRequest());
    return DataFrameUtils.evaluateResponse(response, rc);
  }

  @Override
  public Map<MetricSlice, DataFrame> loadTimeSeries(Collection<MetricSlice> slices) throws Exception {
    Map<MetricSlice, DataFrame> output = new HashMap<>();
    // get all query groups
    Collection<QueryGroup> queryGroups = getQueryGroups(slices);
    for (QueryGroup queryGroup : queryGroups) {
      if (queryGroup.slices.size() == 1) {
        MetricSlice slice = queryGroup.slices.stream().findFirst().get();
        output.put(slice, this.load(slice));
      } else {
        MetricConfigDTO metric = retriveMetric(queryGroup.metricId);
        DatasetConfigDTO dataset = retriveDataset(metric);
        if (dataset.getDataSource().equals(PinotThirdEyeDataSource.class.getSimpleName()) && dataset.isAdditive()
            && !metric.isDimensionAsMetric()) {
          // if it's pinot data source, batch load the data for multiple dimension values in one query
          output.putAll(loadFromBatchQuery(queryGroup, metric, dataset));
        } else {
          for (MetricSlice metricSlice : queryGroup.slices) {
            output.put(metricSlice, this.load(metricSlice));
          }
        }
      }
    }
    return output;
  }

  private Map<MetricSlice, DataFrame> loadFromBatchQuery(QueryGroup queryGroup, MetricConfigDTO metric,
      DatasetConfigDTO dataset) throws Exception {
    LOG.info("Loading time series in batch for {}", queryGroup);
    TimeSeriesRequestContainer rc =
        DataFrameUtils.makeTimeSeriesRequestAlignedBatch(queryGroup.startTime, queryGroup.endTime,
            queryGroup.slices.stream().map(MetricSlice::getFilters).collect(Collectors.toList()), queryGroup.dimensions,
            queryGroup.granularity, "ref", metric, dataset);
    ThirdEyeResponse response = this.cache.getQueryResult(rc.getRequest());
    DataFrame df = DataFrameUtils.evaluateResponse(response, rc);
    return evaluateResultForQueryGroup(queryGroup, df);
  }

  private Map<MetricSlice, DataFrame> evaluateResultForQueryGroup(QueryGroup queryGroup, DataFrame df){
    Map<MetricSlice, DataFrame> output = new HashMap<>();
    for (MetricSlice slice: queryGroup.slices) {
      DataFrame result = df;
      for (Map.Entry<String, Collection<String>> entry: slice.getFilters().asMap().entrySet()){
        // pick the result for the respective dimension values
        result = result.filter(result.getStrings(entry.getKey()).map(
            (Series.StringConditional) values -> entry.getValue().contains(values[0]))).dropNull(entry.getKey());
      }
      result.retainSeries(COL_TIME, COL_VALUE);
      output.put(slice, result);
    }
    return output;
  }

  private MetricConfigDTO retriveMetric(long id) {
    MetricConfigDTO metric = metricDAO.findById(id);
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", id));
    }
    return metric;
  }

  private DatasetConfigDTO retriveDataset(MetricConfigDTO metric) {
    DatasetConfigDTO dataset = datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(
          String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));
    }
    return dataset;
  }

  private Collection<QueryGroup> getQueryGroups(Collection<MetricSlice> slices) {
    List<QueryGroup> queryGroups = new ArrayList<>();
    // group by metric, start time, end time and granularity
    Map<List<Object>, List<MetricSlice>> timeRangeAndMetricGroups = slices.stream()
        .collect(Collectors.groupingBy(
            slice -> Arrays.asList(slice.getStart(), slice.getEnd(), slice.getMetricId(), slice.getGranularity()),
            Collectors.toList()));

    // for slices with the same metric, time range and granularity, group by dimension filters
    for (List<MetricSlice> timeRangeAndMetricGroup : timeRangeAndMetricGroups.values()) {
      // first filter out slices with dimensions with multiple values, have to send separate queries for those slices
      // for example, slice one : country in ("us", "mx"), slice two: country in ("us", "cn")
      List<MetricSlice> slicesWithMultipleFilterValues = timeRangeAndMetricGroup.stream()
          .filter(metricSlice -> metricSlice.getFilters()
              .asMap()
              .entrySet()
              .stream()
              .anyMatch(entry -> entry.getValue().size() > 1))
          .collect(Collectors.toList());

      queryGroups.addAll(slicesWithMultipleFilterValues.stream().map(QueryGroup::fromSlice).collect(Collectors.toList()));
      timeRangeAndMetricGroup.removeAll(slicesWithMultipleFilterValues);

      // group by dimension filters
      Map<Set<String>, List<MetricSlice>> groups = timeRangeAndMetricGroup.stream()
          .collect(Collectors.groupingBy(slice -> slice.getFilters().keySet(), Collectors.toList()));

      for (List<MetricSlice> metricSlices : groups.values()) {
        MetricSlice slice = metricSlices.stream().findFirst().get();
        // create a query group for each group of slices with the same metric id, start time, end time, granularity
        // but different dimension filters, so that we can fetch the data in one query
        queryGroups.add(
            new QueryGroup(metricSlices, slice.getMetricId(), slice.getStart(), slice.getEnd(), slice.getGranularity(),
                new ArrayList<>(slice.getFilters().keySet())));
      }
    }
    return queryGroups;
  }

  /**
   * The query groups. For slices in the same query group, we send only one query to fetch the time series when possible.
   * Because they share the same start time, end time and granularities.
   */
  private static class QueryGroup {
    final Collection<MetricSlice> slices;
    final long metricId;
    final long startTime;
    final long endTime;
    final TimeGranularity granularity;
    final List<String> dimensions;

    static QueryGroup fromSlice(MetricSlice slice) {
      return new QueryGroup(Collections.singleton(slice), slice.getMetricId(), slice.getStart(), slice.getEnd(),
          slice.getGranularity(), new ArrayList<>(slice.getFilters().keySet()));
    }

    QueryGroup(Collection<MetricSlice> slices, long metricId, long startTime, long endTime, TimeGranularity granularity,
        List<String> dimensions) {
      this.slices = slices;
      this.metricId = metricId;
      this.startTime = startTime;
      this.endTime = endTime;
      this.granularity = granularity;
      this.dimensions = dimensions;
    }

    @Override
    public String toString() {
      return "QueryGroup{" + "slices=" + slices + ", metricId=" + metricId + ", startTime=" + startTime + ", endTime="
          + endTime + ", granularity=" + granularity + ", dimensions=" + dimensions + '}';
    }
  }
}
