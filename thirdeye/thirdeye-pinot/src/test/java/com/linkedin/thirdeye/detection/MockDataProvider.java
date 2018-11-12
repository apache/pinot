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

package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Grouping;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.spi.model.EventSlice;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class MockDataProvider implements DataProvider {
  private static final String COL_KEY = Grouping.GROUP_KEY;

  private static final Multimap<String, String> NO_FILTERS = HashMultimap.create();

  private Map<MetricSlice, DataFrame> timeseries;
  private Map<MetricSlice, DataFrame> aggregates;
  private List<EventDTO> events;
  private List<MergedAnomalyResultDTO> anomalies;
  private List<MetricConfigDTO> metrics;
  private List<DatasetConfigDTO> datasets;
  private DetectionPipelineLoader loader;

  public MockDataProvider() {
    // left blank
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices) {
    Map<MetricSlice, DataFrame> result = new HashMap<>();
    for (final MetricSlice slice : slices) {
      List<String> filters = new ArrayList<>(slice.getFilters().keySet());
      final String[] arrCols = filters.toArray(new String[filters.size()]);

      List<String> groupBy = new ArrayList<>(filters);
      groupBy.add(COL_TIME);

      List<String> groupByExpr = new ArrayList<>();
      for (String dim : groupBy) {
        groupByExpr.add(dim + ":first");
      }
      groupByExpr.add(COL_VALUE + ":sum");

      DataFrame out = this.timeseries.get(slice.withFilters(NO_FILTERS));

      if (!filters.isEmpty()) {
        out = out.filter(new Series.StringConditional() {
          @Override
          public boolean apply(String... values) {
            for (int i = 0; i < arrCols.length; i++) {
              if (!slice.getFilters().containsEntry(arrCols[i], values[i])) {
                return false;
              }
            }
            return true;
          }
        }, arrCols);
      }

      out = out.filter(new Series.LongConditional() {
        @Override
        public boolean apply(long... values) {
          return values[0] >= slice.getStart() && values[0] < slice.getEnd();
        }
      }, COL_TIME).dropNull();

      result.put(slice, out.groupByValue(groupBy).aggregate(groupByExpr).dropSeries(COL_KEY).setIndex(groupBy));
    }
    return result;
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, final List<String> dimensions) {
    Map<MetricSlice, DataFrame> result = new HashMap<>();
    for (MetricSlice slice : slices) {
      List<String> expr = new ArrayList<>();
      for (String dimName : dimensions) {
        expr.add(dimName + ":first");
      }
      expr.add(COL_VALUE + ":sum");

      if (dimensions.isEmpty()) {
        result.put(slice, this.aggregates.get(slice.withFilters(NO_FILTERS)));

      } else {
        result.put(slice, this.aggregates.get(slice.withFilters(NO_FILTERS))
            .groupByValue(new ArrayList<>(dimensions)).aggregate(expr)
            .dropSeries(COL_KEY).setIndex(dimensions));
      }
    }
    return result;
  }

  @Override
  public Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices, long configId) {
    Multimap<AnomalySlice, MergedAnomalyResultDTO> result = ArrayListMultimap.create();
    for (AnomalySlice slice : slices) {
      for (MergedAnomalyResultDTO anomaly : this.anomalies) {
        if (slice.match(anomaly)) {
          if (configId >= 0 && (anomaly.getDetectionConfigId() == null || anomaly.getDetectionConfigId() != configId)){
            continue;
          }
          result.put(slice, anomaly);
        }
      }
    }
    return result;
  }

  @Override
  public Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices) {
    Multimap<EventSlice, EventDTO> result = ArrayListMultimap.create();
    for (EventSlice slice : slices) {
      for (EventDTO event  :this.events) {
        if (slice.match(event)) {
          result.put(slice, event);
        }
      }
    }
    return result;
  }

  @Override
  public Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids) {
    Map<Long, MetricConfigDTO> result = new HashMap<>();
    for (Long id : ids) {
      for (MetricConfigDTO metric : this.metrics) {
        if (id.equals(metric.getId())) {
          result.put(id, metric);
        }
      }
    }
    return result;
  }

  @Override
  public Map<String, DatasetConfigDTO> fetchDatasets(Collection<String> datasetNames) {
    Map<String, DatasetConfigDTO> result = new HashMap<>();
    for (String datasetName : datasetNames) {
      for (DatasetConfigDTO dataset : this.datasets) {
        if (datasetName.equals(dataset.getDataset())) {
          result.put(datasetName, dataset);
        }
      }
    }
    return result;
  }

  @Override
  public MetricConfigDTO fetchMetric(String metricName, String datasetName) {
      for (MetricConfigDTO metric : this.metrics) {
        if (metricName.equals(metric.getName()) && datasetName.equals(metric.getDataset())) {
          return metric;
        }
      }
      return null;
  }

  @Override
  public DetectionPipeline loadPipeline(DetectionConfigDTO config, long start, long end) throws Exception {
    return this.loader.from(this, config, start, end);
  }

  public Map<MetricSlice, DataFrame> getTimeseries() {
    return timeseries;
  }

  public MockDataProvider setTimeseries(Map<MetricSlice, DataFrame> timeseries) {
    this.timeseries = timeseries;
    return this;
  }

  public Map<MetricSlice, DataFrame> getAggregates() {
    return aggregates;
  }

  public MockDataProvider setAggregates(Map<MetricSlice, DataFrame> aggregates) {
    this.aggregates = aggregates;
    return this;
  }

  public List<EventDTO> getEvents() {
    return events;
  }

  public MockDataProvider setEvents(List<EventDTO> events) {
    this.events = events;
    return this;
  }

  public List<MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public MockDataProvider setAnomalies(List<MergedAnomalyResultDTO> anomalies) {
    this.anomalies = anomalies;
    return this;
  }

  public List<MetricConfigDTO> getMetrics() {
    return metrics;
  }

  public MockDataProvider setMetrics(List<MetricConfigDTO> metrics) {
    this.metrics = metrics;
    return this;
  }

  public List<DatasetConfigDTO> getDatasets() {
    return datasets;
  }

  public MockDataProvider setDatasets(List<DatasetConfigDTO> datasets) {
    this.datasets = datasets;
    return this;
  }

  public DetectionPipelineLoader getLoader() {
    return loader;
  }

  public MockDataProvider setLoader(DetectionPipelineLoader loader) {
    this.loader = loader;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MockDataProvider that = (MockDataProvider) o;
    return Objects.equals(timeseries, that.timeseries) && Objects.equals(aggregates, that.aggregates)
        && Objects.equals(events, that.events) && Objects.equals(anomalies, that.anomalies)
        && Objects.equals(metrics, that.metrics) && Objects.equals(loader, that.loader);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeseries, aggregates, events, anomalies, metrics, loader);
  }
}
