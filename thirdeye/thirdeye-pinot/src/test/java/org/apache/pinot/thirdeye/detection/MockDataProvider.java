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

package org.apache.pinot.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.Grouping;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EvaluationSlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class MockDataProvider implements DataProvider {
  private static final String COL_KEY = Grouping.GROUP_KEY;

  private static final Multimap<String, String> NO_FILTERS = HashMultimap.create();

  private Map<MetricSlice, DataFrame> timeseries;
  private Map<MetricSlice, DataFrame> aggregates;
  private List<EventDTO> events;
  private List<MergedAnomalyResultDTO> anomalies;
  private List<MetricConfigDTO> metrics;
  private List<DatasetConfigDTO> datasets;
  private List<EvaluationDTO>  evaluations;
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
      if (out == null) {
        return result;
      }

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
  public Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, final List<String> dimensions, int limit) {
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
        DataFrame aggResult = this.aggregates.get(slice.withFilters(NO_FILTERS))
            .groupByValue(new ArrayList<>(dimensions)).aggregate(expr);

        if (limit > 0) {
          aggResult = aggResult.sortedBy(COL_VALUE).reverse().head(limit);
        }
        result.put(slice, aggResult.dropSeries(COL_KEY).setIndex(dimensions));
      }
    }
    return result;
  }

  @Override
  public Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices) {
    Multimap<AnomalySlice, MergedAnomalyResultDTO> result = ArrayListMultimap.create();
    for (AnomalySlice slice : slices) {
      for (MergedAnomalyResultDTO anomaly : this.anomalies) {
        if (slice.match(anomaly)) {
          if (slice.getDetectionId() >= 0 && (anomaly.getDetectionConfigId() == null
              || anomaly.getDetectionConfigId() != slice.getDetectionId())) {
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
  public Multimap<EvaluationSlice, EvaluationDTO> fetchEvaluations(Collection<EvaluationSlice> slices,
      long configId) {
    Multimap<EvaluationSlice, EvaluationDTO> result = ArrayListMultimap.create();
    for (EvaluationSlice slice : slices) {
      for (EvaluationDTO evaluation  :this.evaluations) {
        if (slice.match(evaluation) && evaluation.getDetectionConfigId() == configId) {
          result.put(slice, evaluation);
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

  @Override
  public List<DatasetConfigDTO> fetchDatasetByDisplayName(String datasetDisplayName) {
    List<DatasetConfigDTO> datasetConfigDTOs = new ArrayList<>();
    for (DatasetConfigDTO datasetConfigDTO : getDatasets()) {
      if (datasetConfigDTO.getDisplayName().equals(datasetDisplayName)) {
        datasetConfigDTOs.add(datasetConfigDTO);
      }
    }

    return datasetConfigDTOs;
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

  public List<EvaluationDTO> getEvaluations() {
    return evaluations;
  }

  public void setEvaluations(List<EvaluationDTO> evaluations) {
    this.evaluations = evaluations;
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
