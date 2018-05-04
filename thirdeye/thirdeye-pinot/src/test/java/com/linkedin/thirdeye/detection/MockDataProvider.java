package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class MockDataProvider implements DataProvider {
  private Map<MetricSlice, DataFrame> timeseries;
  private Map<MetricSlice, DataFrame> aggregates;
  private List<EventDTO> events;
  private List<MergedAnomalyResultDTO> anomalies;
  private List<MetricConfigDTO> metrics;
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

      DataFrame out = this.timeseries.get(slice);

      if (!filters.isEmpty()) {
        out = out.filter(new Series.StringConditional() {
          @Override
          public boolean apply(String... values) {
            for (int i = 0; i < arrCols.length; i++) {
              if (!slice.getFilters().containsEntry(arrCols[i], values[0])) {
                return false;
              }
            }
            return true;
          }
        }, arrCols).dropNull();
      }

      result.put(slice, out.groupByValue(groupBy).aggregate(groupByExpr));
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

      result.put(slice, this.aggregates.get(slice).groupByValue(new ArrayList<>(dimensions)).aggregate(expr));
    }
    return result;
  }

  @Override
  public Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices) {
    Multimap<AnomalySlice, MergedAnomalyResultDTO> result = ArrayListMultimap.create();
    for (AnomalySlice slice : slices) {
      for (MergedAnomalyResultDTO anomaly : this.anomalies) {
        if (slice.match(anomaly)) {
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
