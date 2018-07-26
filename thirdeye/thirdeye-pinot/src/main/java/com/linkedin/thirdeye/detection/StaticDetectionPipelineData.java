package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Map;


public class StaticDetectionPipelineData {
  final StaticDetectionPipelineModel model;
  final Map<MetricSlice, DataFrame> timeseries;
  final Map<MetricSlice, DataFrame> aggregates;
  final Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies;
  final Multimap<EventSlice, EventDTO> events;

  public StaticDetectionPipelineData(StaticDetectionPipelineModel model, Map<MetricSlice, DataFrame> timeseries,
      Map<MetricSlice, DataFrame> aggregates, Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies,
      Multimap<EventSlice, EventDTO> events) {
    this.model = model;
    this.timeseries = timeseries;
    this.aggregates = aggregates;
    this.anomalies = anomalies;
    this.events = events;
  }

  public StaticDetectionPipelineModel getModel() {
    return model;
  }

  public Map<MetricSlice, DataFrame> getTimeseries() {
    return timeseries;
  }

  public Map<MetricSlice, DataFrame> getAggregates() {
    return aggregates;
  }

  public Multimap<AnomalySlice, MergedAnomalyResultDTO> getAnomalies() {
    return anomalies;
  }

  public Multimap<EventSlice, EventDTO> getEvents() {
    return events;
  }
}
