package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collection;
import java.util.Collections;


public class StaticDetectionPipelineModel {
  final Collection<MetricSlice> timeseriesSlices;
  final Collection<MetricSlice> aggregateSlices;
  final Collection<AnomalySlice> anomalySlices;
  final Collection<EventSlice> eventSlices;

  public StaticDetectionPipelineModel() {
    this.timeseriesSlices = Collections.emptyList();
    this.aggregateSlices = Collections.emptyList();
    this.anomalySlices = Collections.emptyList();
    this.eventSlices = Collections.emptyList();
  }

  public StaticDetectionPipelineModel(Collection<MetricSlice> timeseriesSlices, Collection<MetricSlice> aggregateSlices,
      Collection<AnomalySlice> anomalySlices, Collection<EventSlice> eventSlices) {
    this.timeseriesSlices = timeseriesSlices;
    this.aggregateSlices = aggregateSlices;
    this.anomalySlices = anomalySlices;
    this.eventSlices = eventSlices;
  }

  public Collection<MetricSlice> getTimeseriesSlices() {
    return timeseriesSlices;
  }

  public Collection<MetricSlice> getAggregateSlices() {
    return aggregateSlices;
  }

  public Collection<AnomalySlice> getAnomalySlices() {
    return anomalySlices;
  }

  public Collection<EventSlice> getEventSlices() {
    return eventSlices;
  }

  public StaticDetectionPipelineModel withTimeseriesSlices(Collection<MetricSlice> timeseriesSlices) {
    return new StaticDetectionPipelineModel(timeseriesSlices, this.aggregateSlices, this.anomalySlices, this.eventSlices);
  }

  public StaticDetectionPipelineModel withAggregateSlices(Collection<MetricSlice> aggregateSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, aggregateSlices, this.anomalySlices, this.eventSlices);
  }

  public StaticDetectionPipelineModel withAnomalySlices(Collection<AnomalySlice> anomalySlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, anomalySlices, this.eventSlices);
  }

  public StaticDetectionPipelineModel withEventSlices(Collection<EventSlice> eventSlices) {
    return new StaticDetectionPipelineModel(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices);
  }
}
