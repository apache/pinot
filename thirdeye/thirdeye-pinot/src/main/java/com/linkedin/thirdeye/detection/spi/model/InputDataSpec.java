/*
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

package com.linkedin.thirdeye.detection.spi.model;

import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collection;
import java.util.Collections;


/**
 * The data spec to describe all the input data for a detection stage perform the detection.
 */
public class InputDataSpec {
  /*
    Specs for time series. Describe what time series to fetch.
    Each slice defines the time range, granularity, metric id, and filters of each time series to fetch.
  */
  final Collection<MetricSlice> timeseriesSlices;

  /*
    Specs for aggregates. Describe what aggregate values to fetch.
    Each slice defines the time range, granularity, metric id, and filters of each aggregate values to fetch.
  */
  final Collection<MetricSlice> aggregateSlices;

  /*
    Specs for anomalies. Describe what anomalies to fetch.
    Each slice defines the time range and pipeline config id of the anomalies to fetch.
  */
  final Collection<AnomalySlice> anomalySlices;

  /*
    Specs for events. Describe what events to fetch.
    Each slice defines the time range and dimensions of the events to fetch.
  */
  final Collection<EventSlice> eventSlices;

  /*
    Metric ids to fetch the MetricConfigDTO for.
   */
  final Collection<Long> metricIds;

  /*
    Dataset names to fetch the DatasetConfigDTO for.
  */
  final Collection<String> datasetNames;

  /*
    Metric ids to fetch the DatasetConfigDTO for.
  */
  final Collection<Long> metricIdsForDatasets;

  public InputDataSpec() {
    this.timeseriesSlices = Collections.emptyList();
    this.aggregateSlices = Collections.emptyList();
    this.anomalySlices = Collections.emptyList();
    this.eventSlices = Collections.emptyList();
    this.metricIds = Collections.emptyList();
    this.datasetNames = Collections.emptyList();
    this.metricIdsForDatasets = Collections.emptyList();
  }

  public InputDataSpec(Collection<MetricSlice> timeseriesSlices, Collection<MetricSlice> aggregateSlices,
      Collection<AnomalySlice> anomalySlices, Collection<EventSlice> eventSlices, Collection<Long> metricIds, Collection<String> datasetNames,
      Collection<Long> metricIdsForDatasets) {
    this.timeseriesSlices = timeseriesSlices;
    this.aggregateSlices = aggregateSlices;
    this.anomalySlices = anomalySlices;
    this.eventSlices = eventSlices;
    this.metricIds = metricIds;
    this.datasetNames = datasetNames;
    this.metricIdsForDatasets = metricIdsForDatasets;
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

  public Collection<Long> getMetricIds() {
    return metricIds;
  }

  public Collection<String> getDatasetNames() {
    return datasetNames;
  }

  public Collection<Long> getMetricIdsForDatasets() {
    return metricIdsForDatasets;
  }

  public InputDataSpec withTimeseriesSlices(Collection<MetricSlice> timeseriesSlices) {
    return new InputDataSpec(timeseriesSlices, this.aggregateSlices, this.anomalySlices, this.eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets);
  }

  public InputDataSpec withAggregateSlices(Collection<MetricSlice> aggregateSlices) {
    return new InputDataSpec(this.timeseriesSlices, aggregateSlices, this.anomalySlices, this.eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets);
  }

  public InputDataSpec withAnomalySlices(Collection<AnomalySlice> anomalySlices) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, anomalySlices, this.eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets);
  }

  public InputDataSpec withEventSlices(Collection<EventSlice> eventSlices) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets);
  }

  public InputDataSpec withMetricIds(Collection<Long> metricIds) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, metricIds, this.datasetNames, this.metricIdsForDatasets);
  }

  public InputDataSpec withDatasetNames(Collection<String> datasetNames) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, this.metricIds, datasetNames, this.metricIdsForDatasets);
  }

  public InputDataSpec withMetricIdsForDataset(Collection<Long> metricIdsForDatasets) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, this.metricIds, this.datasetNames, metricIdsForDatasets);

  }
}
