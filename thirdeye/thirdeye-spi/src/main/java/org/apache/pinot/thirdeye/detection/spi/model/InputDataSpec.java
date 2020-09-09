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

package org.apache.pinot.thirdeye.detection.spi.model;

import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
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
    Specs for evaluations. Describe what evaluations to fetch.
    Each slice defines the time range and detection config id of the evaluations to fetch.
  */
  final Collection<EvaluationSlice> evaluationSlices;

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

  /*
    Metric name and data set name to fetch the MetricConfigDTO for.
  */
  final Collection<MetricAndDatasetName> metricAndDatasetNames;

  public static class MetricAndDatasetName {
    private final String metricName;
    private final String datasetName;

    public MetricAndDatasetName(String metricName, String datasetName) {
      this.metricName = metricName;
      this.datasetName = datasetName;
    }

    public String getMetricName() {
      return metricName;
    }

    public String getDatasetName() {
      return datasetName;
    }
  }

  public InputDataSpec() {
    this(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
  }

  public InputDataSpec(Collection<MetricSlice> timeseriesSlices, Collection<MetricSlice> aggregateSlices,
      Collection<AnomalySlice> anomalySlices, Collection<EventSlice> eventSlices, Collection<Long> metricIds, Collection<String> datasetNames,
      Collection<Long> metricIdsForDatasets, Collection<MetricAndDatasetName> metricAndDatasetNames) {
    this(timeseriesSlices, aggregateSlices, anomalySlices, eventSlices, Collections.emptyList(), metricIds,
        datasetNames, metricIdsForDatasets, metricAndDatasetNames);
  }

  public InputDataSpec(Collection<MetricSlice> timeseriesSlices, Collection<MetricSlice> aggregateSlices,
      Collection<AnomalySlice> anomalySlices, Collection<EventSlice> eventSlices,
      Collection<EvaluationSlice> evaluationSlices, Collection<Long> metricIds, Collection<String> datasetNames,
      Collection<Long> metricIdsForDatasets, Collection<MetricAndDatasetName> metricAndDatasetNames) {
    this.timeseriesSlices = timeseriesSlices;
    this.aggregateSlices = aggregateSlices;
    this.anomalySlices = anomalySlices;
    this.eventSlices = eventSlices;
    this.evaluationSlices = evaluationSlices;
    this.metricIds = metricIds;
    this.datasetNames = datasetNames;
    this.metricIdsForDatasets = metricIdsForDatasets;
    this.metricAndDatasetNames = metricAndDatasetNames;
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

  public Collection<EvaluationSlice> getEvaluationSlices() {
    return evaluationSlices;
  }

  public Collection<Long> getMetricIdsForDatasets() {
    return metricIdsForDatasets;
  }

  public Collection<MetricAndDatasetName> getMetricAndDatasetNames() {
    return metricAndDatasetNames;
  }

  public InputDataSpec withTimeseriesSlices(Collection<MetricSlice> timeseriesSlices) {
    return new InputDataSpec(timeseriesSlices, this.aggregateSlices, this.anomalySlices, this.eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withAggregateSlices(Collection<MetricSlice> aggregateSlices) {
    return new InputDataSpec(this.timeseriesSlices, aggregateSlices, this.anomalySlices, this.eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withAnomalySlices(Collection<AnomalySlice> anomalySlices) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, anomalySlices, this.eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withEventSlices(Collection<EventSlice> eventSlices) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withEvaluationSlices(Collection<EvaluationSlice> evaluationSlices) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, this.eventSlices, evaluationSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withMetricIds(Collection<Long> metricIds) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, metricIds, this.datasetNames, this.metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withDatasetNames(Collection<String> datasetNames) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, this.metricIds, datasetNames, this.metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withMetricIdsForDataset(Collection<Long> metricIdsForDatasets) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, this.metricIds, this.datasetNames, metricIdsForDatasets, this.metricAndDatasetNames);
  }

  public InputDataSpec withMetricNamesAndDatasetNames(Collection<MetricAndDatasetName> metricNameAndDatasetNames) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices, this.metricIds, this.datasetNames, this.metricIdsForDatasets, metricNameAndDatasetNames);
  }
}
