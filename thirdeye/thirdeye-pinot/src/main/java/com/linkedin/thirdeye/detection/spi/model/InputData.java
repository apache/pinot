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

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Collections;
import java.util.Map;


/**
 * Input data for each detection stage
 */
public class InputData {
  final InputDataSpec dataSpec;
  final Map<MetricSlice, DataFrame> timeseries;
  final Map<MetricSlice, DataFrame> aggregates;
  final Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies;
  final Multimap<EventSlice, EventDTO> events;
  final Map<Long, MetricConfigDTO> metrics;
  final Map<String, DatasetConfigDTO> datasets;
  final Map<Long, DatasetConfigDTO> datasetForMetricId;

  public InputData(InputDataSpec spec, Map<MetricSlice, DataFrame> timeseries, Map<MetricSlice, DataFrame> aggregates,
      Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies, Multimap<EventSlice, EventDTO> events) {
    this.dataSpec = spec;
    this.timeseries = timeseries;
    this.aggregates = aggregates;
    this.anomalies = anomalies;
    this.events = events;
    this.metrics = Collections.emptyMap();
    this.datasets = Collections.emptyMap();
    this.datasetForMetricId = Collections.emptyMap();
  }

  public InputData(InputDataSpec spec, Map<MetricSlice, DataFrame> timeseries, Map<MetricSlice, DataFrame> aggregates,
      Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies, Multimap<EventSlice, EventDTO> events,
      Map<Long, MetricConfigDTO> metrics, Map<String, DatasetConfigDTO> datasets, Map<Long, DatasetConfigDTO> datasetForMetricId) {
    this.dataSpec = spec;
    this.timeseries = timeseries;
    this.aggregates = aggregates;
    this.anomalies = anomalies;
    this.events = events;
    this.metrics = metrics;
    this.datasets = datasets;
    this.datasetForMetricId = datasetForMetricId;
  }

  public InputDataSpec getDataSpec() {
    return dataSpec;
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

  public Map<Long, MetricConfigDTO> getMetrics() {
    return metrics;
  }

  public Map<String, DatasetConfigDTO> getDatasets() {
    return datasets;
  }

  public Map<Long, DatasetConfigDTO> getDatasetForMetricId(){
    return datasetForMetricId;
  }
}
