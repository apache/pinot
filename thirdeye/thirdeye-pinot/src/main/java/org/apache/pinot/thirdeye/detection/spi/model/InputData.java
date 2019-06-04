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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Collection;
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
  final Multimap<EvaluationSlice, EvaluationDTO> evaluations;

  /**
   * The data set config dtos for metric ids
   * @see InputDataSpec#withMetricIdsForDataset(Collection)
   */
  final Map<Long, DatasetConfigDTO> datasetForMetricId;

  /**
   * The metric config dtos for metric and data set names
   * @see InputDataSpec#withMetricNamesAndDatasetNames(Collection)
   */
  final Map<InputDataSpec.MetricAndDatasetName, MetricConfigDTO> metricForMetricAndDatasetNames;

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
    this.metricForMetricAndDatasetNames = Collections.emptyMap();
    this.evaluations = ArrayListMultimap.create();
  }

  public InputData(InputDataSpec spec, Map<MetricSlice, DataFrame> timeseries, Map<MetricSlice, DataFrame> aggregates,
      Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies, Multimap<EventSlice, EventDTO> events,
      Map<Long, MetricConfigDTO> metrics, Map<String, DatasetConfigDTO> datasets,
      Multimap<EvaluationSlice, EvaluationDTO> evaluations, Map<Long, DatasetConfigDTO> datasetForMetricId,
      Map<InputDataSpec.MetricAndDatasetName, MetricConfigDTO> metricForMetricAndDatasetNames) {
    this.dataSpec = spec;
    this.timeseries = timeseries;
    this.aggregates = aggregates;
    this.anomalies = anomalies;
    this.events = events;
    this.metrics = metrics;
    this.datasets = datasets;
    this.evaluations = evaluations;
    this.datasetForMetricId = datasetForMetricId;
    this.metricForMetricAndDatasetNames = metricForMetricAndDatasetNames;
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

  public Map<InputDataSpec.MetricAndDatasetName, MetricConfigDTO> getMetricForMetricAndDatasetNames() {
    return metricForMetricAndDatasetNames;
  }

  public Multimap<EvaluationSlice, EvaluationDTO> getEvaluations() {
    return evaluations;
  }
}
