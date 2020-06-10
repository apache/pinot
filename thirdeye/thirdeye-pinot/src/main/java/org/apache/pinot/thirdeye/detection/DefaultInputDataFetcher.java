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

import com.google.common.collect.Multimap;
import java.util.HashSet;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EvaluationSlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Input data fetcher.
 * For components to fetch the input data it need.
 */
public class DefaultInputDataFetcher implements InputDataFetcher {
  private DataProvider provider;
  private long configId;

  public DefaultInputDataFetcher(DataProvider provider, long configId) {
    this.provider = provider;
    this.configId = configId;
  }

  /**
   * Fetch data for input data spec
   */
  public InputData fetchData(InputDataSpec inputDataSpec) {
    Map<MetricSlice, DataFrame> timeseries = provider.fetchTimeseries(inputDataSpec.getTimeseriesSlices());
    Map<MetricSlice, DataFrame> aggregates = provider.fetchAggregates(inputDataSpec.getAggregateSlices(), Collections.<String>emptyList(), -1);

    Collection<AnomalySlice> slicesWithConfigId = new HashSet<>();
    for (AnomalySlice slice : inputDataSpec.getAnomalySlices()) {
      slicesWithConfigId.add(slice.withDetectionId(configId));
    }
    Multimap<AnomalySlice, MergedAnomalyResultDTO> existingAnomalies = provider.fetchAnomalies(slicesWithConfigId);

    Multimap<EventSlice, EventDTO> events = provider.fetchEvents(inputDataSpec.getEventSlices());
    Map<Long, MetricConfigDTO> metrics = provider.fetchMetrics(inputDataSpec.getMetricIds());
    Map<String, DatasetConfigDTO> datasets = provider.fetchDatasets(inputDataSpec.getDatasetNames());
    Multimap<EvaluationSlice, EvaluationDTO> evaluations = provider.fetchEvaluations(inputDataSpec.getEvaluationSlices(), configId);
    Map<Long, DatasetConfigDTO> datasetForMetricId = fetchDatasetForMetricId(inputDataSpec.getMetricIdsForDatasets());
    Map<InputDataSpec.MetricAndDatasetName, MetricConfigDTO> metricForMetricAndDatasetName = fetchMetricForDatasetAndMetricNames(inputDataSpec.getMetricAndDatasetNames());
    return new InputData(inputDataSpec, timeseries, aggregates, existingAnomalies, events, metrics, datasets, evaluations, datasetForMetricId, metricForMetricAndDatasetName);
  }

  private Map<InputDataSpec.MetricAndDatasetName, MetricConfigDTO> fetchMetricForDatasetAndMetricNames(Collection<InputDataSpec.MetricAndDatasetName> metricNameAndDatasetNames){
    Map<InputDataSpec.MetricAndDatasetName, MetricConfigDTO> result = new HashMap<>();
    for (InputDataSpec.MetricAndDatasetName pair : metricNameAndDatasetNames) {
      result.put(pair, this.provider.fetchMetric(pair.getMetricName(), pair.getDatasetName()));
    }
    return result;
  }

  private Map<Long, DatasetConfigDTO> fetchDatasetForMetricId(Collection<Long> metricIdsForDatasets) {
    Map<Long, MetricConfigDTO> metrics = provider.fetchMetrics(metricIdsForDatasets);
    Map<Long, String> metricIdToDataSet = new HashMap<>();
    for (Map.Entry<Long, MetricConfigDTO> entry : metrics.entrySet()){
      metricIdToDataSet.put(entry.getKey(), entry.getValue().getDataset());
    }
    Map<String, DatasetConfigDTO> datasets = provider.fetchDatasets(metricIdToDataSet.values());
    Map<Long, DatasetConfigDTO> result = new HashMap<>();
    for (Map.Entry<Long, MetricConfigDTO> entry : metrics.entrySet()){
      result.put(entry.getKey(), datasets.get(entry.getValue().getDataset()));
    }
    return result;
  }

}
