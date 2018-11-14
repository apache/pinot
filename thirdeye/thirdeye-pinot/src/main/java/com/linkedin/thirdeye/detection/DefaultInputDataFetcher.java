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

package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.spi.model.EventSlice;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
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
    Map<MetricSlice, DataFrame> aggregates = provider.fetchAggregates(inputDataSpec.getAggregateSlices(), Collections.<String>emptyList());
    Multimap<AnomalySlice, MergedAnomalyResultDTO> existingAnomalies = provider.fetchAnomalies(inputDataSpec.getAnomalySlices(), configId);
    Multimap<EventSlice, EventDTO> events = provider.fetchEvents(inputDataSpec.getEventSlices());
    Map<Long, MetricConfigDTO> metrics = provider.fetchMetrics(inputDataSpec.getMetricIds());
    Map<String, DatasetConfigDTO> datasets = provider.fetchDatasets(inputDataSpec.getDatasetNames());

    Map<Long, DatasetConfigDTO> datasetForMetricId = fetchDatasetForMetricId(provider, inputDataSpec);
    return new InputData(inputDataSpec, timeseries, aggregates, existingAnomalies, events, metrics, datasets, datasetForMetricId);
  }

  private Map<Long, DatasetConfigDTO> fetchDatasetForMetricId(DataProvider provider, InputDataSpec inputDataSpec) {
    Map<Long, MetricConfigDTO> metrics = provider.fetchMetrics(inputDataSpec.getMetricIdsForDatasets());
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
