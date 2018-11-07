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

package com.linkedin.thirdeye.detection.algorithm.stage;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.spi.model.EventSlice;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import java.util.Collections;
import java.util.Map;


public class StageUtils {
  /**
   * Get the data for a data spec from provider
   * @param provider the data provider
   * @param inputDataSpec the spec of input data for the detection stage
   * @return data
   */
  public static InputData getDataForSpec(DataProvider provider, InputDataSpec inputDataSpec) {
    Map<MetricSlice, DataFrame> timeseries = provider.fetchTimeseries(inputDataSpec.getTimeseriesSlices());
    Map<MetricSlice, DataFrame> aggregates =
        provider.fetchAggregates(inputDataSpec.getAggregateSlices(), Collections.<String>emptyList());
    Multimap<AnomalySlice, MergedAnomalyResultDTO> existingAnomalies =
        provider.fetchAnomalies(inputDataSpec.getAnomalySlices());
    Multimap<EventSlice, EventDTO> events = provider.fetchEvents(inputDataSpec.getEventSlices());
    Map<Long, MetricConfigDTO> metrics = provider.fetchMetrics(inputDataSpec.getMetricIds());
    Map<String, DatasetConfigDTO> datasets = provider.fetchDatasets(inputDataSpec.getDatasetNames());
    return new InputData(inputDataSpec, timeseries, aggregates, existingAnomalies, events, metrics, datasets);
  }

  // TODO anomaly should support multimap
  public static DimensionMap toFilterMap(Multimap<String, String> filters) {
    DimensionMap map = new DimensionMap();
    for (Map.Entry<String, String> entry : filters.entries()) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  public static boolean isReferenceName(String key) {
    return key.startsWith("$");
  }

  public static String getReferenceKey(String str) {
    return str.toString().substring(1);
  }
}
