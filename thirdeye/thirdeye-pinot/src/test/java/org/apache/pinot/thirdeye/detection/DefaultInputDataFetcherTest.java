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

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.detection.spi.model.InputData;
import org.apache.pinot.thirdeye.detection.spi.model.InputDataSpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultInputDataFetcherTest {
  @Test
  public void testFetchData() {
    MockDataProvider mockDataProvider = new MockDataProvider();
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    MetricSlice slice = MetricSlice.from(123L, 0, 10);
    timeSeries.put(slice,
        new DataFrame().addSeries(DataFrame.COL_VALUE, 0, 100, 200, 500, 1000).addSeries(
            DataFrame.COL_TIME, 0, 2, 4, 6, 8));

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(123L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setId(124L);
    datasetConfigDTO.setDataset("thirdeye-test-dataset");
    datasetConfigDTO.setTimeDuration(2);
    datasetConfigDTO.setTimeUnit(TimeUnit.MILLISECONDS);
    datasetConfigDTO.setTimezone("UTC");

    mockDataProvider.setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
    InputDataFetcher dataFetcher = new DefaultInputDataFetcher(mockDataProvider, -1);

    InputData data = dataFetcher.fetchData(new InputDataSpec().withTimeseriesSlices(Collections.singletonList(slice))
        .withMetricIds(Collections.singletonList(123L))
        .withMetricIdsForDataset(Collections.singletonList(123L))
        .withDatasetNames(Collections.singletonList("thirdeye-test-dataset")));
    Assert.assertEquals(data.getTimeseries().get(slice), timeSeries.get(slice));
    Assert.assertEquals(data.getMetrics().get(123L), metricConfigDTO);
    Assert.assertEquals(data.getDatasetForMetricId().get(123L), datasetConfigDTO);
    Assert.assertEquals(data.getDatasets().get("thirdeye-test-dataset"), datasetConfigDTO);
  }
}