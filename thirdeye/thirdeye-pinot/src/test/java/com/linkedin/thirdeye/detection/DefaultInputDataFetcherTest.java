package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class DefaultInputDataFetcherTest {
  @Test
  public void testFetchData() {
    MockDataProvider mockDataProvider = new MockDataProvider();
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    MetricSlice slice = MetricSlice.from(123L, 0, 10);
    timeSeries.put(slice,
        new DataFrame().addSeries(COL_VALUE, 0, 100, 200, 500, 1000).addSeries(COL_TIME, 0, 2, 4, 6, 8));

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