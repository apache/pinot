/**
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.csv.CSVThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.datasource.loader.TimeSeriesLoader;
import com.linkedin.thirdeye.detection.spi.model.AnomalySlice;
import com.linkedin.thirdeye.detection.spi.model.EventSlice;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class DataProviderTest {
  private static final double EPSILON_MEAN = 20.0;

  private DAOTestBase testBase;
  private EventManager eventDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private QueryCache cache;
  private TimeSeriesLoader timeseriesLoader;

  private DataFrame data;

  private DataProvider provider;

  private List<Long> eventIds;
  private List<Long> anomalyIds;
  private List<Long> metricIds;
  private List<Long> datasetIds;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.testBase = DAOTestBase.getInstance();

    DAORegistry reg = DAORegistry.getInstance();
    this.eventDAO = reg.getEventDAO();
    this.anomalyDAO = reg.getMergedAnomalyResultDAO();
    this.metricDAO = reg.getMetricConfigDAO();
    this.datasetDAO = reg.getDatasetConfigDAO();

    // events
    this.eventIds = new ArrayList<>();
    this.eventIds.add(this.eventDAO.save(makeEvent(3600000L, 7200000L)));
    this.eventIds.add(this.eventDAO.save(makeEvent(10800000L, 14400000L)));
    this.eventIds.add(this.eventDAO.save(makeEvent(14400000L, 18000000L, Arrays.asList("a=1", "b=4", "b=2"))));
    this.eventIds.add(this.eventDAO.save(makeEvent(604800000L, 1209600000L, Arrays.asList("b=2", "c=3"))));
    this.eventIds.add(this.eventDAO.save(makeEvent(1209800000L, 1210600000L, Collections.singleton("b=4"))));

    // anomalies
    this.anomalyIds = new ArrayList<>();
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 100L, 4000000L, 8000000L, Arrays.asList("a=1", "c=3", "b=2"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 100L, 8000000L, 12000000L, Arrays.asList("a=1", "c=4"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 200L, 604800000L, 1209600000L, Collections.<String>emptyList())));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, 200L, 14400000L, 18000000L, Arrays.asList("a=1", "c=3"))));

    // metrics
    this.metricIds = new ArrayList<>();
    this.metricIds.add(this.metricDAO.save(makeMetric(null, "myMetric1", "myDataset1")));
    this.metricIds.add(this.metricDAO.save(makeMetric(null, "myMetric2", "myDataset2")));
    this.metricIds.add(this.metricDAO.save(makeMetric(null, "myMetric3", "myDataset1")));

    // datasets
    this.datasetIds = new ArrayList<>();
    this.datasetIds.add(this.datasetDAO.save(makeDataset(null, "myDataset1")));
    this.datasetIds.add(this.datasetDAO.save(makeDataset(null, "myDataset2")));

    // data
    try (Reader dataReader = new InputStreamReader(this.getClass().getResourceAsStream("algorithm/timeseries-4w.csv"))) {
      this.data = DataFrame.fromCsv(dataReader);
      this.data.setIndex(COL_TIME);
      this.data.addSeries(COL_TIME, this.data.getLongs(COL_TIME).multiply(1000));
    }

    Map<String, DataFrame> datasets = new HashMap<>();
    datasets.put("myDataset1", this.data);
    datasets.put("myDataset2", this.data);

    Map<Long, String> id2name = new HashMap<>();
    id2name.put(this.metricIds.get(0), "value");
    id2name.put(this.metricIds.get(1), "value");
    id2name.put(this.metricIds.get(2), "value");

    Map<String, ThirdEyeDataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put("myDataSource", CSVThirdEyeDataSource.fromDataFrame(datasets, id2name));

    this.cache = new QueryCache(dataSourceMap, Executors.newSingleThreadExecutor());
    ThirdEyeCacheRegistry.getInstance().registerQueryCache(this.cache);
    ThirdEyeCacheRegistry.initMetaDataCaches();

    // loaders
    this.timeseriesLoader = new DefaultTimeSeriesLoader(this.metricDAO, this.datasetDAO, this.cache);

    // provider
    this.provider = new DefaultDataProvider(this.metricDAO, this.datasetDAO, this.eventDAO, this.anomalyDAO,
        this.timeseriesLoader, null, null);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    this.testBase.cleanup();
  }

  //
  // timeseries
  //

  @Test
  public void testTimeseriesSingle() {
    MetricSlice slice = MetricSlice.from(this.metricIds.get(0), 604800000L, 1814400000L);

    DataFrame df = this.provider.fetchTimeseries(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(df.size(), 336);

    double mean = df.getDoubles(COL_VALUE).mean().doubleValue();
    Assert.assertTrue(Math.abs(mean - 1000) < EPSILON_MEAN);
  }

  @Test
  public void testTimeseriesMultiple() {
    MetricSlice slice1 = MetricSlice.from(this.metricIds.get(0), 604800000L, 1814400000L);
    MetricSlice slice2 = MetricSlice.from(this.metricIds.get(1), 604800000L, 1209600000L);

    Map<MetricSlice, DataFrame> output = this.provider.fetchTimeseries(Arrays.asList(slice1, slice2));

    Assert.assertEquals(output.size(), 2);

    double mean1 = output.get(slice1).getDoubles(COL_VALUE).mean().doubleValue();
    Assert.assertTrue(Math.abs(mean1 - 1000) < EPSILON_MEAN);

    double mean2 = output.get(slice2).getDoubles(COL_VALUE).mean().doubleValue();
    Assert.assertTrue(Math.abs(mean2 - 1000) < EPSILON_MEAN);

    Assert.assertNotEquals(mean1, mean2);
  }

  //
  // aggregates
  //

  // TODO fetch aggregates tests

  //
  // metric
  //

  @Test
  public void testMetricInvalid() {
    Assert.assertTrue(this.provider.fetchMetrics(Collections.singleton(-1L)).isEmpty());
  }

  @Test
  public void testMetricSingle() {
    MetricConfigDTO metric = this.provider.fetchMetrics(Collections.singleton(this.metricIds.get(1))).get(this.metricIds.get(1));

    Assert.assertNotNull(metric);
    Assert.assertEquals(metric, makeMetric(this.metricIds.get(1), "myMetric2", "myDataset2"));
  }

  @Test
  public void testMetricMultiple() {
    Collection<MetricConfigDTO> metrics = this.provider.fetchMetrics(Arrays.asList(this.metricIds.get(1), this.metricIds.get(2))).values();

    Assert.assertEquals(metrics.size(), 2);
    Assert.assertTrue(metrics.contains(makeMetric(this.metricIds.get(1), "myMetric2", "myDataset2")));
    Assert.assertTrue(metrics.contains(makeMetric(this.metricIds.get(2), "myMetric3", "myDataset1")));
  }

  //
  // datasets
  //

  @Test
  public void testDatasetInvalid() {
    Assert.assertTrue(this.provider.fetchDatasets(Collections.singleton("invalid")).isEmpty());
  }

  @Test
  public void testDatasetSingle() {
    DatasetConfigDTO dataset = this.provider.fetchDatasets(Collections.singleton("myDataset1")).get("myDataset1");

    Assert.assertNotNull(dataset);
    Assert.assertEquals(dataset, makeDataset(this.datasetIds.get(0), "myDataset1"));
  }

  @Test
  public void testDatasetMultiple() {
    Collection<DatasetConfigDTO> datasets = this.provider.fetchDatasets(Arrays.asList("myDataset1", "myDataset2")).values();

    Assert.assertEquals(datasets.size(), 2);
    Assert.assertTrue(datasets.contains(makeDataset(this.datasetIds.get(0), "myDataset1")));
    Assert.assertTrue(datasets.contains(makeDataset(this.datasetIds.get(1), "myDataset2")));
  }

  //
  // events
  //

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEventInvalid() {
    this.provider.fetchEvents(Collections.singleton(new EventSlice()));
  }

  @Test
  public void testEventSingle() {
    EventSlice slice = makeEventSlice(4000000L, 4100000L, Collections.<String>emptyList());

    Collection<EventDTO> events = this.provider.fetchEvents(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(events.size(), 1);
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(0), 3600000L, 7200000L, Collections.<String>emptyList())));
  }

  @Test
  public void testEventDimension() {
    EventSlice slice = makeEventSlice(10000000L, 1200000000L, Collections.singleton("b=2"));

    Collection<EventDTO> events = this.provider.fetchEvents(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(events.size(), 3);
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(1), 10800000L, 14400000L, Collections.<String>emptyList())));
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(2), 14400000L, 18000000L, Arrays.asList("a=1", "b=4", "b=2"))));
    Assert.assertTrue(events.contains(makeEvent(this.eventIds.get(3), 604800000L, 1209600000L, Arrays.asList("b=2", "c=3"))));
  }

  //
  // anomalies
  //

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAnomalyInvalid() {
    this.provider.fetchAnomalies(Collections.singleton(new AnomalySlice()), -1);
  }

  @Test
  public void testAnomalySingle() {
    AnomalySlice slice = makeAnomalySlice(1209000000L, -1, Collections.<String>emptyList());

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(slice), -1).get(slice);

    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(2), 200L, 604800000L, 1209600000L, Collections.<String>emptyList())));
  }

  @Test
  public void testAnomalyDimension() {
    AnomalySlice slice = makeAnomalySlice(0, -1, Arrays.asList("a=1", "c=3"));

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(slice), -1).get(slice);

    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(0), 100L, 4000000L, 8000000L, Arrays.asList("a=1", "c=3", "b=2"))));
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(2), 200L, 604800000L, 1209600000L, Collections.<String>emptyList())));
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(3), 200L, 14400000L, 18000000L, Arrays.asList("a=1", "c=3"))));
  }

  //
  // utils
  //

  private static MergedAnomalyResultDTO makeAnomaly(Long id, Long configId, long start, long end, Iterable<String> filterStrings) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setDetectionConfigId(configId);
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);
    anomaly.setId(id);

    DimensionMap filters = new DimensionMap();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      filters.put(parts[0], parts[1]);
    }

    anomaly.setDimensions(filters);
    return anomaly;
  }

  private static EventDTO makeEvent(long start, long end) {
    return makeEvent(null, start, end, Collections.<String>emptyList());
  }

  private static EventDTO makeEvent(long start, long end, Iterable<String> filterStrings) {
    return makeEvent(null, start, end, filterStrings);
  }

  private static EventDTO makeEvent(Long id, long start, long end, Iterable<String> filterStrings) {
    EventDTO event = new EventDTO();
    event.setId(id);
    event.setName(String.format("event-%d-%d", start, end));
    event.setStartTime(start);
    event.setEndTime(end);

    Map<String, List<String>> filters = new HashMap<>();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      if (!filters.containsKey(parts[0])) {
        filters.put(parts[0], new ArrayList<String>());
      }
      filters.get(parts[0]).add(parts[1]);
    }

    event.setTargetDimensionMap(filters);

    return event;
  }

  private static EventSlice makeEventSlice(long start, long end, Iterable<String> filterStrings) {
    SetMultimap<String, String> filters = HashMultimap.create();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      filters.put(parts[0], parts[1]);
    }
    return new EventSlice(start, end, filters);
  }

  private static AnomalySlice makeAnomalySlice(long start, long end, Iterable<String> filterStrings) {
    SetMultimap<String, String> filters = HashMultimap.create();
    for (String fs : filterStrings) {
      String[] parts = fs.split("=");
      filters.put(parts[0], parts[1]);
    }
    return new AnomalySlice(start, end, filters);
  }

  private static MetricConfigDTO makeMetric(Long id, String metric, String dataset) {
    MetricConfigDTO metricDTO = new MetricConfigDTO();
    metricDTO.setId(id);
    metricDTO.setName(metric);
    metricDTO.setDataset(dataset);
    metricDTO.setAlias(dataset + "::" + metric);
    return metricDTO;
  }

  private static DatasetConfigDTO makeDataset(Long id, String dataset) {
    DatasetConfigDTO datasetDTO = new DatasetConfigDTO();
    datasetDTO.setId(id);
    datasetDTO.setDataSource("myDataSource");
    datasetDTO.setDataset(dataset);
    datasetDTO.setTimeDuration(3600000);
    datasetDTO.setTimeUnit(TimeUnit.MILLISECONDS);
    return datasetDTO;
  }
}
