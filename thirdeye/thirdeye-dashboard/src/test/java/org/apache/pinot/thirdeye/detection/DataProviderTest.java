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


package org.apache.pinot.thirdeye.detection;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.csv.CSVThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProviderTest {

  private DAOTestBase testBase;
  private EventManager eventDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private EvaluationManager evaluationDAO;
  private DetectionConfigManager detectionDAO;
  private QueryCache queryCache;
  private TimeSeriesLoader timeseriesLoader;
  private AggregationLoader aggregationLoader;

  private DataFrame data;

  private DataProvider provider;

  private List<Long> eventIds;
  private List<Long> anomalyIds;
  private List<Long> metricIds;
  private List<Long> datasetIds;
  private List<Long> detectionIds;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.testBase = DAOTestBase.getInstance();

    DAORegistry reg = DAORegistry.getInstance();
    this.eventDAO = reg.getEventDAO();
    this.anomalyDAO = reg.getMergedAnomalyResultDAO();
    this.metricDAO = reg.getMetricConfigDAO();
    this.datasetDAO = reg.getDatasetConfigDAO();
    this.evaluationDAO = reg.getEvaluationManager();
    this.detectionDAO = reg.getDetectionConfigManager();
    // events
    this.eventIds = new ArrayList<>();
    this.eventIds.add(this.eventDAO.save(makeEvent(3600000L, 7200000L)));
    this.eventIds.add(this.eventDAO.save(makeEvent(10800000L, 14400000L)));
    this.eventIds.add(this.eventDAO.save(makeEvent(14400000L, 18000000L, Arrays.asList("a=1", "b=4", "b=2"))));
    this.eventIds.add(this.eventDAO.save(makeEvent(604800000L, 1209600000L, Arrays.asList("b=2", "c=3"))));
    this.eventIds.add(this.eventDAO.save(makeEvent(1209800000L, 1210600000L, Collections.singleton("b=4"))));

    // detections
    this.detectionIds = new ArrayList<>();
    DetectionConfigDTO detectionConfig = new DetectionConfigDTO();
    detectionConfig.setName("test_detection_1");
    detectionConfig.setDescription("test_description_1");
    this.detectionIds.add(this.detectionDAO.save(detectionConfig));
    detectionConfig.setName("test_detection_2");
    detectionConfig.setDescription("test_description_2");
    this.detectionIds.add(this.detectionDAO.save(detectionConfig));

    // anomalies
    this.anomalyIds = new ArrayList<>();
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, detectionIds.get(0), 4000000L, 8000000L, Arrays.asList("a=1", "c=3", "b=2"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, detectionIds.get(0), 8000000L, 12000000L, Arrays.asList("a=1", "c=4"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, detectionIds.get(1), 604800000L, 1209600000L, Collections.<String>emptyList())));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "c=3"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "a=2", "c=3"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "a=3", "c=3"))));
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(null, detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "a=2", "c=3", "d=4"))));

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
      this.data.setIndex(DataFrame.COL_TIME);
      this.data.addSeries(DataFrame.COL_TIME, this.data.getLongs(DataFrame.COL_TIME).multiply(1000));
    }

    // register caches

    LoadingCache<String, DatasetConfigDTO> mockDatasetConfigCache = Mockito.mock(LoadingCache.class);
    DatasetConfigDTO datasetConfig = this.datasetDAO.findByDataset("myDataset2");
    Mockito.when(mockDatasetConfigCache.get("myDataset2")).thenReturn(datasetConfig);


    LoadingCache<String, Long> mockDatasetMaxDataTimeCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockDatasetMaxDataTimeCache.get("myDataset2"))
        .thenReturn(Long.MAX_VALUE);

    MetricDataset metricDataset = new MetricDataset("myMetric2", "myDataset2");
    LoadingCache<MetricDataset, MetricConfigDTO> mockMetricConfigCache = Mockito.mock(LoadingCache.class);
    MetricConfigDTO metricConfig = this.metricDAO.findByMetricAndDataset("myMetric2", "myDataset2");
    Mockito.when(mockMetricConfigCache.get(metricDataset)).thenReturn(metricConfig);

    Map<String, DataFrame> datasets = new HashMap<>();
    datasets.put("myDataset1", data);
    datasets.put("myDataset2", data);

    Map<Long, String> id2name = new HashMap<>();
    id2name.put(this.metricIds.get(1), "value");
    Map<String, ThirdEyeDataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put("myDataSource", CSVThirdEyeDataSource.fromDataFrame(datasets, id2name));
    this.queryCache = new QueryCache(dataSourceMap, Executors.newSingleThreadExecutor());

    ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();
    cacheRegistry.registerMetricConfigCache(mockMetricConfigCache);
    cacheRegistry.registerDatasetConfigCache(mockDatasetConfigCache);
    cacheRegistry.registerQueryCache(this.queryCache);
    cacheRegistry.registerDatasetMaxDataTimeCache(mockDatasetMaxDataTimeCache);

    // time series loader
    this.timeseriesLoader = new DefaultTimeSeriesLoader(this.metricDAO, this.datasetDAO, this.queryCache, null);

    // aggregation loader
    this.aggregationLoader = new DefaultAggregationLoader(this.metricDAO, this.datasetDAO, this.queryCache, mockDatasetMaxDataTimeCache);

    // provider
    this.provider = new DefaultDataProvider(this.metricDAO, this.datasetDAO, this.eventDAO, this.anomalyDAO,
        this.evaluationDAO, this.timeseriesLoader, aggregationLoader, null,
        TimeSeriesCacheBuilder.getInstance(), AnomaliesCacheBuilder.getInstance());
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    this.testBase.cleanup();
  }

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

  @Test
  public void testFetchAggregation() {
    MetricSlice metricSlice = MetricSlice.from(this.metricIds.get(1), 0L, 32400000L, ArrayListMultimap.create());
    Map<MetricSlice, DataFrame> aggregates = this.provider.fetchAggregates(Collections.singletonList(metricSlice), Collections.emptyList(), 1);
    Assert.assertEquals(aggregates.keySet().size(), 1);
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

  @Test(expectedExceptions = RuntimeException.class)
  public void testAnomalyInvalid() {
    this.provider.fetchAnomalies(Collections.singleton(new AnomalySlice()));
  }

  @Test
  public void testAnomalySingle() {
    AnomalySlice slice = makeAnomalySlice(1209000000L, -1, Collections.<String>emptyList());

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(2), detectionIds.get(1), 604800000L, 1209600000L, Collections.<String>emptyList())));
  }

  @Test
  public void testAnomalyDimension() {
    AnomalySlice slice = makeAnomalySlice(0, -1, Arrays.asList("a=1", "c=3"));

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(slice)).get(slice);

    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(0), detectionIds.get(0), 4000000L, 8000000L, Arrays.asList("a=1", "c=3", "b=2"))));
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(3), detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "c=3"))));

    Assert.assertFalse(anomalies.contains(makeAnomaly(this.anomalyIds.get(2), detectionIds.get(1), 604800000L, 1209600000L, Collections.<String>emptyList())));
  }

  @Test
  public void testAnomalyMultiDimensions() {
    AnomalySlice slice = makeAnomalySlice(0, -1, Arrays.asList("a=1", "a=2", "c=3"));

    Collection<MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(Collections.singleton(slice)).get(slice);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(4), detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "a=2", "c=3"))));
    Assert.assertTrue(anomalies.contains(makeAnomaly(this.anomalyIds.get(6), detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "a=2", "c=3", "d=4"))));
    Assert.assertFalse(anomalies.contains(makeAnomaly(this.anomalyIds.get(3), detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "c=3"))));
    Assert.assertFalse(anomalies.contains(makeAnomaly(this.anomalyIds.get(5), detectionIds.get(1), 14400000L, 18000000L, Arrays.asList("a=1", "a=3", "c=3"))));
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
    anomaly.setChildIds(new HashSet<>());
    anomaly.setType(AnomalyType.DEVIATION);

    StringBuilder filterUrn = new StringBuilder();
    for (String fs : filterStrings) {
      filterUrn.append(":").append(fs);
    }

    anomaly.setMetricUrn("thirdeye:metric:1234" + filterUrn.toString());
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
    return new AnomalySlice().withStart(start).withEnd(end).withFilters(filters);
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
