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

package org.apache.pinot.thirdeye.detection.dataquality;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AbstractDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipelineLoader;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import org.apache.pinot.thirdeye.detection.components.ThresholdRuleDetector;
import org.apache.pinot.thirdeye.detection.dataquality.components.DataSlaQualityChecker;
import org.apache.pinot.thirdeye.detection.validators.ConfigValidationException;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class DataQualityTaskRunnerTest {
  private DetectionPipelineTaskInfo info;
  private TaskContext context;
  private DAOTestBase testDAOProvider;
  private DetectionConfigManager detectionDAO;
  private DetectionAlertConfigManager subscriptionDAO;
  private DatasetConfigManager datasetDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private EvaluationManager evaluationDAO;
  private MetricConfigDTO metricConfigDTO;
  private DatasetConfigDTO datasetConfigDTO;
  private DetectionConfigDTO detectionConfigDTO;

  private long detectorId;
  private DetectionPipelineLoader loader;
  private DataProvider provider;

  private static final long GRANULARITY = TimeUnit.DAYS.toMillis(1);
  private static final long START_TIME = new DateTime(System.currentTimeMillis(), DateTimeZone.forID("UTC"))
      .withMillisOfDay(0).getMillis();

  @BeforeMethod
  public void beforeMethod() throws Exception {
    this.testDAOProvider = DAOTestBase.getInstance();
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.subscriptionDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.loader = new DetectionPipelineLoader();

    DetectionRegistry.registerComponent(DataSlaQualityChecker.class.getName(), "DATA_SLA");
    DetectionRegistry.registerComponent(ThresholdRuleDetector.class.getName(), "THRESHOLD");

    metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(123L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setId(124L);
    datasetConfigDTO.setDataset("thirdeye-test-dataset");
    datasetConfigDTO.setDisplayName("thirdeye-test-dataset");
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimezone("UTC");
    datasetConfigDTO.setLastRefreshTime(START_TIME + 4 * GRANULARITY - 1);
    datasetConfigDTO.setDataSource("TestSource");
    this.datasetDAO.save(datasetConfigDTO);

    this.provider = new MockDataProvider()
        .setLoader(this.loader)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(Collections.emptyList());

    detectionConfigDTO = translateSlaConfig(-1, "sla-config-1.yaml");
    this.detectorId = detectionConfigDTO.getId();

    this.info = new DetectionPipelineTaskInfo();
    this.info.setConfigId(this.detectorId);
    this.context = new TaskContext();
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    this.testDAOProvider.cleanup();
  }

  /**
   * Load and update the detection config from filePath into detectionId
   */
  private DetectionConfigDTO translateSlaConfig(long detectionId, String filePath) throws Exception {
    String yamlConfig = IOUtils.toString(this.getClass().getResourceAsStream(filePath), StandardCharsets.UTF_8);
    DetectionConfigTranslator translator = new DetectionConfigTranslator(yamlConfig, this.provider);
    DetectionConfigDTO detectionConfigDTO = translator.translate();
    if (detectionId < 0) {
      detectionConfigDTO.setId(this.detectionDAO.save(detectionConfigDTO));
    } else {
      detectionConfigDTO.setId(detectionId);
      this.detectionDAO.update(detectionConfigDTO);
    }
    return detectionConfigDTO;
  }

  /**
   * Prepare slice [offsetStart, offsetEnd)
   */
  private MetricSlice prepareMetricSlice(long offsetStart, long offsetEnd) {
    long start = START_TIME + offsetStart * GRANULARITY;
    long end = START_TIME + offsetEnd * GRANULARITY;
    return MetricSlice.from(123L, start, end);
  }

  private Map<MetricSlice, DataFrame> prepareMockTimeseriesMap(long offsetStart, long offsetEnd) {
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    MetricSlice slice = prepareMetricSlice(offsetStart, offsetEnd);
    timeSeries.put(slice, prepareMockTimeseries(offsetStart, offsetEnd));
    return timeSeries;
  }

  /**
   * Prepares a mock time-series [offsetStart, offsetEnd] with Geometric progression values starting from 100
   */
  private DataFrame prepareMockTimeseries(long offsetStart, long offsetEnd) {
    long[] values = new long[(int) (offsetEnd - offsetStart + 1)];
    long[] timestamps = new long[(int) (offsetEnd - offsetStart + 1)];

    long gpFactor = 2;
    long currentValue = 100;
    for (long i = offsetStart; i <= offsetEnd; i++) {
      int index = (int) (i - offsetStart);
      values[index] = currentValue;
      timestamps[index] = START_TIME + i * GRANULARITY;
      currentValue = currentValue * gpFactor;
    }

    return new DataFrame()
        .addSeries(COL_VALUE, values)
        .addSeries(COL_TIME, timestamps);
  }

  private List<MergedAnomalyResultDTO> retrieveAllAnomalies() {
    List<MergedAnomalyResultDTO> anomalies = anomalyDAO.findAll();
    // remove id and create_time for comparison purposes
    anomalies.forEach(anomaly -> anomaly.setId(null));
    anomalies.forEach(anomaly -> anomaly.setCreatedTime(null));
    return anomalies;
  }

  private void cleanUpAnomalies() {
    List<MergedAnomalyResultDTO> anomalies = anomalyDAO.findAll();
    anomalyDAO.deleteByIds(anomalies.stream().map(AbstractDTO::getId).collect(Collectors.toList()));
  }

  private MergedAnomalyResultDTO makeSlaAnomaly(long offsetStart, long offsetEnd, String sla, String ruleName) {
    MergedAnomalyResultDTO anomalyDTO = new MergedAnomalyResultDTO();
    anomalyDTO.setStartTime(START_TIME + offsetStart * GRANULARITY);
    anomalyDTO.setEndTime(START_TIME + offsetEnd * GRANULARITY);
    anomalyDTO.setMetricUrn("thirdeye:metric:123");
    anomalyDTO.setMetric("thirdeye-test");
    anomalyDTO.setCollection("thirdeye-test-dataset");
    anomalyDTO.setType(AnomalyType.DATA_SLA);
    anomalyDTO.setAnomalyResultSource(AnomalyResultSource.DATA_QUALITY_DETECTION);
    anomalyDTO.setDetectionConfigId(this.detectorId);
    anomalyDTO.setChildIds(Collections.emptySet());

    Map<String, String> anomalyProps = new HashMap<>();
    long start = START_TIME + offsetStart * GRANULARITY;
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(start - 1));
    anomalyProps.put("sla", sla);
    anomalyProps.put("detectorComponentName", ruleName);
    anomalyProps.put("subEntityName", "test_sla_alert");
    anomalyDTO.setProperties(anomalyProps);
    return anomalyDTO;
  }

  /**
   * Test if a Data SLA anomaly is crested when data is not available
   */
  @Test
  public void testDataSlaWhenDataIsMissing() throws Exception {
    Map<MetricSlice, DataFrame> timeSeries = prepareMockTimeseriesMap(0, 3);
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(Collections.emptyList());
    DataQualityPipelineTaskRunner runner = new DataQualityPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
        this.evaluationDAO,
        this.loader,
        mockDataProvider
    );

    // CHECK 0: Report data missing immediately if delayed even by a minute (sla = 0_DAYS)
    // This comes into effect if the detection runs more frequently than the dataset granularity.
    detectionConfigDTO = translateSlaConfig(detectorId, "sla-config-0.yaml");

    // 1st scan - sla breach
    //  time:  3____4    5     // We have data till 3rd, data for 4th is missing/delayed
    //  scan:       |----|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 5 * GRANULARITY);
    runner.execute(this.info, this.context);
    // 1 data sla anomaly should be created
    List<MergedAnomalyResultDTO> anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    List<MergedAnomalyResultDTO> expectedAnomalies = new ArrayList<>();
    expectedAnomalies.add(makeSlaAnomaly(4, 5, "1_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
    // clean up
    expectedAnomalies.clear();
    cleanUpAnomalies();

    // CHECK 1: Report data delayed by at least a day (sla = 1_DAYS)
    detectionConfigDTO = translateSlaConfig(detectorId, "sla-config-1.yaml");

    // 1st scan - sla breach
    //  time:  3____4    5     // We have data till 3rd, data for 4th is missing/delayed
    //  scan:       |----|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 5 * GRANULARITY);
    runner.execute(this.info, this.context);
    // No data sla anomaly should be created
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // CHECK 2: Report data missing when delayed (sla = 2_DAYS)
    detectionConfigDTO = translateSlaConfig(detectorId, "sla-config-2.yaml");

    // 1st scan after issue - no sla breach
    //  time:  3____4    5           // We have data till 3rd, data for 4th is missing/delayed
    //  scan:       |----|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 5 * GRANULARITY);
    runner.execute(this.info, this.context);
    // 0 data sla anomaly should be created as there is no delay
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // 2nd scan after issue - still no sla breach
    //  time:    3____4    5    6    // Data for 4th still hasn't arrived
    //  scan:         |---------|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 6 * GRANULARITY);
    runner.execute(this.info, this.context);
    // 1 data sla anomaly should be created as data is delayed by 1 ms
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // 3rd scan after issue - sla breach. It has cross 2 days.
    //  time:    3____4    5    6    7    // Data for 4th still hasn't arrived
    //  scan:         |--------------|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 7 * GRANULARITY);
    runner.execute(this.info, this.context);
    // 1 data sla anomaly should be created as data is delayed by 1 ms
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    expectedAnomalies.add(makeSlaAnomaly(4, 7, "2_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
    // clean up
    expectedAnomalies.clear();
    cleanUpAnomalies();


    // CHECK 3: Report data missing when delayed by (sla = 3_DAYS)
    detectionConfigDTO = translateSlaConfig(detectorId, "sla-config-3.yaml");

    // 1st scan after issue - no sla breach
    //  time:  3____4    5           // We have data till 3rd, data for 4th is missing/delayed
    //  scan:       |----|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 5 * GRANULARITY);
    runner.execute(this.info, this.context);
    // 0 data sla anomaly should be created as there is no delay
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // 2nd scan after issue - no sla breach
    //  time:    3____4    5    6         // Data for 4th still hasn't arrived, no sla breach
    //  scan:         |---------|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 6 * GRANULARITY);
    runner.execute(this.info, this.context);
    // 0 data sla anomaly should be created as there is no delay
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // 3rd scan after issue - no sla breach
    //  time:    3____4    5    6    7    // Data for 4th still hasn't arrived
    //  scan:         |--------------|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 7 * GRANULARITY);
    runner.execute(this.info, this.context);
    // No data sla anomaly should be created from 4 to 7; sla not breached
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // 4th scan after issue - sla breach
    //  time:    3____4    5    6    7    8    // Data for 4th still hasn't arrived
    //  scan:         |-------------------|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 8 * GRANULARITY);
    runner.execute(this.info, this.context);
    // 1 data sla anomaly should be created from 4 to 8 as the data is missing since 3 days
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    expectedAnomalies.add(makeSlaAnomaly(4, 8, "3_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
    // clean up
    expectedAnomalies.clear();
    cleanUpAnomalies();


    // CHECK 4: Report data missing when there is a gap btw LastRefreshTime & SLA window start
    detectionConfigDTO = translateSlaConfig(detectorId, "sla-config-1.yaml");

    // 2 cases are possible:
    // a. with availability events
    // b. no availability events - directly query source for just the window

    // a. with availability events
    // 1st scan after issue - sla breach
    //  time:  3____4    5    6           // We have data till 3rd, data since 4th is missing/delayed
    //  scan:            |----|
    this.info.setStart(START_TIME + 5 * GRANULARITY);
    this.info.setEnd(START_TIME + 6 * GRANULARITY);
    runner.execute(this.info, this.context);

    // 1 data sla anomaly should be created from 4 to 6
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    expectedAnomalies.add(makeSlaAnomaly(4, 6, "1_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
    // clean up
    expectedAnomalies.clear();
    cleanUpAnomalies();


    // b. no availability events - directly query source
    datasetConfigDTO.setLastRefreshTime(0);
    datasetDAO.update(datasetConfigDTO);
    // 1st scan after issue - sla breach
    //  time:  3____4    5    6    7           // We have data till 3rd, data since 4th is missing/delayed
    //  scan:            |---------|
    this.info.setStart(START_TIME + 5 * GRANULARITY);
    this.info.setEnd(START_TIME + 7 * GRANULARITY);
    runner.execute(this.info, this.context);

    // 1 data sla anomaly should be created from 4 to 7
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    expectedAnomalies.add(makeSlaAnomaly(5, 7, "1_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
    // clean up
    expectedAnomalies.clear();
    cleanUpAnomalies();
  }

  /**
   * Test that data SLA anomalies are created when data is partially available
   */
  @Test
  public void testDataSlaWhenDataPartiallyAvailable() throws Exception {
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    // Assumption - As per data availability info, we have data till 3rd.
    datasetConfigDTO.setLastRefreshTime(START_TIME + 4 * GRANULARITY - 1);
    datasetDAO.update(datasetConfigDTO);

    MockDataProvider mockDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(Collections.emptyList());
    DataQualityPipelineTaskRunner runner = new DataQualityPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
        this.evaluationDAO,
        this.loader,
        mockDataProvider
    );

    // Create detection window (2 - 10) with a lot of empty data points (no data after 3rd).
    MetricSlice sliceWithManyEmptyDataPoints = prepareMetricSlice(2, 10);
    timeSeries.put(sliceWithManyEmptyDataPoints, prepareMockTimeseries(2, 3));
    this.info.setStart(sliceWithManyEmptyDataPoints.getStart());
    this.info.setEnd(sliceWithManyEmptyDataPoints.getEnd());
    runner.execute(this.info, this.context);

    // 1 data sla anomaly should be created for the missing days
    List<MergedAnomalyResultDTO> anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    List<MergedAnomalyResultDTO> expectedAnomalies = new ArrayList<>();
    expectedAnomalies.add(makeSlaAnomaly(4, 10, "1_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
    //clean up
    expectedAnomalies.clear();
    cleanUpAnomalies();


    // Create another detection window (2 - 10) with no empty data points.
    MetricSlice sliceWithFullDataPoints = prepareMetricSlice(2, 10);
    timeSeries.put(sliceWithFullDataPoints, prepareMockTimeseries(2, 9));
    this.info.setStart(sliceWithFullDataPoints.getStart());
    this.info.setEnd(sliceWithFullDataPoints.getEnd());
    runner.execute(this.info, this.context);

    // 0 data sla anomaly should be created as data is considered to be completely available (above threshold)
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);


    // Create another detection window (2 - 10) with more data points than the data availability event.
    MetricSlice sliceWithMoreDataPointsThanEvent = prepareMetricSlice(2, 10);
    timeSeries.put(sliceWithMoreDataPointsThanEvent, prepareMockTimeseries(2, 7));
    this.info.setStart(sliceWithMoreDataPointsThanEvent.getStart());
    this.info.setEnd(sliceWithMoreDataPointsThanEvent.getEnd());
    runner.execute(this.info, this.context);

    // 1 data sla anomaly should be created for the missing days
    // Data source has more data than what the availability event reports, the sla anomaly should reflect accordingly.
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    expectedAnomalies = new ArrayList<>();
    expectedAnomalies.add(makeSlaAnomaly(8, 10, "1_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
    //clean up
    expectedAnomalies.clear();
    cleanUpAnomalies();
  }

  /**
   * Test that no Data SLA anomaly is created when data is complete and available
   */
  @Test
  public void testDataSlaWhenDataAvailable() throws Exception {
    Map<MetricSlice, DataFrame> timeSeries = prepareMockTimeseriesMap(0, 3);
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(Collections.emptyList());
    DataQualityPipelineTaskRunner runner = new DataQualityPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
        this.evaluationDAO,
        this.loader,
        mockDataProvider
    );

    // Prepare the data sla task over existing data in time-series
    this.info.setStart(START_TIME + 1 * GRANULARITY);
    this.info.setEnd(START_TIME + 3 * GRANULARITY);
    runner.execute(this.info, this.context);

    // No data sla anomaly should be created
    List<MergedAnomalyResultDTO> anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // Prepare the data sla task over existing data in time-series
    this.info.setStart(START_TIME);
    this.info.setEnd(START_TIME + 4 * GRANULARITY);
    runner.execute(this.info, this.context);

    // No data sla anomaly should be created
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);
  }

  /**
   * Note that we do not support sla on dimensional data. This test validates that no SLA anomalies are created
   * when some dimensional data is missing but the overall dataset is complete.
   */
  @Test
  public void testDataSlaWhenDimensionalDataUnavailable() throws Exception {
    Map<MetricSlice, DataFrame> timeSeries = prepareMockTimeseriesMap(0, 3);
    // Prepare the mock time-series where 2 dimensional data points are missing but the overall metric has complete data
    // Cached slice time window represents that we have complete data (4 points)
    Multimap<String, String> filters = HashMultimap.create();
    filters.put("dim1", "1");
    MetricSlice dimensionSlice = MetricSlice.from(123L, START_TIME, START_TIME + 4 * GRANULARITY, filters);
    // create 2 data points
    timeSeries.put(dimensionSlice, prepareMockTimeseries(0, 1));
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(Collections.emptyList());
    DataQualityPipelineTaskRunner runner = new DataQualityPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
        this.evaluationDAO,
        this.loader,
        mockDataProvider
    );

    // Scan a period where dimensional data is missing but dataset is complete.
    this.info.setStart(START_TIME + 3 * GRANULARITY);
    this.info.setEnd(START_TIME + 4 * GRANULARITY);
    runner.execute(this.info, this.context);

    // 0 SLA anomaly should be created. Though dimensional data is missing, the dataset as a whole is complete.
    List<MergedAnomalyResultDTO> anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // Prepare the data sla task on unavailable data
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 6 * GRANULARITY);
    runner.execute(this.info, this.context);

    // 1 SLA anomaly should be created
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), START_TIME + 4 * GRANULARITY);
    Assert.assertEquals(anomalies.get(0).getEndTime(), START_TIME + 6 * GRANULARITY);
    Assert.assertEquals(anomalies.get(0).getType(), AnomalyType.DATA_SLA);
    Assert.assertTrue(anomalies.get(0).getAnomalyResultSource().equals(AnomalyResultSource.DATA_QUALITY_DETECTION));
    Map<String, String> anomalyProps = new HashMap<>();
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(START_TIME + 4 * GRANULARITY - 1));
    anomalyProps.put("datasetLastRefreshTimeRoundUp", String.valueOf(START_TIME + 4 * GRANULARITY));
    anomalyProps.put("sla", "1_DAYS");
    anomalyProps.put("detectorComponentName", "slaRule1:DATA_SLA");
    anomalyProps.put("subEntityName", "test_sla_alert");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
  }

  /**
   * Test if data sla anomalies are properly merged
   */
  @Test
  public void testDataSlaAnomalyMerge() throws Exception {
    Map<MetricSlice, DataFrame> timeSeries = prepareMockTimeseriesMap( 0, 3);
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setLoader(this.loader)
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO))
        .setAnomalies(Collections.emptyList());
    DataQualityPipelineTaskRunner runner = new DataQualityPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
        this.evaluationDAO,
        this.loader,
        mockDataProvider
    );

    // 1st scan
    //  time:  3____4    5      // We have data till 3rd, data for 4th is missing/delayed
    //  scan:       |----|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 5 * GRANULARITY);
    runner.execute(this.info, this.context);

    // No sla anomaly should be created as the data is within 1_DAY
    List<MergedAnomalyResultDTO> anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 0);

    // 2nd scan
    //  time:    3____4    5    6    // Data for 4th still hasn't arrived
    //  scan:         |---------|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 6 * GRANULARITY);
    runner.execute(this.info, this.context);

    // We should now have 1 anomalies in our database (4 to 6)
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 1);
    List<MergedAnomalyResultDTO> expectedAnomalies = new ArrayList<>();
    expectedAnomalies.add(makeSlaAnomaly(4, 6, "1_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));

    // 3rd scan
    //  time:  3____4____5    6    7    // Data for 4th has arrived by now, data for 5th is still missing
    //  scan:       |--------------|
    this.info.setStart(START_TIME + 4 * GRANULARITY);
    this.info.setEnd(START_TIME + 7 * GRANULARITY);
    datasetConfigDTO.setLastRefreshTime(START_TIME + 5 * GRANULARITY - 1);
    datasetDAO.update(datasetConfigDTO);
    runner = new DataQualityPipelineTaskRunner(
        this.detectionDAO,
        this.anomalyDAO,
        this.evaluationDAO,
        this.loader,
        mockDataProvider
    );
    runner.execute(this.info, this.context);

    // We will now have 2 anomalies in our database
    // In the current iteration we will create 1 new anomaly from (5 to 7)
    anomalies = retrieveAllAnomalies();
    Assert.assertEquals(anomalies.size(), 2);
    expectedAnomalies = new ArrayList<>();
    expectedAnomalies.add(makeSlaAnomaly(4, 6, "1_DAYS", "slaRule1:DATA_SLA"));
    expectedAnomalies.add(makeSlaAnomaly(5, 7, "1_DAYS", "slaRule1:DATA_SLA"));
    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
  }
}
