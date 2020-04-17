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

package org.apache.pinot.thirdeye.detection.datasla;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.anomaly.AnomalyType;
import org.apache.pinot.thirdeye.anomaly.task.TaskContext;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.DetectionPipelineTaskInfo;
import org.apache.pinot.thirdeye.detection.MockDataProvider;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class DatasetSlaTaskRunnerTest {
  private DetectionPipelineTaskInfo info;
  private TaskContext context;
  private DAOTestBase testDAOProvider;
  private DetectionConfigManager detectionDAO;
  private DatasetConfigManager datasetDAO;
  private MergedAnomalyResultManager anomalyDAO;
  private EvaluationManager evaluationDAO;
  private MetricConfigDTO metricConfigDTO;
  private DatasetConfigDTO datasetConfigDTO;
  private DetectionConfigDTO detectionConfigDTO;
  private long detectorId;

  private final Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
  private DataFrame dataFrame;

  private static long period = TimeUnit.DAYS.toMillis(1);
  private static DateTimeZone timezone = DateTimeZone.forID("UTC");
  private static long startTime = new DateTime(System.currentTimeMillis(), timezone).withMillisOfDay(0).getMillis();

  @BeforeMethod
  public void beforeMethod() {
    this.testDAOProvider = DAOTestBase.getInstance();
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();

    metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setId(123L);
    metricConfigDTO.setName("thirdeye-test");
    metricConfigDTO.setDataset("thirdeye-test-dataset");

    datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setId(124L);
    datasetConfigDTO.setDataset("thirdeye-test-dataset");
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimezone("UTC");
    datasetConfigDTO.setLastRefreshTime(startTime + 4 * period - 1);
    this.datasetDAO.save(datasetConfigDTO);

    detectionConfigDTO = new DetectionConfigDTO();
    detectionConfigDTO.setName("myName");
    detectionConfigDTO.setDescription("myDescription");
    detectionConfigDTO.setCron("myCron");
    Map<String, Object> metricSla = new HashMap<>();
    metricSla.put("sla", "1_DAYS");
    Map<String, Object> props = new HashMap<>();
    props.put("thirdeye:metric:123", metricSla);
    detectionConfigDTO.setDataSLAProperties(props);
    this.detectorId = this.detectionDAO.save(detectionConfigDTO);

    this.info = new DetectionPipelineTaskInfo();
    this.info.setConfigId(this.detectorId);
    this.context = new TaskContext();

    // Prepare the mock time-series data with 1 ms granularity
    this.dataFrame = new DataFrame()
        .addSeries(COL_VALUE, 100, 200, 500, 1000)
        .addSeries(COL_TIME, startTime, startTime + period, startTime + 2*period, startTime + 3*period);
    MetricSlice slice = MetricSlice.from(123L, startTime, startTime + 4*period);
    this.timeSeries.put(slice, dataFrame);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    this.testDAOProvider.cleanup();
  }

  /**
   * Test if a Data SLA anomaly is crested when data is not available
   */
  @Test
  public void testDataSlaWhenDataIsMissing() throws Exception {
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
    Map<String, Object> metricSla = new HashMap<>();
    Map<String, Object> props = new HashMap<>();
    DatasetSlaTaskRunner runner = new DatasetSlaTaskRunner(detectionDAO, anomalyDAO, evaluationDAO, mockDataProvider);

    // CHECK 1: Report data missing immediately (sla - 0 ms)
    metricSla.put("sla", "1_DAYS");
    props.put("thirdeye:metric:123", metricSla);
    detectionConfigDTO.setDataSLAProperties(props);
    this.detectorId = this.detectionDAO.update(detectionConfigDTO);

    // 1st scan after issue
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 5 * period);
    runner.execute(this.info, this.context);
    // 1 data sla anomaly should be created
    List<MergedAnomalyResultDTO> anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), startTime + 4 * period);
    Assert.assertEquals(anomalies.get(0).getEndTime(), startTime + 5 * period);
    Assert.assertEquals(anomalies.get(0).getType(), AnomalyType.DATA_MISSING);
    anomalyDAO.delete(anomalies.get(0));  // clean up


    // CHECK 2: Report data missing when delayed by (sla - 1 day)
    metricSla.put("sla", "2_DAYS");
    props.put("thirdeye:metric:123", metricSla);
    detectionConfigDTO.setDataSLAProperties(props);
    this.detectorId = this.detectionDAO.update(detectionConfigDTO);

    // 1st scan after issue
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 5 * period);
    runner.execute(this.info, this.context);
    // 0 data sla anomaly should be created as there is no delay
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 0);

    // 2nd scan after issue
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 6 * period);
    runner.execute(this.info, this.context);
    // 1 data sla anomaly should be created as data is delayed by 1 ms
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), startTime + 4 * period);
    Assert.assertEquals(anomalies.get(0).getEndTime(), startTime + 6 * period);
    Assert.assertEquals(anomalies.get(0).getType(), AnomalyType.DATA_MISSING);
    Map<String, String> anomalyProps = new HashMap<>();
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
    anomalyProps.put("sla", "2_DAYS");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
    anomalyDAO.delete(anomalies.get(0));  // clean up


    // CHECK 3: Report data missing when delayed by (sla - 2 ms)
    metricSla.put("sla", "3_DAYS");
    props.put("thirdeye:metric:123", metricSla);
    detectionConfigDTO.setDataSLAProperties(props);
    this.detectorId = this.detectionDAO.update(detectionConfigDTO);

    // 1st scan after issue
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 5 * period);
    runner.execute(this.info, this.context);
    // 0 data sla anomaly should be created as there is no delay
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 0);

    // 2nd scan after issue
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 6 * period);
    runner.execute(this.info, this.context);
    // 0 data sla anomaly should be created as there is no delay
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 0);

    // 3rd scan after issue
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 7 * period);
    runner.execute(this.info, this.context);
    // 1 data sla anomaly should be created as the data is delayed by 2 ms
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), startTime + 4 * period);
    Assert.assertEquals(anomalies.get(0).getEndTime(), startTime + 7 * period);
    Assert.assertEquals(anomalies.get(0).getType(), AnomalyType.DATA_MISSING);
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
    anomalyProps.put("sla", "3_DAYS");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
    anomalyDAO.delete(anomalies.get(0));  // clean up


    // CHECK 4: Report data missing when there is a gap btw LastRefreshTime & SLA window start
    metricSla.put("sla", "1_DAYS");
    props.put("thirdeye:metric:123", metricSla);
    detectionConfigDTO.setDataSLAProperties(props);
    this.detectorId = this.detectionDAO.update(detectionConfigDTO);
    this.info.setStart(startTime + 5 * period);
    this.info.setEnd(startTime + 6 * period);
    runner.execute(this.info, this.context);

    // 1 data sla anomaly should be created
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), startTime + 4 * period);
    Assert.assertEquals(anomalies.get(0).getEndTime(), startTime + 6 * period);
    Assert.assertEquals(anomalies.get(0).getType(), AnomalyType.DATA_MISSING);
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
    anomalyProps.put("sla", "1_DAYS");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
    anomalyDAO.delete(anomalies.get(0));  // clean up
  }

  /**
   * Test that no data SLA anomalies are created when data is partially available
   * We do not deal with partial data in this current iteration/phase
   */
  @Test
  public void testDataSlaWhenDataPartiallyAvailable() throws Exception {
    MetricSlice sliceOverlapBelowThreshold = MetricSlice.from(123L, startTime + 2 * period, startTime + 10 * period);
    timeSeries.put(sliceOverlapBelowThreshold, new DataFrame()
        .addSeries(COL_VALUE, 500, 1000)
        .addSeries(COL_TIME, startTime + 2 * period, startTime + 3 * period));
    MetricSlice sliceOverlapAboveThreshold = MetricSlice.from(123L, startTime + 2 * period, startTime + 4 * period);
    timeSeries.put(sliceOverlapAboveThreshold, new DataFrame()
        .addSeries(COL_VALUE, 500, 1000)
        .addSeries(COL_TIME, startTime + 2 * period, startTime + 3 * period));
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
    DatasetSlaTaskRunner runner = new DatasetSlaTaskRunner(detectionDAO, anomalyDAO, evaluationDAO, mockDataProvider);
    datasetConfigDTO.setLastRefreshTime(0);
    datasetDAO.update(datasetConfigDTO);

    this.info.setStart(sliceOverlapBelowThreshold.getStart());
    this.info.setEnd(sliceOverlapBelowThreshold.getEnd());
    runner.execute(this.info, this.context);

    // 1 data sla anomaly should be created for the missing days
    List<MergedAnomalyResultDTO> anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), startTime + 4 * period);
    Assert.assertEquals(anomalies.get(0).getEndTime(), startTime + 10 * period);
    Assert.assertEquals(anomalies.get(0).getType(), AnomalyType.DATA_MISSING);
    Map<String, String> anomalyProps = new HashMap<>();
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 3 * period));
    anomalyProps.put("sla", "1_DAYS");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
    anomalyDAO.delete(anomalies.get(0));  // clean up

    this.info.setStart(sliceOverlapAboveThreshold.getStart());
    this.info.setEnd(sliceOverlapAboveThreshold.getEnd());
    runner.execute(this.info, this.context);

    // 0 data sla anomaly should be created as data is considered to be completely availability (above threshold)
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 0);
  }

  /**
   * Test that no Data SLA anomaly is created when data is complete and available
   */
  @Test
  public void testDataSlaWhenDataAvailable() throws Exception {
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
    DatasetSlaTaskRunner runner = new DatasetSlaTaskRunner(detectionDAO, anomalyDAO, evaluationDAO, mockDataProvider);

    // Prepare the data sla task over existing data in time-series
    this.info.setStart(startTime + period);
    this.info.setEnd(startTime + 3 * period);
    runner.execute(this.info, this.context);

    // No data sla anomaly should be created
    List<MergedAnomalyResultDTO> anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 0);

    // Prepare the data sla task over existing data in time-series
    this.info.setStart(startTime);
    this.info.setEnd(startTime + 4 * period);
    runner.execute(this.info, this.context);

    // No data sla anomaly should be created
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 0);
  }

  /**
   * Test that no data SLA anomaly is created when dataset is available but dimensional data is missing
   */
  @Test
  public void testDataSlaWhenDimensionalDataUnavailable() throws Exception {
    // Prepare the mock dimensional time-series where 2 data points are missing but the overall dataset has complete data
    Multimap<String, String> filters = HashMultimap.create();
    filters.put("dim1", "1");
    MetricSlice dimensionSlice = MetricSlice.from(123L, startTime, startTime + 4 * period, filters);
    Map<MetricSlice, DataFrame> timeSeries = new HashMap<>();
    // has only 2 data points
    DataFrame dataFrame = new DataFrame()
        .addSeries(COL_VALUE, 100, 200)
        .addSeries(COL_TIME, startTime, startTime + period);
    timeSeries.put(dimensionSlice, dataFrame);
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
    DatasetSlaTaskRunner runner = new DatasetSlaTaskRunner(detectionDAO, anomalyDAO, evaluationDAO, mockDataProvider);

    Map<String, Object> metricSla = new HashMap<>();
    metricSla.put("sla", "1_DAYS");
    Map<String, Object> props = new HashMap<>();
    props.put("thirdeye:metric:123:dim1%3D1", metricSla);
    detectionConfigDTO.setDataSLAProperties(props);
    this.detectorId = this.detectionDAO.update(detectionConfigDTO);

    // Scan a period where dimensional data is missing but dataset is complete.
    this.info.setConfigId(this.detectorId);
    this.info.setStart(startTime + 3 * period);
    this.info.setEnd(startTime + 4 * period);
    runner.execute(this.info, this.context);

    // 0 SLA anomaly should be created. Though dimensional data is missing, the dataset as a whole is complete.
    List<MergedAnomalyResultDTO> anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 0);

    // Prepare the data sla task on unavailable data
    this.info.setConfigId(this.detectorId);
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 5 * period);
    runner.execute(this.info, this.context);

    // 1 SLA anomaly should be created
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(anomalies.get(0).getStartTime(), startTime + 4 * period);
    Assert.assertEquals(anomalies.get(0).getEndTime(), startTime + 5 * period);
    Assert.assertEquals(anomalies.get(0).getType(), AnomalyType.DATA_MISSING);
    Map<String, String> anomalyProps = new HashMap<>();
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
    anomalyProps.put("sla", "1_DAYS");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
  }

  /**
   * Test if data sla anomalies are properly merged
   */
  @Test
  public void testDataSlaAnomalyMerge() throws Exception {
    MockDataProvider mockDataProvider = new MockDataProvider()
        .setTimeseries(timeSeries)
        .setMetrics(Collections.singletonList(metricConfigDTO))
        .setDatasets(Collections.singletonList(datasetConfigDTO));
    DatasetSlaTaskRunner runner = new DatasetSlaTaskRunner(detectionDAO, anomalyDAO, evaluationDAO, mockDataProvider);

    // 1st scan
    //  time:  3____4    5      // We have data till 3rd, data for 4th is missing/delayed
    //  scan:       |----|
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 5 * period);
    runner.execute(this.info, this.context);

    // 1 data sla anomaly should be created from 4 to 5
    List<MergedAnomalyResultDTO> anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 1);
    MergedAnomalyResultDTO slaAnomaly = anomalies.get(0);
    Assert.assertEquals(slaAnomaly.getStartTime(), startTime + 4 * period);
    Assert.assertEquals(slaAnomaly.getEndTime(), startTime + 5 * period);
    Assert.assertEquals(slaAnomaly.getType(), AnomalyType.DATA_MISSING);
    Map<String, String> anomalyProps = new HashMap<>();
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
    anomalyProps.put("sla", "1_DAYS");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);

    // 2nd scan
    //  time:    3____4    5    6    // Data for 4th still hasn't arrived
    //  scan:         |---------|
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 6 * period);
    runner.execute(this.info, this.context);

    // We should now have 2 anomalies in our database (1 parent, 1 child)
    // The new anomaly (4 to 6) will be parent of the existing anomaly (4 to 5) created during the previous scan
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 2);
    for (int i = 0; i < 2; i++) {
      MergedAnomalyResultDTO anomalyDTO = anomalies.get(i);
      if (!anomalyDTO.isChild()) {
        // Parent anomaly
        Assert.assertEquals(anomalyDTO.getStartTime(), startTime + 4 * period);
        Assert.assertEquals(anomalyDTO.getEndTime(), startTime + 6 * period);
        Assert.assertEquals(anomalyDTO.getType(), AnomalyType.DATA_MISSING);
        Assert.assertTrue(anomalyDTO.getChildIds().contains(slaAnomaly.getId()));
        anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
        anomalyProps.put("sla", "1_DAYS");
        Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
      } else {
        // Child anomaly
        Assert.assertEquals(anomalyDTO.getStartTime(), startTime + 4 * period);
        Assert.assertEquals(anomalyDTO.getEndTime(), startTime + 5 * period);
        Assert.assertEquals(anomalyDTO.getType(), AnomalyType.DATA_MISSING);
        Assert.assertTrue(anomalyDTO.isChild());
        anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
        anomalyProps.put("sla", "1_DAYS");
        Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
      }
    }

    // 3rd scan
    //  time:  3____4____5    6    7    // Data for 4th has arrived by now, data for 5th is still missing
    //  scan:       |--------------|
    this.info.setStart(startTime + 4 * period);
    this.info.setEnd(startTime + 7 * period);
    datasetConfigDTO.setLastRefreshTime(startTime + 5 * period - 1);
    datasetDAO.update(datasetConfigDTO);
    runner = new DatasetSlaTaskRunner(detectionDAO, anomalyDAO, evaluationDAO, mockDataProvider);
    runner.execute(this.info, this.context);

    // We will now have 3 anomalies in our database (2 parent and 1 child)
    // 1 parent and 1 child created during the previous iterations
    // In the current iteration we will create 1 new anomaly from (5 to 7)
    anomalies = anomalyDAO.findAll();
    Assert.assertEquals(anomalies.size(), 3);
    anomalies = anomalies.stream().filter(anomaly -> !anomaly.isChild()).collect(Collectors.toList());
    // We should now have 2 parent anomalies
    Assert.assertEquals(anomalies.size(), 2);
    anomalies.get(0).setId(null);
    anomalies.get(1).setId(null);

    List<MergedAnomalyResultDTO> expectedAnomalies = new ArrayList<>();
    MergedAnomalyResultDTO anomalyDTO = new MergedAnomalyResultDTO();
    anomalyDTO.setStartTime(startTime + 5 * period);
    anomalyDTO.setEndTime(startTime + 7 * period);
    anomalyDTO.setType(AnomalyType.DATA_MISSING);
    anomalyDTO.setDetectionConfigId(this.detectorId);
    anomalyDTO.setChildIds(Collections.emptySet());
    anomalyProps.put("datasetLastRefreshTime", String.valueOf(startTime + 4 * period - 1));
    anomalyProps.put("sla", "1_DAYS");
    Assert.assertEquals(anomalies.get(0).getProperties(), anomalyProps);
    expectedAnomalies.add(anomalyDTO);

    Assert.assertTrue(anomalies.containsAll(expectedAnomalies));
  }
}
