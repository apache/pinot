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

package org.apache.pinot.thirdeye.anomaly.detection.trigger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.filter.ActiveDatasetFilter;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.filter.OnTimeFilter;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.filter.DataAvailabilityEventFilter;
import org.apache.pinot.thirdeye.anomaly.detection.trigger.utils.DatasetTriggerInfoRepo;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataAvailabilityEventListenerTest {
  private Logger LOG = LoggerFactory.getLogger(this.getClass());
  static String TEST_DATA_SOURCE = "TestSource";
  static String TEST_DATASET_PREFIX = "ds_trigger_listener_";
  static String TEST_METRIC_PREFIX = "metric_trigger_listener_";
  private DAOTestBase testDAOProvider;
  private DataAvailabilityEventListener _dataAvailabilityEventListener;
  private MockConsumerDataAvailability consumer = new MockConsumerDataAvailability();

  @BeforeMethod
  public void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
    DetectionConfigManager detectionConfigManager = DAORegistry.getInstance().getDetectionConfigManager();
    MetricConfigManager metricConfigManager = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetConfigDAO = DAORegistry.getInstance().getDatasetConfigDAO();

    MetricConfigDTO metric1 = new MetricConfigDTO();
    metric1.setDataset(TEST_DATASET_PREFIX + 1);
    metric1.setName(TEST_METRIC_PREFIX + 1);
    metric1.setActive(true);
    metric1.setAlias("");
    long metricId1 = metricConfigManager.save(metric1);

    MetricConfigDTO metric2 = new MetricConfigDTO();
    metric2.setDataset(TEST_DATASET_PREFIX + 2);
    metric2.setName(TEST_METRIC_PREFIX + 2);
    metric2.setActive(true);
    metric2.setAlias("");
    long metricId2 = metricConfigManager.save(metric2);

    DetectionConfigDTO detect1 = new DetectionConfigDTO();
    detect1.setName("detection_trigger_listener1");
    detect1.setActive(true);
    Map<String, Object> props = new HashMap<>();
    List<String> metricUrns = new ArrayList<>();
    metricUrns.add("thirdeye:metric:" + metricId1);
    metricUrns.add("thirdeye:metric:" + metricId2);
    props.put("nestedMetricUrns", metricUrns);
    detect1.setProperties(props);
    detectionConfigManager.save(detect1);

    DatasetConfigDTO ds1 = new DatasetConfigDTO();
    ds1.setDataset(TEST_DATASET_PREFIX + 1);
    ds1.setDataSource(TEST_DATA_SOURCE);
    ds1.setLastRefreshTime(1000);
    datasetConfigDAO.save(ds1);

    DatasetConfigDTO ds2 = new DatasetConfigDTO();
    ds2.setDataset(TEST_DATASET_PREFIX + 2);
    ds2.setDataSource(TEST_DATA_SOURCE);
    ds2.setLastRefreshTime(2000);
    datasetConfigDAO.save(ds2);

    DatasetTriggerInfoRepo.init(100, Collections.singletonList(TEST_DATA_SOURCE));
    List<DataAvailabilityEventFilter> filters = new ArrayList<>();
    filters.add(new OnTimeFilter());
    filters.add(new ActiveDatasetFilter());
    _dataAvailabilityEventListener = new DataAvailabilityEventListener(consumer, filters, 0, 5_000);
  }

  @Test
  public void testUpdateOneDataset() throws InterruptedException {
    _dataAvailabilityEventListener.processOneBatch();
    DatasetConfigManager datasetConfigManager = DAORegistry.getInstance().getDatasetConfigDAO();
    DatasetConfigDTO dataset1 = datasetConfigManager.findByDataset(TEST_DATASET_PREFIX + 1);
    DatasetConfigDTO dataset2 = datasetConfigManager.findByDataset(TEST_DATASET_PREFIX + 2);
    DatasetTriggerInfoRepo datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 1), 2000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 2), 3000);
    Assert.assertEquals(dataset1.getLastRefreshTime(), 2000);
    Assert.assertEquals(dataset2.getLastRefreshTime(), 3000);
  }

  @Test(dependsOnMethods = { "testUpdateOneDataset" })
  public void testUpdateTwoDataset() throws InterruptedException {
    _dataAvailabilityEventListener.processOneBatch();
    DatasetConfigManager datasetConfigManager = DAORegistry.getInstance().getDatasetConfigDAO();
    DatasetConfigDTO dataset1 = datasetConfigManager.findByDataset(TEST_DATASET_PREFIX + 1);
    DatasetConfigDTO dataset2 = datasetConfigManager.findByDataset(TEST_DATASET_PREFIX + 2);
    DatasetTriggerInfoRepo datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 1), 3000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 2), 3000);
    Assert.assertEquals(dataset1.getLastRefreshTime(), 3000);
    Assert.assertEquals(dataset2.getLastRefreshTime(), 3000);
  }

  @Test(dependsOnMethods = { "testUpdateTwoDataset" })
  public void testNoUpdate() throws InterruptedException {
    _dataAvailabilityEventListener.processOneBatch();
    DatasetConfigManager datasetConfigManager = DAORegistry.getInstance().getDatasetConfigDAO();
    DatasetConfigDTO dataset1 = datasetConfigManager.findByDataset(TEST_DATASET_PREFIX + 1);
    DatasetConfigDTO dataset2 = datasetConfigManager.findByDataset(TEST_DATASET_PREFIX + 2);
    DatasetTriggerInfoRepo datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 1), 1000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 2), 2000);
    Assert.assertEquals(dataset1.getLastRefreshTime(), 1000);
    Assert.assertEquals(dataset2.getLastRefreshTime(), 2000);
  }

  @AfterMethod()
  public void afterMethod() {
    _dataAvailabilityEventListener.close();
    testDAOProvider.cleanup();
  }
}
