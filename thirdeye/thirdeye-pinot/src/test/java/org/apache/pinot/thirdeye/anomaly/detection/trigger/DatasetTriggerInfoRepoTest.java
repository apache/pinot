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
import org.apache.pinot.thirdeye.anomaly.detection.trigger.utils.DatasetTriggerInfoRepo;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DatasetTriggerInfoRepoTest {
  private static String TEST_DATA_SOURCE = "TestSource";
  private static String TEST_DATASET_PREFIX = "test_dataset_";
  private static String TEST_METRIC_PREFIX = "test_metric_";
  private DAOTestBase testDAOProvider;

  @BeforeMethod
  public void BeforeMethod() {
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
    detect1.setName("test_detection_1");
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
    DatasetTriggerInfoRepo.init(1, Collections.singletonList(TEST_DATA_SOURCE));
  }

  @Test
  public void testInitAndGetInstance () {
    DatasetTriggerInfoRepo datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();
        Assert.assertSame(datasetTriggerInfoRepo, DatasetTriggerInfoRepo.getInstance());
    Assert.assertSame(datasetTriggerInfoRepo, DatasetTriggerInfoRepo.getInstance());
    Assert.assertTrue(datasetTriggerInfoRepo.isDatasetActive(TEST_DATASET_PREFIX + 1));
    Assert.assertTrue(datasetTriggerInfoRepo.isDatasetActive(TEST_DATASET_PREFIX + 2));
    Assert.assertFalse(datasetTriggerInfoRepo.isDatasetActive(TEST_DATASET_PREFIX + 3));
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 1), 1000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 2), 2000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 3), 0);
  }

  @Test(dependsOnMethods = { "testInitAndGetInstance" })
  public void testSetLastUpdateTimestamp() {
    DatasetTriggerInfoRepo datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();
    datasetTriggerInfoRepo.setLastUpdateTimestamp(TEST_DATASET_PREFIX + 1, 3000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 1), 3000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 2), 2000);
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 4), 0);
  }

  @Test(dependsOnMethods = { "testSetLastUpdateTimestamp" })
  public void testRefresh() throws InterruptedException {
    DetectionConfigManager detectionConfigManager = DAORegistry.getInstance().getDetectionConfigManager();
    MetricConfigManager metricConfigManager = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetConfigDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    DatasetTriggerInfoRepo datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();

    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setDataset(TEST_DATASET_PREFIX + 3);
    metric.setName(TEST_METRIC_PREFIX + 3);
    metric.setActive(true);
    metric.setAlias("");
    long metricId = metricConfigManager.save(metric);

    DetectionConfigDTO detect2 = new DetectionConfigDTO();
    detect2.setName("test_detection_2");
    detect2.setActive(true);
    Map<String, Object> props = new HashMap<>();
    List<String> metricUrns = new ArrayList<>();
    metricUrns.add("thirdeye:metric:" + metricId);
    props.put("nestedMetricUrns", metricUrns);
    detect2.setProperties(props);
    detectionConfigManager.save(detect2);

    DatasetConfigDTO ds = new DatasetConfigDTO();
    ds.setDataset(TEST_DATASET_PREFIX + 3);
    ds.setDataSource(TEST_DATA_SOURCE);
    ds.setLastRefreshTime(3000);
    datasetConfigDAO.save(ds);

    Thread.sleep(65 * 1000); // wait for datasetTriggerInfoRepo to refresh
    Assert.assertTrue(datasetTriggerInfoRepo.isDatasetActive(TEST_DATASET_PREFIX + 3));
    Assert.assertEquals(datasetTriggerInfoRepo.getLastUpdateTimestamp(TEST_DATASET_PREFIX + 3), 3000);
  }

  @AfterMethod
  public void afterMethod() {
    DatasetTriggerInfoRepo datasetTriggerInfoRepo = DatasetTriggerInfoRepo.getInstance();
    datasetTriggerInfoRepo.close();
    testDAOProvider.cleanup();
  }
}
