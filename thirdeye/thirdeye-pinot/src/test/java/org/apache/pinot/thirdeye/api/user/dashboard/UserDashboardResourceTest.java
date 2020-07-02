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

package org.apache.pinot.thirdeye.api.user.dashboard;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.AnomalySummary;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class UserDashboardResourceTest {
  DAOTestBase testBase;
  UserDashboardResource resource;

  MergedAnomalyResultManager anomalyDAO;
  DetectionConfigManager detectionDAO;
  DetectionAlertConfigManager detectionAlertDAO;
  MetricConfigManager metricDAO;
  DatasetConfigManager datasetDAO;

  List<Long> anomalyIds;
  List<Long> detectionIds;
  List<Long> alertIds;

  @BeforeMethod
  public void beforeMethod() {
    this.testBase = DAOTestBase.getInstance();

    // metrics
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    long metricid = this.metricDAO.save(makeMetric("test_metric", "test_dataset"));
    this.metricDAO.save(makeMetric("test_metric_2", "test_dataset"));

    // datasets
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.datasetDAO.save(makeDataset("test_dataset"));

    // detections
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.detectionIds = new ArrayList<>();
    this.detectionIds.add(this.detectionDAO.save(makeDetection("myDetectionA")));
    this.detectionIds.add(this.detectionDAO.save(makeDetection("myDetectionB")));
    this.detectionIds.add(this.detectionDAO.save(makeDetection("myDetectionC")));

    for (Long id : this.detectionIds) {
      Assert.assertNotNull(id);
    }

    // anomalies
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.anomalyIds = new ArrayList<>();
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(100, 500, this.detectionIds.get(0), "test_metric", "test_dataset"))); // myDetectionA
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(800, 1200, this.detectionIds.get(0), "test_metric", "test_dataset"))); // myDetectionA
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1500, this.detectionIds.get(1), "test_metric", "test_dataset"))); // myDetectionB
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1600, this.detectionIds.get(2), "test_metric", "test_dataset"))); // myDetectionC
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1600, this.detectionIds.get(2), "test_metric_2", "test_dataset"))); // myDetectionC

    for (Long id : this.anomalyIds) {
      Assert.assertNotNull(id);
    }

    // alerts
    this.detectionAlertDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.alertIds = new ArrayList<>();
    this.alertIds.add(this.detectionAlertDAO.save(makeAlert("myAlertA", "myApplicationA", Arrays.asList(this.detectionIds.get(0), this.detectionIds.get(1))))); // myDetectionA, myDetectionB
    this.alertIds.add(this.detectionAlertDAO.save(makeAlert("myAlertB", "myApplicationB", Collections.singletonList(this.detectionIds.get(2))))); // none

    for (Long id : this.alertIds) {
      Assert.assertNotNull(id);
    }

    // new framework detectors
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();

    // resource
    this.resource = new UserDashboardResource(this.anomalyDAO, this.metricDAO, this.datasetDAO, this.detectionDAO, this.detectionAlertDAO);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    if (this.testBase != null) {
      this.testBase.cleanup();
    }
  }

  @Test
  public void testAnomaliesByApplication() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, "myApplicationA", null, null, null, null, false, null);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2)));
  }

  @Test
  public void testAnomaliesByApplicationInvalid() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, "Invalid", null, null, null, null, false, null);
    Assert.assertEquals(anomalies.size(), 0);
  }

  @Test
  public void testAnomaliesByGroup() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, "myAlertB", null, null, null, false, null);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(3), this.anomalyIds.get(4)));
  }

  @Test
  public void testAnomaliesByGroupInvalid() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, "Invalid", null, null, null, false, null);
    Assert.assertEquals(anomalies.size(), 0);
  }

  @Test
  public void testAnomaliesLimit() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, "myApplicationA", null, null, null, null, false, 1);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1)));
  }

  @Test
  public void testAnomaliesByMetric() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, "test_metric", "test_dataset", null, false, null);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2), this.anomalyIds.get(3)));
  }

  @Test
  public void testAnomaliesByDataset() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, null, "test_dataset", null, false, null);
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2), this.anomalyIds.get(3), this.anomalyIds.get(4)));
  }

  @Test
  public void testAnomaliesByMetricDatasetPairs() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, null, null,
        Collections.singletonList(new UserDashboardResource.MetricDatasetPair("test_dataset", "test_metric")), false, null);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2), this.anomalyIds.get(3)));
  }

  @Test
  public void testAnomaliesByMultipleMetricDatasetPairs() throws Exception {
    List<UserDashboardResource.MetricDatasetPair> metricPairs = new ArrayList<>();
    metricPairs.add(new UserDashboardResource.MetricDatasetPair("test_dataset", "test_metric"));
    metricPairs.add(new UserDashboardResource.MetricDatasetPair("test_dataset", "test_metric_2"));
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, null, null, metricPairs, false, null);
    Assert.assertEquals(anomalies.size(), 4);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2), this.anomalyIds.get(3), this.anomalyIds.get(4)));
  }

  @Test
  public void testAnomaliesByApplicationAndMetric() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, "myApplicationB", null, "test_metric_2", "test_dataset", null, false, null);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(4)));
  }

  @Test
  public void testAnomaliesByApplicationAndMetricEmpty() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, "myApplicationA", null, "test_metric_2", "test_dataset", null, false, null);
    Assert.assertEquals(anomalies.size(), 0);
  }

  private MergedAnomalyResultDTO makeAnomaly(long start, long end, Long detectionId, String metric, String dataset) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);
    anomaly.setMetric(metric);
    anomaly.setCollection(dataset);
    anomaly.setDetectionConfigId(detectionId);
    anomaly.setNotified(true);
    return anomaly;
  }

  private DetectionConfigDTO makeDetection(String name) {
    DetectionConfigDTO detection = new DetectionConfigDTO();
    detection.setName(name);
    return detection;
  }

  private MetricConfigDTO makeMetric(String metricName, String datasetName) {
    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setName(metricName);
    metric.setDataset(datasetName);
    metric.setAlias(metricName + "::" + datasetName);
    return metric;
  }

  private DatasetConfigDTO makeDataset(String name) {
    DatasetConfigDTO dataset = new DatasetConfigDTO();
    dataset.setDataset(name);
    return dataset;
  }

  private DetectionAlertConfigDTO makeAlert(String name, String application, List<Long> detectionIds) {
    DetectionAlertConfigDTO notification = new DetectionAlertConfigDTO();
    notification.setName(name);
    notification.setApplication(application);

    Map<Long, Long> vectorClocks = new HashMap<>();
    detectionIds.forEach(id -> vectorClocks.put(id, 0l));
    notification.setVectorClocks(vectorClocks);
    return notification;
  }

  private Set<Long> extractIds(Collection<AnomalySummary> anomalies) {
    Set<Long> ids = new HashSet<>();
    for (AnomalySummary anomaly : anomalies) {
      ids.add(anomaly.getId());
    }
    return ids;
  }

  private Set<Long> makeSet(Long... ids) {
    return new HashSet<>(Arrays.asList(ids));
  }
}
