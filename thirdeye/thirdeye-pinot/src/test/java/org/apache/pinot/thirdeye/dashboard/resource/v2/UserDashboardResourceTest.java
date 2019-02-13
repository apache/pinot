package org.apache.pinot.thirdeye.dashboard.resource.v2;

import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.api.user.dashboard.UserDashboardResource;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.AnomalySummary;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
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
  AnomalyFunctionManager functionDAO;
  AlertConfigManager alertDAO;
  DetectionConfigManager detectionDAO;
  DetectionAlertConfigManager detectionAlertDAO;
  MetricConfigManager metricDAO;
  DatasetConfigManager datasetDAO;

  List<Long> anomalyIds;
  List<Long> functionIds;
  List<Long> alertIds;

  @BeforeMethod
  public void beforeMethod() {
    this.testBase = DAOTestBase.getInstance();

    // metrics
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();

    // datasets
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();

    // functions
    this.functionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
    this.functionIds = new ArrayList<>();
    this.functionIds.add(this.functionDAO.save(makeFunction("myFunctionA")));
    this.functionIds.add(this.functionDAO.save(makeFunction("myFunctionB")));
    this.functionIds.add(this.functionDAO.save(makeFunction("myFunctionC")));

    for (Long id : this.functionIds) {
      Assert.assertNotNull(id);
    }

    // anomalies
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.anomalyIds = new ArrayList<>();
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(100, 500, this.functionIds.get(0), "test_metric", "test_dataset"))); // func A
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(800, 1200, this.functionIds.get(0), "test_metric", "test_dataset"))); // func A
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1500, this.functionIds.get(1), "test_metric", "test_dataset"))); // func B
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1600, this.functionIds.get(2), "test_metric_2", "test_dataset"))); // func C

    for (Long id : this.anomalyIds) {
      Assert.assertNotNull(id);
    }

    // alerts
    this.alertDAO = DAORegistry.getInstance().getAlertConfigDAO();
    this.alertIds = new ArrayList<>();
    this.alertIds.add(this.alertDAO.save(makeAlert("myAlertA", "myApplicationA", Arrays.asList(this.functionIds.get(0), this.functionIds.get(1))))); // funcA, funcB
    this.alertIds.add(this.alertDAO.save(makeAlert("myAlertB", "myApplicationB", Collections.singletonList(this.functionIds.get(2))))); // none

    for (Long id : this.alertIds) {
      Assert.assertNotNull(id);
    }

    // new framework detectors
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();

    // new framework alerts
    this.detectionAlertDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();

    // resource
    this.resource = new UserDashboardResource(this.anomalyDAO, this.functionDAO, this.metricDAO, this.datasetDAO, this.alertDAO, this.detectionDAO, this.detectionAlertDAO);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    if (this.testBase != null) {
      this.testBase.cleanup();
    }
  }

  @Test
  public void testAnomaliesByApplication() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, "myApplicationA", null, null, null, false, null);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2)));
  }

  @Test
  public void testAnomaliesByApplicationInvalid() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, "Invalid", null, null, null, false, null);
    Assert.assertEquals(anomalies.size(), 0);
  }

  @Test
  public void testAnomaliesByGroup() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, "myAlertB", null, null, false, null);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(3)));
  }

  @Test
  public void testAnomaliesByGroupInvalid() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, "Invalid", null, null, false, null);
    Assert.assertEquals(anomalies.size(), 0);
  }

  @Test
  public void testAnomaliesLimit() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, "myApplicationA", null, null, null, false, 1);
    Assert.assertEquals(anomalies.size(), 1);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1)));
  }

  @Test
  public void testAnomaliesByMetric() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, null, "test_metric", "test_dataset", false, null);
    Assert.assertEquals(anomalies.size(), 2);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2)));
  }

  @Test
  public void testAnomaliesByDataset() throws Exception {
    List<AnomalySummary> anomalies = this.resource.queryAnomalies(1000L, null, null, null, null, null, "test_dataset", false, null);
    Assert.assertEquals(anomalies.size(), 3);
    Assert.assertEquals(extractIds(anomalies), makeSet(this.anomalyIds.get(1), this.anomalyIds.get(2), this.anomalyIds.get(3)));
  }

  private MergedAnomalyResultDTO makeAnomaly(long start, long end, Long functionId, String metric, String dataset) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);
    anomaly.setMetric(metric);
    anomaly.setCollection(dataset);
    anomaly.setFunctionId(functionId);
    anomaly.setNotified(true);
    return anomaly;
  }

  private AnomalyFunctionDTO makeFunction(String name) {
    AnomalyFunctionDTO function = new AnomalyFunctionDTO();
    function.setFunctionName(name);
    return function;
  }

  private AlertConfigDTO makeAlert(String name, String application, List<Long> functionIds) {
    AlertConfigBean.EmailConfig emailConfig = new AlertConfigBean.EmailConfig();
    emailConfig.setFunctionIds(functionIds);

    AlertConfigDTO alert = new AlertConfigDTO();
    alert.setName(name);
    alert.setApplication(application);
    alert.setEmailConfig(emailConfig);
    return alert;
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
