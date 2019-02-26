package org.apache.pinot.thirdeye.api.application;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.datalayer.bao.AlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApplicationResourceTest {
  DAOTestBase testBase;
  ApplicationResource resource;

  ApplicationManager appDAO;
  MergedAnomalyResultManager anomalyDAO;
  DetectionConfigManager detectionDAO;
  DetectionAlertConfigManager detectionAlertDAO;
  MetricConfigManager metricDAO;
  DatasetConfigManager datasetDAO;
  AnomalyFunctionManager functionDAO;

  List<Long> anomalyIds;
  List<Long> functionIds;
  List<Long> alertIds;

  @BeforeMethod
  public void beforeMethod() {
    this.testBase = DAOTestBase.getInstance();

    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.functionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();

    // Application
    this.appDAO = DAORegistry.getInstance().getApplicationDAO();
    this.appDAO.save(makeApplication("myApplicationA"));
    this.appDAO.save(makeApplication("myApplicationB"));

    // functions
    this.functionDAO = DAORegistry.getInstance().getAnomalyFunctionDAO();
    this.functionIds = new ArrayList<>();
    this.functionIds.add(this.functionDAO.save(makeDetection("myFunctionA")));
    this.functionIds.add(this.functionDAO.save(makeDetection("myFunctionB")));
    this.functionIds.add(this.functionDAO.save(makeDetection("myFunctionC")));
    for (Long id : this.functionIds) {
      Assert.assertNotNull(id);
    }

    // anomalies
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.anomalyIds = new ArrayList<>();
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(100, 500, this.functionIds.get(0), "test_metric", "test_dataset"))); // func A
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(800, 1200, this.functionIds.get(0), "test_metric", "test_dataset"))); // func A
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1500, this.functionIds.get(1), "test_metric", "test_dataset"))); // func B
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1600, this.functionIds.get(2), "test_metric", "test_dataset"))); // func C
    this.anomalyIds.add(this.anomalyDAO.save(makeAnomaly(300, 1600, this.functionIds.get(2), "test_metric_2", "test_dataset"))); // func C

    for (Long id : this.anomalyIds) {
      Assert.assertNotNull(id);
    }

    // alerts
    this.detectionAlertDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();
    this.alertIds = new ArrayList<>();
    this.alertIds.add(this.detectionAlertDAO.save(makeAlert("myGroupA", "myApplicationA", Arrays.asList(this.functionIds.get(0), this.functionIds.get(1))))); // funcA, funcB
    this.alertIds.add(this.detectionAlertDAO.save(makeAlert("myGroupB", "myApplicationB", Collections.singletonList(this.functionIds.get(2))))); // none

    for (Long id : this.alertIds) {
      Assert.assertNotNull(id);
    }

    // new framework detectors
    this.detectionDAO = DAORegistry.getInstance().getDetectionConfigManager();

    // new framework alerts
    this.detectionAlertDAO = DAORegistry.getInstance().getDetectionAlertConfigManager();

    // resource
    this.resource = new ApplicationResource(this.appDAO, this.anomalyDAO, this.detectionDAO, this.detectionAlertDAO);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethod() {
    if (this.testBase != null) {
      this.testBase.cleanup();
    }
  }

  @Test
  public void testDeleteApplication() throws Exception {
    Response response = this.resource.deleteApplication("myApplicationA");
    Assert.assertEquals(response.getStatus(), 200);

    // Expected:
    // Application myApplicationA should be removed
    // Subscription Group myGroupA should be removed
    // Anomaly Functions myFunctionA and myFunctionB should be removed
    // Anomalies detected by myFunctionA and myFunctionA should be removed
    Assert.assertEquals(this.appDAO.findAll().size(), 1);
    Assert.assertEquals(this.detectionAlertDAO.findAll().size(), 1);
    Assert.assertEquals(this.functionDAO.findAll().size(), 1);
    Assert.assertEquals(this.anomalyDAO.findAll().size(), 2);
  }

  private ApplicationDTO makeApplication(String applicationName) {
    ApplicationDTO appDTO = new ApplicationDTO();
    appDTO.setApplication(applicationName);
    appDTO.setRecipients("test@test.com");
    return appDTO;
  }

  private AnomalyFunctionDTO makeDetection(String name) {
    AnomalyFunctionDTO function = new AnomalyFunctionDTO();
    function.setFunctionName(name);
    return function;
  }

  private MergedAnomalyResultDTO makeAnomaly(long start, long end, Long functionId, String metric, String dataset) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(start);
    anomaly.setEndTime(end);
    anomaly.setMetric(metric);
    anomaly.setCollection(dataset);
    anomaly.setDetectionConfigId(functionId);
    return anomaly;
  }

  private DetectionAlertConfigDTO makeAlert(String name, String application, List<Long> functionIds) {
    DetectionAlertConfigDTO subsGroup = new DetectionAlertConfigDTO();
    subsGroup.setName(name);
    subsGroup.setApplication(application);
    Map<String, Object> properties = new HashMap<>();
    properties.put("detectionConfigIds", functionIds);
    subsGroup.setProperties(properties);
    return subsGroup;
  }
}
