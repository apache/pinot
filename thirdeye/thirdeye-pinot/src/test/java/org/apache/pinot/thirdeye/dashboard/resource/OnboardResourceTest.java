package com.linkedin.thirdeye.dashboard.resource;

import com.linkedin.thirdeye.dashboard.ThirdEyeDashboardConfiguration;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class OnboardResourceTest {

  private DAOTestBase testDAOProvider;
  private DAORegistry daoRegistry;

  @BeforeMethod
  public void beforeClass() {
    // Prepare database
    testDAOProvider = DAOTestBase.getInstance();
    daoRegistry = DAORegistry.getInstance();

    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset("test_dataset");
    datasetConfigDTO.setOwners(new HashSet<String>(Arrays.asList("user_1", "user_2")));
    daoRegistry.getDatasetConfigDAO().save(datasetConfigDTO);

    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    metricConfigDTO.setName("test_metric");
    metricConfigDTO.setDataset("test_dataset");
    metricConfigDTO.setAlias("test_alias");
    metricConfigDTO.setTags(new HashSet<String>(Arrays.asList("test_tag", "random_tag")));
    daoRegistry.getMetricConfigDAO().save(metricConfigDTO);
  }

  @AfterMethod(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testBulkOnboard() throws Exception {
    ThirdEyeDashboardConfiguration config = new ThirdEyeDashboardConfiguration();
    config.setFailureFromAddress("thirdeye@test");
    OnboardResource onboardResource = new OnboardResource(config);
    Response response = onboardResource.bulkOnboardAlert("test_tag", null, "test_prefix_", true, null, null, null, null);

    // Check if the alert group is automatically created
    List<AlertConfigDTO> alertConfigDTOList = this.daoRegistry.getAlertConfigDAO().findAll();
    Assert.assertEquals(alertConfigDTOList.size(), 1);
    Assert.assertEquals(alertConfigDTOList.get(0).getName(), "auto_onboard_dataset_testDataset_alert");
    Assert.assertEquals(alertConfigDTOList.get(0).getApplication(), "others");
    Assert.assertEquals(alertConfigDTOList.get(0).getCronExpression(), "0 0/5 * * * ? *");

    // Check if anomaly function is created
    List<AnomalyFunctionDTO> anomalyFunctionDTOList = this.daoRegistry.getAnomalyFunctionDAO().findAll();
    Assert.assertEquals(anomalyFunctionDTOList.size(), 1);
    Assert.assertEquals(anomalyFunctionDTOList.get(0).getFunctionName(), "test_prefix_testMetric_testDataset");

    // Check if alert group has subscribed to the function
    Assert.assertEquals(alertConfigDTOList.get(0).getEmailConfig().getFunctionIds().size(), 1);
    Assert.assertEquals(alertConfigDTOList.get(0).getEmailConfig().getFunctionIds().get(0), anomalyFunctionDTOList.get(0).getId());

    // Verify response
    Assert.assertEquals(response.getStatus(), 200);
    Assert.assertEquals(((Map<String, String>)response.getEntity()).get("metric test_metric"),
        "success! onboarded and added function id " + anomalyFunctionDTOList.get(0).getId()
            + " to subscription alertGroup = auto_onboard_dataset_testDataset_alert");
    Assert.assertEquals(((Map<String, String>)response.getEntity()).get("message"),
        "successfully onboarded 1 metrics with function ids [" + anomalyFunctionDTOList.get(0).getId() + "]");
  }

  @Test
  public void testBulkOnboardWithInvalidAlertGroup() throws Exception {
    OnboardResource onboardResource = new OnboardResource(null);
    Response response = onboardResource.bulkOnboardAlert("test_tag", null, null, true, "group_name", null, null, null);
    Assert.assertEquals(response.getStatus(), 400);
    Map<String, String> responseMap = (Map<String, String>) response.getEntity();
    Assert.assertEquals(responseMap.get("message"), "cannot find an alert group with name group_name.");
  }

  @Test
  public void testSoftBulkOnboardWithNoAlertGroup() throws Exception {
    OnboardResource onboardResource = new OnboardResource(null);
    Response response = onboardResource.bulkOnboardAlert("test_tag", null, null, false, null, null, null, null);
    Assert.assertEquals(response.getStatus(), 400);
    Map<String, String> responseMap = (Map<String, String>) response.getEntity();
    Assert.assertEquals(responseMap.get("message"), "cannot find an alert group for metric test_metric.");
  }
}
