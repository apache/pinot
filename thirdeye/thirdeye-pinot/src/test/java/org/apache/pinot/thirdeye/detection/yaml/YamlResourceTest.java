package org.apache.pinot.thirdeye.detection.yaml;

import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class YamlResourceTest {

  private DAOTestBase testDAOProvider;
  private YamlResource yamlResource;
  private DAORegistry daoRegistry;

  @BeforeClass
  public void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    this.yamlResource = new YamlResource();
    this.daoRegistry = DAORegistry.getInstance();
    DetectionConfigManager detectionDAO = this.daoRegistry.getDetectionConfigManager();
    DetectionConfigDTO config = new DetectionConfigDTO();
    config.setName("test_detection_1");
    detectionDAO.save(config);

    DetectionAlertRegistry.getInstance().registerAlertScheme("EMAIL", "EmailClass");
    DetectionAlertRegistry.getInstance().registerAlertScheme("IRIS", "IrisClass");
    DetectionAlertRegistry.getInstance().registerAlertSuppressor("TIME_WINDOW", "TimeWindowClass");
    DetectionAlertRegistry.getInstance().registerAlertFilter("DIMENSIONAL_ALERTER_PIPELINE", "DimClass");
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test(priority=1)
  public void testCreateDetectionAlertConfig() throws IOException {
    Map<String, String> responseMessage = new HashMap<>();
    DetectionAlertConfigDTO alertDTO;

    String blankYaml = "";
    alertDTO = this.yamlResource.createDetectionAlertConfig(blankYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "The config file cannot be blank.");

    String inValidYaml = "application:test:application";
    alertDTO = this.yamlResource.createDetectionAlertConfig(inValidYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "There was an error parsing the yaml file. Check for syntax issues.");

    String noSubscriptGroupYaml = "application: test_application";
    alertDTO = this.yamlResource.createDetectionAlertConfig(noSubscriptGroupYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "Subscription group name field cannot be left empty.");

    String appFieldMissingYaml = IOUtils.toString(this.getClass().getResourceAsStream("alertconfig/alert-config-1.yaml"));
    alertDTO = this.yamlResource.createDetectionAlertConfig(appFieldMissingYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "Application field cannot be left empty");

    String appMissingYaml = IOUtils.toString(this.getClass().getResourceAsStream("alertconfig/alert-config-2.yaml"));
    alertDTO = this.yamlResource.createDetectionAlertConfig(appMissingYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "Application name doesn't exist in our registry."
        + " Please use an existing application name. You may search for registered applications from the ThirdEye"
        + " dashboard or reach out to ask_thirdeye if you wish to setup a new application.");

    DetectionAlertConfigDTO oldAlertDTO = new DetectionAlertConfigDTO();
    oldAlertDTO.setName("test_group");
    daoRegistry.getDetectionAlertConfigManager().save(oldAlertDTO);

    String groupExists = IOUtils.toString(this.getClass().getResourceAsStream("alertconfig/alert-config-3.yaml"));
    alertDTO = this.yamlResource.createDetectionAlertConfig(groupExists, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "Subscription group name is already taken. Please use a different name.");

    ApplicationDTO request = new ApplicationDTO();
    request.setApplication("test_application");
    request.setRecipients("abc@abc.in");
    daoRegistry.getApplicationDAO().save(request);

    String validYaml = IOUtils.toString(this.getClass().getResourceAsStream("alertconfig/alert-config-4.yaml"));
    alertDTO = this.yamlResource.createDetectionAlertConfig(validYaml, responseMessage);
    Assert.assertNotNull(alertDTO);
    Assert.assertEquals(alertDTO.getName(), "Subscription Group Name");
  }

  @Test(priority=2)
  public void testUpdateDetectionAlertConfig() throws IOException {
    DetectionAlertConfigDTO oldAlertDTO = new DetectionAlertConfigDTO();
    oldAlertDTO.setName("Subscription Group Name");
    oldAlertDTO.setApplication("Random Application");
    daoRegistry.getDetectionAlertConfigManager().save(oldAlertDTO);

    Map<String, String> responseMessage = new HashMap<>();
    DetectionAlertConfigDTO alertDTO;

    alertDTO = this.yamlResource.updateDetectionAlertConfig(null, "", responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "Cannot find subscription group");

    String blankYaml = "";
    alertDTO = this.yamlResource.updateDetectionAlertConfig(oldAlertDTO, blankYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "The config file cannot be blank.");

    String inValidYaml = "application:test:application";
    alertDTO = this.yamlResource.updateDetectionAlertConfig(oldAlertDTO, inValidYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "There was an error parsing the yaml file. Check for syntax issues.");

    String noSubscriptGroupYaml = "application: test_application";
    alertDTO = this.yamlResource.updateDetectionAlertConfig(oldAlertDTO, noSubscriptGroupYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "Subscription group name field cannot be left empty.");

    String appFieldMissingYaml = IOUtils.toString(this.getClass().getResourceAsStream("alertconfig/alert-config-1.yaml"));
    alertDTO = this.yamlResource.updateDetectionAlertConfig(oldAlertDTO, appFieldMissingYaml, responseMessage);
    Assert.assertNull(alertDTO);
    Assert.assertEquals(responseMessage.get("message"), "Application field cannot be left empty");

    ApplicationDTO request = new ApplicationDTO();
    request.setApplication("test_application");
    request.setRecipients("abc@abc.in");
    daoRegistry.getApplicationDAO().save(request);

    String validYaml = IOUtils.toString(this.getClass().getResourceAsStream("alertconfig/alert-config-3.yaml"));
    alertDTO = this.yamlResource.updateDetectionAlertConfig(oldAlertDTO, validYaml, responseMessage);
    Assert.assertNotNull(alertDTO);
    Assert.assertEquals(alertDTO.getName(), "test_group");
    Assert.assertEquals(alertDTO.getApplication(), "test_application");
  }
}

