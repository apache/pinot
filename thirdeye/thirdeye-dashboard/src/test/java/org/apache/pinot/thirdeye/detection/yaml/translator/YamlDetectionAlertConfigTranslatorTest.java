package org.apache.pinot.thirdeye.detection.yaml.translator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.validators.SubscriptionConfigValidator;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

import static org.apache.pinot.thirdeye.detection.yaml.translator.SubscriptionConfigTranslator.*;
import static org.mockito.Mockito.*;


public class YamlDetectionAlertConfigTranslatorTest {

  private DAOTestBase testDAOProvider;
  private DetectionConfigManager detectionConfigManager;

  @Test
  public void testTranslateAlert() throws Exception {
    DetectionAlertRegistry.getInstance().registerAlertScheme("EMAIL", "EmailClass");
    DetectionAlertRegistry.getInstance().registerAlertSuppressor("TIME_WINDOW", "TimeWindowClass");

    Map<String, Object> alertYamlConfigs = new HashMap<>();
    alertYamlConfigs.put(PROP_SUBS_GROUP_NAME, "test_group_name");
    alertYamlConfigs.put(PROP_APPLICATION, "test_application");
    alertYamlConfigs.put(PROP_FROM, "thirdeye@thirdeye");
    alertYamlConfigs.put(PROP_TYPE, "DEFAULT_ALERTER_PIPELINE");
    alertYamlConfigs.put(PROP_CRON, CRON_SCHEDULE_DEFAULT);
    alertYamlConfigs.put(PROP_ACTIVE, true);
    alertYamlConfigs.put(PROP_DETECTION_NAMES, Collections.singletonList("test_pipeline_1"));


    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("Test Link", "test_url");
    alertYamlConfigs.put(PROP_REFERENCE_LINKS, refLinks);

    Map<String, Object> alertSchemes = new HashMap<>();
    alertSchemes.put(PROP_TYPE, "EMAIL");
    List<Map<String, Object>> alertSchemesHolder = new ArrayList<>();
    alertSchemesHolder.add(alertSchemes);
    alertYamlConfigs.put(PROP_ALERT_SCHEMES, alertSchemesHolder);

    Map<String, Object> alertSuppressors = new HashMap<>();
    alertSuppressors.put(PROP_TYPE, "TIME_WINDOW");
    Map<String, Object> suppressorParams = new HashMap<>();
    suppressorParams.put("windowStartTime", 1542888000000L);
    suppressorParams.put("windowEndTime", 1543215600000L);
    alertSuppressors.put(PROP_PARAM, suppressorParams);
    List<Map<String, Object>> alertSuppressorsHolder = new ArrayList<>();
    alertSuppressorsHolder.add(alertSuppressors);
    alertYamlConfigs.put(PROP_ALERT_SUPPRESSORS, alertSuppressorsHolder);

    Map<String, List<String>> recipients = new HashMap<>();
    recipients.put("to", new ArrayList<>(Collections.singleton("userTo@thirdeye.com")));
    recipients.put("cc", new ArrayList<>(Collections.singleton("userCc@thirdeye.com")));
    alertYamlConfigs.put(PROP_RECIPIENTS, recipients);

    SubscriptionConfigValidator validateMocker = mock(SubscriptionConfigValidator.class);
    doNothing().when(validateMocker).staticValidation(new Yaml().dump(alertYamlConfigs));

    String yamlConfig = new Yaml().dump(alertYamlConfigs);
    DetectionAlertConfigDTO alertConfig = new SubscriptionConfigTranslator(yamlConfig, validateMocker).translate();

    Assert.assertTrue(alertConfig.isActive());
    Assert.assertEquals(alertConfig.getName(), "test_group_name");
    Assert.assertEquals(alertConfig.getApplication(), "test_application");
    Assert.assertEquals(alertConfig.getFrom(), "thirdeye@thirdeye");
    Assert.assertEquals(alertConfig.getCronExpression(), "0 0/5 * * * ? *");
    Assert.assertEquals(alertConfig.getSubjectType(), AlertConfigBean.SubjectType.METRICS);
    Assert.assertEquals(alertConfig.getReferenceLinks().size(), 1);
    Assert.assertEquals(alertConfig.getReferenceLinks().get("Test Link"), "test_url");

    Assert.assertEquals(alertConfig.getAlertSchemes().size(), 1);
    Assert.assertNotNull(alertConfig.getAlertSchemes().get("emailScheme"));
    Assert.assertEquals(ConfigUtils.getMap(alertConfig.getAlertSchemes().get("emailScheme")).get(PROP_CLASS_NAME),
        "EmailClass");

    Assert.assertEquals(alertConfig.getAlertSuppressors().size(), 1);
    Map<String, Object> timeWindowSuppressor = ConfigUtils.getMap(alertConfig.getAlertSuppressors().get("timeWindowSuppressor"));
    Assert.assertEquals(timeWindowSuppressor.get(PROP_CLASS_NAME), "TimeWindowClass");
    Map<String, Object> timeWindow = ((ArrayList<Map<String, Object>>) timeWindowSuppressor.get(PROP_TIME_WINDOWS)).get(0);
    Assert.assertEquals(timeWindow.get("windowStartTime"), 1542888000000L);
    Assert.assertEquals(timeWindow.get("windowEndTime"), 1543215600000L);

    Assert.assertNotNull(alertConfig.getProperties());
    Assert.assertEquals(ConfigUtils.getLongs(alertConfig.getProperties().get(PROP_DETECTION_CONFIG_IDS)).size(), 1);

    Map<String, Object> recipient = (Map<String, Object>) alertConfig.getProperties().get(PROP_RECIPIENTS);
    Assert.assertEquals(recipient.size(), 2);
    Assert.assertEquals(((List<String>) recipient.get("to")).get(0), "userTo@thirdeye.com");
    Assert.assertEquals(((List<String>) recipient.get("cc")).get(0), "userCc@thirdeye.com");

  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    detectionConfigManager = daoRegistry.getDetectionConfigManager();
    DetectionConfigDTO detectionConfigDTO = new DetectionConfigDTO();
    detectionConfigDTO.setName("test_pipeline_1");
    detectionConfigManager.save(detectionConfigDTO);

    DetectionAlertRegistry.getInstance().registerAlertFilter("DEFAULT_ALERTER_PIPELINE", "RECIPIENTClass");
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    testDAOProvider.cleanup();
  }

}
