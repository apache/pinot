package org.apache.pinot.thirdeye.detection.yaml;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionAlertRegistry;
import org.apache.pinot.thirdeye.detection.annotation.registry.DetectionRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.detection.yaml.YamlDetectionAlertConfigTranslator.*;


public class YamlDetectionAlertConfigTranslatorTest {
  private Map<String, Object> alertYamlConfigs;
  private YamlDetectionAlertConfigTranslator translator;

  @Test
  public void testGenerateDetectionAlertConfig() {
    List<Long> ids = Collections.singletonList(1234567L);
    DetectionAlertConfigDTO
        alertConfigDTO = this.translator.generateDetectionAlertConfig(this.alertYamlConfigs, ids, null);
    Assert.assertEquals(alertConfigDTO.getName(), alertYamlConfigs.get(PROP_SUBS_GROUP_NAME));
    Assert.assertEquals(alertConfigDTO.getApplication(), alertYamlConfigs.get("application"));
    Assert.assertEquals(alertConfigDTO.getVectorClocks().get(ids.get(0)), new Long(0L));
    Assert.assertEquals(alertConfigDTO.getCronExpression(), CRON_SCHEDULE_DEFAULT);
    Map<String, Object> properties = alertConfigDTO.getProperties();
    Assert.assertEquals(properties.get(PROP_DETECTION_CONFIG_IDS), ids);
    Assert.assertEquals(properties.get("to"), alertYamlConfigs.get("to"));
  }

  @Test
  public void testGenerateDetectionAlertConfigWithExistingVectorClocks() {
    List<Long> ids = Arrays.asList(1234567L, 7654321L);
    Map<Long, Long> vectorClocks = new HashMap<>();
    vectorClocks.put(ids.get(0), 1536173395000L);
    vectorClocks.put(7654321L, 1536173395000L);
    DetectionAlertConfigDTO
        alertConfigDTO = this.translator.generateDetectionAlertConfig(this.alertYamlConfigs, ids, vectorClocks);
    Assert.assertEquals(alertConfigDTO.getName(), alertYamlConfigs.get(PROP_SUBS_GROUP_NAME));
    Assert.assertEquals(alertConfigDTO.getApplication(), alertYamlConfigs.get("application"));
    Assert.assertEquals(alertConfigDTO.getVectorClocks().get(ids.get(0)), vectorClocks.get(ids.get(0)));
    Assert.assertEquals(alertConfigDTO.getVectorClocks().get(7654321L), vectorClocks.get(7654321L));
    Assert.assertEquals(alertConfigDTO.getCronExpression(), CRON_SCHEDULE_DEFAULT);

    Map<String, Object> properties = alertConfigDTO.getProperties();
    Assert.assertEquals(properties.get("detectionConfigIds"), ids);
    Assert.assertEquals(properties.get("to"), alertYamlConfigs.get("to"));
  }

  @Test
  public void testTranslateAlert() {
    DetectionAlertRegistry.getInstance().registerAlertScheme("EMAIL", "EmailClass");
    DetectionAlertRegistry.getInstance().registerAlertSuppressor("TIME_WINDOW", "TimeWindowClass");

    Map<String, Object> alertYamlConfigs = new HashMap<>();
    alertYamlConfigs.put(PROP_SUBS_GROUP_NAME, "test_group_name");
    alertYamlConfigs.put(PROP_APPLICATION, "test_application");
    alertYamlConfigs.put(PROP_FROM, "thirdeye@thirdeye");
    alertYamlConfigs.put(PROP_CRON, CRON_SCHEDULE_DEFAULT);
    alertYamlConfigs.put(PROP_ACTIVE, true);

    Map<String, String> refLinks = new HashMap<>();
    refLinks.put("Test Link", "test_url");
    alertYamlConfigs.put(PROP_REFERENCE_LINKS, refLinks);

    Set<Integer> detectionIds = new HashSet<>(Arrays.asList(1234, 6789));
    alertYamlConfigs.put(PROP_DETECTION_CONFIG_IDS, detectionIds);

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

    DetectionAlertConfigDTO alertConfig = YamlDetectionAlertConfigTranslator.getInstance().translate(alertYamlConfigs);

    Assert.assertTrue(alertConfig.isActive());
    Assert.assertFalse(alertConfig.isOnlyFetchLegacyAnomalies());
    Assert.assertEquals(alertConfig.getName(), "test_group_name");
    Assert.assertEquals(alertConfig.getApplication(), "test_application");
    Assert.assertEquals(alertConfig.getFrom(), "thirdeye@thirdeye");
    Assert.assertEquals(alertConfig.getCronExpression(), "0 0/5 * * * ? *");
    Assert.assertEquals(alertConfig.getSubjectType(), AlertConfigBean.SubjectType.METRICS);
    Assert.assertEquals(alertConfig.getReferenceLinks().size(), 3);
    Assert.assertEquals(alertConfig.getReferenceLinks().get("Test Link"), "test_url");

    Assert.assertEquals(alertConfig.getAlertSchemes().size(), 1);
    Assert.assertNotNull(alertConfig.getAlertSchemes().get("emailScheme"));
    Assert.assertEquals(alertConfig.getAlertSchemes().get("emailScheme").get(PROP_CLASS_NAME), "EmailClass");

    Assert.assertEquals(alertConfig.getAlertSuppressors().size(), 1);
    Map<String, Object> timeWindowSuppressor = alertConfig.getAlertSuppressors().get("timeWindowSuppressor");
    Assert.assertEquals(timeWindowSuppressor.get(PROP_CLASS_NAME), "TimeWindowClass");
    Map<String, Object> timeWindow = ((ArrayList<Map<String, Object>>) timeWindowSuppressor.get(PROP_TIME_WINDOWS)).get(0);
    Assert.assertEquals(timeWindow.get("windowStartTime"), 1542888000000L);
    Assert.assertEquals(timeWindow.get("windowEndTime"), 1543215600000L);

    Assert.assertNotNull(alertConfig.getProperties());
    Assert.assertEquals(((Set<Long>) alertConfig.getProperties().get(PROP_DETECTION_CONFIG_IDS)).size(), 2);

    Map<String, Object> recipient = (Map<String, Object>) alertConfig.getProperties().get(PROP_RECIPIENTS);
    Assert.assertEquals(recipient.size(), 2);
    Assert.assertEquals(((List<String>) recipient.get("to")).get(0), "userTo@thirdeye.com");
    Assert.assertEquals(((List<String>) recipient.get("cc")).get(0), "userCc@thirdeye.com");

    Assert.assertEquals(((Set<Long>) alertConfig.getProperties().get(PROP_DETECTION_CONFIG_IDS)).size(), 2);
  }

  @BeforeMethod
  public void setUp() {
    DetectionAlertRegistry.getInstance().registerAlertFilter("DEFAULT_ALERTER_PIPELINE", "RECIPIENTClass");
    this.alertYamlConfigs = new HashMap<>();
    alertYamlConfigs.put(PROP_SUBS_GROUP_NAME, "test_alert");
    alertYamlConfigs.put("type", "DEFAULT_ALerTeR_PipeLIne");
    Map<String, Object> recipients = new HashMap<>();
    recipients.put("to", Arrays.asList("test1", "test2"));
    alertYamlConfigs.put("recipients", recipients);
    alertYamlConfigs.put("application", "TestApplication");
    this.translator = new YamlDetectionAlertConfigTranslator();
  }
}
