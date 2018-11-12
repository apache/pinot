package com.linkedin.thirdeye.detection.yaml;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter;
import com.linkedin.thirdeye.detection.annotation.DetectionRegistry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class YamlDetectionAlertConfigTranslatorTest {
  private Map<String, Object> alertYamlConfigs;
  private YamlDetectionAlertConfigTranslator translator;

  @Test
  public void testGenerateDetectionAlertConfig() {
    List<Long> ids = Collections.singletonList(1234567L);
    DetectionAlertConfigDTO
        alertConfigDTO = this.translator.generateDetectionAlertConfig(this.alertYamlConfigs, ids, null);
    Assert.assertEquals(alertConfigDTO.getName(), alertYamlConfigs.get("name"));
    Assert.assertEquals(alertConfigDTO.getApplication(), alertYamlConfigs.get("application"));
    Assert.assertEquals(alertConfigDTO.getVectorClocks().get(ids.get(0)), new Long(0L));
    Assert.assertEquals(alertConfigDTO.getCronExpression(), "0 21 * * * ? *");
    Map<String, Object> properties = alertConfigDTO.getProperties();
    Assert.assertEquals(properties.get("detectionConfigIds"), ids);
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
    Assert.assertEquals(alertConfigDTO.getName(), alertYamlConfigs.get("name"));
    Assert.assertEquals(alertConfigDTO.getApplication(), alertYamlConfigs.get("application"));
    Assert.assertEquals(alertConfigDTO.getVectorClocks().get(ids.get(0)), vectorClocks.get(ids.get(0)));
    Assert.assertEquals(alertConfigDTO.getVectorClocks().get(7654321L), vectorClocks.get(7654321L));
    Assert.assertEquals(alertConfigDTO.getCronExpression(), "0 21 * * * ? *");

    Map<String, Object> properties = alertConfigDTO.getProperties();
    Assert.assertEquals(properties.get("detectionConfigIds"), ids);
    Assert.assertEquals(properties.get("to"), alertYamlConfigs.get("to"));
  }

  @BeforeMethod
  public void setUp() {
    DetectionRegistry.registerComponent("testclassname", "TO_ALL_RECIPIENTS");
    this.alertYamlConfigs = new HashMap<>();
    alertYamlConfigs.put("name", "test_alert");
    alertYamlConfigs.put("type", "TO_ALL_RECIPIEnts");
    alertYamlConfigs.put("to", Arrays.asList("test1", "test2"));
    alertYamlConfigs.put("application", "TestApplication");
    this.translator = new YamlDetectionAlertConfigTranslator();
  }
}
