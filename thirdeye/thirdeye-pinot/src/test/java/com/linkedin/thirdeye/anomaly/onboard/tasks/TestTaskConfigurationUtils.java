package com.linkedin.thirdeye.anomaly.onboard.tasks;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTaskConfigurationUtils {
  @Test
  public void testGetString(){
    Map<String, String> configMap = new HashMap<>();
    configMap.put("pattern", "UP,DOWN");
    Configuration configuration = new MapConfiguration(configMap);

    Assert.assertEquals(TaskConfigurationUtils.getString(configuration, "pattern", "", ","),
        configMap.get("pattern"));
  }
}
