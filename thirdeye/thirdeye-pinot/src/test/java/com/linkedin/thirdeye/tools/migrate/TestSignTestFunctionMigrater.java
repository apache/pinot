package com.linkedin.thirdeye.tools.migrate;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testng.annotations.Test;


public class TestSignTestFunctionMigrater {
  private Map<String, String> defaultProperties = (new SignTestFunctionMigrater()).getDefaultProperties();

  @Test
  public void TestSignTestFunctionMigraterWithTrivialProperties() {
    Properties trivialProperties = new Properties();
    trivialProperties.put("no use", "no use");
    AnomalyFunctionDTO trivialPropertiesFunction = MigraterTestUtils.getAnomalyFunctionDTO("SIGN_TEST_VANILLA",
        5, TimeUnit.MINUTES, trivialProperties);
    new SignTestFunctionMigrater().migrate(trivialPropertiesFunction);
    Properties properties = trivialPropertiesFunction.toProperties();

    Assert.assertEquals("SIGN_TEST_WRAPPER", trivialPropertiesFunction.getType());
    Assert.assertFalse(properties.containsKey("no use"));
    for (Entry<String, String> entry : defaultProperties.entrySet()) {
      Assert.assertTrue(properties.containsKey(entry.getKey()));
      Assert.assertEquals(entry.getValue(), properties.getProperty(entry.getKey()));
    }
  }

  @Test
  public void TestSignTestFunctionMigraterWithSomeProperties() {
    Properties someProperties = new Properties();
    someProperties.put("signTestPattern", "UP");
    someProperties.put("signTestBaselineLift", "1.1");
    someProperties.put("signTestWindowSize", "12");
    AnomalyFunctionDTO trivialPropertiesFunction = MigraterTestUtils.getAnomalyFunctionDTO("SIGN_TEST_VANILLA",
        1, TimeUnit.MINUTES, someProperties);
    new SignTestFunctionMigrater().migrate(trivialPropertiesFunction);
    Properties properties = trivialPropertiesFunction.toProperties();

    Assert.assertEquals("SIGN_TEST_WRAPPER", trivialPropertiesFunction.getType());
    for (Entry<String, String> entry : defaultProperties.entrySet()) {
      Assert.assertTrue(properties.containsKey(entry.getKey()));
      if (entry.getKey().equals("variables.pattern")) {
        Assert.assertEquals("UP", properties.getProperty(entry.getKey()));
      } else if (entry.getKey().equals("variables.signTestBaselineLift")) {
        Assert.assertEquals("1.1,0.9", properties.getProperty(entry.getKey()));
      } else if (entry.getKey().equals("variables.signTestWindowSize")) {
        Assert.assertEquals("12", properties.getProperty(entry.getKey()));
      } else {
        Assert.assertEquals(String.format("Assert Error on property key %s", entry.getKey()),
            entry.getValue(), properties.getProperty(entry.getKey()));
      }
    }
  }
}
