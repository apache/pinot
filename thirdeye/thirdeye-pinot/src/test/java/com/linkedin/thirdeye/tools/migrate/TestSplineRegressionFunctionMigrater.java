package com.linkedin.thirdeye.tools.migrate;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testng.annotations.Test;


public class TestSplineRegressionFunctionMigrater {
  private Map<String, String> defaultProperties = (new SplineRegressionFunctionMigrater()).getDefaultProperties();

  @Test
  public void TestRegressionGaussianScanFunctionMigraterWithTrivialProperties() {
    Properties trivialProperties = new Properties();
    trivialProperties.put("no use", "no use");
    AnomalyFunctionDTO trivialPropertiesFunction = MigraterTestUtils.getAnomalyFunctionDTO("SPLINE_REGRESSION_VANILLA",
        1, TimeUnit.DAYS, trivialProperties);
    new SplineRegressionFunctionMigrater().migrate(trivialPropertiesFunction);
    Properties properties = trivialPropertiesFunction.toProperties();

    Assert.assertEquals("SPLINE_REGRESSION_WRAPPER", trivialPropertiesFunction.getType());
    Assert.assertFalse(properties.containsKey("no use"));
    for (Entry<String, String> entry : defaultProperties.entrySet()) {
      Assert.assertTrue(properties.containsKey(entry.getKey()));
      Assert.assertEquals(entry.getValue(), properties.getProperty(entry.getKey()));
    }
  }

  @Test
  public void TestRegressionGaussianScanFunctionMigraterWithSomeProperties() {
    Properties someProperties = new Properties();
    someProperties.put("useRobustBaseline", "false");
    someProperties.put("applyLogTransform", "true");
    someProperties.put("weeklyEffectModeled", "false");
    AnomalyFunctionDTO somePropertiesFunction = MigraterTestUtils.getAnomalyFunctionDTO("SPLINE_REGRESSION_VANILLA",
        1, TimeUnit.DAYS, someProperties);
    new SplineRegressionFunctionMigrater().migrate(somePropertiesFunction);
    Properties properties = somePropertiesFunction.toProperties();

    Assert.assertEquals("SPLINE_REGRESSION_WRAPPER", somePropertiesFunction.getType());
    for (Entry<String, String> entry : defaultProperties.entrySet()) {
      if (entry.getKey().equals("module." + MigraterTestUtils.TRAINING)) {
        Assert.assertEquals("parametric.SplineRegressionTrainingModule", properties.getProperty(entry.getKey()));
      } else if (entry.getKey().equals("variables.transform")) {
        Assert.assertEquals("BOX_COX_TRANSFORM", properties.getProperty(entry.getKey()));
      } else if (entry.getKey().equals("variables.seasonalities")) {
        Assert.assertNull(properties.getProperty(entry.getKey()));
      } else if (entry.getKey().equals("variables.numberOfKnots")) {
        Assert.assertEquals("0", properties.getProperty(entry.getKey()));
      } else {
        Assert.assertTrue(properties.containsKey(entry.getKey()));
        Assert.assertEquals(String.format("Assert Error on property key %s", entry.getKey()),
            entry.getValue(), properties.getProperty(entry.getKey()));
      }
    }
  }
}
