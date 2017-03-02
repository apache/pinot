package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public class TestAnomalyFunctionFactory {

  private static AnomalyFunctionFactory anomalyFunctionFactory;

  @BeforeClass
  public static void setup() {
    String mappingsPath = ClassLoader.getSystemResource("sample-functions.properties").getPath();
    anomalyFunctionFactory = new AnomalyFunctionFactory(mappingsPath);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void fromSpecIllegalType() throws Exception {
    anomalyFunctionFactory.fromSpec(specWithType("NONEXISTENT"));
  }

  @DataProvider(name = "validMappings")
  public static Object[][] validMappings() {
    return new Object[][] {
        new Object[] {
            "WEEK_OVER_WEEK_RULE", WeekOverWeekRuleFunction.class
        },
        new Object[] {
            "MIN_MAX_THRESHOLD", MinMaxThresholdFunction.class
        }
    };
  }

  @Test(dataProvider = "validMappings")
  public void fromSpec(String type, Class<AnomalyFunction> clazz) throws Exception {
    AnomalyFunction spec = anomalyFunctionFactory.fromSpec(specWithType(type));
    Assert.assertTrue(clazz.isInstance(spec));
  }

  // helper to abstract specific implementation details.
  private AnomalyFunctionDTO specWithType(String type) {
    AnomalyFunctionDTO spec = new AnomalyFunctionDTO();
    spec.setType(type);
    return spec;
  }
}
