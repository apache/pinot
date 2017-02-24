package com.linkedin.thirdeye.detector.function;

import com.linkedin.thirdeye.anomalydetection.function.WeekOverWeekRuleFunction;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public class AnomalyFunctionFactoryTest {

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
