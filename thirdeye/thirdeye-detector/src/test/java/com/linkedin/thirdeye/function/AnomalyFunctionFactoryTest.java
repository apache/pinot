package com.linkedin.thirdeye.function;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;

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
            "KALMAN_FILTER", KalmanFilterAnomalyFunction.class
        }, new Object[] {
            "SCAN_STATISTICS", ScanStatisticsAnomalyFunction.class
        }, new Object[] {
            "USER_RULE", UserRuleAnomalyFunction.class
        }
    };
  }

  @Test(dataProvider = "validMappings")
  public void fromSpec(String type, Class<AnomalyFunction> clazz) throws Exception {
    AnomalyFunction spec = anomalyFunctionFactory.fromSpec(specWithType(type));
    Assert.assertTrue(clazz.isInstance(spec));
  }

  // helper to abstract specific implementation details.
  private AnomalyFunctionSpec specWithType(String type) {
    AnomalyFunctionSpec spec = new AnomalyFunctionSpec();
    spec.setType(type);
    if ("SCAN_STATISTICS".equals(type)) {
      spec.setProperties(
          "pattern=UP;seasonal=168;minWindowLength=1;numSimulations=1000;monitoringWindow=72");
    }
    return spec;
  }
}
