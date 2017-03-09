package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.bao.AbstractManagerTestBase;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.testng.Assert;
import org.testng.annotations.Test;



public class TestAlertFilterFactory extends AbstractManagerTestBase {
  private static AlertFilterFactory alertFilterFactory;
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  private TestAlertFilterFactory() {
    String mappingsPath = ClassLoader.getSystemResource("sample-alertfilter.properties").getPath();
    alertFilterFactory = new AlertFilterFactory(mappingsPath);
  }

  @Test
  public void fromSpecNullAlertFilter() throws Exception {
    AlertFilter alertFilter = alertFilterFactory.fromSpec(null);
    Assert.assertEquals(alertFilter.getClass(), DummyAlertFilter.class);
  }

  @Test
  public void testFromAnomalyFunctionSpecToAlertFilter() throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = getTestFunctionSpec(metricName, collection);
    AlertFilter alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    Assert.assertEquals(alertFilter.getClass(), DummyAlertFilter.class);

    anomalyFunctionSpec = getTestFunctionAlphaBetaAlertFilterSpec(metricName, collection);
    alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    Assert.assertEquals(alertFilter.getClass(), AlphaBetaAlertFilter.class);
  }
}