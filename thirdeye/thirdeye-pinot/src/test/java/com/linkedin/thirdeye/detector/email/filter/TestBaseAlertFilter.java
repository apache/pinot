package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBaseAlertFilter {
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  // test set up double, Double, string for alpha_beta
  // also test missing input parameter, will use default value defined in the specified class
  @Test
  public void testSetAlphaBetaParamter(){
    AnomalyFunctionDTO anomalyFunctionSpec = DaoTestUtils.getTestFunctionAlphaBetaAlertFilterSpec(metricName, collection);
    Map<String, String> alertfilter = anomalyFunctionSpec.getAlertFilter();
    AlphaBetaAlertFilter alphaBetaAlertFilter = new AlphaBetaAlertFilter();
    alphaBetaAlertFilter.setParameters(alertfilter);
    Assert.assertEquals(alphaBetaAlertFilter.getAlpha(), Double.valueOf(alertfilter.get("alpha")));
    Assert.assertEquals(alphaBetaAlertFilter.getBeta(), Double.valueOf(alertfilter.get("beta")));
    Assert.assertEquals(alphaBetaAlertFilter.getType(), alertfilter.get("type"));
    Assert.assertEquals(alphaBetaAlertFilter.getThreshold(), Double.valueOf(alertfilter.get("threshold")));

    // test scientific decimal
    double threshold = 1E-10;
    alertfilter.put("threshold", String.valueOf(threshold));
    alphaBetaAlertFilter.setParameters(alertfilter);
    Assert.assertEquals(alphaBetaAlertFilter.getThreshold(), Double.valueOf(alertfilter.get("threshold")));

    // test missing field
    alertfilter.remove("threshold");
    AlphaBetaAlertFilter alphaBetaAlertFilter1 = new AlphaBetaAlertFilter();
    alphaBetaAlertFilter1.setParameters(alertfilter);
    Assert.assertEquals(alphaBetaAlertFilter1.getThreshold(), Double.valueOf(AlphaBetaAlertFilter.DEFAULT_THRESHOLD));
  }

}