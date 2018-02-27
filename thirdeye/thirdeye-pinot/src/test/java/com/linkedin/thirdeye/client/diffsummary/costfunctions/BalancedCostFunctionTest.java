package com.linkedin.thirdeye.client.diffsummary.costfunctions;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.client.diffsummary.costfunctions.BalancedCostFunction.*;


public class BalancedCostFunctionTest {

  @Test
  public void testCreate() {
    double threshold = 3.54d;
    Map<String, String> params = new HashMap<>();
    params.put(CHANGE_CONTRIBUTION_THRESHOLD_PARAM, Double.toString(threshold));

    BalancedCostFunction function = new BalancedCostFunction(params);
    Assert.assertEquals(function.getChangeContributionThreshold(), threshold);
  }

}