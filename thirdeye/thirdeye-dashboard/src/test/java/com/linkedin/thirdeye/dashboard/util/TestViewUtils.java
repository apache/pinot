package com.linkedin.thirdeye.dashboard.util;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.dashboard.util.ViewUtils;

public class TestViewUtils {
  // TODO add tests for other existing cases
  @Test(dataProvider = "metricFunctionStrings")
  public void testGetMetricFunctionLevels(String metricFunction, String expectedStr) {
    List<String> result = ViewUtils.getMetricFunctionLevels(metricFunction);
    List<String> expected = Arrays.asList(expectedStr.replaceAll("\\s", "").split(","));
    Assert.assertEquals(result, expected);
  }

  @DataProvider(name = "metricFunctionStrings", parallel = true)
  public Object[][] metricFunctionStrings() {
    return new String[][] {
        new String[] {
            "A(B)", "A"
        }, new String[] {
            "A(B,C)", "A"
        }, new String[] {
            "A(B(C), D)", "A"
        }, new String[] {
            "A(B, C(D))", "A"
        }, new String[] {
            "A(B(C))", "A, B"
        }, new String[] {
            "A(B(C,D))", "A, B"
        }, new String[] {
            "A(B(C(D(E,F))), F)", "A"
        }, new String[] {
            "A(F, B(C(D(E))))", "A"
        }, new String[] {
            "A(B(C, D(E), F))", "A, B"
        }, new String[] {
            "A(B(C, D(E), F), G)", "A"
        }
    };
  }
}
