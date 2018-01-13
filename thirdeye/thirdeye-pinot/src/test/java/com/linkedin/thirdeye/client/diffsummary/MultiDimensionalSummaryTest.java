package com.linkedin.thirdeye.client.diffsummary;

import com.linkedin.thirdeye.client.diffsummary.costfunction.BalancedCostFunction;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiDimensionalSummaryTest {
  @Test
  public void testInitiateCostFunction()
      throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException,
             InvocationTargetException, ClassNotFoundException {
    String paramString = String.format("{\"className\":\"%s\"}", BalancedCostFunction.class.getName());

    MultiDimensionalSummary.initiateCostFunction(paramString);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyCostFunction()
      throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException,
             InvocationTargetException, ClassNotFoundException {
    String paramString = "{}";

    MultiDimensionalSummary.initiateCostFunction(paramString);
  }

  @Test
  public void testSanitizeDimensions() {
    List<String> dims = Arrays.asList("environment", "page", "page" + MultiDimensionalSummary.TOP_K_POSTFIX);
    Dimensions dimensions = new Dimensions(dims);
    Dimensions sanitizedDimensions = MultiDimensionalSummary.sanitizeDimensions(dimensions);

    List<String> expectedSanitizedDims = Arrays.asList("page" + MultiDimensionalSummary.TOP_K_POSTFIX);
    Dimensions expectedSanitizedDimensions = new Dimensions(expectedSanitizedDims);

    Assert.assertEquals(sanitizedDimensions, expectedSanitizedDimensions);
  }
}