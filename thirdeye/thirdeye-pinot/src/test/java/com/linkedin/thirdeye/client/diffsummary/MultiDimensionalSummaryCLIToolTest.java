package com.linkedin.thirdeye.client.diffsummary;

import com.linkedin.thirdeye.client.diffsummary.costfunctions.BalancedCostFunction;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiDimensionalSummaryCLIToolTest {
  @Test
  public void testInitiateCostFunction()
      throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException,
             InvocationTargetException, ClassNotFoundException {
    String paramString = String.format("{\"className\":\"%s\"}", BalancedCostFunction.class.getName());

    MultiDimensionalSummaryCLITool.initiateCostFunction(paramString);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyCostFunction()
      throws NoSuchMethodException, IOException, InstantiationException, IllegalAccessException,
             InvocationTargetException, ClassNotFoundException {
    String paramString = "{}";

    MultiDimensionalSummaryCLITool.initiateCostFunction(paramString);
  }

  @Test
  public void testSanitizeDimensions() {
    List<String> dims = Arrays.asList("environment", "page", "page" + MultiDimensionalSummaryCLITool.TOP_K_POSTFIX);
    Dimensions dimensions = new Dimensions(dims);
    Dimensions sanitizedDimensions = MultiDimensionalSummaryCLITool.sanitizeDimensions(dimensions);

    List<String> expectedSanitizedDims = Arrays.asList("page" + MultiDimensionalSummaryCLITool.TOP_K_POSTFIX);
    Dimensions expectedSanitizedDimensions = new Dimensions(expectedSanitizedDims);

    Assert.assertEquals(sanitizedDimensions, expectedSanitizedDimensions);
  }
}