package org.apache.pinot.client.utils;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BigDecimalUtilsTest {

  @Test
  public void testGetCalculatedScale() {
    int calculatedResult;

    calculatedResult = BigDecimalUtils.getCalculatedScale("1");
    Assert.assertEquals(calculatedResult, 0);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.0");
    Assert.assertEquals(calculatedResult, 1);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.2");
    Assert.assertEquals(calculatedResult, 1);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.23");
    Assert.assertEquals(calculatedResult, 2);

    calculatedResult = BigDecimalUtils.getCalculatedScale("1.234");
    Assert.assertEquals(calculatedResult, 3);

    calculatedResult = BigDecimalUtils.getCalculatedScale("-1.234");
    Assert.assertEquals(calculatedResult, 3);
  }
}
