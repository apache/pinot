package com.linkedin.thirdeye.client.diffsummary;

import org.testng.annotations.Test;

public class DimNameValueCostEntryTest {

  @Test
  public void testCreation() {
    new DimNameValueCostEntry("", "", 0, 0, 0, 0);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullDimensionNameCreation() {
    new DimNameValueCostEntry(null, "", 0, 0, 0, 0);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullDimensionValueCreation() {
    new DimNameValueCostEntry("", null, 0, 0, 0, 0);
  }
}
