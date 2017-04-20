package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.linkedin.thirdeye.api.DimensionMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertGroupKeyTest {
  @Test
  public void testEqualsEmptyKey() {
    AlertGroupKey<DimensionMap> alertGroupKey = AlertGroupKey.emptyKey();
    Assert.assertEquals(alertGroupKey.getKey(), null);
    Assert.assertEquals(alertGroupKey, AlertGroupKey.emptyKey());
  }

  @Test
  public void testEqualsDimensionKey() {
    DimensionMap dimensionMap1 = new DimensionMap();
    dimensionMap1.put("D1", "V1");
    AlertGroupKey<DimensionMap> alertGroupKey1 = new AlertGroupKey<>(dimensionMap1);

    DimensionMap dimensionMap2 = new DimensionMap();
    dimensionMap2.put("D1", "V1");
    AlertGroupKey<DimensionMap> alertGroupKey2 = new AlertGroupKey<>(dimensionMap2);

    Assert.assertEquals(alertGroupKey1, alertGroupKey2);
  }
}
