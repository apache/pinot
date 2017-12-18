package com.linkedin.thirdeye.rootcause.impl;

import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetricEntityTest {
  @Test
  public void testType() {
    MetricEntity.fromURN("thirdeye:metric:12345", 1.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTypeFail() {
    MetricEntity.fromURN("thirdeye:other:12345", 1.0);
  }

  @Test
  public void testFilter() {
    MetricEntity e = MetricEntity.fromURN("thirdeye:metric:12345:key=value", 1.0);
    Assert.assertEquals(e.getFilters().size(), 1);
    Assert.assertEquals(e.getFilters().get("key").iterator().next(), "value");
  }

  @Test
  public void testFiltersMultikey() {
    MetricEntity e = MetricEntity.fromURN("thirdeye:metric:12345:key=value:key=other:otherKey=yetAnotherValue", 1.0);
    Assert.assertEquals(e.getFilters().size(), 3);
    Assert.assertEquals(e.getFilters().get("key"), Arrays.asList("value", "other"));
    Assert.assertEquals(e.getFilters().get("otherKey").iterator().next(), "yetAnotherValue");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFiltersShortFail() {
    MetricEntity.fromURN("thirdeye:metric:12345:key:key2=value2", 1.0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFiltersLongFail() {
    MetricEntity.fromURN("thirdeye:metric:12345:key=value=1", 1.0);
  }
}
