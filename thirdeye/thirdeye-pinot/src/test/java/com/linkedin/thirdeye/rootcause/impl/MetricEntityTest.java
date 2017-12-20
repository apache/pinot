package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
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

  @Test
  public void testCreateWithFilter() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("key", "value");
    filters.put("key", "otherValue");
    filters.put("anotherKey", "otherValue");

    MetricEntity e = MetricEntity.fromMetric(1.0, 123, filters);

    Assert.assertEquals(e.getUrn(), "thirdeye:metric:123:anotherKey=otherValue:key=value:key=otherValue");
  }

  @Test
  public void testSetFilters() {
    Multimap<String, String> filters1 = ArrayListMultimap.create();
    filters1.put("key", "otherValue");
    filters1.put("anotherKey", "otherValue");

    Multimap<String, String> filters2 = ArrayListMultimap.create();
    filters2.put("key", "value");

    MetricEntity e = MetricEntity.fromMetric(1.0, 123, filters1);
    Assert.assertEquals(e.getUrn(), "thirdeye:metric:123:anotherKey=otherValue:key=otherValue");

    MetricEntity aug = e.withFilters(filters2);
    Assert.assertEquals(aug.getUrn(), "thirdeye:metric:123:key=value");
  }

  @Test
  public void testRemoveFilters() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("key", "value");
    filters.put("key", "otherValue");
    filters.put("anotherKey", "otherValue");

    MetricEntity e = MetricEntity.fromMetric(1.0, 123, filters);
    Assert.assertEquals(e.getUrn(), "thirdeye:metric:123:anotherKey=otherValue:key=value:key=otherValue");

    MetricEntity drop = e.withoutFilters();
    Assert.assertEquals(drop.getUrn(), "thirdeye:metric:123");
  }

}
