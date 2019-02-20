/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.rootcause.impl;

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
  public void testFilterEncoded() {
    MetricEntity e = MetricEntity.fromURN("thirdeye:metric:12345:key%3Dvalue", 1.0);
    Assert.assertEquals(e.getFilters().size(), 1);
    Assert.assertEquals(e.getFilters().get("key").iterator().next(), "value");
  }

  @Test
  public void testFilterEncodedMultipleEquals() {
    MetricEntity e = MetricEntity.fromURN("thirdeye:metric:12345:key%3Dvalue%3D1", 1.0);
    Assert.assertEquals(e.getFilters().size(), 1);
    Assert.assertEquals(e.getFilters().get("key").iterator().next(), "value=1");
  }

  @Test
  public void testFiltersMultikey() {
    MetricEntity e = MetricEntity.fromURN("thirdeye:metric:12345:key=value:key=other:otherKey=yetAnotherValue", 1.0);
    Assert.assertEquals(e.getFilters().size(), 3);
    Assert.assertEquals(e.getFilters().get("key"), Arrays.asList("other", "value")); // stratification
    Assert.assertEquals(e.getFilters().get("otherKey").iterator().next(), "yetAnotherValue");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFiltersShortFail() {
    MetricEntity.fromURN("thirdeye:metric:12345:key:key2=value2", 1.0);
  }

  @Test
  public void testCreateWithFilter() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("key", "value");
    filters.put("key", "otherValue");
    filters.put("anotherKey", "otherValue");

    MetricEntity e = MetricEntity.fromMetric(1.0, 123, filters);

    Assert.assertEquals(e.getUrn().split(":").length, 6);
    Assert.assertTrue(e.getUrn().startsWith("thirdeye:metric:123"));
    Assert.assertTrue(e.getUrn().contains(":anotherKey%3DotherValue"));
    Assert.assertTrue(e.getUrn().contains(":key%3Dvalue"));
    Assert.assertTrue(e.getUrn().contains(":key%3DotherValue"));
  }

  @Test
  public void testSetFilters() {
    Multimap<String, String> filters1 = ArrayListMultimap.create();
    filters1.put("key", "otherValue");
    filters1.put("anotherKey", "otherValue");

    Multimap<String, String> filters2 = ArrayListMultimap.create();
    filters2.put("key", "value");

    MetricEntity e = MetricEntity.fromMetric(1.0, 123, filters1);
    Assert.assertEquals(e.getUrn().split(":").length, 5);
    Assert.assertTrue(e.getUrn().startsWith("thirdeye:metric:123"));
    Assert.assertTrue(e.getUrn().contains(":anotherKey%3DotherValue"));
    Assert.assertTrue(e.getUrn().contains(":key%3DotherValue"));

    MetricEntity aug = e.withFilters(filters2);
    Assert.assertEquals(aug.getUrn().split(":").length, 4);
    Assert.assertTrue(aug.getUrn().startsWith("thirdeye:metric:123"));
    Assert.assertTrue(aug.getUrn().contains(":key%3Dvalue"));
  }

  @Test
  public void testRemoveFilters() {
    Multimap<String, String> filters = ArrayListMultimap.create();
    filters.put("key", "value");
    filters.put("key", "otherValue");
    filters.put("anotherKey", "otherValue");

    MetricEntity e = MetricEntity.fromMetric(1.0, 123, filters);
    Assert.assertEquals(e.getUrn().split(":").length, 6);
    Assert.assertTrue(e.getUrn().startsWith("thirdeye:metric:123"));
    Assert.assertTrue(e.getUrn().contains(":anotherKey%3DotherValue"));
    Assert.assertTrue(e.getUrn().contains(":key%3Dvalue"));
    Assert.assertTrue(e.getUrn().contains(":key%3DotherValue"));

    MetricEntity drop = e.withoutFilters();
    Assert.assertEquals(drop.getUrn(), "thirdeye:metric:123");
  }

}
