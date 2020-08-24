/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.anomaly.views.AnomalyTimelinesView;
import org.apache.pinot.thirdeye.dashboard.views.TimeBucket;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ThirdEyeUtilsTest {

  /*@Test(dataProvider = "testSortedFiltersDataProvider")
  public void testSortedFilters(String filters, String expectedFilters) {
    String sortedFilters = ThirdEyeUtils.getSortedFilters(filters);
    Assert.assertEquals(sortedFilters, expectedFilters);
  }

  @Test(dataProvider = "testSortedFiltersFromJsonDataProvider")
  public void testSortedFiltersFromJson(String filterJson, String expectedFilters) {
    String sortedFilters = ThirdEyeUtils.getSortedFiltersFromJson(filterJson);
    Assert.assertEquals(sortedFilters, expectedFilters);
  }

  @Test(dataProvider = "testSortedFiltersFromMultimapDataProvider")
  public void testSortedFiltersFromMultimap(Multimap<String, String> filterMultimap,
      String expectedFilters) {
    String sortedFilters = ThirdEyeUtils.getSortedFiltersFromMultiMap(filterMultimap);
    Assert.assertEquals(sortedFilters, expectedFilters);
  }

  @DataProvider(name = "testConstructCronDataProvider")
  public Object[][] testConstructCronDataProvider() {
    return new Object[][] {
        {
          "10", "12", "DAYS", "0 10 12 * * ?"
        }, {
          null, null, "DAYS", "0 0 0 * * ?"
        }, {
          null, null, "HOURS", "0 0 * * * ?"
        }, {
          "70", "12", "DAYS", null
        }, {
          "20", "25", "DAYS", null
        }, {
          "70", "12", "WEEKS", null
        }
    };
  }

  @DataProvider(name = "testSortedFiltersDataProvider")
  public Object[][] testSortedFiltersDataProvider() {
    return new Object[][] {
        {
            "a=z;z=d;a=f;a=e;k=m;k=f;z=c;f=g;", "a=e;a=f;a=z;f=g;k=f;k=m;z=c;z=d"
        }, {
            ";", null
        }, {
            "a=b", "a=b"
        }, {
            "a=z;a=b", "a=b;a=z"
        }
    };
  }

  @DataProvider(name = "testSortedFiltersFromJsonDataProvider")
  public Object[][] testSortedFiltersFromJsonDataProvider() {
    return new Object[][] {
        {
            "{\"a\":[\"b\",\"c\"]}", "a=b;a=c"
        }, {
            "{\"z\":[\"g\"],\"x\":[\"l\"],\"a\":[\"b\",\"c\"]}", "a=b;a=c;x=l;z=g"
        }
    };
  }

  @DataProvider(name = "testSortedFiltersFromMultimapDataProvider")
  public Object[][] testSortedFiltersFromMultimapDataProvider() {
    ListMultimap<String, String> multimap1 = ArrayListMultimap.create();
    multimap1.put("a", "b");
    multimap1.put("a", "c");

    ListMultimap<String, String> multimap2 = ArrayListMultimap.create();
    multimap2.put("a", "b");
    multimap2.put("z", "g");
    multimap2.put("k", "b");
    multimap2.put("i", "c");
    multimap2.put("a", "c");

    return new Object[][] {
        {
            multimap1, "a=b;a=c"
        }, {
            multimap2, "a=b;a=c;i=c;k=b;z=g"
        }
    };
  }*/


  @Test
  public void testGetRoundedDouble() {
    double value = 123.456789;
    Assert.assertEquals(ThirdEyeUtils.getRoundedDouble(value), 123.46);
    value = 0.056789;
    Assert.assertEquals(ThirdEyeUtils.getRoundedDouble(value), 0.057);
  }

  @Test
  public void testGetRoundedValue() throws Exception {
    double value = 123;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "123");
    value = 123.24;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "123.24");
    value = 123.246;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "123.25");
    value = 123.241;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "123.24");
    value = 0.23;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.23");
    value = 0.236;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.24");
    value = 0.01;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.01");
    value = 0.016;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.016");
    value = 0.0167;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.017");
    value = 0.001;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.001");
    value = 0.0013;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.0013");
    value = 0.00135;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.0014");
    value = 0;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0");
    value = 0.0000;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0");
    value = 0.0000009;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0");
    value = 0.00123456789;
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(value), "0.0012");
  }

  @Test
  public void testGetRoundedValueNonRegularNumber() {
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(Double.NaN), Double.toString(Double.NaN));
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(Double.POSITIVE_INFINITY), Double.toString(Double.POSITIVE_INFINITY));
    Assert.assertEquals(ThirdEyeUtils.getRoundedValue(Double.NEGATIVE_INFINITY), Double.toString(Double.NEGATIVE_INFINITY));
  }

  @Test
  public void testExceptionToStringUnlimited() {
    List<Exception> exceptions = new ArrayList<>();
    exceptions.add(new Exception("e1"));
    exceptions.add(new Exception("e2"));
    exceptions.add(new Exception("e3"));
    String errorMessage = ThirdEyeUtils.exceptionsToString(exceptions, 0);
    Assert.assertTrue(errorMessage.contains("e3"));
  }

  @Test
  public void testExceptionToStringLimited() {
    List<Exception> exceptions = new ArrayList<>();
    exceptions.add(new Exception("e1"));
    exceptions.add(new Exception("e2"));
    exceptions.add(new Exception("e3"));
    String errorMessage = ThirdEyeUtils.exceptionsToString(exceptions, 100);
    Assert.assertFalse(errorMessage.contains("e3"));
  }

  @Test
  public void testMergeAnomalyProperties() {
    Map<String, String> parentProperties = new HashMap<>();
    parentProperties.put("p1", "value1");
    parentProperties.put("p2", "value2");
    parentProperties.put("detectorComponentName", "rule1");

    Map<String, String> childProperties = new HashMap<>();
    childProperties.put("p1", "value3");
    childProperties.put("c1", "value4");
    childProperties.put("detectorComponentName", "rule2");

    ThirdEyeUtils.mergeAnomalyProperties(parentProperties, childProperties);
    Assert.assertEquals(parentProperties.get("p1"), "value1");
    Assert.assertEquals(parentProperties.get("p2"), "value2");
    Assert.assertEquals(parentProperties.get("c1"), "value4");
    Assert.assertEquals(parentProperties.get("detectorComponentName"), "rule1,rule2");
  }

  @Test
  public void testMergeAnomalySnapshotWithOverlap() throws Exception{
    final long now = System.currentTimeMillis();
    final long bucketSize = 300_000;
    final double delta = 0.000001;
    final double parentVal = 1.0, childVal = 2.0;
    Map<String, String> parentProperties = new HashMap<>();
    Map<String, String> childProperties = new HashMap<>();
    parentProperties.put("anomalyTimelinesView",
        generateAnomalyTimelineView(now, bucketSize, 10, parentVal).toJsonString());
    childProperties.put("anomalyTimelinesView",
        generateAnomalyTimelineView(now + bucketSize, bucketSize, 10,  childVal).toJsonString());
    ThirdEyeUtils.mergeAnomalyProperties(parentProperties, childProperties);
    AnomalyTimelinesView merged = AnomalyTimelinesView.fromJsonString(parentProperties.get("anomalyTimelinesView"));
    Assert.assertEquals(merged.getTimeBuckets().size(), 11);
    Assert.assertTrue(Math.abs(merged.getCurrentValues().get(0) - parentVal) < delta);
    Assert.assertTrue(Math.abs(merged.getCurrentValues().get(1) - parentVal) < delta);
    Assert.assertTrue(Math.abs(merged.getCurrentValues().get(10) - childVal) < delta);
  }

  @Test
  public void testMergeAnomalySnapshotWithGap() throws Exception{
    final long now = System.currentTimeMillis();
    final long bucketSize = 300_000;
    final double delta = 0.000001;
    final double parentVal = 1.0, childVal = 2.0;
    Map<String, String> parentProperties = new HashMap<>();
    Map<String, String> childProperties = new HashMap<>();
    parentProperties.put("anomalyTimelinesView",
        generateAnomalyTimelineView(now, bucketSize, 10, parentVal).toJsonString());
    childProperties.put("anomalyTimelinesView",
        generateAnomalyTimelineView(now + bucketSize * 20, bucketSize, 10,  childVal).toJsonString());
    ThirdEyeUtils.mergeAnomalyProperties(parentProperties, childProperties);
    AnomalyTimelinesView merged = AnomalyTimelinesView.fromJsonString(parentProperties.get("anomalyTimelinesView"));
    Assert.assertEquals(merged.getTimeBuckets().size(), 20);
    Assert.assertTrue(Math.abs(merged.getCurrentValues().get(0) - parentVal) < delta);
    Assert.assertTrue(Math.abs(merged.getCurrentValues().get(1) - parentVal) < delta);
    Assert.assertTrue(Math.abs(merged.getCurrentValues().get(19) - childVal) < delta);

  }

  private AnomalyTimelinesView generateAnomalyTimelineView(
      long startMillis, long bucketSizeMillis, int numBucket, double metricVal) {
    AnomalyTimelinesView res = new AnomalyTimelinesView();
    for (int i = 0; i < numBucket; i++) {
      res.addTimeBuckets(new TimeBucket(
          bucketSizeMillis * i + startMillis,
          (bucketSizeMillis + 1) * i +startMillis,
          bucketSizeMillis * i + startMillis,
          (bucketSizeMillis + 1) * i +startMillis
      ));
      res.addBaselineValues(metricVal);
      res.addCurrentValues(metricVal);
    }
    return res;
  }
}
