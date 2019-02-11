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

package org.apache.pinot.thirdeye.common.metric;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.thirdeye.common.time.TimeRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MetricTimeSeriesTest {
  private static final Logger LOG = LoggerFactory.getLogger(MetricTimeSeriesTest.class);
  private static final double NULL_DOUBLE = Double.NaN;
  private static final int NULL_INT = Integer.MIN_VALUE;
  private static final long NULL_LONG = Long.MIN_VALUE;

  @DataProvider
  public static Object[][] defaultMetricTimeSeries() {
    Object[] objectsArray = buildEmptyMetricTimeSeries()[0];
    List<String> metricNames = (List<String>) objectsArray[0];
    MetricTimeSeries metricTimeSeries = (MetricTimeSeries) objectsArray[1];

    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] intValues = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] longValues = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };

    initializeMetricTimeSeries(metricNames, metricTimeSeries, timestamps, doubleValues, intValues, longValues);

    return new Object[][] {
        {metricNames, metricTimeSeries, timestamps}
    };
  }

  @DataProvider
  public static Object[][] emptyTwoMetricTimeSeries() {
    Object[] objectsArray1 = buildEmptyMetricTimeSeries()[0];
    List<String> metricNames = (List<String>) objectsArray1[0];
    MetricTimeSeries metricTimeSeries1 = (MetricTimeSeries) objectsArray1[1];

    Object[] objectsArray2 = buildEmptyMetricTimeSeries()[0];
    MetricTimeSeries metricTimeSeries2 = (MetricTimeSeries) objectsArray2[1];

    return new Object[][] {
        {metricNames, metricTimeSeries1, metricTimeSeries2}
    };
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testGetOrDefault(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    double[] expectedDValues = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] expectedIValues = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] expectedLValues = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };

    checkActualWithNullValues(metricNames, metricTimeSeries, timestamps, expectedDValues, expectedIValues,
        expectedLValues);
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testGet(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    double[] expectedDValues = new double[] {1.0, 2.0, 3.0, 5.0};
    int[] expectedIValues = new int[] {};
    long[] expectedLValues = new long[] { 1, 3, 5 };

    checkActualWithPresetNullValues(metricNames, metricTimeSeries, timestamps, expectedDValues, expectedIValues,
        expectedLValues);
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testIncrement(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    double[] doubleDeltas = new double[] {1.0, NULL_DOUBLE, NULL_DOUBLE, 4.0, 5.0};
    int[] intDeltas = new int[] {1, NULL_INT, 3, NULL_INT, 5};
    long[] longDeltas = new long[] { NULL_LONG, 2, 2, NULL_LONG, 3 };

    for (int i = 0; i < timestamps.length; i++) {
      double doubleDelta = doubleDeltas[i];
      if (Double.compare(doubleDelta, NULL_DOUBLE) != 0) {
        metricTimeSeries.increment(timestamps[i], metricNames.get(0), doubleDelta);
      }

      int intDelta = intDeltas[i];
      if (Integer.compare(intDelta, NULL_INT) != 0) {
        metricTimeSeries.increment(timestamps[i], metricNames.get(1), intDelta);
      }

      long longDelta = longDeltas[i];
      if (Long.compare(longDelta, NULL_LONG) != 0) {
        metricTimeSeries.increment(timestamps[i], metricNames.get(2), longDelta);
      }
    }

    double[] expectedDValues = new double[] {2.0, 2.0, 3.0, 4.0, 10.0};
    int[] expectedIValues = new int[] {1, NULL_INT, 3, NULL_INT, 5};
    long[] expectedLValues = new long[] { 1, 2, 5, NULL_LONG, 8 };

    checkActualWithNullValues(metricNames, metricTimeSeries, timestamps, expectedDValues, expectedIValues,
        expectedLValues);
  }

  @Test(dataProvider = "emptyTwoMetricTimeSeries")
  public void testAggregate(List<String> metricNames, MetricTimeSeries metricTimeSeries1, MetricTimeSeries metricTimeSeries2) {
    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues1 = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] intValues1 = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] longValues1 = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };
    initializeMetricTimeSeries(metricNames, metricTimeSeries1, timestamps, doubleValues1, intValues1, longValues1);

    double[] doubleValues2 = new double[] {1.0, NULL_DOUBLE, NULL_DOUBLE, 4.0, 5.0};
    int[] intValues2 = new int[] {1, NULL_INT, 3, NULL_INT, 5};
    long[] longValues2 = new long[] { NULL_LONG, 2, 2, NULL_LONG, 3 };
    initializeMetricTimeSeries(metricNames, metricTimeSeries2, timestamps, doubleValues2, intValues2, longValues2);

    metricTimeSeries1.aggregate(metricTimeSeries2);

    double[] expectedDValues = new double[] {2.0, 2.0, 3.0, 4.0, 10.0};
    int[] expectedIValues = new int[] {1, NULL_INT, 3, NULL_INT, 5};
    long[] expectedLValues = new long[] { 1, 2, 5, NULL_LONG, 8 };

    checkActualWithNullValues(metricNames, metricTimeSeries1, timestamps, expectedDValues, expectedIValues,
        expectedLValues);
  }

  @Test(dataProvider = "emptyTwoMetricTimeSeries")
  public void testAggregateWithTimeRange(List<String> metricNames, MetricTimeSeries metricTimeSeries1, MetricTimeSeries metricTimeSeries2) {
    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues1 = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] intValues1 = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] longValues1 = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };
    initializeMetricTimeSeries(metricNames, metricTimeSeries1, timestamps, doubleValues1, intValues1, longValues1);

    double[] doubleValues2 = new double[] {1.0, NULL_DOUBLE, NULL_DOUBLE, 4.0, 5.0};
    int[] intValues2 = new int[] {1, NULL_INT, 3, NULL_INT, 5};
    long[] longValues2 = new long[] { NULL_LONG, 2, 2, NULL_LONG, 3 };
    initializeMetricTimeSeries(metricNames, metricTimeSeries2, timestamps, doubleValues2, intValues2, longValues2);

    TimeRange timeRange = new TimeRange(2L, 4L);
    metricTimeSeries1.aggregate(metricTimeSeries2, timeRange);

    double[] expectedDValues = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    int[] expectedIValues = new int[] {NULL_INT, NULL_INT, 3, NULL_INT, NULL_INT};
    long[] expectedLValues = new long[] { 1, 2, 5, NULL_LONG, 5 };

    checkActualWithNullValues(metricNames, metricTimeSeries1, timestamps, expectedDValues, expectedIValues,
        expectedLValues);
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testGetTimeWindowSet(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    Set<Long> expectedTimestamps = new HashSet<Long>() {{
      add(1L);
      add(2L);
      add(3L);
      add(5L);
    }};

    Assert.assertEquals(metricTimeSeries.getTimeWindowSet(), expectedTimestamps);
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testToString(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    LOG.info(metricTimeSeries.toString());
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testGetMetricSums(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    Number[] actualSums = metricTimeSeries.getMetricSums();
    Number[] expectedSums = new Number[] {11.0, 0, 9L};
    Assert.assertEquals(actualSums, expectedSums);
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testGetMetricAvgs(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    // Fill Double.NaN for value of divided by zero
    Double[] actualAvgs = metricTimeSeries.getMetricAvgs(Double.NaN);
    Double[] expectedAvgs = new Double[] {2.75d, Double.NaN, 3d};
    Assert.assertEquals(actualAvgs, expectedAvgs);

    // Fill null for value of divided by zero
    actualAvgs = metricTimeSeries.getMetricAvgs();
    expectedAvgs = new Double[] {2.75d, null, 3d};
    Assert.assertEquals(actualAvgs, expectedAvgs);
  }

  @Test(dataProvider = "defaultMetricTimeSeries")
  public void testGetHasValueSums(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps) {
    Integer[] actualSums = metricTimeSeries.getHasValueSums();
    Integer[] expectedSums = new Integer[] {4, 0, 3};
    Assert.assertEquals(actualSums, expectedSums);
  }

  private static Object[][] buildEmptyMetricTimeSeries() {
    List<String> metricNames = new ArrayList<String>() {{
      add("mDouble");
      add("mInteger");
      add("mLong");
    }};
    List<MetricType> types = new ArrayList<MetricType>() {{
      add(MetricType.DOUBLE);
      add(MetricType.INT);
      add(MetricType.LONG);
    }};

    MetricSchema schema = new MetricSchema(metricNames, types);

    MetricTimeSeries metricTimeSeries = new MetricTimeSeries(schema);

    return new Object[][] {
        {metricNames, metricTimeSeries}
    };
  }

  private static void initializeMetricTimeSeries(List<String> metricNames, MetricTimeSeries metricTimeSeries,
      long[] timestamps, double[] doubleValues, int[] intValues, long[] longValues) {
    for (int i = 0; i < timestamps.length; i++) {
      double doubleValue = doubleValues[i];
      if (Double.compare(doubleValue, NULL_DOUBLE) != 0) {
        metricTimeSeries.set(timestamps[i], metricNames.get(0), doubleValue);
      }

      int intValue = intValues[i];
      if (Integer.compare(intValue, NULL_INT) != 0) {
        metricTimeSeries.set(timestamps[i], metricNames.get(1), intValue);
      }

      long longValue = longValues[i];
      if (Long.compare(longValue, NULL_LONG) != 0) {
        metricTimeSeries.set(timestamps[i], metricNames.get(2), longValue);
      }
    }
  }

  private static void checkActualWithNullValues(List<String> metricNames, MetricTimeSeries metricTimeSeries,
      long[] timestamps, double[] expectedDValues, int[] expectedIValues, long[] expectedLValues) {

    double[] actualDValues = new double[5];
    int[] actualIValues = new int[5];
    long[] actualLValues = new long[5];
    for (int i = 0; i < timestamps.length; i++) {
      actualDValues[i] = metricTimeSeries.getOrDefault(timestamps[i], metricNames.get(0), NULL_DOUBLE).doubleValue();
      actualIValues[i] = metricTimeSeries.getOrDefault(timestamps[i], metricNames.get(1), NULL_INT).intValue();
      actualLValues[i] = metricTimeSeries.getOrDefault(timestamps[i], metricNames.get(2), NULL_LONG).longValue();
    }

    Assert.assertEquals(actualDValues, expectedDValues);
    Assert.assertEquals(actualIValues, expectedIValues);
    Assert.assertEquals(actualLValues, expectedLValues);
  }

  private static void checkActualWithPresetNullValues(List<String> metricNames, MetricTimeSeries metricTimeSeries,
      long[] timestamps, double[] expectedDValues, int[] expectedIValues, long[] expectedLValues) {

    List<Double> doubleList = new ArrayList<>();
    List<Integer> intList = new ArrayList<>();
    List<Long> longList = new ArrayList<>();

    for (long timestamp : timestamps) {
      Number doubleNumber = metricTimeSeries.get(timestamp, metricNames.get(0));
      if (doubleNumber != null) {
        doubleList.add(doubleNumber.doubleValue());
      }
      Number intNumber = metricTimeSeries.get(timestamp, metricNames.get(1));
      if (intNumber != null) {
        intList.add(intNumber.intValue());
      }
      Number longNumber = metricTimeSeries.get(timestamp, metricNames.get(2));
      if (longNumber != null) {
        longList.add(longNumber.longValue());
      }
    }

    double[] actualDValues = new double[doubleList.size()];
    int[] actualIValues = new int[intList.size()];
    long[] actualLValues = new long[longList.size()];

    for (int i = 0; i < doubleList.size(); i++) {
      actualDValues[i] = doubleList.get(i);
    }
    for (int i = 0; i < intList.size(); i++) {
      actualIValues[i] = intList.get(i);
    }
    for (int i = 0; i < longList.size(); i++) {
      actualLValues[i] = longList.get(i);
    }

    Assert.assertEquals(actualDValues, expectedDValues);
    Assert.assertEquals(actualIValues, expectedIValues);
    Assert.assertEquals(actualLValues, expectedLValues);
  }

}
