package com.linkedin.thirdeye.api;

import java.util.ArrayList;
import java.util.List;
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
  public static Object[][] emptyMetricTimeSeries() {
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

  @DataProvider
  public static Object[][] emptyTwoMetricTimeSeries() {
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

    MetricTimeSeries metricTimeSeries1 = new MetricTimeSeries(schema);
    MetricTimeSeries metricTimeSeries2 = new MetricTimeSeries(schema);

    return new Object[][] {
        {metricNames, metricTimeSeries1, metricTimeSeries2}
    };
  }

  @Test(dataProvider = "emptyMetricTimeSeries")
  public void testGetOrDefault(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] intValues = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] longValues = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };

    initializeMetricTimeSeries(metricNames, metricTimeSeries, timestamps, doubleValues, intValues, longValues);

    checkActualWithNullValues(metricNames, metricTimeSeries, timestamps, doubleValues, intValues, longValues);
  }

  @Test(dataProvider = "emptyMetricTimeSeries")
  public void testGet(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] intValues = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] longValues = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };

    initializeMetricTimeSeries(metricNames, metricTimeSeries, timestamps, doubleValues, intValues, longValues);

    double[] expectedDValues = new double[] {1.0, 2.0, 3.0, 0.0, 5.0};
    int[] expectedIValues = new int[] {0, 0, 0, 0, 0};
    long[] expectedLValues = new long[] { 1, 0, 3, 0, 5 };

    double[] actualDValues = new double[5];
    int[] actualIValues = new int[5];
    long[] actualLValues = new long[5];
    for (int i = 0; i < timestamps.length; i++) {
      actualDValues[i] = metricTimeSeries.get(timestamps[i], metricNames.get(0)).doubleValue();
      actualIValues[i] = metricTimeSeries.get(timestamps[i], metricNames.get(1)).intValue();
      actualLValues[i] = metricTimeSeries.get(timestamps[i], metricNames.get(2)).longValue();
    }

    Assert.assertEquals(actualDValues, expectedDValues);
    Assert.assertEquals(actualIValues, expectedIValues);
    Assert.assertEquals(actualLValues, expectedLValues);
  }

  @Test(dataProvider = "emptyMetricTimeSeries")
  public void testIncrement(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] intValues = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] longValues = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };

    initializeMetricTimeSeries(metricNames, metricTimeSeries, timestamps, doubleValues, intValues, longValues);

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

  @Test(dataProvider = "emptyMetricTimeSeries")
  public void testToString(List<String> metricNames, MetricTimeSeries metricTimeSeries) {
    long[] timestamps = new long[] { 1, 2, 3, 4, 5 };
    double[] doubleValues = new double[] {1.0, 2.0, 3.0, NULL_DOUBLE, 5.0};
    int[] intValues = new int[] {NULL_INT, NULL_INT, NULL_INT, NULL_INT, NULL_INT};
    long[] longValues = new long[] { 1, NULL_LONG, 3, NULL_LONG, 5 };

    initializeMetricTimeSeries(metricNames, metricTimeSeries, timestamps, doubleValues, intValues, longValues);
    LOG.info(metricTimeSeries.toString());
  }

  private void initializeMetricTimeSeries(List<String> metricNames, MetricTimeSeries metricTimeSeries,
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

  private void checkActualWithNullValues(List<String> metricNames, MetricTimeSeries metricTimeSeries, long[] timestamps,
      double[] expectedDValues, int[] expectedIValues, long[] expectedLValues) {

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

}
