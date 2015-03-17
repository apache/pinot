package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.impl.NumberUtils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;


/**
 *
 * @author kgopalak
 *
 */
public class MetricTimeSeriesTest {
  @Test
  public void testSimple() throws Exception {
    List<String> names = Lists.newArrayList("metric1", "metric2", "metric3", "metric4", "metric5");
    List<MetricType> types =
        Lists.newArrayList(MetricType.INT, MetricType.INT, MetricType.INT, MetricType.INT, MetricType.INT);
    MetricSchema schema = new MetricSchema(names, types);
    MetricTimeSeries series = new MetricTimeSeries(schema);
    long startHourSinceEpoch = TimeUnit.HOURS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Random rand = new Random();
    int NUM_TIME_WINDOWS = 100;
    int[][] data = new int[NUM_TIME_WINDOWS][];
    for (int i = 0; i < NUM_TIME_WINDOWS; i++) {
      data[i] = new int[names.size()];
      for (int j = 0; j < names.size(); j++) {
        String name = names.get(j);
        int value = Math.abs(rand.nextInt(5000));
        data[i][j] = value;
        series.set(startHourSinceEpoch + i, name, value);
      }
    }
    // serialize to bytes
    byte[] bytes = series.toBytes();
    MetricTimeSeries newSeries = MetricTimeSeries.fromBytes(bytes, schema);
    Set<Long> timeWindowSet = newSeries.getTimeWindowSet();
    byte[] newBytes = newSeries.toBytes();
    Assert.assertEquals(bytes.length, newBytes.length);
    for (long timeWindow : timeWindowSet) {
      for (String name : names) {
        Number m1 = series.get(timeWindow, name);
        Number m2 = newSeries.get(timeWindow, name);
        Assert.assertEquals(m1, m2);
      }
    }

    MetricTimeSeries aggSeries = new MetricTimeSeries(schema);
    aggSeries.aggregate(series);
    aggSeries.aggregate(newSeries);
    Assert.assertEquals(aggSeries.getTimeWindowSet().size(), series.getTimeWindowSet().size());

    for (long timeWindow : series.getTimeWindowSet()) {
      for (int j = 0; j < names.size(); j++) {
        String name = names.get(j);
        int v1 = (Integer) series.get(timeWindow, name);
        int v2 = (Integer) newSeries.get(timeWindow, name);
        Assert.assertEquals(v1, v2);
        int v3 = (Integer) aggSeries.get(timeWindow, name);
        Assert.assertEquals(v3, v1 + v2);
      }

    }

  }
}
