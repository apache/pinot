package com.linkedin.thirdeye.bootstrap.rollup;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoReduceOutput;

public class RollupPhaseTwoReduceOutputTest {
  @Test
  public void simple() throws IOException {
    List<String> names = Lists.newArrayList("metric1", "metric2", "metric3",
        "metric4", "metric5");
    String[] rawDimensionValues = new String[] { "dim1", "dim2", "dim3",
        "dim4", "dim5", "dim6", "dim7", "dim8" };
    String[] rollupDimensionValues = new String[] { "dim1", "?", "dim3",
        "dim4", "dim5", "dim6", "?", "dim8" };
    DimensionKey rawDimensionKey = new DimensionKey(rawDimensionValues);

    DimensionKey rollupDimensionKey = new DimensionKey(rollupDimensionValues);
    List<MetricType> types = Lists.newArrayList(MetricType.INT, MetricType.INT,
        MetricType.INT, MetricType.INT, MetricType.INT);
    MetricSchema schema = new MetricSchema(names, types);

    long startHourSinceEpoch = TimeUnit.HOURS.convert(
        System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Random rand = new Random();
    int NUM_TIME_WINDOWS = 100;

    // rollup series
    MetricTimeSeries rollUpSeries = new MetricTimeSeries(schema);
    int[][] rollupData = new int[NUM_TIME_WINDOWS][];
    for (int i = 0; i < NUM_TIME_WINDOWS; i++) {
      rollupData[i] = new int[names.size()];
      for (int j = 0; j < names.size(); j++) {
        String name = names.get(j);
        int value = Math.abs(rand.nextInt(5000));
        rollupData[i][j] = value;
        rollUpSeries.set(startHourSinceEpoch + i, name, value);
      }
    }
    // raw series
    MetricTimeSeries rawSeries = new MetricTimeSeries(schema);
    int[][] rawData = new int[NUM_TIME_WINDOWS][];
    for (int i = 0; i < NUM_TIME_WINDOWS; i++) {
      rawData[i] = new int[names.size()];
      for (int j = 0; j < names.size(); j++) {
        String name = names.get(j);
        int value = Math.abs(rand.nextInt(5000));
        rawData[i][j] = value;
        rawSeries.set(startHourSinceEpoch + i, name, value);
      }
    }

    RollupPhaseTwoReduceOutput oldValue = new RollupPhaseTwoReduceOutput(
        rollupDimensionKey, rollUpSeries, rawDimensionKey, rawSeries);

    byte[] bytes = oldValue.toBytes();
    RollupPhaseTwoReduceOutput newValue = RollupPhaseTwoReduceOutput.fromBytes(
        bytes, schema);
    Assert.assertEquals(oldValue.getRollupDimensionKey(),
        newValue.getRollupDimensionKey());
    Assert.assertEquals(oldValue.getRawDimensionKey(),
        newValue.getRawDimensionKey());

    Assert.assertEquals(oldValue.getRawTimeSeries().getTimeWindowSet(),
        newValue.getRawTimeSeries().getTimeWindowSet());

    // Assert.assertEquals(oldValue.getRollupTimeSeries().,
    // newValue.getRollupTimeSeries());

  }
}
