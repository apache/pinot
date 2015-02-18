package com.linkedin.thirdeye.bootstrap.rollup;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
public class TestMultiMetricTotalAggregateBasedRollupFunction {

  private MetricTimeSeries generateSeries(){
    List<String> names = Lists.newArrayList("metric1", "metric2", "metric3",
        "metric4", "metric5");
    List<MetricType> types = Lists.newArrayList(MetricType.INT, MetricType.INT,
        MetricType.INT, MetricType.INT, MetricType.INT);
    MetricSchema schema = new MetricSchema(names, types);
    MetricTimeSeries series = new MetricTimeSeries(schema);
    long startHourSinceEpoch = TimeUnit.HOURS.convert(
        System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Random rand = new Random();
    int NUM_TIME_WINDOWS = 10;
    int[][] data = new int[NUM_TIME_WINDOWS][];
    for (int i = 0; i < NUM_TIME_WINDOWS; i++) {
      data[i] = new int[names.size()];
      for (int j = 0; j < names.size(); j++) {
        String name = names.get(j);
        int value = Math.abs(rand.nextInt(500));
        data[i][j] = value;
        series.set(startHourSinceEpoch + i, name, value);
      }
    }
    return series;
  }

  @Test
  public void test1() throws Exception{

    MetricTimeSeries series = generateSeries();
    Map<String, String> thresholdFuncParams = Maps.newHashMap();
    thresholdFuncParams.put("metricNames", "metric1,metric2");
    thresholdFuncParams.put("thresholdExpr", "(metric1 > 5000 || metric2 > 5000)");
    MultiMetricTotalAggregateBasedRollupFunction func = new MultiMetricTotalAggregateBasedRollupFunction(thresholdFuncParams);
    Assert.assertFalse(func.isAboveThreshold(series));
  }

  @Test
  public void test2() throws Exception{

    MetricTimeSeries series = generateSeries();
    Map<String, String> thresholdFuncParams = Maps.newHashMap();
    thresholdFuncParams.put("metricNames", "metric1,metric2,metric3");
    thresholdFuncParams.put("thresholdExpr", "(metric1 > 5000 || metric2 > 5000 || metric3 > 0)");
    MultiMetricTotalAggregateBasedRollupFunction func = new MultiMetricTotalAggregateBasedRollupFunction(thresholdFuncParams);
    Assert.assertTrue(func.isAboveThreshold(series));
  }
}
