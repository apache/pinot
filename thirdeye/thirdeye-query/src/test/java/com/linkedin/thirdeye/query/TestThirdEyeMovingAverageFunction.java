package com.linkedin.thirdeye.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class TestThirdEyeMovingAverageFunction {
  private StarTreeConfig config;
  private MetricSchema metricSchema;
  private ThirdEyeQuery query;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("test-config.yml"));
    metricSchema = new MetricSchema(ImmutableList.of("L", "D"), ImmutableList.of(MetricType.LONG, MetricType.DOUBLE));
    query = new ThirdEyeQuery();
    query.addMetricName("L");
  }

  @Test
  public void testValidWindow_simple() {
    MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
    for (int i = 0; i < 21; i++) {
      timeSeries.increment(i, "L", 1);
      timeSeries.increment(i, "D", 1.0);
    }

    MetricTimeSeries derived = new ThirdEyeMovingAverageFunction(
        new TimeGranularity(7, TimeUnit.HOURS)).apply(config, query, timeSeries);

    Assert.assertEquals(derived.getTimeWindowSet().size(), 14);
    Assert.assertEquals(derived.getSchema().getNames().size(), 2);

    System.out.println(derived);

    for (Long time : derived.getTimeWindowSet()) {
      Assert.assertEquals(derived.get(time, "L").doubleValue(), 1.0, 0.001);
    }
  }

}
