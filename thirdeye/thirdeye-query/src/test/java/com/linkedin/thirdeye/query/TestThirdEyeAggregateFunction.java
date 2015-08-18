package com.linkedin.thirdeye.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class TestThirdEyeAggregateFunction {
  private StarTreeConfig config;
  private MetricSchema metricSchema;
  private MetricTimeSeries timeSeries;
  private ThirdEyeQuery query;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("test-config.yml"));
    metricSchema = new MetricSchema(ImmutableList.of("L", "D"), ImmutableList.of(MetricType.LONG, MetricType.DOUBLE));
    timeSeries = new MetricTimeSeries(metricSchema);
    for (int i = 0; i < 16; i++) {
      timeSeries.increment(i, "L", 1);
      timeSeries.increment(i, "D", 1.0);
    }
    query = new ThirdEyeQuery();
    query.addMetricName("L");
    query.addMetricName("D");
  }

  @Test
  public void testValid_simple() {
    MetricTimeSeries derived = new ThirdEyeAggregateFunction(
        new TimeGranularity(4, TimeUnit.HOURS)).apply(config, query, timeSeries);
    Assert.assertEquals(derived.getTimeWindowSet().size(), 4);

    for (long i = 0; i < 16; i += 4) {
      Assert.assertEquals(derived.get(i, "L"), 4L);
      Assert.assertEquals(derived.get(i, "D"), 4.0);
    }
  }
}
