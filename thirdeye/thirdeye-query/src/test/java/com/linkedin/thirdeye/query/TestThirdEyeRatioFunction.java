package com.linkedin.thirdeye.query;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestThirdEyeRatioFunction {
  private StarTreeConfig config;
  private MetricSchema metricSchema;
  private MetricTimeSeries timeSeries;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("test-config.yml"));
    metricSchema = new MetricSchema(ImmutableList.of("L", "D"), ImmutableList.of(MetricType.LONG, MetricType.DOUBLE));
    timeSeries = new MetricTimeSeries(metricSchema);
    for (int i = 0; i < 16; i++) {
      timeSeries.increment(i, "L", 1);
      timeSeries.increment(i, "D", 4);
    }
  }

  @Test
  public void testValid_simple() {
    MetricTimeSeries derived = new ThirdEyeRatioFunction(ImmutableList.of("L", "D")).apply(config, null, timeSeries);
    Assert.assertEquals(derived.getSchema().getNumMetrics(), 3); // contains originals
    for (int i = 0; i < 16; i++) {
      Assert.assertEquals(derived.get(i, "RATIO(L,D)").doubleValue(), 0.25);
    }
  }

  @Test
  public void testToString() {
    ThirdEyeFunction function = new ThirdEyeRatioFunction(ImmutableList.of("N", "D"));
    Assert.assertEquals(function.toString(), "RATIO(N,D)");
  }
}
