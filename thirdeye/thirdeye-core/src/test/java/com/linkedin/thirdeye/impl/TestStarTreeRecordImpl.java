package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;

public class TestStarTreeRecordImpl {
  private StarTreeConfig config;
  private StarTreeRecord record;

  @BeforeClass
  public void beforeClass() throws Exception {
    config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("sample-config.yml"));

    MetricTimeSeries timeSeries =
        new MetricTimeSeries(MetricSchema.fromMetricSpecs(config.getMetrics()));
    timeSeries.set(100L, "M", 1);

    record = new StarTreeRecordImpl.Builder().setDimensionKey(getDimensionKey("A0", "B0", "C0"))
        .setMetricTimeSeries(timeSeries).build(config);
  }

  @Test
  public void testAliasOther() throws Exception {
    StarTreeRecord aliased = record.aliasOther("A");
    Assert.assertEquals(aliased.getDimensionKey().getDimensionValue(config.getDimensions(), "A"),
        StarTreeConstants.OTHER);
  }

  @Test
  public void testUpdateDimensionValues() throws Exception {
    StarTreeRecordImpl.Builder builder =
        new StarTreeRecordImpl.Builder().setDimensionKey(getDimensionKey("A0", "B0", "C0"));

    // Update with same, should retain
    builder.updateDimensionKey(getDimensionKey("A0", "B0", "C0"));
    Assert.assertEquals(builder.getDimensionKey().getDimensionValue(config.getDimensions(), "A"),
        "A0");

    // Update with another, should go to star
    builder.updateDimensionKey(getDimensionKey("A1", "B0", "C0"));
    Assert.assertEquals(builder.getDimensionKey().getDimensionValue(config.getDimensions(), "A"),
        StarTreeConstants.STAR);
  }

  private DimensionKey getDimensionKey(String a, String b, String c) {
    return new DimensionKey(new String[] {
        a, b, c
    });
  }
}
