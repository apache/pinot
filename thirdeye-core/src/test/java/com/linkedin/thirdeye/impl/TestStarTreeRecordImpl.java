package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;

public class TestStarTreeRecordImpl
{
  private StarTreeRecord record;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    record = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0")
            .setMetricValue("M", 1)
            .setMetricType("M", MetricType.INT)
            .setTime(100L)
            .build();
  }

  @Test
  public void testGetKey() throws Exception
  {
    Assert.assertEquals(record.getKey(true), record.getDimensionValues() + "@" + 100L);
    Assert.assertEquals(record.getKey(false), record.getDimensionValues().toString());
  }

  @Test
  public void testCopy() throws Exception
  {
    StarTreeRecord copy = record.copy(true);
    Assert.assertEquals(copy, record);
    Assert.assertFalse(copy == record);
  }

  @Test
  public void testAliasOther() throws Exception
  {
    StarTreeRecord aliased = record.aliasOther("A");
    Assert.assertEquals(aliased.getDimensionValues().get("A"), StarTreeConstants.OTHER);
  }

  @Test
  public void testUpdateDimensionValues() throws Exception
  {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder()
            .setDimensionValue("A", "A0")
            .setDimensionValue("B", "B0")
            .setDimensionValue("C", "C0");

    // Update with same, should retain
    builder.updateDimensionValues(Collections.singletonMap("A", "A0"));
    Assert.assertEquals(builder.getDimensionValues().get("A"), "A0");

    // Update with another, should go to star
    builder.updateDimensionValues(Collections.singletonMap("A", "A1"));
    Assert.assertEquals(builder.getDimensionValues().get("A"), StarTreeConstants.STAR);
  }
}
