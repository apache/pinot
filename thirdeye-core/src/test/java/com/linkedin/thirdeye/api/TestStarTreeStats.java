package com.linkedin.thirdeye.api;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

public class TestStarTreeStats
{
  @Test
  public void testStats() throws Exception
  {
    StarTreeStats stats = new StarTreeStats(Arrays.asList("A", "B", "C"), Arrays.asList("M"), "T");

    stats.countNode();
    stats.countLeaf();
    stats.countBytes(1024);
    stats.countRecords(100);
    stats.updateMaxTime(2000L);
    stats.updateMaxTime(1000L);
    stats.updateMinTime(1000L);
    stats.updateMinTime(2000L);

    Assert.assertEquals(stats.getNodeCount(), 1);
    Assert.assertEquals(stats.getLeafCount(), 1);
    Assert.assertEquals(stats.getDimensionNames(), Arrays.asList("A", "B", "C"));
    Assert.assertEquals(stats.getMetricNames(), Arrays.asList("M"));
    Assert.assertEquals(stats.getTimeColumnName(), "T");
    Assert.assertEquals(stats.getMinTime(), 1000L);
    Assert.assertEquals(stats.getMaxTime(), 2000L);
    Assert.assertEquals(stats.getByteCount(), 1024);
  }
}
