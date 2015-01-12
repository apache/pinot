package com.linkedin.thirdeye.api;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestStarTreeStats
{
  private static final List<DimensionSpec> DIMENSION_SPECS = Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C"));

  @Test
  public void testList(){
    Collection<File> listFiles = FileUtils.listFiles(new File("."), FileFileFilter.FILE, DirectoryFileFilter.DIRECTORY);
    for(File f: listFiles){
      System.out.println(f.getAbsolutePath());
    }
    UUID orig = UUID.randomUUID();
    System.out.println(orig);
    byte bytes[] = orig.toString().getBytes();
    System.out.println(new String(bytes));
    UUID temp = UUID.fromString("4c200d28-2c3d-4a6b-aecb-57ba8715be1d");
    Long.parseLong("57ba8715be1d", 16);
    System.out.println(temp);
  }
  @Test
  public void testStats() throws Exception
  {
    StarTreeStats stats = new StarTreeStats(
            Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C")),
            Arrays.asList(new MetricSpec("M", MetricType.INT)), "T", 1, TimeUnit.HOURS);

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
    Assert.assertEquals(stats.getDimensions(), DIMENSION_SPECS);
    Assert.assertEquals(stats.getMetrics(), Arrays.asList(new MetricSpec("M", MetricType.INT)));

    Assert.assertEquals(stats.getTimeColumnName(), "T");
    Assert.assertEquals(stats.getMinTime(), 1000L);
    Assert.assertEquals(stats.getMaxTime(), 2000L);
    Assert.assertEquals(stats.getByteCount(), 1024);
  }
}
