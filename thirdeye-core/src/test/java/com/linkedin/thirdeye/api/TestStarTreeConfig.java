package com.linkedin.thirdeye.api;

import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestStarTreeConfig
{
  @Test
  public void testBuild() throws Exception
  {
    TimeSpec timeSpec = new TimeSpec("T",
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(128, TimeUnit.HOURS));

    // Builder
    StarTreeConfig.Builder builder = new StarTreeConfig.Builder();
    builder.setCollection("myCollection")
           .setDimensionNames(Arrays.asList("A", "B", "C"))
           .setMetricNames(Arrays.asList("M"))
           .setMetricTypes(Arrays.asList("INT"))
           .setTime(timeSpec)
           .setSplit(new SplitSpec(1000, null))
           .setRecordStoreFactoryClass(StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName())
           .setRecordStoreFactoryConfig(new Properties());
    Assert.assertEquals(builder.getCollection(), "myCollection");
    Assert.assertEquals(builder.getDimensionNames(), Arrays.asList("A", "B", "C"));
    Assert.assertEquals(builder.getMetricNames(), Arrays.asList("M"));
    Assert.assertEquals(builder.getTime().getColumnName(), "T");
    Assert.assertEquals(builder.getSplit().getThreshold(), 1000);
    Assert.assertEquals(builder.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName());

    // Built config
    StarTreeConfig config = builder.build();
    Assert.assertEquals(config.getCollection(), "myCollection");
    Assert.assertEquals(config.getDimensionNames(), Arrays.asList("A", "B", "C"));
    Assert.assertEquals(config.getMetricNames(), Arrays.asList("M"));
    Assert.assertEquals(config.getTime().getColumnName(), "T");
    Assert.assertEquals(config.getSplit().getThreshold(), 1000);
    Assert.assertEquals(config.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName());
  }

  @Test
  public void testFromJson() throws Exception
  {
    StarTreeConfig config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("SampleConfig.json"));
    Assert.assertEquals(config.getCollection(), "myCollection");
    Assert.assertEquals(config.getDimensionNames(), Arrays.asList("A", "B", "C"));
    Assert.assertEquals(config.getMetricNames(), Arrays.asList("M"));
    Assert.assertEquals(config.getTime().getColumnName(), "T");
    Assert.assertEquals(config.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryCircularBufferImpl.class.getCanonicalName());
  }

  @Test
  public void testMissingRequired() throws Exception
  {
    String collection = "myCollection";
    List<String> dimensionNames = Arrays.asList("A", "B", "C");
    List<String> metricNames = Arrays.asList("M");

    StarTreeConfig.Builder builder = new StarTreeConfig.Builder();

    // Missing collection
    builder.setDimensionNames(dimensionNames).setMetricNames(metricNames);
    try { builder.build(); Assert.fail(); } catch (Exception e) { /* Good */ }
    builder.setDimensionNames(null).setMetricNames(null);

    // Missing dimension names
    builder.setCollection(collection).setMetricNames(metricNames);
    try { builder.build(); Assert.fail(); } catch (Exception e) { /* Good */ }
    builder.setCollection(null).setMetricNames(null);

    // Missing metric names
    builder.setCollection(collection).setDimensionNames(dimensionNames);
    try { builder.build(); Assert.fail(); } catch (Exception e) { /* Good */ }
    builder.setCollection(null).setDimensionNames(null);
  }
}
