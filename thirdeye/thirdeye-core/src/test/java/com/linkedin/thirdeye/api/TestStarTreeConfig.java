package com.linkedin.thirdeye.api;

import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;
import com.linkedin.thirdeye.impl.storage.StarTreeRecordStoreFactoryDefaultImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestStarTreeConfig
{
  private static final List<DimensionSpec> DIMENSION_SPECS = Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C"));

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
           .setDimensions(Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C")))
            .setMetrics(Arrays.asList(new MetricSpec("M", MetricType.INT)))
           .setTime(timeSpec)
           .setSplit(new SplitSpec(1000, null))
           .setRecordStoreFactoryClass(StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName())
           .setRecordStoreFactoryConfig(new Properties());
    Assert.assertEquals(builder.getCollection(), "myCollection");
    Assert.assertEquals(builder.getDimensions(), DIMENSION_SPECS);
    Assert.assertEquals(builder.getMetrics(), Arrays.asList(new MetricSpec("M", MetricType.INT)));
    Assert.assertEquals(builder.getTime().getColumnName(), "T");
    Assert.assertEquals(builder.getSplit().getThreshold(), 1000);
    Assert.assertEquals(builder.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName());

    // Built config
    StarTreeConfig config = builder.build();
    Assert.assertEquals(config.getCollection(), "myCollection");
    Assert.assertEquals(config.getDimensions(), DIMENSION_SPECS);
    Assert.assertEquals(config.getMetrics(), Arrays.asList(new MetricSpec("M", MetricType.INT)));
    Assert.assertEquals(config.getTime().getColumnName(), "T");
    Assert.assertEquals(config.getSplit().getThreshold(), 1000);
    Assert.assertEquals(config.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName());
  }

  @Test
  public void testFromJson() throws Exception
  {
    StarTreeConfig config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("SampleConfig.json"));
    Assert.assertEquals(config.getCollection(), "myCollection");
    Assert.assertEquals(config.getDimensions(), DIMENSION_SPECS);
    Assert.assertEquals(config.getMetrics(), Arrays.asList(new MetricSpec("M", MetricType.INT)));
    Assert.assertEquals(config.getTime().getColumnName(), "T");
    Assert.assertEquals(config.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryDefaultImpl.class.getCanonicalName());
  }

  @Test
  public void testMissingRequired() throws Exception
  {
    String collection = "myCollection";
    List<DimensionSpec> dimensionSpecs = Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C"));
    List<MetricSpec> metricSpecs = Arrays.asList(new MetricSpec("M", MetricType.INT));

    StarTreeConfig.Builder builder = new StarTreeConfig.Builder();

    // Missing collection
    builder.setDimensions(dimensionSpecs).setMetrics(metricSpecs);
    try { builder.build(); Assert.fail(); } catch (Exception e) { /* Good */ }
    builder.setDimensions(null).setMetrics(null);

    // Missing dimension names
    builder.setCollection(collection).setMetrics(metricSpecs);
    try { builder.build(); Assert.fail(); } catch (Exception e) { /* Good */ }
    builder.setCollection(null).setMetrics(null);

    // Missing metric names
    builder.setCollection(collection).setDimensions(dimensionSpecs);
    try { builder.build(); Assert.fail(); } catch (Exception e) { /* Good */ }
    builder.setCollection(null).setDimensions(null);
  }

  @Test
  public void testFromYaml() throws Exception
  {
    StarTreeConfig config = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("sample-config.yml"));
    Assert.assertEquals(config.getCollection(), "myCollection");
    Assert.assertEquals(config.getDimensions(), DIMENSION_SPECS);
    Assert.assertEquals(config.getMetrics(), Arrays.asList(new MetricSpec("M", MetricType.INT)));
    Assert.assertEquals(config.getTime().getColumnName(), "T");
    Assert.assertEquals(config.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryDefaultImpl.class.getCanonicalName());
  }
}
