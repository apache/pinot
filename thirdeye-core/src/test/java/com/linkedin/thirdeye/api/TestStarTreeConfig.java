package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordThresholdFunctionAbsImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Properties;

public class TestStarTreeConfig
{
  @Test
  public void testBuild() throws Exception
  {
    // Builder
    StarTreeConfig.Builder builder = new StarTreeConfig.Builder();
    builder.setCollection("myCollection")
           .setDimensionNames(Arrays.asList("A", "B", "C"))
           .setMetricNames(Arrays.asList("M"))
           .setTimeColumnName("T")
           .setMaxRecordStoreEntries(1000)
           .setRecordStoreFactoryClass(StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName())
           .setRecordStoreFactoryConfig(new Properties())
           .setThresholdFunctionClass(StarTreeRecordThresholdFunctionAbsImpl.class.getCanonicalName())
           .setThresholdFunctionConfig(new Properties());
    Assert.assertEquals(builder.getCollection(), "myCollection");
    Assert.assertEquals(builder.getDimensionNames(), Arrays.asList("A", "B", "C"));
    Assert.assertEquals(builder.getMetricNames(), Arrays.asList("M"));
    Assert.assertEquals(builder.getTimeColumnName(), "T");
    Assert.assertEquals(builder.getMaxRecordStoreEntries(), 1000);
    Assert.assertEquals(builder.getRecordStoreFactoryClass(), StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName());
    Assert.assertEquals(builder.getThresholdFunctionClass(), StarTreeRecordThresholdFunctionAbsImpl.class.getCanonicalName());

    // Built config
    StarTreeConfig config = builder.build();
    Assert.assertEquals(config.getCollection(), "myCollection");
    Assert.assertEquals(config.getDimensionNames(), Arrays.asList("A", "B", "C"));
    Assert.assertEquals(config.getMetricNames(), Arrays.asList("M"));
    Assert.assertEquals(config.getTimeColumnName(), "T");
    Assert.assertEquals(config.getMaxRecordStoreEntries(), 1000);
    Assert.assertTrue(config.getThresholdFunction() instanceof StarTreeRecordThresholdFunctionAbsImpl);
    Assert.assertTrue(config.getRecordStoreFactory() instanceof StarTreeRecordStoreFactoryLogBufferImpl);
  }

  @Test
  public void testFromJson() throws Exception
  {
    JsonNode jsonNode = new ObjectMapper().readTree(ClassLoader.getSystemResourceAsStream("SampleConfig.json"));
    StarTreeConfig config = StarTreeConfig.fromJson(jsonNode);
    Assert.assertEquals(config.getCollection(), "myCollection");
    Assert.assertEquals(config.getDimensionNames(), Arrays.asList("A", "B", "C"));
    Assert.assertEquals(config.getMetricNames(), Arrays.asList("M"));
    Assert.assertEquals(config.getTimeColumnName(), "T");
    Assert.assertTrue(config.getThresholdFunction() instanceof StarTreeRecordThresholdFunctionAbsImpl);
    Assert.assertTrue(config.getRecordStoreFactory() instanceof StarTreeRecordStoreFactoryLogBufferImpl); // default
  }
}
