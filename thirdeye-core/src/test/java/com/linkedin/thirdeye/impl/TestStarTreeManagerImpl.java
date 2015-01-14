package com.linkedin.thirdeye.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestStarTreeManagerImpl
{
  private File baseDir;
  private File collectionDir;
  private File rootDir;
  private StarTreeManager starTreeManager;
  private StarTreeConfig config;
  private MetricSchema metricSchema;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    baseDir = new File(System.getProperty("java.io.tmpdir"), TestStarTreeManagerImpl.class.getSimpleName());
    collectionDir = new File(baseDir, "myCollection");
    rootDir = new File(collectionDir, "data");

    TimeSpec timeSpec = new TimeSpec("hoursSinceEpoch",
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(128, TimeUnit.HOURS));


    StarTreeConfig config = new StarTreeConfig.Builder()
            .setCollection("myCollection")
            .setDimensions(Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C")))
            .setMetrics(Arrays.asList(new MetricSpec("M", MetricType.INT)))
            .setTime(timeSpec)
            .setRecordStoreFactoryClass(StarTreeRecordStoreFactoryCircularBufferImpl.class.getCanonicalName())
            .build();

    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());

    FileUtils.forceMkdir(baseDir);
    FileUtils.forceMkdir(rootDir);

    UUID nodeId = UUID.randomUUID();

    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    for (int i = 0; i < 100; i++)
    {
      MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
      ts.set(i, "M", 1);

      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setDimensionKey(getDimensionKey("A" + (i % 2), "B" + (i % 4), "C" + (i % 8)));
      builder.setMetricTimeSeries(ts);
      records.add(builder.build(config));
    }

    // Create a forward index
    int currentValueId = StarTreeConstants.FIRST_VALUE;
    Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
    for (String dimensionName : Arrays.asList("A", "B", "C"))
    {
      forwardIndex.put(dimensionName, new HashMap<String, Integer>());
      for (int i = 0; i < 8; i++)
      {
        forwardIndex.get(dimensionName).put(dimensionName + i, currentValueId++);
      }
      forwardIndex.get(dimensionName).put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
      forwardIndex.get(dimensionName).put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
    }

    // Write store buffer
    OutputStream outputStream = new FileOutputStream(new File(rootDir, nodeId + ".buf"));
    StarTreeRecordStoreCircularBufferImpl.fillBuffer(
            outputStream,
            config,
            forwardIndex,
            records,
            128,
            true);
    outputStream.flush();
    outputStream.close();

    // Write index
    outputStream = new FileOutputStream(new File(rootDir, nodeId + ".idx"));
    new ObjectMapper().writeValue(outputStream, forwardIndex);
    outputStream.flush();
    outputStream.close();

    // Create a tree with just that record store at root
    StarTree starTree = new StarTreeImpl(config, rootDir, new StarTreeNodeImpl(
            nodeId,
            StarTreeConstants.STAR,
            StarTreeConstants.STAR,
            new ArrayList<String>(),
            new HashMap<String, String>(),
            new HashMap<String, StarTreeNode>(),
            null,
            null));

    // tree.bin
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(new File(collectionDir, StarTreeConstants.TREE_FILE_NAME)));
    objectOutputStream.writeObject(starTree.getRoot());
    objectOutputStream.flush();
    objectOutputStream.close();

    // config.json
    outputStream = new FileOutputStream(new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME));
    outputStream.write(config.encode().getBytes());
    outputStream.flush();
    outputStream.close();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    if (baseDir.exists())
    {
      FileUtils.forceDelete(baseDir);
    }
  }

  @BeforeMethod
  public void beforeMethod() throws Exception
  {
    List<String> dimensionNames = Arrays.asList("A", "B", "C");

    TimeSpec timeSpec = new TimeSpec("hoursSinceEpoch",
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(1, TimeUnit.HOURS),
                                     new TimeGranularity(128, TimeUnit.HOURS));

    starTreeManager = new StarTreeManagerImpl(Executors.newSingleThreadExecutor(), baseDir);
    config = new StarTreeConfig.Builder()
            .setCollection("myCollection")
            .setMetrics(Arrays.asList(new MetricSpec("M", MetricType.INT)))
            .setDimensions(Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C")))
            .setTime(timeSpec)
            .build();
  }

  @AfterMethod
  public void afterMethod() throws Exception
  {
    if (starTreeManager != null)
    {
      for (String collection : starTreeManager.getCollections())
      {
        starTreeManager.close(collection);
      }
    }
  }

  @Test
  public void testRegisterConfig() throws Exception
  {
    starTreeManager.registerConfig("myCollection", config);
    Assert.assertNotNull(starTreeManager.getConfig("myCollection"));
    Assert.assertNull(starTreeManager.getConfig("yourCollection"));
  }

  @Test
  public void testRestore() throws Exception
  {
    // Restore tree
    starTreeManager.registerConfig(config.getCollection(), config);
    starTreeManager.restore(baseDir, config.getCollection());
    starTreeManager.open(config.getCollection());
    StarTree starTree = starTreeManager.getStarTree(config.getCollection());

    // Query and ensure data restored
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey("*", "*", "*"))
            .build(config);
    StarTreeRecord result = starTree.getAggregate(query);
    Assert.assertEquals(result.getMetricTimeSeries().getMetricSums()[0].intValue(), 100);
  }

  @Test
  public void testStub() throws Exception
  {
    // Stub tree
    starTreeManager.registerConfig(config.getCollection(), config);
    starTreeManager.stub(baseDir, config.getCollection());
    starTreeManager.open(config.getCollection());
    StarTree starTree = starTreeManager.getStarTree(config.getCollection());

    // Query and ensure no data
    StarTreeQuery query = new StarTreeQueryImpl.Builder()
            .setDimensionKey(getDimensionKey("*", "*", "*"))
            .build(config);
    StarTreeRecord result = starTree.getAggregate(query);
    Assert.assertEquals(result.getMetricTimeSeries().getMetricSums()[0].intValue(), 0);
  }

  private DimensionKey getDimensionKey(String a, String b, String c)
  {
    return new DimensionKey(new String[] {a, b, c});
  }
}
