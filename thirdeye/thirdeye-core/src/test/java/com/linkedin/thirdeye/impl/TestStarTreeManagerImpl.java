package com.linkedin.thirdeye.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.storage.IndexFormat;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;

public class TestStarTreeManagerImpl {

  private File rootDir;
  private String collection;
  private File collectionDir;
  private StarTreeConfig baseConfig;
  private StarTreeConfig config;
  private MetricSchema metricSchema;
  private StarTree starTree;
  ObjectMapper objectMapper;
  Map<File, StarTree> starTrees;
  Properties properties;
  private StarTreeManager manager;
  private long timeout = 25000;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    rootDir = new File(System.getProperty("java.io.tmpdir"), TestStarTreeImpl.class.getName());
    baseConfig = StarTreeConfig.decode(ClassLoader.getSystemResourceAsStream("sample-config.yml"));
    collection = baseConfig.getCollection();
    collectionDir = new File(rootDir, collection);
    try { FileUtils.forceDelete(collectionDir); } catch (Exception e) { /* ok */ }
    try { FileUtils.forceMkdir(collectionDir); } catch (Exception e) { /* ok */ }

    SplitSpec split = new SplitSpec(5, baseConfig.getSplit().getOrder());
    config = new StarTreeConfig(
        baseConfig.getCollection(),
        StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName(),
        new Properties(),
        baseConfig.getAnomalyDetectionFunctionClass(),
        baseConfig.getAnomalyDetectionFunctionConfig(),
        baseConfig.getAnomalyHandlerClass(),
        baseConfig.getAnomalyHandlerConfig(),
        baseConfig.getAnomalyDetectionMode(),
        baseConfig.getDimensions(),
        baseConfig.getMetrics(),
        baseConfig.getTime(),
        baseConfig.getJoinSpec(),
        baseConfig.getRollup(),
        split,
        false);

    // create config file
    File configFile = new File(collectionDir, "config.yml");
    objectMapper = new ObjectMapper();
    objectMapper.writeValue(configFile, config);

    // create star tree
    starTree = new StarTreeImpl(config);
    starTree.open();
    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    MetricTimeSeries ts = new MetricTimeSeries(metricSchema);
    ts.set(0, "M", 1);
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
    builder.setDimensionKey(getDimensionKey("A", "B", "C"));
    builder.setMetricTimeSeries(ts);
    starTree.add(builder.build(config));
    starTree.close();

    // create metadata
    IndexMetadata indexMetadata = new IndexMetadata(0L, 1L, 0L, 1L, 0L, 1L, 0L, 1L, "HOURLY", "MILLISECONDS", 1, IndexFormat.VARIABLE_SIZE);
    properties = indexMetadata.toProperties();

  }

  @Test
  void testDataRefresh() throws Exception {

    manager = new StarTreeManagerImpl();
    manager.restore(rootDir, collection);

    starTrees = manager.getStarTrees(collection);
    Assert.assertEquals(starTrees.size(), 0);

    // create data directory
    File dataDir1 = new File(collectionDir, "data_1");
    StarTreePersistanceUtil.saveTree(starTree, dataDir1.getPath());
    FileUtils.moveFile(new File(dataDir1, collection+"-tree.bin"), new File(dataDir1, StarTreeConstants.TREE_FILE_NAME));
    properties.store(new FileOutputStream(new File(dataDir1, StarTreeConstants.METADATA_FILE_NAME)), "properties file");

    pollForStarTreesAddition(dataDir1);

    Assert.assertEquals("starTrees not updated", starTrees.size(), 1);
    Assert.assertTrue(starTrees.containsKey(dataDir1));

    // delete segment
    FileUtils.deleteDirectory(dataDir1);
    // create new data directory
    File dataDir2 = new File(collectionDir, "data_2");
    StarTreePersistanceUtil.saveTree(starTree, dataDir2.getPath());
    FileUtils.moveFile(new File(dataDir2, collection+"-tree.bin"), new File(dataDir2, StarTreeConstants.TREE_FILE_NAME));
    properties.store(new FileOutputStream(new File(dataDir2, StarTreeConstants.METADATA_FILE_NAME)), "properties file");

    pollForStarTreesAddition(dataDir2);

    Assert.assertEquals(starTrees.size(), 1);
    Assert.assertFalse("starTrees did not delete old tree", starTrees.containsKey(dataDir1));
    Assert.assertTrue(starTrees.containsKey(dataDir2));

    FileUtils.deleteDirectory(dataDir1);

  }


  @AfterClass
  public void afterClass() throws Exception
  {
    starTree.close();
    try { FileUtils.forceDelete(rootDir); } catch (Exception e) { /* ok */ }
  }

  private DimensionKey getDimensionKey(String a, String b, String c)
  {
    return new DimensionKey(new String[] {a, b, c});
  }

  private void pollForStarTreesAddition(File dataDir) throws InterruptedException {

    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < timeout && !starTrees.containsKey(dataDir)) {
      TimeUnit.MILLISECONDS.sleep(1000);
    }
    Assert.assertTrue("Watch system did not trigger", starTrees.containsKey(dataDir));
  }

}
