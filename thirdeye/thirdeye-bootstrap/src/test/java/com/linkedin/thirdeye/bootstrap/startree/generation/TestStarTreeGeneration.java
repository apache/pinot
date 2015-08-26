package com.linkedin.thirdeye.bootstrap.startree.generation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationJob.StarTreeGenerationMapper;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
*
* @author ajaspal
*
*/

public class TestStarTreeGeneration
{
  private static final String CONF_FILE = "config.yml";
  private static MapDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> mapDriver;
  private static StarTreeGenerationConfig starTreeGenerationConfig;
  private static String thirdEyeRoot;

  private List<Pair<BytesWritable,  BytesWritable>> generateTestData() throws IOException
  {
    List<Pair<BytesWritable,  BytesWritable>> inputRecords = new ArrayList<Pair<BytesWritable,  BytesWritable>>();
    String []combination1 = {"A1", "B1", "C1"};
    DimensionKey dimKey = new DimensionKey(combination1);
    MetricTimeSeries timeSeries = TestHelper.generateRandomMetricTimeSeries(starTreeGenerationConfig);
    Pair<BytesWritable,  BytesWritable> record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination2 = {"A1", "B1", "C2"};
    dimKey = new DimensionKey(combination2);
    timeSeries = TestHelper.generateRandomMetricTimeSeries(starTreeGenerationConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination3 = {"A1", "B1", "C3"};
    dimKey = new DimensionKey(combination3);
    timeSeries = TestHelper.generateRandomMetricTimeSeries(starTreeGenerationConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination4 = {"A2", "B1", "C3"};
    dimKey = new DimensionKey(combination4);
    timeSeries = TestHelper.generateRandomMetricTimeSeries(starTreeGenerationConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);
    return inputRecords;
  }

  @BeforeSuite
  public void setUp() throws IOException
  {
    StarTreeGenerationMapper mapper = new StarTreeGenerationMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration config = mapDriver.getConfiguration();
    config.set(StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
    Path configPath = new Path(ClassLoader.getSystemResource(CONF_FILE).toString());
    FileSystem fs = FileSystem.get(config);
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fs.open(configPath));
    starTreeGenerationConfig = StarTreeGenerationConfig.fromStarTreeConfig(starTreeConfig);
    thirdEyeRoot = System.getProperty("java.io.tmpdir") ;
    config.set(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(), thirdEyeRoot + File.separator + "startree_generation");
  }

  @Test
  public void testStarTreeGeneration() throws Exception
  {
    List<Pair<BytesWritable,  BytesWritable>> input = generateTestData();
    mapDriver.addAll(input);
    mapDriver.run();

    // verify that the tree can be deserialized appropriately
    String starTreeOutputPath = mapDriver.getConfiguration().get(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString());
    String collectionName = starTreeGenerationConfig.getCollectionName();
    Path pathToTree = new Path(starTreeOutputPath,  "tree.bin");
    FileSystem dfs = FileSystem.get(mapDriver.getConfiguration());
    InputStream is = dfs.open(pathToTree);
    StarTreeNode starTreeRootNode = StarTreePersistanceUtil.loadStarTree(is);
    Assert.assertNotNull(starTreeRootNode);
    Assert.assertFalse(starTreeRootNode.isLeaf());

    LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
    Assert.assertEquals(5, leafNodes.size());
  }

  @AfterSuite
  public void deleteStarTree() throws IOException
  {
    File f = new File(thirdEyeRoot + File.separator + "startree_generation");
    FileUtils.deleteDirectory(f);
  }
}

class TestHelper {

  public static MetricTimeSeries generateRandomMetricTimeSeries(StarTreeGenerationConfig config)
  {
    List<String> names = config.getMetricNames();
    List<MetricType> types = config.getMetricTypes();
    MetricSchema schema = new MetricSchema(names, types);
    MetricTimeSeries series = new MetricTimeSeries(schema);
    long timeStamp = TimeUnit.HOURS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    RandomDataGenerator rand = new RandomDataGenerator();
    for(int i = 0;i<names.size();i++){
      series.set(timeStamp, names.get(i), rand.nextInt(0, 100));
    }
    return series;
  }

  public static MetricSchema getMetricSchema(StarTreeGenerationConfig config)
  {
    List<String> names = config.getMetricNames();
    List<MetricType> types = config.getMetricTypes();
    return new MetricSchema(names, types);
  }
}
