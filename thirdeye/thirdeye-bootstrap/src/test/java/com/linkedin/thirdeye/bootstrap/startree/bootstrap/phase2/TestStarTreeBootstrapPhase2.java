package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.BootstrapPhaseMapOutputKey;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.BootstrapPhaseMapOutputValue;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoJob.BootstrapPhaseTwoMapper;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoJob.BootstrapPhaseTwoReducer;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationJob.StarTreeGenerationMapper;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
*
* @author ajaspal
*
*/
public class TestStarTreeBootstrapPhase2
{
  private static final String CONF_FILE = "config.yml";
  private static MapDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> mapDriver;
  private static ReduceDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> reduceDriver;
  private static StarTreeBootstrapPhaseTwoConfig starTreeBootstrapConfig;
  private static String thirdEyeRoot;

  private List<Pair<BytesWritable, BytesWritable>> generateTestDataMapper(StarTreeNode root) throws Exception
  {
    List<Pair<BytesWritable, BytesWritable>> inputRecords = new ArrayList<Pair<BytesWritable, BytesWritable>>();
    UUID uuid = null;
    LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, root);
    for (StarTreeNode node : leafNodes) {
      if(node.getDimensionValue().equals("C1")){
        uuid = node.getId();
        break;
      }
    }

    String []combination = {"A1", "B1", "C1"};
    DimensionKey key = new DimensionKey(combination);
    BootstrapPhaseMapOutputKey outputKey1 = new BootstrapPhaseMapOutputKey(uuid, key.toMD5());
    BootstrapPhaseMapOutputValue outputValue1 = new BootstrapPhaseMapOutputValue(key, TestStarTreeBootstrapPhase2.TestHelper.generateMetricTimeSeries(starTreeBootstrapConfig, 10));
    inputRecords.add(new Pair<BytesWritable, BytesWritable>(new BytesWritable(outputKey1.toBytes()), new BytesWritable(outputValue1.toBytes())));

    String []combination2 = {"A2", "B2", "C1"};
    key = new DimensionKey(combination2);
    BootstrapPhaseMapOutputKey outputKey2 = new BootstrapPhaseMapOutputKey(uuid, key.toMD5());
    BootstrapPhaseMapOutputValue outputValue2 = new BootstrapPhaseMapOutputValue(key, TestStarTreeBootstrapPhase2.TestHelper.generateMetricTimeSeries(starTreeBootstrapConfig, 10));
    inputRecords.add(new Pair<BytesWritable, BytesWritable>(new BytesWritable(outputKey2.toBytes()), new BytesWritable(outputValue2.toBytes())));

    return inputRecords;
  }

  private List<Pair<BytesWritable,List<BytesWritable>>> generateTestDataReducer(StarTreeNode root) throws IOException
  {
    List<Pair<BytesWritable,List<BytesWritable>>> inputRecords = new ArrayList<Pair<BytesWritable,List<BytesWritable>>>();
    List<BytesWritable> list = new ArrayList<BytesWritable>();

    UUID uuid = null;
    LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, root);
    for (StarTreeNode node : leafNodes) {
      if(node.getDimensionValue().equals("C1")){
        uuid = node.getId();
        break;
      }
    }

    String []combination1 = {"A1", "B1", "C1"};
    DimensionKey dimKey = new DimensionKey(combination1);
    BootstrapPhaseMapOutputValue outputValue1 = new BootstrapPhaseMapOutputValue(dimKey, TestStarTreeBootstrapPhase2.TestHelper.generateMetricTimeSeries(starTreeBootstrapConfig, 10));
    list.add(new BytesWritable(outputValue1.toBytes()));

    BootstrapPhaseMapOutputValue outputValue2 = new BootstrapPhaseMapOutputValue(dimKey, TestStarTreeBootstrapPhase2.TestHelper.generateMetricTimeSeries(starTreeBootstrapConfig, 10));
    list.add(new BytesWritable(outputValue2.toBytes()));

    inputRecords.add(new Pair<BytesWritable,List<BytesWritable>>(new BytesWritable(uuid.toString().getBytes()),list));
    return inputRecords;
  }


  private void generateAndSerializeStarTree() throws IOException
  {
    List<Pair<BytesWritable,  BytesWritable>> inputRecords = new ArrayList<Pair<BytesWritable,  BytesWritable>>();
    String []combination1 = {"A1", "B1", "C1"};
    DimensionKey dimKey = new DimensionKey(combination1);
    MetricTimeSeries timeSeries = TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
    Pair<BytesWritable,  BytesWritable> record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination2 = {"A1", "B1", "C2"};
    dimKey = new DimensionKey(combination2);
    timeSeries = TestStarTreeBootstrapPhase2.TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination3 = {"A1", "B1", "C3"};
    dimKey = new DimensionKey(combination3);
    timeSeries = TestStarTreeBootstrapPhase2.TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination4 = {"A2", "B1", "C3"};
    dimKey = new DimensionKey(combination4);
    timeSeries = TestStarTreeBootstrapPhase2.TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    MapDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> mapDriver;
    StarTreeGenerationMapper mapper = new StarTreeGenerationMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration config = mapDriver.getConfiguration();
    config.set(StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
    config.set(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(), thirdEyeRoot + File.separator + "startree_generation");
    mapDriver.addAll(inputRecords);
    mapDriver.run();
  }

  @BeforeClass
  public void setUp() throws IOException
  {
    BootstrapPhaseTwoMapper mapper = new BootstrapPhaseTwoMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration config = mapDriver.getConfiguration();
    config.set(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());

    Path configPath = new Path(ClassLoader.getSystemResource(CONF_FILE).toString());
    FileSystem fs = FileSystem.get(config);
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fs.open(configPath));
    starTreeBootstrapConfig = StarTreeBootstrapPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
    thirdEyeRoot = System.getProperty("java.io.tmpdir") ;
    config.set(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(), thirdEyeRoot + File.separator + "startree_generation");

    BootstrapPhaseTwoReducer reducer = new BootstrapPhaseTwoReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    config = reduceDriver.getConfiguration();
    config.set(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
    config.set(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString(), thirdEyeRoot + File.separator + "startree_bootstrap_phase2");
    config.set(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(), thirdEyeRoot + File.separator + "startree_generation");
  }

  @Test
  public void testStarTreeBootstrapPhase2() throws Exception
  {
   generateAndSerializeStarTree();
    String starTreeOutputPath = mapDriver.getConfiguration().get(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString());
    String collectionName = starTreeBootstrapConfig.getCollectionName();
    Path pathToTree = new Path(starTreeOutputPath + "/" + "tree.bin");
    FileSystem dfs = FileSystem.get(mapDriver.getConfiguration());
    InputStream is = dfs.open(pathToTree);
    StarTreeNode starTreeRootNode = StarTreePersistanceUtil.loadStarTree(is);
    List<Pair<BytesWritable, BytesWritable>> inputRecords = generateTestDataMapper(starTreeRootNode);

    mapDriver.addAll(inputRecords);
    List<Pair<BytesWritable, BytesWritable>> result = mapDriver.run();
    // should give two records for {"A1", "B1", "C1"} and {"A1", "B1", "?"}
    // since there are 5 leaf node, the mapper will write 2 entries for leaf at C1
    // and 4 empty entries for other leaves.
    Assert.assertEquals(6, result.size());

    List<Pair<BytesWritable,List<BytesWritable>>> input =  generateTestDataReducer(starTreeRootNode);
    reduceDriver.addAll(input);
    result = reduceDriver.run();
  }

  @AfterClass
  public void deleteDir() throws IOException
  {
    File f = new File(thirdEyeRoot + File.separator + "startree_generation");
    FileUtils.deleteDirectory(f);
    f = new File(thirdEyeRoot + File.separator + "startree_bootstrap_phase2");
    FileUtils.deleteDirectory(f);
    f = new File("./leaf-data-input");
    FileUtils.deleteDirectory(f);
    f = new File("./leaf-data-output");
    FileUtils.deleteDirectory(f);
  }

  static class TestHelper
  {

    public static long generateRandomHoursSinceEpoch(){
      Random rng = new Random();
      // setting base value to year 2012
      long unixtime=(long) (1293861599+ rng.nextDouble()*(60*60*24*365));
      return TimeUnit.SECONDS.toHours(unixtime);
    }

    public static long generateRandomTime(){
      Random rng = new Random();
      long unixtime=(long) (1293861599+rng.nextDouble()*(60*60*24*365));
      return unixtime;
    }

    public static MetricTimeSeries generateRandomMetricTimeSeries(StarTreeBootstrapPhaseTwoConfig config)
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

    public static MetricTimeSeries generateMetricTimeSeries(StarTreeBootstrapPhaseTwoConfig config, Integer value)
    {
      List<String> names = config.getMetricNames();
      List<MetricType> types = config.getMetricTypes();
      MetricSchema schema = new MetricSchema(names, types);
      MetricTimeSeries series = new MetricTimeSeries(schema);
      long timeStamp = TimeUnit.HOURS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      for(int i = 0;i<names.size();i++){
        series.set(timeStamp, names.get(i), value);
      }
      return series;
    }

    public static MetricSchema getMetricSchema(StarTreeBootstrapPhaseTwoConfig config)
    {
      List<String> names = config.getMetricNames();
      List<MetricType> types = config.getMetricTypes();
      return new MetricSchema(names, types);
    }
  }
}


