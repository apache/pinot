package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoJob.RollupPhaseTwoMapper;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoJob.RollupPhaseTwoReducer;

/**
*
* @author ajaspal
*
*/

public class TestRollUpPhase2
{
  private static final String CONF_FILE = "config.yml";
  private static final int INPUT_RECORDS = 10;
  private static MapDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> mapDriver;
  private static ReduceDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> reduceDriver;
  private static RollupPhaseTwoConfig rollUpConfig;

  private List<Pair<BytesWritable, BytesWritable>> generateTestMapperData() throws Exception
  {
    List<Pair<BytesWritable, BytesWritable>> inputRecords = new ArrayList<Pair<BytesWritable, BytesWritable>>();
    List<String> dimensionNames = rollUpConfig.getDimensionNames();
    for(int i = 0;i< INPUT_RECORDS;i++){
      String []dimensionValues = TestHelper.generateDimensionValues(dimensionNames.size());
      DimensionKey key = new DimensionKey(dimensionValues);
      MetricTimeSeries m = TestHelper.generateRandomMetricTimeSeries(rollUpConfig);
      byte[] serializedKey = key.toBytes();
      byte[] serializedMetrics = m.toBytes();
      inputRecords.add(new Pair<BytesWritable, BytesWritable>(new BytesWritable(serializedKey), new BytesWritable(serializedMetrics)));
    }
    return inputRecords;
  }

  private List<Pair<BytesWritable,  List<BytesWritable>>> generateTestReducerData() throws Exception
  {
    List<Pair<BytesWritable,  List<BytesWritable>>> inputRecords = new ArrayList<Pair<BytesWritable,  List<BytesWritable>>>();
    BytesWritable keyWritable = new BytesWritable();
    List<BytesWritable> list = new ArrayList<BytesWritable>();
    Pair<BytesWritable, List<BytesWritable>> record;
    //Input : {"A1", "B1", "C1"}, {"A1", "B2", "C1"};
    String[] input1 = {"A1", "B1", "C1"};
    DimensionKey rawKey1 = new DimensionKey(input1);
    MetricTimeSeries rawSeries1 = TestHelper.generateRandomMetricTimeSeries(rollUpConfig);

    String[] input2 = {"A1", "B2", "C1"};
    DimensionKey rawKey2 = new DimensionKey(input2);
    MetricTimeSeries rawSeries2 = TestHelper.generateRandomMetricTimeSeries(rollUpConfig);

    String[] combination1 = {"A1", "B1", "C1"};
    DimensionKey combinationKey = new DimensionKey(combination1);
    keyWritable.set(combinationKey.toMD5(), 0, combinationKey.toMD5().length);
    RollupPhaseTwoMapOutput wrapper;
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey1, rawSeries1);
    list = new ArrayList<BytesWritable>();
    list.add(new BytesWritable(wrapper.toBytes()));
    record = new Pair<BytesWritable, List<BytesWritable>>(keyWritable, list);
    inputRecords.add(record);

    String[] combination2 = {"?", "B1", "C1"};
    combinationKey = new DimensionKey(combination2);
    keyWritable.set(combinationKey.toMD5(), 0, combinationKey.toMD5().length);
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey1, rawSeries1);
    list = new ArrayList<BytesWritable>();
    list.add(new BytesWritable(wrapper.toBytes()));
    record = new Pair<BytesWritable, List<BytesWritable>>(keyWritable, list);
    inputRecords.add(record);

    String[] combination3 = {"?", "?", "C1"};
    combinationKey = new DimensionKey(combination3);
    keyWritable.set(combinationKey.toMD5(), 0, combinationKey.toMD5().length);
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey1, rawSeries1);
    list = new ArrayList<BytesWritable>();
    list.add(new BytesWritable(wrapper.toBytes()));
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey2, rawSeries2);
    list.add(new BytesWritable(wrapper.toBytes()));
    record = new Pair<BytesWritable, List<BytesWritable>>(keyWritable, list);
    inputRecords.add(record);

    String[] combination4 = {"?", "?", "?"};
    combinationKey = new DimensionKey(combination4);
    keyWritable.set(combinationKey.toMD5(), 0, combinationKey.toMD5().length);
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey1, rawSeries1);
    list = new ArrayList<BytesWritable>();
    list.add(new BytesWritable(wrapper.toBytes()));
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey2, rawSeries2);
    list.add(new BytesWritable(wrapper.toBytes()));
    record = new Pair<BytesWritable, List<BytesWritable>>(keyWritable, list);
    inputRecords.add(record);

    String[] combination5 = {"A1", "B2", "C1"};
    combinationKey = new DimensionKey(combination5);
    keyWritable.set(combinationKey.toMD5(), 0, combinationKey.toMD5().length);
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey2, rawSeries2);
    list = new ArrayList<BytesWritable>();
    list.add(new BytesWritable(wrapper.toBytes()));
    record = new Pair<BytesWritable, List<BytesWritable>>(keyWritable, list);
    inputRecords.add(record);

    String[] combination6 = {"?", "B2", "C1"};
    combinationKey = new DimensionKey(combination6);
    keyWritable.set(combinationKey.toMD5(), 0, combinationKey.toMD5().length);
    wrapper = new RollupPhaseTwoMapOutput(combinationKey, rawKey2, rawSeries2);
    list = new ArrayList<BytesWritable>();
    list.add(new BytesWritable(wrapper.toBytes()));
    record = new Pair<BytesWritable, List<BytesWritable>>(keyWritable, list);
    inputRecords.add(record);

    return inputRecords;
  }

  @BeforeSuite
  public void setUp() throws Exception
  {
    RollupPhaseTwoMapper mapper = new RollupPhaseTwoMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration configuration = mapDriver.getConfiguration();
    configuration.set(RollupPhaseTwoConstants.ROLLUP_PHASE2_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
    configuration.set(RollupPhaseTwoConstants.ROLLUP_PHASE2_ANALYSIS_PATH.toString(), "dummy analysis path");
    Path configPath = new Path(ClassLoader.getSystemResource(CONF_FILE).toString());
    FileSystem fileSystem = FileSystem.get(configuration);
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
    rollUpConfig = RollupPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
    RollupPhaseTwoReducer reducer = new RollupPhaseTwoReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    configuration = reduceDriver.getConfiguration();
    configuration.set(RollupPhaseTwoConstants.ROLLUP_PHASE2_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());

  }

  @Test
  public void testRollUpPhase2() throws  Exception
  {
    List<Pair<BytesWritable, BytesWritable>> inputRecordsMapper = generateTestMapperData();
    mapDriver.addAll(inputRecordsMapper);
    List<Pair<BytesWritable, BytesWritable>> result = mapDriver.run();
    // make sure that all combinations are generated
    // inputRecords(10) * numberOfDimensions(3) = 30
    Assert.assertEquals(result.size(), 30);

    List<Pair<BytesWritable,  List<BytesWritable>>> inputRecordsReducer = generateTestReducerData();
    reduceDriver.addAll(inputRecordsReducer);
    result = reduceDriver.run();
    // make sure that the reducer emits correct number of outputs
    Assert.assertEquals(result.size(), 8);
    // make sure that the rolled up values are same of each combination which roll up to the
    // same dimension combination.
    Map<DimensionKey, MetricTimeSeries> store = new HashMap<DimensionKey, MetricTimeSeries>();
    for(Pair<BytesWritable, BytesWritable> p : result){
      RollupPhaseTwoReduceOutput output = RollupPhaseTwoReduceOutput.fromBytes(p.getSecond().getBytes(), TestHelper.getMetricSchema(rollUpConfig));
      if(store.get(output.getRollupDimensionKey()) != null)
      {
        Assert.assertEquals(store.get(output.rollupDimensionKey), output.getRollupTimeSeries());
      }
      else
      {
        store.put(output.getRollupDimensionKey(), output.getRollupTimeSeries());
       }
     }
   }
}

class TestHelper
{
  public static MetricTimeSeries generateRandomMetricTimeSeries(RollupPhaseTwoConfig rollUpConfig)
  {
    List<String> names = rollUpConfig.getMetricNames();
    List<MetricType> types = rollUpConfig.getMetricTypes();
    MetricSchema schema = new MetricSchema(names, types);
    MetricTimeSeries series = new MetricTimeSeries(schema);
    long timeStamp = TimeUnit.HOURS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    RandomDataGenerator rand = new RandomDataGenerator();
    for(int i = 0;i<names.size();i++){
      series.set(timeStamp, names.get(i), rand.nextInt(0, 100));
    }
    return series;
  }

  public static MetricSchema getMetricSchema(RollupPhaseTwoConfig rollUpConfig)
  {
    List<String> names = rollUpConfig.getMetricNames();
    List<MetricType> types = rollUpConfig.getMetricTypes();
    return new MetricSchema(names, types);
  }

  public static String generateRandomString(int length)
  {
    Random rng = new Random();
    StringBuffer str = new StringBuffer();
    for(int i = 0;i<length;i++){
        str.append(97 + rng.nextInt(26));
    }
    return str.toString();
  }

  public static String[] generateDimensionValues(int size)
  {
    String []input = new String[size];
    for(int i = 0;i<size;i++){
      input[i] = generateRandomString(5);
    }
    return input;
  }
}

