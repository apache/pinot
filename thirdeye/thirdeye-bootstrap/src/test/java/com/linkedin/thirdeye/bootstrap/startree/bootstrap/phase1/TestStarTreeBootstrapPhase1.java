package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
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
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneJob.BootstrapMapper;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneJob.StarTreeBootstrapReducer;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationJob.StarTreeGenerationMapper;

/**
*
* @author ajaspal
*
*/
public class TestStarTreeBootstrapPhase1
{
  private static final String CONF_FILE = "config.yml";
  private static final String SCHEMA_FILE = "test.avsc";
  private static final String HADOOP_IO_SERIALIZATION = "io.serializations";
  private static final String HADOOP_AVRO_KEY_WRITER_SERIALIZATION = "avro.serialization.key.writer.schema";
  private static final String HADOOP_AVRO_VALUE_WRITER_SERIALIZATION = "avro.serialization.value.writer.schema";
  private static MapDriver<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> mapDriver;
  private static ReduceDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> reduceDriver;
  private static StarTreeBootstrapPhaseOneConfig starTreeBootstrapConfig;
  private static String thirdEyeRoot;

  private List<Pair<GenericRecord,NullWritable>> generateTestDataMapper() throws Exception
  {
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_FILE));
    List<Pair<GenericRecord, NullWritable>> inputRecords = new ArrayList<Pair<GenericRecord, NullWritable>>();

    GenericRecord input = new GenericData.Record(schema);
    input.put("d1", "A1");
    input.put("d2", "B1");
    input.put("d3", "C1");
    input.put("time", TestStarTreeBootstrapPhase1.TestHelper.generateRandomTime());
    input.put("hoursSinceEpoch", TestStarTreeBootstrapPhase1.TestHelper.generateRandomHoursSinceEpoch());
    input.put("m1", 10);
    input.put("m2", 20);
    inputRecords.add(new Pair<GenericRecord, NullWritable>(input, NullWritable.get()));

    return inputRecords;
  }

  private List<Pair<BytesWritable,List<BytesWritable>>> generateTestDataReducer() throws Exception
  {

    List<Pair<BytesWritable,List<BytesWritable>>> inputRecords = new ArrayList<Pair<BytesWritable,List<BytesWritable>>>();
    List<BytesWritable> list = new ArrayList<BytesWritable>();
    String []combination = {"?", "?", "?"};
    DimensionKey key = new DimensionKey(combination);
    UUID uuid = UUID.randomUUID();
    BootstrapPhaseMapOutputKey outputKey = new BootstrapPhaseMapOutputKey(uuid, key.toMD5());
    BootstrapPhaseMapOutputValue outputValue1 = new BootstrapPhaseMapOutputValue(key, TestStarTreeBootstrapPhase1.TestHelper.generateMetricTimeSeries(starTreeBootstrapConfig, 10));
    list.add(new BytesWritable(outputValue1.toBytes()));
    BootstrapPhaseMapOutputValue outputValue2 = new BootstrapPhaseMapOutputValue(key, TestStarTreeBootstrapPhase1.TestHelper.generateMetricTimeSeries(starTreeBootstrapConfig, 20));
    list.add(new BytesWritable(outputValue2.toBytes()));

    inputRecords.add(new Pair<BytesWritable, List<BytesWritable>>(new BytesWritable(outputKey.toBytes()), list));
    return inputRecords;
  }


  private void generateAndSerializeStarTree() throws IOException
  {
    List<Pair<BytesWritable,  BytesWritable>> inputRecords = new ArrayList<Pair<BytesWritable,  BytesWritable>>();
    String []combination1 = {"A1", "B1", "C1"};
    DimensionKey dimKey = new DimensionKey(combination1);
    MetricTimeSeries timeSeries = TestStarTreeBootstrapPhase1.TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
    Pair<BytesWritable,  BytesWritable> record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination2 = {"A1", "B1", "C2"};
    dimKey = new DimensionKey(combination2);
    timeSeries = TestStarTreeBootstrapPhase1.TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination3 = {"A1", "B1", "C3"};
    dimKey = new DimensionKey(combination3);
    timeSeries = TestStarTreeBootstrapPhase1.TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
    record = new Pair<BytesWritable, BytesWritable>(new BytesWritable(dimKey.toBytes())
                                                                          , new BytesWritable(timeSeries.toBytes()));
    inputRecords.add(record);

    String []combination4 = {"A2", "B1", "C3"};
    dimKey = new DimensionKey(combination4);
    timeSeries = TestStarTreeBootstrapPhase1.TestHelper.generateRandomMetricTimeSeries(starTreeBootstrapConfig);
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

  private void setUpAvroSerialization(Configuration hadoopConf, Schema schema)
  {
    String[] currentSerializations = hadoopConf.getStrings(HADOOP_IO_SERIALIZATION);
    String[] finalSerializations  = new String[currentSerializations.length + 1];
    System.arraycopy(currentSerializations, 0, finalSerializations, 0, currentSerializations.length);
    finalSerializations[finalSerializations.length - 1] = AvroSerialization.class.getName();
    mapDriver.getConfiguration().setStrings(HADOOP_IO_SERIALIZATION, finalSerializations);
    mapDriver.getConfiguration().setStrings(HADOOP_AVRO_KEY_WRITER_SERIALIZATION , schema.toString());
    mapDriver.getConfiguration().setStrings(HADOOP_AVRO_VALUE_WRITER_SERIALIZATION, Schema.create(Schema.Type.NULL).toString());
  }


  @BeforeClass
  public void setUp() throws IOException
  {
    BootstrapMapper mapper = new BootstrapMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration config = mapDriver.getConfiguration();
    config.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    config.set(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_FILE));
    setUpAvroSerialization(mapDriver.getConfiguration(), schema);

    Path configPath = new Path(ClassLoader.getSystemResource(CONF_FILE).toString());
    FileSystem fs = FileSystem.get(config);
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fs.open(configPath));
    starTreeBootstrapConfig = StarTreeBootstrapPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
    thirdEyeRoot = System.getProperty("java.io.tmpdir") ;
    config.set(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(), thirdEyeRoot + File.separator + "startree_generation");

    StarTreeBootstrapReducer reducer = new StarTreeBootstrapReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    config = reduceDriver.getConfiguration();
    config.set(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
  }

  @AfterClass
  public void tearDown() throws IOException
  {
    FileUtils.forceDelete(new File("dimensionStore.tar.gz"));
    FileUtils.forceDelete(new File(".dimensionStore.tar.gz.crc"));
  }

  @Test
  public void testStarTreeBootstrapPhase1() throws Exception
  {
    List<Pair<GenericRecord,NullWritable>> inputRecords = generateTestDataMapper();
    generateAndSerializeStarTree();
    for(Pair<GenericRecord,NullWritable> p : inputRecords){
      AvroKey<GenericRecord> inKey = new AvroKey<GenericRecord>();
      inKey.datum(p.getFirst());
      mapDriver.addInput(new Pair<AvroKey<GenericRecord>, NullWritable> (inKey, NullWritable.get()));
    }
    List<Pair<BytesWritable, BytesWritable>> result = mapDriver.run();
    // should give two records for {"A1", "B1", "C1"} and {"A1", "B1", "?"}
    Assert.assertEquals(2, result.size());

    List<Pair<BytesWritable,List<BytesWritable>>> input =  generateTestDataReducer();
    reduceDriver.addAll(input);
    result = reduceDriver.run();
    BootstrapPhaseMapOutputValue output = BootstrapPhaseMapOutputValue.fromBytes(result.get(0).getSecond().getBytes(), TestHelper.getMetricSchema(starTreeBootstrapConfig));
    Assert.assertEquals(TestStarTreeBootstrapPhase1.TestHelper.generateMetricTimeSeries(starTreeBootstrapConfig, 30),output.getMetricTimeSeries());
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

    public static MetricTimeSeries generateRandomMetricTimeSeries(StarTreeBootstrapPhaseOneConfig config)
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

    public static MetricTimeSeries generateMetricTimeSeries(StarTreeBootstrapPhaseOneConfig config, Integer value)
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

    public static MetricSchema getMetricSchema(StarTreeBootstrapPhaseOneConfig config)
    {
      List<String> names = config.getMetricNames();
      List<MetricType> types = config.getMetricTypes();
      return new MetricSchema(names, types);
    }
  }
}


