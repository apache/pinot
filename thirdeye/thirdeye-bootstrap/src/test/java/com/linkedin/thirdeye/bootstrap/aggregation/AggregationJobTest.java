package com.linkedin.thirdeye.bootstrap.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregatePhaseJob.AggregationMapper;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregatePhaseJob.AggregationReducer;

public class AggregationJobTest {

  private static final String HADOOP_IO_SERIALIZATION = "io.serializations";
  private static final String HADOOP_AVRO_KEY_WRITER_SERIALIZATION = "avro.serialization.key.writer.schema";
  private static final String HADOOP_AVRO_VALUE_WRITER_SERIALIZATION = "avro.serialization.value.writer.schema";
  private static final String CONF_FILE = "config.yml";
  private static final String SCHEMA_FILE = "test.avsc";

  private MetricSchema metricSchema;
  private List<String> metricNames = Lists.newArrayList("m1", "m2");
  private List<MetricType> metricTypes = Lists.newArrayList(MetricType.INT, MetricType.INT);
  private MapDriver<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> mapDriver;
  private ReduceDriver<BytesWritable, BytesWritable, BytesWritable, BytesWritable> reduceDriver;

  private long generateRandomHoursSinceEpoch(){
    Random r =new Random();
    // setting base value to year 2012
    long unixtime=(long) (1293861599+r.nextDouble()*60*60*24*365);
    return TimeUnit.SECONDS.toHours(unixtime);
  }

  private long generateRandomTime(){
    Random r =new Random();
    // setting base value to year 2012
    long unixtime=(long) (1293861599+r.nextDouble()*60*60*24*365);
    return unixtime;
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

  private List<GenericRecord> generateTestData() throws Exception{
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_FILE));
    List<GenericRecord> inputRecords = new ArrayList<GenericRecord>();
    GenericRecord input = new GenericData.Record(schema);
    for(int i = 0; i< 3;i++){
      input.put("d1", "abc");
      input.put("d2", "pqr");
      input.put("d1", "xyz");
      input.put("time", generateRandomTime());
      input.put("hoursSinceEpoch", generateRandomHoursSinceEpoch());
      input.put("m1", 10);
      input.put("m2", 20);
      inputRecords.add(input);
    }
    return inputRecords;
  }

  @Before
  public void setUp() throws Exception
  {
    AggregationMapper mapper = new AggregationMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration configuration = mapDriver.getConfiguration();
    configuration.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"
                    + "org.apache.hadoop.io.serializer.WritableSerialization");
    configuration.set(AggregationJobConstants.AGG_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_FILE));
    setUpAvroSerialization(mapDriver.getConfiguration(), schema);
    metricSchema = new MetricSchema(metricNames, metricTypes);
    AggregationReducer reducer = new AggregationReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    configuration = reduceDriver.getConfiguration();
    configuration.set(AggregationJobConstants.AGG_CONFIG_PATH.toString(), ClassLoader.getSystemResource(CONF_FILE).toString());
   }


  @Test
  public void testAggregationPhase() throws Exception {
    int recordCount = 0;

    List<GenericRecord> inputRecords = generateTestData();
    for(GenericRecord record : inputRecords){
      AvroKey<GenericRecord> inKey = new AvroKey<GenericRecord>();
      inKey.datum(record);
      mapDriver.addInput(new Pair<AvroKey<GenericRecord>, NullWritable> (inKey, NullWritable.get()));
      recordCount++;
    }
    List<Pair<BytesWritable, BytesWritable>> result = mapDriver.run();
    Assert.assertEquals(recordCount, result.size());

    BytesWritable reducerKey = result.get(0).getFirst();
    List<BytesWritable> reducerInputValues = new ArrayList<BytesWritable>();
    for(Pair<BytesWritable, BytesWritable> p : result){
      reducerInputValues.add(p.getSecond());
    }
    reduceDriver.addInput(new Pair<BytesWritable, List<BytesWritable>>(reducerKey, reducerInputValues));
    List<Pair<BytesWritable, BytesWritable>> r = reduceDriver.run();

    // since we have just one unique key combination.
    Assert.assertEquals(1, r.size());
    MetricTimeSeries series = MetricTimeSeries.fromBytes(
                                r.get(0).getSecond().copyBytes(), metricSchema);

    Assert.assertEquals(30, series.get(-1, "m1"));
    Assert.assertEquals(60, series.get(-1, "m2"));
  }
}
