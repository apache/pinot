package com.linkedin.thirdeye.bootstrap.analysis;

import java.io.File;
import java.io.IOException;
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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.thirdeye.bootstrap.analysis.AnalysisPhaseJob.AnalyzeMapper;
import com.linkedin.thirdeye.bootstrap.analysis.AnalysisPhaseJob.AnalyzeReducer;

public class AnalysisJobTest {

  private static final String HADOOP_IO_SERIALIZATION = "io.serializations";
  private static final String HADOOP_AVRO_KEY_WRITER_SERIALIZATION =
      "avro.serialization.key.writer.schema";
  private static final String HADOOP_AVRO_VALUE_WRITER_SERIALIZATION =
      "avro.serialization.value.writer.schema";
  private static final String CONF_FILE = "config.yml";
  private static final String SCHEMA_FILE = "test.avsc";
  private static final String RESULT_FILE = "results.json";
  private static String outputPath;
  private static final Random rng = new Random();

  private MapDriver<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> mapDriver;
  private ReduceDriver<BytesWritable, BytesWritable, BytesWritable, NullWritable> reduceDriver;

  private String generateRandomString(int length) {
    StringBuffer str = new StringBuffer();
    for (int i = 0; i < length; i++) {
      str.append(97 + rng.nextInt(26));
    }
    return str.toString();
  }

  private long generateRandomHoursSinceEpoch() {
    // setting base value to year 2012
    long unixtime = (long) (1293861599 + rng.nextDouble() * (60 * 60 * 24 * 365));
    return TimeUnit.SECONDS.toHours(unixtime);
  }

  private long generateRandomTime() {
    long unixtime = (long) (1293861599 + rng.nextDouble() * (60 * 60 * 24 * 365));
    return unixtime;
  }

  private void setUpAvroSerialization(Configuration hadoopConf, Schema schema) {
    String[] currentSerializations = hadoopConf.getStrings(HADOOP_IO_SERIALIZATION);
    String[] finalSerializations = new String[currentSerializations.length + 1];
    System.arraycopy(currentSerializations, 0, finalSerializations, 0,
        currentSerializations.length);
    finalSerializations[finalSerializations.length - 1] = AvroSerialization.class.getName();
    mapDriver.getConfiguration().setStrings(HADOOP_IO_SERIALIZATION, finalSerializations);
    mapDriver.getConfiguration().setStrings(HADOOP_AVRO_KEY_WRITER_SERIALIZATION,
        schema.toString());
    mapDriver.getConfiguration().setStrings(HADOOP_AVRO_VALUE_WRITER_SERIALIZATION,
        Schema.create(Schema.Type.NULL).toString());
  }

  private List<GenericRecord> generateTestData() throws Exception {
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_FILE));
    List<GenericRecord> inputRecords = new ArrayList<GenericRecord>();
    for (int i = 0; i < 10; i++) {
      GenericRecord input = new GenericData.Record(schema);
      input.put("d1", generateRandomString(10));
      input.put("d2", generateRandomString(5));
      input.put("d1", generateRandomString(7));
      input.put("time", generateRandomTime());
      input.put("hoursSinceEpoch", generateRandomHoursSinceEpoch());
      input.put("m1", 10);
      input.put("m2", 20);
      inputRecords.add(input);
    }
    return inputRecords;
  }

  @Before
  public void setUp() throws Exception {
    AnalyzeMapper mapper = new AnalyzeMapper();
    AnalyzeReducer reducer = new AnalyzeReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    Configuration configuration = mapDriver.getConfiguration();
    configuration.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    configuration.set(AnalysisJobConstants.ANALYSIS_CONFIG_PATH.toString(),
        ClassLoader.getSystemResource(CONF_FILE).toString());
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(SCHEMA_FILE));
    setUpAvroSerialization(mapDriver.getConfiguration(), schema);

    configuration = reduceDriver.getConfiguration();
    TemporaryPath tmpDir = new TemporaryPath();
    outputPath = tmpDir.toString();
    configuration.set(AnalysisJobConstants.ANALYSIS_OUTPUT_PATH.toString(), tmpDir.toString());
    configuration.set(AnalysisJobConstants.ANALYSIS_FILE_NAME.toString(), RESULT_FILE);
  }

  @Test
  public void testAnalysisPhase() throws Exception {
    int recordCount = 0;
    long MAX_TIME_EXPECTED = Long.MIN_VALUE;
    long MIN_TIME_EXPECTED = Long.MAX_VALUE;
    long MAX_TIME_ACTUAL = Long.MIN_VALUE;
    long MIN_TIME_ACTUAL = Long.MAX_VALUE;
    List<GenericRecord> inputRecords = generateTestData();
    for (GenericRecord record : inputRecords) {
      AvroKey<GenericRecord> inKey = new AvroKey<GenericRecord>();
      inKey.datum(record);
      mapDriver.addInput(new Pair<AvroKey<GenericRecord>, NullWritable>(inKey, NullWritable.get()));
      recordCount++;
      MAX_TIME_EXPECTED =
          Math.max(MAX_TIME_EXPECTED, Long.parseLong(record.get("hoursSinceEpoch").toString()));
      MIN_TIME_EXPECTED =
          Math.max(MIN_TIME_EXPECTED, Long.parseLong(record.get("hoursSinceEpoch").toString()));
    }
    List<Pair<BytesWritable, BytesWritable>> result = mapDriver.run();
    // check if the size of the result set output by the Mapper is of appropriate size;
    Assert.assertEquals(recordCount, result.size());

    for (Pair<BytesWritable, BytesWritable> p : result) {
      List<BytesWritable> tmp = new ArrayList<BytesWritable>();
      tmp.add(p.getSecond());
      reduceDriver.addInput(new Pair<BytesWritable, List<BytesWritable>>(p.getFirst(), tmp));
    }

    List<Pair<BytesWritable, NullWritable>> r = reduceDriver.run();
    for (Pair<BytesWritable, NullWritable> p : r) {
      BytesWritable value = p.getFirst();
      MAX_TIME_ACTUAL =
          Math.max(MAX_TIME_ACTUAL, AnalysisPhaseStats.fromBytes(value.copyBytes()).getMaxTime());
      MIN_TIME_ACTUAL =
          Math.max(MIN_TIME_EXPECTED, AnalysisPhaseStats.fromBytes(value.copyBytes()).getMaxTime());
    }
    Assert.assertEquals(MAX_TIME_EXPECTED, MAX_TIME_ACTUAL);
    Assert.assertEquals(MIN_TIME_EXPECTED, MIN_TIME_ACTUAL);
  }

  @After
  public void cleanUp() throws IOException {
    File f = new File(outputPath);
    FileUtils.deleteDirectory(f);
  }

}
