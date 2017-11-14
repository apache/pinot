/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop.topk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfigProperties;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;
import com.linkedin.thirdeye.hadoop.topk.TopKPhaseJob.TopKPhaseMapper;
import com.linkedin.thirdeye.hadoop.topk.TopKPhaseJob.TopKPhaseReducer;

/**
 * This test will test mapper of Topk phase,
 * to ensure the right pairs being emitted
 * This will also test the topk file generated
 */
public class TopkPhaseTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String HADOOP_IO_SERIALIZATION = "io.serializations";
  private static final String AVRO_SCHEMA = "schema.avsc";

  private String outputPath;
  private Schema inputSchema;
  private ThirdEyeConfig thirdeyeConfig;
  Properties props = new Properties();

  private MapDriver<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> mapDriver;
  private ReduceDriver<BytesWritable, BytesWritable, NullWritable, NullWritable> reduceDriver;

  private long generateRandomHoursSinceEpoch() {
    Random r = new Random();
    // setting base value to year 2012
    long unixtime = (long) (1293861599 + r.nextDouble() * 60 * 60 * 24 * 365);
    return TimeUnit.SECONDS.toHours(unixtime);
  }


  private void setUpAvroSerialization(Configuration conf, Schema inputSchema) {
    String[] currentSerializations = conf.getStrings(HADOOP_IO_SERIALIZATION);
    String[] finalSerializations = new String[currentSerializations.length + 1];
    System.arraycopy(currentSerializations, 0, finalSerializations, 0,
        currentSerializations.length);
    finalSerializations[finalSerializations.length - 1] = AvroSerialization.class.getName();
    mapDriver.getConfiguration().setStrings(HADOOP_IO_SERIALIZATION, finalSerializations);

    AvroSerialization.addToConfiguration(conf);
    AvroSerialization.setKeyWriterSchema(conf, inputSchema);
    AvroSerialization.setValueWriterSchema(conf, Schema.create(Schema.Type.NULL));
  }

  private List<GenericRecord> generateTestMapperData() throws Exception {
    List<GenericRecord> inputRecords = new ArrayList<GenericRecord>();

    GenericRecord input = new GenericData.Record(inputSchema);
    input.put("d1", "abc1");
    input.put("d2", 501L);
    input.put("d3", "xyz1");
    input.put("hoursSinceEpoch", generateRandomHoursSinceEpoch());
    input.put("m1", 100);
    input.put("m2", 20);
    inputRecords.add(input);

    input = new GenericData.Record(inputSchema);
    input.put("d1", "abc2");
    input.put("d2", 502L);
    input.put("d3", "xyz2");
    input.put("hoursSinceEpoch", generateRandomHoursSinceEpoch());
    input.put("m1", 10);
    input.put("m2", 20);
    inputRecords.add(input);

    return inputRecords;
  }


  private List<Pair<BytesWritable,List<BytesWritable>>> generateTestReduceData(List<Pair<BytesWritable, BytesWritable>> result) throws Exception {
    List<Pair<BytesWritable, List<BytesWritable>>> inputRecords = new ArrayList<>();
    Map<BytesWritable, List<BytesWritable>> inputMap = new TreeMap<>();

    for (Pair<BytesWritable, BytesWritable> pair : result) {
      inputMap.put(pair.getFirst(), Lists.newArrayList(pair.getSecond()));
    }
    for (Entry<BytesWritable, List<BytesWritable>> listPair : inputMap.entrySet()) {
      inputRecords.add(new Pair<BytesWritable, List<BytesWritable>>(listPair.getKey(), listPair.getValue()));
    }
    return inputRecords;
  }

  @Before
  public void setUp() throws Exception {

    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TABLE_NAME.toString(), "collection");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString(), "d1,d2,d3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_TYPES.toString(), "STRING,LONG,STRING");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString(), "m1,m2");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString(), "INT,INT");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString(), "hoursSinceEpoch");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_DIMENSION_NAMES.toString(), "d2,");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRICS.toString() + ".d2", "m1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_KVALUES.toString() + ".d2", "1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION_NAMES.toString(), "d3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_WHITELIST_DIMENSION.toString() + ".d3", "xyz2");
    thirdeyeConfig = ThirdEyeConfig.fromProperties(props);

    // Mapper config
    TopKPhaseMapper mapper = new TopKPhaseMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration configuration = mapDriver.getConfiguration();
    configuration.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");

    configuration.set(TopKPhaseConstants.TOPK_PHASE_THIRDEYE_CONFIG.toString(),
        OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    inputSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    setUpAvroSerialization(mapDriver.getConfiguration(), inputSchema);

    // Reducer config
    TopKPhaseReducer reducer = new TopKPhaseReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    configuration = reduceDriver.getConfiguration();
    configuration.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");

    configuration.set(TopKPhaseConstants.TOPK_PHASE_THIRDEYE_CONFIG.toString(),
        OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    TemporaryPath tmpPath = new TemporaryPath();
    outputPath = tmpPath.toString();
    configuration.set(TopKPhaseConstants.TOPK_PHASE_OUTPUT_PATH.toString(), outputPath);

  }

  @Test
  public void testTopKColumnTransformationPhase() throws Exception {

    int recordCount = 0;
    List<GenericRecord> inputRecords = generateTestMapperData();
    for (GenericRecord record : inputRecords) {
      AvroKey<GenericRecord> inKey = new AvroKey<GenericRecord>();
      inKey.datum(record);
      mapDriver.addInput(new Pair<AvroKey<GenericRecord>, NullWritable>(inKey, NullWritable.get()));
      recordCount++;
    }

    List<Pair<BytesWritable, BytesWritable>> result = mapDriver.run();
    // for each record, we emit
    // a records per dimension
    // and one record for ALL,ALL
    Assert.assertEquals("Incorrect number of records emitted by mapper", recordCount * (3 + 1), result.size());

    Map<String, Integer> counts = new HashMap<>();
    for (Pair<BytesWritable, BytesWritable> pair : result) {
      TopKPhaseMapOutputKey key = TopKPhaseMapOutputKey.fromBytes(pair.getFirst().getBytes());
      String dimensionName = key.getDimensionName();
      Integer count = counts.get(dimensionName);
      if (count == null) {
        count = 0;
      }
      counts.put(dimensionName , count + 1);
    }
    Assert.assertEquals("Incorrect number of records emitted from map", 2, (int) counts.get("d1"));
    Assert.assertEquals("Incorrect number of records emitted from map", 2, (int) counts.get("d2"));
    Assert.assertEquals("Incorrect number of records emitted from map", 2, (int) counts.get("d3"));
    Assert.assertEquals("Incorrect number of records emitted from map", 2, (int) counts.get("0"));

    List<Pair<BytesWritable, List<BytesWritable>>> reduceInput = generateTestReduceData(result);
    reduceDriver.addAll(reduceInput);
    reduceDriver.run();

    File topKFile = new File(outputPath, ThirdEyeConstants.TOPK_VALUES_FILE);
    Assert.assertTrue("Topk file failed to generate!", topKFile.exists());
    TopKDimensionValues topk = OBJECT_MAPPER.readValue(new FileInputStream(topKFile), TopKDimensionValues.class);
    Map<String, Set<String>> topkMap = topk.getTopKDimensions();
    Assert.assertEquals("Incorrect topk object", topkMap.size(), 1);
    Set<String> expected = new HashSet<>();
    expected.add("501");
    Assert.assertEquals("Incorrect topk values in topk object", expected, topkMap.get("d2"));
    Assert.assertEquals("Incorrect whitelist values in topk object", null, topkMap.get("d3"));
  }



  @After
  public void cleanUp() throws IOException {

    File f = new File(outputPath);
    FileUtils.deleteDirectory(f);
  }
}
