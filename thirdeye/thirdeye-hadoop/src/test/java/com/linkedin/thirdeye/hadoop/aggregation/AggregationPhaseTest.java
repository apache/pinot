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
package com.linkedin.thirdeye.hadoop.aggregation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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
import com.linkedin.thirdeye.hadoop.aggregation.AggregationPhaseJob.AggregationMapper;
import com.linkedin.thirdeye.hadoop.aggregation.AggregationPhaseJob.AggregationReducer;

/**
 * This tests mapper of Aggregation phase, to check conversion of time column to bucket time
 * This also tests reducer to check aggregation using new time values
 */
public class AggregationPhaseTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String HADOOP_IO_SERIALIZATION = "io.serializations";
  private static final String AVRO_SCHEMA = "schema.avsc";

  private String outputPath;
  private Schema inputSchema;
  private ThirdEyeConfig thirdeyeConfig;
  private AggregationPhaseConfig aggPhaseConfig;
  Properties props = new Properties();

  private MapDriver<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> mapDriver;
  private ReduceDriver<BytesWritable, BytesWritable, AvroKey<GenericRecord>, NullWritable> reduceDriver;

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

    // 2016-04-27T190000
    GenericRecord input = new GenericData.Record(inputSchema);
    input.put("d1", "abc1");
    input.put("d2", 501L);
    input.put("d3", "xyz1");
    input.put("hoursSinceEpoch", 1461808800000L);
    input.put("m1", 100);
    input.put("m2", 20);
    inputRecords.add(input);

    // 2016-04-27T191000
    input = new GenericData.Record(inputSchema);
    input.put("d1", "abc1");
    input.put("d2", 501L);
    input.put("d3", "xyz1");
    input.put("hoursSinceEpoch", 1461809400000L);
    input.put("m1", 100);
    input.put("m2", 20);
    inputRecords.add(input);

    // 2016-04-27T20
    input = new GenericData.Record(inputSchema);
    input.put("d1", "abc2");
    input.put("d2", 502L);
    input.put("d3", "xyz2");
    input.put("hoursSinceEpoch", 1461812400000L);
    input.put("m1", 10);
    input.put("m2", 2);
    inputRecords.add(input);

    return inputRecords;
  }


  private List<Pair<BytesWritable,List<BytesWritable>>> generateTestReduceData(List<Pair<BytesWritable, BytesWritable>> result) throws Exception {
    List<Pair<BytesWritable, List<BytesWritable>>> inputRecords = new ArrayList<>();
    Map<BytesWritable, List<BytesWritable>> inputMap = new TreeMap<>();

    for (Pair<BytesWritable, BytesWritable> pair : result) {
      inputMap.put(pair.getFirst(), new ArrayList<BytesWritable>());
    }

    for (Pair<BytesWritable, BytesWritable> pair : result) {
      inputMap.get(pair.getFirst()).add(pair.getSecond());
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
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_INPUT_TIMECOLUMN_SIZE.toString(), "1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_INPUT_TIMECOLUMN_TYPE.toString(), TimeUnit.HOURS.toString());
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_INPUT_TIMECOLUMN_SIZE.toString(), "1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_INPUT_TIMECOLUMN_TYPE.toString(), TimeUnit.MILLISECONDS.toString());
    thirdeyeConfig = ThirdEyeConfig.fromProperties(props);
    aggPhaseConfig = AggregationPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);

    // Mapper config
    AggregationMapper mapper = new AggregationMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration configuration = mapDriver.getConfiguration();
    configuration.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");

    configuration.set(AggregationPhaseConstants.AGG_PHASE_THIRDEYE_CONFIG.toString(),
        OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    inputSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    setUpAvroSerialization(mapDriver.getConfiguration(), inputSchema);

    // Reducer config
    AggregationReducer reducer = new AggregationReducer();
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    configuration = reduceDriver.getConfiguration();
    configuration.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");

    Schema reducerSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    configuration.set(AggregationPhaseConstants.AGG_PHASE_AVRO_SCHEMA.toString(), reducerSchema.toString());

    configuration.set(AggregationPhaseConstants.AGG_PHASE_THIRDEYE_CONFIG.toString(),
        OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    TemporaryPath tmpPath = new TemporaryPath();
    outputPath = tmpPath.toString();
    configuration.set(AggregationPhaseConstants.AGG_PHASE_OUTPUT_PATH.toString(), outputPath);
    setUpAvroSerialization(reduceDriver.getConfiguration(), reducerSchema);

  }

  @Test
  public void testAggregationPhase() throws Exception {

    int recordCount = 0;
    List<GenericRecord> inputRecords = generateTestMapperData();
    for (GenericRecord record : inputRecords) {
      AvroKey<GenericRecord> inKey = new AvroKey<GenericRecord>();
      inKey.datum(record);
      mapDriver.addInput(new Pair<AvroKey<GenericRecord>, NullWritable>(inKey, NullWritable.get()));
      recordCount++;
    }

    List<Pair<BytesWritable, BytesWritable>> mapResult = mapDriver.run();
    Assert.assertEquals("Incorrect number of records emitted by mapper", recordCount, mapResult.size());

    AggregationPhaseMapOutputKey keyWrapper =
        AggregationPhaseMapOutputKey.fromBytes(mapResult.get(0).getFirst().getBytes(), aggPhaseConfig.getDimensionTypes());
    Assert.assertEquals(406058, keyWrapper.getTime());
    keyWrapper = AggregationPhaseMapOutputKey.fromBytes(mapResult.get(1).getFirst().getBytes(), aggPhaseConfig.getDimensionTypes());
    Assert.assertEquals(406058, keyWrapper.getTime());
    keyWrapper = AggregationPhaseMapOutputKey.fromBytes(mapResult.get(2).getFirst().getBytes(), aggPhaseConfig.getDimensionTypes());
    Assert.assertEquals(406059, keyWrapper.getTime());

    List<Pair<BytesWritable, List<BytesWritable>>> reduceInput = generateTestReduceData(mapResult);
    reduceDriver.addAll(reduceInput);

    List<Pair<AvroKey<GenericRecord>, NullWritable>> reduceResult = reduceDriver.run();
    Assert.assertEquals("Incorrect number of records returned by aggregation reducer", 2, reduceResult.size());

    GenericRecord record = reduceResult.get(0).getFirst().datum();
    List<Object> dimensionsExpected = Lists.newArrayList();
    dimensionsExpected.add("abc1");
    dimensionsExpected.add(501L);
    dimensionsExpected.add("xyz1");
    List<Object> dimensionsActual = getDimensionsFromRecord(record);
    Assert.assertEquals(dimensionsExpected, dimensionsActual);
    List<Integer> metricsExpected = Lists.newArrayList(200, 40);
    List<Integer> metricsActual = getMetricsFromRecord(record);
    Assert.assertEquals(metricsExpected, metricsActual);
    Assert.assertEquals(406058, (long) record.get("hoursSinceEpoch"));


    record = reduceResult.get(1).getFirst().datum();
    dimensionsExpected = Lists.newArrayList();
    dimensionsExpected.add("abc2");
    dimensionsExpected.add(502L);
    dimensionsExpected.add("xyz2");
    dimensionsActual = getDimensionsFromRecord(record);
    Assert.assertEquals(dimensionsExpected, dimensionsActual);
    metricsExpected = Lists.newArrayList(10, 2);
    metricsActual = getMetricsFromRecord(record);
    Assert.assertEquals(metricsExpected, metricsActual);
    Assert.assertEquals(406059, (long) record.get("hoursSinceEpoch"));
  }

  private List<Object> getDimensionsFromRecord(GenericRecord record) {
    List<Object> dimensionsActual = new ArrayList<>();
    dimensionsActual.add(record.get("d1"));
    dimensionsActual.add(record.get("d2"));
    dimensionsActual.add(record.get("d3"));
    return dimensionsActual;
  }

  private List<Integer> getMetricsFromRecord(GenericRecord record) {
    List<Integer> metricsActual = new ArrayList<>();
    metricsActual.add((int) record.get("m1"));
    metricsActual.add((int) record.get("m2"));
    return metricsActual;
  }


  @After
  public void cleanUp() throws IOException {

    File f = new File(outputPath);
    FileUtils.deleteDirectory(f);
  }
}
