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
package com.linkedin.thirdeye.hadoop.derivedcolumn.transformation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfigProperties;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;
import com.linkedin.thirdeye.hadoop.derivedcolumn.transformation.DerivedColumnTransformationPhaseConstants;
import com.linkedin.thirdeye.hadoop.derivedcolumn.transformation.DerivedColumnTransformationPhaseJob.DerivedColumnTransformationPhaseMapper;

/**
 * This test will test mapper of DerivedColumnTransformation phase,
 * to see if new columns have been added according to the topk values file
 */
public class DerivedColumnTransformationTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String HADOOP_IO_SERIALIZATION = "io.serializations";

  private static final String AVRO_SCHEMA = "schema.avsc";
  private static final String TRANSFORMATION_SCHEMA = "transformation_schema.avsc";
  private static final String TOPK_PATH = "topk_path";
  private String outputPath;

  Properties props = new Properties();

  private MapDriver<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> mapDriver;

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


  private void resetAvroSerialization() throws IOException {
    Configuration conf = mapDriver.getConfiguration();
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    Schema outputSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(TRANSFORMATION_SCHEMA));

    String[] currentSerializations = conf.getStrings(HADOOP_IO_SERIALIZATION);
    String[] finalSerializations = new String[currentSerializations.length + 1];
    System.arraycopy(currentSerializations, 0, finalSerializations, 0,
        currentSerializations.length);
    finalSerializations[finalSerializations.length - 1] = AvroSerialization.class.getName();
    mapDriver.getConfiguration().setStrings(HADOOP_IO_SERIALIZATION, finalSerializations);

    AvroSerialization.addToConfiguration(conf);
    AvroSerialization.setKeyWriterSchema(conf, outputSchema);
    AvroSerialization.setValueWriterSchema(conf, Schema.create(Schema.Type.NULL));

  }

  private List<GenericRecord> generateTestData() throws Exception {
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    List<GenericRecord> inputRecords = new ArrayList<GenericRecord>();

    GenericRecord input = new GenericData.Record(schema);
    input.put("d1", "abc1");
    input.put("d2", "pqr1");
    input.put("d3", "xyz1");
    input.put("hoursSinceEpoch", generateRandomHoursSinceEpoch());
    input.put("m1", 10);
    input.put("m2", 20);
    inputRecords.add(input);

    input = new GenericData.Record(schema);
    input.put("d1", "abc2");
    input.put("d2", "pqr2");
    input.put("d3", "xyz2");
    input.put("hoursSinceEpoch", generateRandomHoursSinceEpoch());
    input.put("m1", 10);
    input.put("m2", 20);
    inputRecords.add(input);

    return inputRecords;
  }

  @Before
  public void setUp() throws Exception {
    DerivedColumnTransformationPhaseMapper mapper = new DerivedColumnTransformationPhaseMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration configuration = mapDriver.getConfiguration();
    configuration.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");

    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TABLE_NAME.toString(), "collection");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString(), "d1,d2,d3");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString(), "m1,m2");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString(), "INT,INT");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TIMECOLUMN_NAME.toString(), "hoursSinceEpoch");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_DIMENSION_NAMES.toString(), "d2,");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_METRICS.toString() + ".d2", "m1");
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_TOPK_KVALUES.toString() + ".d2", "1");

    ThirdEyeConfig thirdeyeConfig = ThirdEyeConfig.fromProperties(props);
    configuration.set(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_THIRDEYE_CONFIG.toString(),
        OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    Schema inputSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(AVRO_SCHEMA));
    setUpAvroSerialization(mapDriver.getConfiguration(), inputSchema);

    Schema outputSchema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(TRANSFORMATION_SCHEMA));
    configuration.set(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_OUTPUT_SCHEMA.toString(),
        outputSchema.toString());

    configuration.set(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_TOPK_PATH.toString(),
        ClassLoader.getSystemResource(TOPK_PATH).toString());

    TemporaryPath tmpPath = new TemporaryPath();
    outputPath = tmpPath.toString();
    configuration.set(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_OUTPUT_PATH.toString(), outputPath);

  }

  @Test
  public void testTopKColumnTransformationPhase() throws Exception {
    int recordCount = 0;

    List<GenericRecord> inputRecords = generateTestData();
    for (GenericRecord record : inputRecords) {
      AvroKey<GenericRecord> inKey = new AvroKey<GenericRecord>();
      inKey.datum(record);
      mapDriver.addInput(new Pair<AvroKey<GenericRecord>, NullWritable>(inKey, NullWritable.get()));
      recordCount++;
    }

    resetAvroSerialization();
    List<Pair<AvroKey<GenericRecord>, NullWritable>> result = mapDriver.run();
    Assert.assertEquals(recordCount, result.size());

    for (Pair<AvroKey<GenericRecord>, NullWritable> pair : result) {
      GenericRecord datum = pair.getFirst().datum();
      Assert.assertEquals("TopKTransformationJob did not add new column for topk column",
          datum.getSchema().getField("d2_raw") != null, true);
      String d2 = (String) datum.get("d2");
      String d2_raw = (String) datum.get("d2_raw");
      Assert.assertEquals("Incorrect topk column transformation", (d2_raw.equals("pqr1") && d2.equals("other")) || (d2_raw.equals("pqr2") && d2.equals("pqr2")), true);
    }
  }



  @After
  public void cleanUp() throws IOException {

    File f = new File(outputPath);
    FileUtils.deleteDirectory(f);
  }
}
