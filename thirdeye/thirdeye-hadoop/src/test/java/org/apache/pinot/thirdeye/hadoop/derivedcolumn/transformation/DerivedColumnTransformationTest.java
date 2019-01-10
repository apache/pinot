/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.hadoop.config.DimensionType;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfigProperties;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;
import com.linkedin.thirdeye.hadoop.topk.TopKDimensionValues;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAvroUtils;

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
    input.put("d2", 501L);
    input.put("d3", "xyz1");
    input.put("hoursSinceEpoch", generateRandomHoursSinceEpoch());
    input.put("m1", 10);
    input.put("m2", 20);
    inputRecords.add(input);

    input = new GenericData.Record(schema);
    input.put("d1", "abc2");
    input.put("d2", 502L);
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
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_TYPES.toString(), "STRING,LONG,STRING");
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
          datum.getSchema().getField("d2_topk") != null, true);
      Long d2 =  (Long) datum.get("d2");
      String d2_topk =  (String) datum.get("d2_topk");
      Assert.assertEquals("Incorrect topk column transformation", (d2_topk.equals("other") && d2 == 501l) || (d2_topk.equals("502") && d2 == 502l), true);
    }
  }

  @After
  public void cleanUp() throws IOException {

    File f = new File(outputPath);
    FileUtils.deleteDirectory(f);
  }

  public static class DerivedColumnTransformationPhaseMapper
  extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

    private Schema outputSchema;
    private ThirdEyeConfig thirdeyeConfig;
    private DerivedColumnTransformationPhaseConfig config;
    private List<String> dimensionsNames;
    private List<DimensionType> dimensionTypes;
    private Map<String, String> nonWhitelistValueMap;
    private List<String> metricNames;
    private TopKDimensionValues topKDimensionValues;
    private Map<String, Set<String>> topKDimensionsMap;
    private String timeColumnName;
    private Map<String, List<String>> whitelist;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      FileSystem fs = FileSystem.get(configuration);

      thirdeyeConfig = OBJECT_MAPPER.readValue(configuration.get(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_THIRDEYE_CONFIG.toString()), ThirdEyeConfig.class);
      config = DerivedColumnTransformationPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
      dimensionsNames = config.getDimensionNames();
      dimensionTypes = config.getDimensionTypes();
      nonWhitelistValueMap = config.getNonWhitelistValue();
      metricNames = config.getMetricNames();
      timeColumnName = config.getTimeColumnName();
      whitelist = config.getWhitelist();

      outputSchema = new Schema.Parser().parse(configuration.get(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_OUTPUT_SCHEMA.toString()));

      Path topKPath = new Path(configuration.get(DerivedColumnTransformationPhaseConstants.DERIVED_COLUMN_TRANSFORMATION_PHASE_TOPK_PATH.toString())
          + File.separator + ThirdEyeConstants.TOPK_VALUES_FILE);
      topKDimensionValues = new TopKDimensionValues();
      if (fs.exists(topKPath)) {
        FSDataInputStream topkValuesStream = fs.open(topKPath);
        topKDimensionValues = OBJECT_MAPPER.readValue((DataInput)topkValuesStream, TopKDimensionValues.class);
        topkValuesStream.close();
      }
      topKDimensionsMap = topKDimensionValues.getTopKDimensions();
    }


    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
        throws IOException, InterruptedException {

      // input record
      GenericRecord inputRecord = key.datum();

      // output record
      GenericRecord outputRecord = new Record(outputSchema);

      // dimensions
      for (int i = 0; i < dimensionsNames.size(); i++) {
        String dimensionName = dimensionsNames.get(i);
        DimensionType dimensionType = dimensionTypes.get(i);
        Object dimensionValue = ThirdeyeAvroUtils.getDimensionFromRecord(inputRecord, dimensionName);
        String dimensionValueStr = String.valueOf(dimensionValue);

        // add original dimension value with whitelist applied
        Object whitelistDimensionValue = dimensionValue;
        if (whitelist != null) {
          List<String> whitelistDimensions = whitelist.get(dimensionName);
          if (CollectionUtils.isNotEmpty(whitelistDimensions)) {
            // whitelist config exists for this dimension but value not present in whitelist
            if (!whitelistDimensions.contains(dimensionValueStr)) {
              whitelistDimensionValue = dimensionType.getValueFromString(nonWhitelistValueMap.get(dimensionName));
            }
          }
        }
        outputRecord.put(dimensionName, whitelistDimensionValue);

        // add column for topk, if topk config exists for that column
        if (topKDimensionsMap.containsKey(dimensionName)) {
          Set<String> topKDimensionValues = topKDimensionsMap.get(dimensionName);
          // if topk config exists for that dimension
          if (CollectionUtils.isNotEmpty(topKDimensionValues)) {
            String topkDimensionName = dimensionName + ThirdEyeConstants.TOPK_DIMENSION_SUFFIX;
            Object topkDimensionValue = dimensionValue;
            // topk config exists for this dimension, but value not present in topk
            if (!topKDimensionValues.contains(dimensionValueStr) &&
                (whitelist == null || whitelist.get(dimensionName) == null || !whitelist.get(dimensionName).contains(dimensionValueStr))) {
              topkDimensionValue = ThirdEyeConstants.OTHER;
            }
            outputRecord.put(topkDimensionName, String.valueOf(topkDimensionValue));
          }
        }
      }


      // metrics
      for (String metric : metricNames) {
        outputRecord.put(metric, ThirdeyeAvroUtils.getMetricFromRecord(inputRecord, metric));
      }

      // time
      outputRecord.put(timeColumnName, ThirdeyeAvroUtils.getMetricFromRecord(inputRecord, timeColumnName));

      AvroKey<GenericRecord> outputKey = new AvroKey<GenericRecord>(outputRecord);
      context.write(outputKey, NullWritable.get());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

    }
  }
}
