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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.hadoop.ThirdEyeJobProperties;
import com.linkedin.thirdeye.hadoop.config.DimensionType;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfigProperties;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;
import com.linkedin.thirdeye.hadoop.config.TimeGranularity;
import com.linkedin.thirdeye.hadoop.config.TimeSpec;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAggregateMetricUtils;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAvroUtils;

import static com.linkedin.thirdeye.hadoop.aggregation.AggregationPhaseConstants.*;

/**
 * Buckets input avro data according to granularity specified in config and aggregates metrics
 * Mapper:
 * Converts time column into bucket granularity
 * Reducer:
 * Aggregates all records with same dimensions in one time bucket
 */
public class AggregationPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregationPhaseJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  public AggregationPhaseJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class AggregationMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> {

    private ThirdEyeConfig thirdeyeConfig;
    private AggregationPhaseConfig config;
    private List<String> dimensionNames;
    private List<DimensionType> dimensionTypes;
    private List<String> metricNames;
    List<MetricType> metricTypes;
    private int numMetrics;
    private String timeColumnName;
    private TimeGranularity inputGranularity;
    private TimeGranularity aggregateGranularity;
    private BytesWritable keyWritable;
    private BytesWritable valWritable;
    private int numRecords;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("AggregationPhaseJob.AggregationPhaseMapper.setup()");
      Configuration configuration = context.getConfiguration();

      thirdeyeConfig = OBJECT_MAPPER.readValue(configuration.get(AGG_PHASE_THIRDEYE_CONFIG.toString()), ThirdEyeConfig.class);
      config = AggregationPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
      dimensionNames = config.getDimensionNames();
      dimensionTypes = config.getDimensionTypes();
      metricNames = config.getMetricNames();
      numMetrics = metricNames.size();
      metricTypes = config.getMetricTypes();
      timeColumnName = config.getTime().getColumnName();
      inputGranularity = config.getInputTime().getTimeGranularity();
      aggregateGranularity = config.getTime().getTimeGranularity();
      keyWritable = new BytesWritable();
      valWritable = new BytesWritable();
      numRecords = 0;
    }

    @Override
    public void map(AvroKey<GenericRecord> record, NullWritable value, Context context) throws IOException, InterruptedException {

      // input record
      GenericRecord inputRecord = record.datum();

      // dimensions
      List<Object> dimensions = new ArrayList<>();
      for (String dimension : dimensionNames) {
        Object dimensionValue = ThirdeyeAvroUtils.getDimensionFromRecord(inputRecord, dimension);
        dimensions.add(dimensionValue);
      }

      // metrics
      Number[] metrics = new Number[numMetrics];
      for (int i = 0; i < numMetrics; i++) {
        Number metricValue = ThirdeyeAvroUtils.getMetricFromRecord(inputRecord, metricNames.get(i), metricTypes.get(i));
        metrics[i] = metricValue;
      }

      // time
      long timeValue = ThirdeyeAvroUtils.getMetricFromRecord(inputRecord, timeColumnName).longValue();
      long inputTimeMillis = inputGranularity.toMillis(timeValue);
      long bucketTime = aggregateGranularity.convertToUnit(inputTimeMillis);

      AggregationPhaseMapOutputKey keyWrapper = new AggregationPhaseMapOutputKey(bucketTime, dimensions, dimensionTypes);
      byte[] keyBytes = keyWrapper.toBytes();
      keyWritable.set(keyBytes, 0, keyBytes.length);

      AggregationPhaseMapOutputValue valWrapper = new AggregationPhaseMapOutputValue(metrics, metricTypes);
      byte[] valBytes = valWrapper.toBytes();
      valWritable.set(valBytes, 0, valBytes.length);

      numRecords ++;
      context.write(keyWritable, valWritable);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      context.getCounter(AggregationCounter.NUMBER_OF_RECORDS).increment(numRecords);
    }
  }

  public static class AggregationReducer
      extends Reducer<BytesWritable, BytesWritable, AvroKey<GenericRecord>, NullWritable> {

    private Schema avroSchema;
    private ThirdEyeConfig thirdeyeConfig;
    private AggregationPhaseConfig config;
    private List<String> dimensionsNames;
    private List<DimensionType> dimensionTypes;
    private List<String> metricNames;
    List<MetricType> metricTypes;
    private int numMetrics;
    private TimeSpec time;
    private int numRecords;
    private Number[] metricSums;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("AggregationPhaseJob.AggregationPhaseReducer.setup()");
      Configuration configuration = context.getConfiguration();

      thirdeyeConfig = OBJECT_MAPPER.readValue(configuration.get(AGG_PHASE_THIRDEYE_CONFIG.toString()), ThirdEyeConfig.class);
      config = AggregationPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
      dimensionsNames = config.getDimensionNames();
      dimensionTypes = config.getDimensionTypes();
      metricNames = config.getMetricNames();
      numMetrics = metricNames.size();
      metricTypes = config.getMetricTypes();
      time = config.getTime();
      avroSchema = new Schema.Parser().parse(configuration.get(AGG_PHASE_AVRO_SCHEMA.toString()));
      numRecords = 0;
      metricSums = new Number[numMetrics];
      Arrays.fill(metricSums, 0);
    }

    @Override
    public void reduce(BytesWritable aggregationKey, Iterable<BytesWritable> values,
        Context context) throws IOException, InterruptedException {

      // output record
      GenericRecord outputRecord = new Record(avroSchema);

      AggregationPhaseMapOutputKey keyWrapper = AggregationPhaseMapOutputKey.fromBytes(aggregationKey.getBytes(), dimensionTypes);

      // time
      long timeValue = keyWrapper.getTime();
      outputRecord.put(time.getColumnName(), timeValue);

      // dimensions
      List<Object> dimensionValues = keyWrapper.getDimensionValues();
      for (int i = 0; i < dimensionsNames.size(); i++) {
        String dimensionName = dimensionsNames.get(i);
        Object dimensionValue = dimensionValues.get(i);
        outputRecord.put(dimensionName, dimensionValue);
      }

      // aggregate metrics
      Number[] aggMetricValues = new Number[numMetrics];
      Arrays.fill(aggMetricValues, 0);
      for (BytesWritable value : values) {
        Number[] metricValues = AggregationPhaseMapOutputValue.fromBytes(value.getBytes(), metricTypes).getMetricValues();
        ThirdeyeAggregateMetricUtils.aggregate(metricTypes, aggMetricValues, metricValues);
      }
      ThirdeyeAggregateMetricUtils.aggregate(metricTypes, metricSums, aggMetricValues);

      // metrics
      for (int i = 0; i < numMetrics; i++) {
        String metricName = metricNames.get(i);
        Number metricValue = aggMetricValues[i];
        outputRecord.put(metricName, metricValue);
      }

      numRecords ++;
      AvroKey<GenericRecord> outputKey = new AvroKey<GenericRecord>(outputRecord);
      context.write(outputKey, NullWritable.get());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      context.getCounter(AggregationCounter.NUMBER_OF_RECORDS_FLATTENED).increment(numRecords);
      for (int i = 0; i < numMetrics; i++) {
        context.getCounter(thirdeyeConfig.getCollection(), metricNames.get(i)).increment(metricSums[i].longValue());
      }
    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(AggregationPhaseJob.class);

    FileSystem fs = FileSystem.get(getConf());
    Configuration configuration = job.getConfiguration();

    // Properties
    LOGGER.info("Properties {}", props);

     // Input Path
    String inputPathDir = getAndSetConfiguration(configuration, AGG_PHASE_INPUT_PATH);
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(ThirdEyeConstants.FIELD_SEPARATOR)) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    // Output path
    Path outputPath = new Path(getAndSetConfiguration(configuration, AGG_PHASE_OUTPUT_PATH));
    LOGGER.info("Output path dir: " + outputPath.toString());
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, outputPath);

    // Schema
    Schema avroSchema = ThirdeyeAvroUtils.getSchema(inputPathDir);
    LOGGER.info("Schema : {}", avroSchema.toString(true));
    job.getConfiguration().set(AGG_PHASE_AVRO_SCHEMA.toString(), avroSchema.toString());

    // ThirdEyeConfig
    String dimensionTypesProperty = ThirdeyeAvroUtils.getDimensionTypesProperty(
        props.getProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString()), avroSchema);
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_TYPES.toString(), dimensionTypesProperty);
    String metricTypesProperty = ThirdeyeAvroUtils.getMetricTypesProperty(
        props.getProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString()),
        props.getProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString()), avroSchema);
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString(), metricTypesProperty);
    ThirdEyeConfig thirdeyeConfig = ThirdEyeConfig.fromProperties(props);
    LOGGER.info("Thirdeye Config {}", thirdeyeConfig.encode());
    job.getConfiguration().set(AGG_PHASE_THIRDEYE_CONFIG.toString(), OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    // Map config
    job.setMapperClass(AggregationMapper.class);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(AggregationReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    AvroJob.setOutputKeySchema(job, avroSchema);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    String numReducers = props.getProperty(ThirdEyeJobProperties.THIRDEYE_NUM_REDUCERS.getName());
    LOGGER.info("Num Reducers : {}", numReducers);
    if (StringUtils.isNotBlank(numReducers)) {
      job.setNumReduceTasks(Integer.valueOf(numReducers));
      LOGGER.info("Setting num reducers {}", job.getNumReduceTasks());
    }

    job.waitForCompletion(true);

    Counter counter = job.getCounters().findCounter(AggregationCounter.NUMBER_OF_RECORDS);
    LOGGER.info(counter.getDisplayName() + " : " + counter.getValue());
    if (counter.getValue() == 0) {
      throw new IllegalStateException("No input records in " + inputPathDir);
    }
    counter = job.getCounters().findCounter(AggregationCounter.NUMBER_OF_RECORDS_FLATTENED);
    LOGGER.info(counter.getDisplayName() + " : " + counter.getValue());

    for (String metric : thirdeyeConfig.getMetricNames()) {
      counter = job.getCounters().findCounter(thirdeyeConfig.getCollection(), metric);
      LOGGER.info(counter.getDisplayName() + " : " + counter.getValue());
    }

    return job;
  }

  private String getAndSetConfiguration(Configuration configuration,
      AggregationPhaseConstants constant) {
    String value = getAndCheck(constant.toString());
    configuration.set(constant.toString(), value);
    return value;
  }

  private String getAndCheck(String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

  public static enum AggregationCounter {
    NUMBER_OF_RECORDS,
    NUMBER_OF_RECORDS_FLATTENED
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: config.properties");
    }

    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));

    AggregationPhaseJob job = new AggregationPhaseJob("aggregate_avro_job", props);
    job.run();
  }
}
