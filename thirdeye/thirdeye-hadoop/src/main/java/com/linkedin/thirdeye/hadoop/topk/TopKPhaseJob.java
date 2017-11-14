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

import static com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants.TOPK_PHASE_INPUT_PATH;
import static com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants.TOPK_PHASE_OUTPUT_PATH;
import static com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants.TOPK_PHASE_THIRDEYE_CONFIG;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.MinMaxPriorityQueue;
import com.linkedin.thirdeye.hadoop.config.DimensionType;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfigProperties;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;
import com.linkedin.thirdeye.hadoop.config.TopKDimensionToMetricsSpec;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAggregateMetricUtils;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAvroUtils;

/**
 * This phase reads avro input, and produces a file with top k values for dimensions
 *
 * Map:
 * Map phase reads avro records, and for each record emits
 * Key=(Dimension name, Dimension Value) Value=(Metrics)
 * For each record, map also emits a
 * Key=(ALL, ALL) Value=(Metrics)
 * This is used for computing the metric sums in the reduce phase
 *
 * Combine:
 * Combine phase receives Key=(DimensionName, DimensionValue)
 * from each map, and aggregates the metric values. This phase
 * helps in reducing the traffic sent to reducer
 *
 * Reduce:
 * We strictly use just 1 reducer.
 * Reduce phase receives Key=(DimensionName, DimensionValue)
 * and aggregates the metric values
 * The very first key received is (ALL, ALL) with helps us compute total metric sum
 * These metric sums are used to check metric thresholds of other
 * (dimensionName, dimensionValue) pairs. If none of the metric
 * thresholds pass, the pair is discarded.
 * In the cleanup, top k dimension values are picked for each dimension
 * based on the metric value
 * The top k dimension values for each dimension are written to a file
 *
 */
public class TopKPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopKPhaseJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TOPK_ALL_DIMENSION_NAME = "0";
  private static final String TOPK_ALL_DIMENSION_VALUE = "0";

  private String name;
  private Properties props;

  /**
   * @param name
   * @param props
   */
  public TopKPhaseJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class TopKPhaseMapper
      extends Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> {

    private TopKPhaseConfig config;
    ThirdEyeConfig thirdeyeConfig;
    private List<String> dimensionNames;
    private List<DimensionType> dimensionTypes;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private int numMetrics;
    BytesWritable keyWritable;
    BytesWritable valWritable;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("TopKPhaseJob.TopKPhaseMapper.setup()");
      Configuration configuration = context.getConfiguration();
      try {
        thirdeyeConfig = OBJECT_MAPPER.readValue(configuration.get(TOPK_PHASE_THIRDEYE_CONFIG.toString()), ThirdEyeConfig.class);
        config = TopKPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
        dimensionNames = config.getDimensionNames();
        dimensionTypes = config.getDimensionTypes();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        numMetrics = metricNames.size();
        valWritable = new BytesWritable();
        keyWritable = new BytesWritable();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }


    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
        throws IOException, InterruptedException {

      // input record
      GenericRecord inputRecord = key.datum();

      // read metrics
      Number[] metricValues = new Number[numMetrics];
      for (int i = 0; i < numMetrics; i++) {
        String metricName = metricNames.get(i);
        Number metricValue = ThirdeyeAvroUtils.getMetricFromRecord(inputRecord, metricName);
        metricValues[i] = metricValue;
      }
      TopKPhaseMapOutputValue valWrapper = new TopKPhaseMapOutputValue(metricValues, metricTypes);
      byte[] valBytes = valWrapper.toBytes();
      valWritable.set(valBytes, 0, valBytes.length);

      // read dimensions
      for (int i = 0; i < dimensionNames.size(); i++) {
        String dimensionName = dimensionNames.get(i);
        DimensionType dimensionType = dimensionTypes.get(i);
        Object dimensionValue = ThirdeyeAvroUtils.getDimensionFromRecord(inputRecord, dimensionName);

        TopKPhaseMapOutputKey keyWrapper = new TopKPhaseMapOutputKey(dimensionName, dimensionValue, dimensionType);
        byte[] keyBytes = keyWrapper.toBytes();
        keyWritable.set(keyBytes, 0, keyBytes.length);
        context.write(keyWritable, valWritable);
      }
      TopKPhaseMapOutputKey allKeyWrapper = new TopKPhaseMapOutputKey(TOPK_ALL_DIMENSION_NAME, TOPK_ALL_DIMENSION_VALUE, DimensionType.STRING);
      byte[] allKeyBytes = allKeyWrapper.toBytes();
      keyWritable.set(allKeyBytes, 0, allKeyBytes.length);
      context.write(keyWritable, valWritable);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

    }
  }

  public static class TopKPhaseCombiner
    extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private TopKPhaseConfig config;
    ThirdEyeConfig thirdeyeConfig;
    private List<MetricType> metricTypes;
    private int numMetrics;
    BytesWritable keyWritable;
    BytesWritable valWritable;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("TopKPhaseJob.TopKPhaseCombiner.setup()");
      Configuration configuration = context.getConfiguration();
      try {
        thirdeyeConfig = OBJECT_MAPPER.readValue(configuration.get(TOPK_PHASE_THIRDEYE_CONFIG.toString()), ThirdEyeConfig.class);
        config = TopKPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
        metricTypes = config.getMetricTypes();
        numMetrics = metricTypes.size();
        valWritable = new BytesWritable();
        keyWritable = new BytesWritable();

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
    throws IOException, InterruptedException {

      Number[] aggMetricValues = new Number[numMetrics];
      Arrays.fill(aggMetricValues, 0);

      for (BytesWritable value : values) {
        TopKPhaseMapOutputValue valWrapper = TopKPhaseMapOutputValue.fromBytes(value.getBytes(), metricTypes);
        Number[] metricValues = valWrapper.getMetricValues();
        ThirdeyeAggregateMetricUtils.aggregate(metricTypes, aggMetricValues, metricValues);
      }

      TopKPhaseMapOutputValue valWrapper = new TopKPhaseMapOutputValue(aggMetricValues, metricTypes);
      byte[] valBytes = valWrapper.toBytes();
      valWritable.set(valBytes, 0, valBytes.length);

      context.write(key, valWritable);
    }
  }

  public static class TopKPhaseReducer
      extends Reducer<BytesWritable, BytesWritable, NullWritable, NullWritable> {

    private FileSystem fileSystem;
    private Configuration configuration;

    private ThirdEyeConfig thirdeyeConfig;
    private TopKPhaseConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private Map<String, Integer> metricToIndexMapping;
    private int numMetrics;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    Number[] metricSums;
    private Map<String, Map<Object, Number[]>> dimensionNameToValuesMap;
    private TopKDimensionValues topkDimensionValues;
    private Map<String, Double> metricThresholds;
    private Map<String, Integer> thresholdPassCount;
    private Map<String, TopKDimensionToMetricsSpec> topKDimensionToMetricsSpecMap;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("TopKPhaseJob.TopKPhaseReducer.setup()");

      configuration = context.getConfiguration();
      fileSystem = FileSystem.get(configuration);
      try {
        thirdeyeConfig = OBJECT_MAPPER.readValue(configuration.get(TOPK_PHASE_THIRDEYE_CONFIG.toString()), ThirdEyeConfig.class);
        config = TopKPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
        LOGGER.info("Metric Thresholds form config {}", config.getMetricThresholds());
        metricThresholds = config.getMetricThresholds();
        topKDimensionToMetricsSpecMap = config.getTopKDimensionToMetricsSpec();
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();

        numMetrics = metricNames.size();

        metricToIndexMapping = new HashMap<>();
        for (int i = 0; i < numMetrics; i ++) {
          metricToIndexMapping.put(metricNames.get(i), i);
        }

        dimensionNameToValuesMap = new HashMap<>();
        thresholdPassCount = new HashMap<>();
        for (String dimension : dimensionNames) {
          dimensionNameToValuesMap.put(dimension, new HashMap<Object, Number[]>());
          thresholdPassCount.put(dimension, 0);
        }
        topkDimensionValues = new TopKDimensionValues();

        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values,
        Context context) throws IOException, InterruptedException {

      TopKPhaseMapOutputKey keyWrapper = TopKPhaseMapOutputKey.fromBytes(key.getBytes());
      String dimensionName = keyWrapper.getDimensionName();
      Object dimensionValue = keyWrapper.getDimensionValue();

      // Get aggregate metric values for dimension name value pair
      Number[] aggMetricValues = new Number[numMetrics];
      Arrays.fill(aggMetricValues, 0);
      for (BytesWritable value : values) {
        TopKPhaseMapOutputValue valWrapper = TopKPhaseMapOutputValue.fromBytes(value.getBytes(), metricTypes);
        Number[] metricValues = valWrapper.getMetricValues();
        ThirdeyeAggregateMetricUtils.aggregate(metricTypes, aggMetricValues, metricValues);
      }

      // Metric sums case
      if (dimensionName.equals(TOPK_ALL_DIMENSION_NAME) && dimensionValue.equals(TOPK_ALL_DIMENSION_VALUE)) {
        LOGGER.info("Setting metric sums");
        metricSums = new Number[numMetrics];
        metricSums = Arrays.copyOf(aggMetricValues, numMetrics);
        return;
      }

      // Check metric percentage threshold
      if (MapUtils.isNotEmpty(metricThresholds)) {
        boolean isPassThreshold = false;
        for (int i = 0; i < numMetrics; i++) {
          String metric = metricNames.get(i);
          double metricValue = aggMetricValues[i].doubleValue();
          double metricSum = metricSums[i].doubleValue();
          double metricThresholdPercentage = metricThresholds.get(metric);
          if (metricValue >= (metricSum * metricThresholdPercentage / 100)) {
            isPassThreshold = true;
            thresholdPassCount.put(dimensionName, thresholdPassCount.get(dimensionName) + 1);
            break;
          }
        }
        if (!isPassThreshold) {
          return;
        }
        dimensionNameToValuesMap.get(dimensionName).put(dimensionValue, aggMetricValues);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

      for (String dimension : dimensionNames) {

        LOGGER.info("{} records passed metric threshold for dimension {}", thresholdPassCount.get(dimension), dimension);

        // Get top k
        TopKDimensionToMetricsSpec topkSpec = topKDimensionToMetricsSpecMap.get(dimension);
        if (topkSpec != null && topkSpec.getDimensionName() != null && topkSpec.getTopk() != null) {

          // Get top k for each metric specified
          Map<String, Integer> topkMetricsMap = topkSpec.getTopk();
          for (Entry<String, Integer> topKEntry : topkMetricsMap.entrySet()) {

            String metric = topKEntry.getKey();
            int k = topKEntry.getValue();
            MinMaxPriorityQueue<DimensionValueMetricPair> topKQueue = MinMaxPriorityQueue.maximumSize(k).create();

            Map<Object, Number[]> dimensionToMetricsMap = dimensionNameToValuesMap.get(dimension);
            for (Entry<Object, Number[]> entry : dimensionToMetricsMap.entrySet()) {
              topKQueue.add(new DimensionValueMetricPair(entry.getKey(), entry.getValue()[metricToIndexMapping.get(metric)]));
            }
            LOGGER.info("Picking Top {} values for {} based on Metric {} : {}", k, dimension, metric, topKQueue);
            for (DimensionValueMetricPair pair : topKQueue) {
              topkDimensionValues.addValue(dimension, String.valueOf(pair.getDimensionValue()));
            }
          }
        }
      }

      if (topkDimensionValues.getTopKDimensions().size() > 0) {
        String topkValuesPath = configuration.get(TOPK_PHASE_OUTPUT_PATH.toString());
        LOGGER.info("Writing top k values to {}",topkValuesPath);
        FSDataOutputStream topKDimensionValuesOutputStream = fileSystem.create(
            new Path(topkValuesPath + File.separator + ThirdEyeConstants.TOPK_VALUES_FILE));
        OBJECT_MAPPER.writeValue((DataOutput) topKDimensionValuesOutputStream, topkDimensionValues);
        topKDimensionValuesOutputStream.close();
      }
    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(TopKPhaseJob.class);

    Configuration configuration = job.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);

    // Properties
    LOGGER.info("Properties {}", props);

     // Input Path
    String inputPathDir = getAndSetConfiguration(configuration, TOPK_PHASE_INPUT_PATH);
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(ThirdEyeConstants.FIELD_SEPARATOR)) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    // Output path
    Path outputPath = new Path(getAndSetConfiguration(configuration, TOPK_PHASE_OUTPUT_PATH));
    LOGGER.info("Output path dir: " + outputPath.toString());
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, outputPath);

    // Schema
    Schema avroSchema = ThirdeyeAvroUtils.getSchema(inputPathDir);
    LOGGER.info("Schema : {}", avroSchema.toString(true));

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
    job.getConfiguration().set(TOPK_PHASE_THIRDEYE_CONFIG.toString(), OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    // Map config
    job.setMapperClass(TopKPhaseMapper.class);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Combiner
    job.setCombinerClass(TopKPhaseCombiner.class);

     // Reduce config
    job.setReducerClass(TopKPhaseReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(1);

    job.waitForCompletion(true);

    return job;
  }


  private String getAndSetConfiguration(Configuration configuration,
      TopKPhaseConstants constant) {
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

}
