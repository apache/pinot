package com.linkedin.thirdeye.hadoop.topk;

import static com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants.TOPK_ROLLUP_PHASE_CONFIG_PATH;
import static com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants.TOPK_ROLLUP_PHASE_INPUT_PATH;
import static com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants.TOPK_ROLLUP_PHASE_OUTPUT_PATH;
import static com.linkedin.thirdeye.hadoop.topk.TopKPhaseConstants.TOPK_ROLLUP_PHASE_SCHEMA_PATH;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Properties;

import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TopKDimensionSpec;
import com.linkedin.thirdeye.hadoop.ThirdEyeConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
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

/**
 * Map Input = Input from avro Key:(Avro record), value:(null)
 * Map Output = Key:(dimensionName, dimensionValue) Value:(Number[] metricValues)
 * Reduce Output = null
 * Reduce generates a file with top k values for specified dimensions
 */
public class TopKPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopKPhaseJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TOPK_ALL_DIMENSION_NAME = "0";
  private static final String TOPK_ALL_DIMENSION_VALUE = "0";
  private static final String EMPTY_STRING = "";
  private static final Number EMPTY_NUMBER = 0;
  private static final String TOPK_VALUES_FILE = "topk_values";

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

  public static class TopKRollupPhaseOneMapper
      extends Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> {

    private TopKPhaseConfig config;
    ThirdEyeConfig thirdeyeConfig;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private int numMetrics;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    Map<String, Integer> dimensionNameToIndexMapping;
    Map<String, Long> metricSums;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("TopKRollupPhaseOneJob.TopKRollupPhaseOneMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE_CONFIG_PATH.toString()));
      try {
        thirdeyeConfig = ThirdEyeConfig.decode(fileSystem.open(configPath));
        config = TopKPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        numMetrics = metricNames.size();
        valWritable = new BytesWritable();
        keyWritable = new BytesWritable();
        dimensionNameToIndexMapping = new HashMap<String, Integer>();
        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }
        metricSums = new HashMap<String, Long>();
        for (String metricName : metricNames) {
          metricSums.put(metricName, 0L);
        }
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
        Number metricValue = getMetricFromRecord(inputRecord, metricName);
        metricValues[i] = metricValue;
      }
      TopKPhaseMapOutputValue valWrapper = new TopKPhaseMapOutputValue(metricValues, metricTypes);
      byte[] valBytes = valWrapper.toBytes();
      valWritable.set(valBytes, 0, valBytes.length);

      // read dimensions
      for (String dimensionName : dimensionNames) {
        String dimensionValue = getDimensionFromRecord(inputRecord, dimensionName);

        TopKPhaseMapOutputKey keyWrapper = new TopKPhaseMapOutputKey(dimensionName, dimensionValue);
        byte[] keyBytes = keyWrapper.toBytes();
        keyWritable.set(keyBytes, 0, keyBytes.length);
        context.write(keyWritable, valWritable);

        keyWrapper = new TopKPhaseMapOutputKey(TOPK_ALL_DIMENSION_NAME, TOPK_ALL_DIMENSION_VALUE);
        keyBytes = keyWrapper.toBytes();
        keyWritable.set(keyBytes, 0, keyBytes.length);
        context.write(keyWritable, valWritable);
      }
    }


    private String getDimensionFromRecord(GenericRecord record, String dimensionName) {
      String dimensionValue = (String) record.get(dimensionName);
      if (dimensionValue == null) {
        dimensionValue = EMPTY_STRING;
      }
      return dimensionValue;
    }

    private Number getMetricFromRecord(GenericRecord record, String metricName) {
      Number metricValue = (Number) record.get(metricName);
      if (metricValue == null) {
        metricValue = EMPTY_NUMBER;
      }
      return metricValue;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

    }
  }

  public static class TopKRollupPhaseCombiner
    extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private TopKPhaseConfig config;
    ThirdEyeConfig thirdeyeConfig;
    private List<MetricType> metricTypes;
    private int numMetrics;
    BytesWritable keyWritable;
    BytesWritable valWritable;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("TopKRollupPhaseOneJob.TopKRollupPhaseOneCombiner.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE_CONFIG_PATH.toString()));
      try {
        thirdeyeConfig = ThirdEyeConfig.decode(fileSystem.open(configPath));
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
        for (int i = 0; i < numMetrics; i++) {
          MetricType metricType = metricTypes.get(i);
          switch (metricType) {
            case SHORT:
              aggMetricValues[i] = aggMetricValues[i].shortValue() + metricValues[i].shortValue();
              break;
            case INT:
              aggMetricValues[i] = aggMetricValues[i].intValue() + metricValues[i].intValue();
              break;
            case FLOAT:
              aggMetricValues[i] = aggMetricValues[i].floatValue() + metricValues[i].floatValue();
              break;
            case DOUBLE:
              aggMetricValues[i] = aggMetricValues[i].doubleValue() + metricValues[i].doubleValue();
              break;
            case LONG:
            default:
              aggMetricValues[i] = aggMetricValues[i].longValue() + metricValues[i].longValue();
              break;
          }
        }
      }

      TopKPhaseMapOutputValue valWrapper = new TopKPhaseMapOutputValue(aggMetricValues, metricTypes);
      byte[] valBytes = valWrapper.toBytes();
      valWritable.set(valBytes, 0, valBytes.length);

      context.write(key, valWritable);
    }
  }

  public static class TopKRollupPhaseOneReducer
      extends Reducer<BytesWritable, BytesWritable, NullWritable, NullWritable> {

    private FileSystem fileSystem;
    private Configuration configuration;

    private ThirdEyeConfig thirdeyeConfig;
    private TopKPhaseConfig config;
    List<String> dimensionNames;
    List<String> metricNames;
    private List<MetricType> metricTypes;
    private Map<String, Integer> metricToIndexMapping;
    private int numMetrics;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    Number[] metricSums;
    private Map<String, Map<String, Number[]>> dimensionNameToValuesMap;
    private TopKDimensionValues topkDimensionValues;
    private Map<String, Double> metricThresholds;
    private Map<String, TopKDimensionSpec> topKDimensionSpecMap;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("TopKRollupPhaseOneJob.TopKRollupPhaseOneReducer.setup()");

      configuration = context.getConfiguration();
      fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE_CONFIG_PATH.toString()));
      try {
        thirdeyeConfig = ThirdEyeConfig.decode(fileSystem.open(configPath));
        config = TopKPhaseConfig.fromThirdEyeConfig(thirdeyeConfig);
        metricThresholds = config.getMetricThresholds();
        topKDimensionSpecMap = config.getTopKDimensionSpec();
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        numMetrics = metricNames.size();
        metricToIndexMapping = new HashMap<>();
        for (int i = 0; i < numMetrics; i ++) {
          metricToIndexMapping.put(metricNames.get(i), i);
        }
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();
        dimensionNameToValuesMap = new HashMap<>();
        for (String dimension : dimensionNames) {
          dimensionNameToValuesMap.put(dimension, new HashMap<String, Number[]>());
        }
        topkDimensionValues = new TopKDimensionValues();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values,
        Context context) throws IOException, InterruptedException {

      TopKPhaseMapOutputKey keyWrapper = TopKPhaseMapOutputKey.fromBytes(key.getBytes());
      String dimensionName = keyWrapper.getDimensionName();
      String dimensionValue = keyWrapper.getDimensionValue();

      // Get aggregate metric values for dimension name value pair
      Number[] aggMetricValues = new Number[numMetrics];
      Arrays.fill(aggMetricValues, 0);
      for (BytesWritable value : values) {
        TopKPhaseMapOutputValue valWrapper = TopKPhaseMapOutputValue.fromBytes(value.getBytes(), metricTypes);
        Number[] metricValues = valWrapper.getMetricValues();
        for (int i = 0; i < numMetrics; i++) {
          MetricType metricType = metricTypes.get(i);
          switch (metricType) {
            case SHORT:
              aggMetricValues[i] = aggMetricValues[i].shortValue() + metricValues[i].shortValue();
              break;
            case INT:
              aggMetricValues[i] = aggMetricValues[i].intValue() + metricValues[i].intValue();
              break;
            case FLOAT:
              aggMetricValues[i] = aggMetricValues[i].floatValue() + metricValues[i].floatValue();
              break;
            case DOUBLE:
              aggMetricValues[i] = aggMetricValues[i].doubleValue() + metricValues[i].doubleValue();
              break;
            case LONG:
            default:
              aggMetricValues[i] = aggMetricValues[i].longValue() + metricValues[i].longValue();
              break;
          }
        }
      }
      // Metric sums case
      if (dimensionName.equals(TOPK_ALL_DIMENSION_NAME) && dimensionValue.equals(TOPK_ALL_DIMENSION_VALUE)) {
        LOGGER.info("Setting metric sums");
        metricSums = new Number[numMetrics];
        metricSums = Arrays.copyOf(aggMetricValues, numMetrics);
        return;
      }
      // Check metric percentage threshold
      boolean isPassThreshold = false;
      for (int i = 0; i < numMetrics; i++) {
        String metric = metricNames.get(i);
        double metricValue = aggMetricValues[i].doubleValue();
        double metricSum = metricSums[i].doubleValue();
        double metricThresholdPercentage = metricThresholds.get(metric);
        if (metricValue > (metricSum * metricThresholdPercentage / 100)) {
          isPassThreshold = true;
          break;
        }
      }
      if (!isPassThreshold) {
        LOGGER.info("No metric passed threshold");
        return;
      }
      dimensionNameToValuesMap.get(dimensionName).put(dimensionValue, aggMetricValues);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (String dimension : dimensionNames) {

        TopKDimensionSpec topkSpec = topKDimensionSpecMap.get(dimension);
        if (topkSpec != null && topkSpec.getDimensionName() != null && topkSpec.getMetricName() != null && topkSpec.getTop() != 0) {
          LOGGER.info("Dimension : {} Metric : {} Top : {}", topkSpec.getDimensionName(), topkSpec.getMetricName(), topkSpec.getTop() );

          Map<String, Number[]> dimensionToMetricsMap = dimensionNameToValuesMap.get(dimension);
          PriorityQueue<DimensionValueMetricPair> topKQueue = new PriorityQueue<>();
          for (Entry<String, Number[]> entry : dimensionToMetricsMap.entrySet()) {
            topKQueue.add(new DimensionValueMetricPair(entry.getKey(), entry.getValue()[metricToIndexMapping.get(topkSpec.getMetricName())]));
            if (topKQueue.size() > topkSpec.getTop()) {
              topKQueue.remove();
            }
          }
          for (DimensionValueMetricPair pair : topKQueue) {
            topkDimensionValues.addValue(dimension, pair.getDimensionValue());
          }
        }
      }

      if (topkDimensionValues.getTopKDimensions().size() > 0) {
        LOGGER.info("Writing top k values to {}",TOPK_ROLLUP_PHASE_OUTPUT_PATH.toString());
        FSDataOutputStream topKDimensionValuesOutputStream =
            fileSystem.create(new Path(configuration.get(TOPK_ROLLUP_PHASE_OUTPUT_PATH.toString())
                + File.separator + TOPK_VALUES_FILE));
        OBJECT_MAPPER.writeValue(topKDimensionValuesOutputStream, topkDimensionValues);
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

    // Input schema
    Path schemaPath = new Path(getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE_SCHEMA_PATH));
    FSDataInputStream schemaStream = fs.open(schemaPath);
    Schema inputSchema = new Schema.Parser().parse(schemaStream);

    // Input Path
    String inputPathDir = getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE_INPUT_PATH);

    // Config path
    getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE_CONFIG_PATH);

    // Output path
    Path outputPath = new Path(getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE_OUTPUT_PATH));
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, outputPath);

    // Map config
    job.setMapperClass(TopKRollupPhaseOneMapper.class);
    AvroJob.setInputKeySchema(job, inputSchema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Combiner
    job.setCombinerClass(TopKRollupPhaseCombiner.class);

     // Reduce config
    job.setReducerClass(TopKRollupPhaseOneReducer.class);
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
