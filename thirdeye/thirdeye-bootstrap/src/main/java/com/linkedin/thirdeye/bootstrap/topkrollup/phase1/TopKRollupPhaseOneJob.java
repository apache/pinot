package com.linkedin.thirdeye.bootstrap.topkrollup.phase1;

import static com.linkedin.thirdeye.bootstrap.topkrollup.phase1.TopKRollupPhaseOneConstants.TOPK_ROLLUP_PHASE1_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase1.TopKRollupPhaseOneConstants.TOPK_ROLLUP_PHASE1_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase1.TopKRollupPhaseOneConstants.TOPK_ROLLUP_PHASE1_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase1.TopKRollupPhaseOneConstants.TOPK_ROLLUP_PHASE1_SCHEMA_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase1.TopKRollupPhaseOneConstants.TOPK_ROLLUP_PHASE1_METRIC_SUMS_PATH;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.thirdeye.api.StarTreeConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.bootstrap.aggregation.MetricSums;

/**
 * Map Input = Input from aggregation Key:(serialized dimension key), value:(serialized
 * metricTimeSeires)
 * Map Output = Key:(dimensionName, dimensionValue) Value:(metricTimeSeries)
 * Reduce Output = Key:(dimensionName, dimensionValue) Value:(aggregated metricTimeSeries)
 * Reduce emits the record only if metric sums
 * are greater than threshold specified in config
 */
public class TopKRollupPhaseOneJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopKRollupPhaseOneJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;

  private Properties props;

  /**
   * @param name
   * @param props
   */
  public TopKRollupPhaseOneJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class TopKRollupPhaseOneMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private TopKRollupPhaseOneConfig config;
    StarTreeConfig starTreeConfig;
    private List<String> dimensionNames;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    Map<String, Integer> dimensionNameToIndexMapping;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("TopKRollupPhaseOneJob.TopKRollupPhaseOneMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE1_CONFIG_PATH.toString()));
      try {
        starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = TopKRollupPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();
        dimensionNameToIndexMapping = new HashMap<String, Integer>();
        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable dimensionKeyBytes, BytesWritable aggSeries, Context context)
        throws IOException, InterruptedException {

      DimensionKey dimensionKey = DimensionKey.fromBytes(dimensionKeyBytes.getBytes());
      String[] dimensionValues = dimensionKey.getDimensionValues();

      for (String dimensionName : config.getDimensionNames()) {

        String dimensionValue = dimensionValues[dimensionNameToIndexMapping.get(dimensionName)];

        TopKRollupPhaseOneMapOutputKey keyWrapper;
        keyWrapper = new TopKRollupPhaseOneMapOutputKey(dimensionName, dimensionValue);
        byte[] keyBytes = keyWrapper.toBytes();
        keyWritable.set(keyBytes, 0, keyBytes.length);

        context.write(keyWritable, aggSeries);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    }
  }

  public static class TopKRollupPhaseOneReducer
      extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private StarTreeConfig starTreeConfig;
    private TopKRollupPhaseOneConfig config;
    private List<String> dimensionNames;
    private List<MetricType> metricTypes;
    private Map<String, Double> metricThresholds;
    private Map<String, List<String>> dimensionExceptions;
    private MetricSchema metricSchema;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    Map<String, Long> metricSums;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("TopKRollupPhaseOneJob.TopKRollupPhaseOneReducer.setup()");

      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE1_CONFIG_PATH.toString()));
      try {
        starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = TopKRollupPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        metricThresholds = config.getMetricThresholds();
        dimensionExceptions = config.getDimensionExceptions();
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();

        MetricSums metricSumsObj = OBJECT_MAPPER.readValue(
            fileSystem
                .open(new Path(configuration.get(TOPK_ROLLUP_PHASE1_METRIC_SUMS_PATH.toString()))),
            MetricSums.class);
        metricSums = metricSumsObj.getMetricSum();

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable topkRollupKey, Iterable<BytesWritable> timeSeriesIterable,
        Context context) throws IOException, InterruptedException {

      TopKRollupPhaseOneMapOutputKey wrapper =
          TopKRollupPhaseOneMapOutputKey.fromBytes(topkRollupKey.getBytes());
      String dimensionName = wrapper.getDimensionName();
      String dimensionValue = wrapper.getDimensionValue();
      LOGGER.info("DimensionName {} DimensionValue {}", dimensionName, dimensionValue);

      MetricTimeSeries aggregateSeries = new MetricTimeSeries(metricSchema);
      for (BytesWritable writable : timeSeriesIterable) {
        MetricTimeSeries series = MetricTimeSeries.fromBytes(writable.copyBytes(), metricSchema);
        aggregateSeries.aggregate(series);
      }

      if (dimensionException(dimensionName, dimensionValue)
          || aboveThreshold(aggregateSeries)) {
        LOGGER.info("Passed threshold");
        valWritable.set(aggregateSeries.toBytes(), 0, aggregateSeries.toBytes().length);
        context.write(topkRollupKey, valWritable);
      }
    }

    private boolean dimensionException(String dimensionName, String dimensionValue) {

      boolean dimensionException = false;
      List<String> dimensionExceptionsList = dimensionExceptions.get(dimensionName);
      if (dimensionExceptionsList != null && dimensionExceptionsList.contains(dimensionValue)) {
        LOGGER.info("Adding exception {} {}", dimensionName, dimensionValue);
        dimensionException = true;
      }
      return dimensionException;
    }

    private boolean aboveThreshold(MetricTimeSeries aggregateSeries) {

      Map<String, Long> metricValues = new HashMap<String, Long>();
      for (MetricSpec metricSpec : starTreeConfig.getMetrics()) {
        metricValues.put(metricSpec.getName(), 0L);
      }
      for (Long time : aggregateSeries.getTimeWindowSet()) {
        for (MetricSpec metricSpec : starTreeConfig.getMetrics()) {
          String metricName = metricSpec.getName();
          long metricValue = aggregateSeries.get(time, metricName).longValue();
          metricValues.put(metricName, metricValues.get(metricName) + metricValue);
        }
      }

      boolean aboveThreshold = true;
      for (MetricSpec metricSpec : starTreeConfig.getMetrics()) {
        String metricName = metricSpec.getName();

        long metricValue = metricValues.get(metricName);
        long metricSum = metricSums.get(metricName);
        double metricThreshold = metricThresholds.get(metricName);

        LOGGER.info("metricValue : {} metricSum : {}", metricValue, metricSum);
        if (metricValue < (metricThreshold / 100) * metricSum) {
          aboveThreshold = false;
          break;
        }
      }
      return aboveThreshold;
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(TopKRollupPhaseOneJob.class);

    // Map config
    job.setMapperClass(TopKRollupPhaseOneMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(TopKRollupPhaseOneReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    String numReducers = props.getProperty("num.reducers");
    if (numReducers != null) {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());

    // topk_rollup_phase1 phase config
    Configuration configuration = job.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);
    String inputPathDir = getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE1_INPUT_PATH);
    getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE1_CONFIG_PATH);
    Path outputPath = new Path(getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE1_OUTPUT_PATH));
    getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE1_METRIC_SUMS_PATH);
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

    job.waitForCompletion(true);

    return job;

  }

  private String getAndSetConfiguration(Configuration configuration,
      TopKRollupPhaseOneConstants constant) {
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
