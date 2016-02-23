package com.linkedin.thirdeye.bootstrap.rollup.phase1;

import static com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneConstants.ROLLUP_PHASE1_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneConstants.ROLLUP_PHASE1_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneConstants.ROLLUP_PHASE1_OUTPUT_PATH;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.thirdeye.api.RollupThresholdFunction;
import com.linkedin.thirdeye.api.StarTreeConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;

/**
 * THis job, splits the input data into aboveThreshold and belowThreshold by applying
 * RollupThresholdFunction on the each record.
 * The belowThreshold records are processed in the subsequent jobs (Phase2 and Phase 3 of Rollup)
 * @author kgopalak
 */
public class RollupPhaseOneJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollupPhaseOneJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;

  private Properties props;

  /**
   * @param name
   * @param props
   */
  public RollupPhaseOneJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  /**
   * @author kgopalak
   */
  public static class RollupPhaseOneMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseOneConfig config;
    private List<String> dimensionNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    RollupThresholdFunction thresholdFunc;
    MultipleOutputs<BytesWritable, BytesWritable> mos;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("RollupPhaseOneJob.RollupPhaseOneMapper.setup()");
      mos = new MultipleOutputs<BytesWritable, BytesWritable>(context);
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE1_CONFIG_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = RollupPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        String className = config.getThresholdFuncClassName();
        Map<String, String> params = config.getThresholdFuncParams();
        Constructor<?> constructor = Class.forName(className).getConstructor(Map.class);
        thresholdFunc = (RollupThresholdFunction) constructor.newInstance(params);

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable dimensionKeyBytes, BytesWritable metricTimeSeriesBytes,
        Context context) throws IOException, InterruptedException {
      DimensionKey dimensionKey;
      dimensionKey = DimensionKey.fromBytes(dimensionKeyBytes.getBytes());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("dimension key {}", dimensionKey);
      }
      MetricTimeSeries timeSeries;
      byte[] bytes = metricTimeSeriesBytes.getBytes();
      timeSeries = MetricTimeSeries.fromBytes(bytes, metricSchema);
      if (thresholdFunc.isAboveThreshold(timeSeries)) {
        // write this to a different output path
        mos.write(dimensionKeyBytes, metricTimeSeriesBytes,
            "aboveThreshold" + "/" + "aboveThreshold");
        context.getCounter(RollupCounter.ABOVE_THRESHOLD).increment(1);
      } else {
        mos.write(dimensionKeyBytes, metricTimeSeriesBytes,
            "belowThreshold" + "/" + "belowThreshold");
        context.getCounter(RollupCounter.BELOW_THRESHOLD).increment(1);

      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("time series  {}", timeSeries);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }

  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(RollupPhaseOneJob.class);

    // Map config
    job.setMapperClass(RollupPhaseOneMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    job.setNumReduceTasks(0);
    // Reduce config
    // job.setReducerClass(RollupPhaseOneReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    // aggregation phase config
    Configuration configuration = job.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);

    String inputPathDir = getAndSetConfiguration(configuration, ROLLUP_PHASE1_INPUT_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE1_CONFIG_PATH);
    Path outputPath = new Path(getAndSetConfiguration(configuration, ROLLUP_PHASE1_OUTPUT_PATH));
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    MultipleOutputs.addNamedOutput(job, "aboveThreshold", SequenceFileOutputFormat.class,
        BytesWritable.class, BytesWritable.class);
    MultipleOutputs.addNamedOutput(job, "belowThreshold", SequenceFileOutputFormat.class,
        BytesWritable.class, BytesWritable.class);

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    Counters counters = job.getCounters();
    for (Enum e : RollupCounter.values()) {
      Counter counter = counters.findCounter(e);
      System.out.println(counter.getDisplayName() + " : " + counter.getValue());
    }

    JobStatus status = job.getStatus();
    if (status.getState() == JobStatus.State.SUCCEEDED) {
      Path belowThresholdPath =
          new Path(new Path(getAndCheck(ROLLUP_PHASE1_OUTPUT_PATH.toString())), "belowThreshold");
      Path aboveThresholdPath =
          new Path(new Path(getAndCheck(ROLLUP_PHASE1_OUTPUT_PATH.toString())), "aboveThreshold");

      if (!fs.exists(belowThresholdPath)) {
        fs.mkdirs(belowThresholdPath);
      }
      if (!fs.exists(aboveThresholdPath)) {
        fs.mkdirs(aboveThresholdPath);
      }
    }

    return job;

  }

  private String getAndSetConfiguration(Configuration configuration,
      RollupPhaseOneConstants constant) {
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

  public static enum RollupCounter {
    ABOVE_THRESHOLD,
    BELOW_THRESHOLD
  }
}
