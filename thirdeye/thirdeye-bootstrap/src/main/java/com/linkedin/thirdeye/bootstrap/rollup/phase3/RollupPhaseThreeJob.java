package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import static com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeConstants.ROLLUP_PHASE3_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeConstants.ROLLUP_PHASE3_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeConstants.ROLLUP_PHASE3_OUTPUT_PATH;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.thirdeye.api.RollupSelectFunction;
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
import com.linkedin.thirdeye.api.RollupThresholdFunction;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoReduceOutput;

/**
 * @author kgopalak
 */
public class RollupPhaseThreeJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollupPhaseThreeJob.class);

  private String name;
  private Properties props;

  public RollupPhaseThreeJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class RollupPhaseThreeMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseThreeConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    RollupThresholdFunction thresholdFunc;
    MultipleOutputs<BytesWritable, BytesWritable> mos;
    Map<String, Integer> dimensionNameToIndexMapping;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("RollupPhaseOneJob.RollupPhaseOneMapper.setup()");
      mos = new MultipleOutputs<BytesWritable, BytesWritable>(context);
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE3_CONFIG_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = RollupPhaseThreeConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        dimensionNameToIndexMapping = new HashMap<String, Integer>();

        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable rawDimensionMD5KeyWritable,
        BytesWritable rollupReduceOutputWritable, Context context)
            throws IOException, InterruptedException {
      // pass through, in the reduce we gather all possible roll up for a given
      // rawDimensionKey
      context.write(rawDimensionMD5KeyWritable, rollupReduceOutputWritable);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }

  }

  public static class RollupPhaseThreeReducer
      extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseThreeConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private RollupSelectFunction rollupFunc;
    private RollupThresholdFunction rollupThresholdFunction;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE3_CONFIG_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = RollupPhaseThreeConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        rollupFunc = new DefaultRollupFunc();
        String className = config.getThresholdFuncClassName();
        Map<String, String> params = config.getThresholdFuncParams();
        Constructor<?> constructor = Class.forName(className).getConstructor(Map.class);
        rollupThresholdFunction = (RollupThresholdFunction) constructor.newInstance(params);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable rawDimensionMD5KeyWritable,
        Iterable<BytesWritable> rollupReduceOutputWritableIterable, Context context)
            throws IOException, InterruptedException {
      DimensionKey rawDimensionKey = null;
      MetricTimeSeries rawMetricTimeSeries = null;
      Map<DimensionKey, MetricTimeSeries> possibleRollupTimeSeriesMap =
          new HashMap<DimensionKey, MetricTimeSeries>();
      for (BytesWritable writable : rollupReduceOutputWritableIterable) {
        RollupPhaseTwoReduceOutput temp;
        temp = RollupPhaseTwoReduceOutput.fromBytes(writable.copyBytes(), metricSchema);
        if (rawMetricTimeSeries == null) {
          rawDimensionKey = temp.getRawDimensionKey();
          rawMetricTimeSeries = temp.getRawTimeSeries();
        }
        possibleRollupTimeSeriesMap.put(temp.getRollupDimensionKey(), temp.getRollupTimeSeries());
      }
      // select the roll up dimension key
      DimensionKey selectedRollup =
          rollupFunc.rollup(rawDimensionKey, possibleRollupTimeSeriesMap, rollupThresholdFunction);
      if (selectedRollup == null)
        throw new IllegalStateException(
            "rollup function could not find any dimension combination that passes threshold - "
                + "Try a lower threshold, or a different threshold metric: " + "rawDimensionKey="
                + rawDimensionKey + ";possibleRollupTimeSeriesMap=" + possibleRollupTimeSeriesMap);
      context.write(new BytesWritable(selectedRollup.toBytes()),
          new BytesWritable(rawMetricTimeSeries.toBytes()));
    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(RollupPhaseThreeJob.class);
    job.getConfiguration().set("mapreduce.reduce.shuffle.input.buffer.percent", "0.40");

    // Map config
    job.setMapperClass(RollupPhaseThreeMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    // job.setCombinerClass(RollupPhaseThreeReducer.class);
    job.setReducerClass(RollupPhaseThreeReducer.class);
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
    // rollup phase 2 config
    Configuration configuration = job.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);
    String inputPathDir = getAndSetConfiguration(configuration, ROLLUP_PHASE3_INPUT_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE3_CONFIG_PATH);
    Path outputPath = new Path(getAndSetConfiguration(configuration, ROLLUP_PHASE3_OUTPUT_PATH));
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
      RollupPhaseThreeConstants constant) {
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
