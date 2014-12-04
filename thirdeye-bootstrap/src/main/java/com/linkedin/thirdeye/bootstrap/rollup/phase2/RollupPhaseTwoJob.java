package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.ROLLUP_PHASE2_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.ROLLUP_PHASE2_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.ROLLUP_PHASE2_OUTPUT_PATH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricSchema;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
import com.linkedin.thirdeye.bootstrap.MetricType;
import com.linkedin.thirdeye.bootstrap.rollup.RollupThresholdFunc;
import com.linkedin.thirdeye.bootstrap.rollup.TotalAggregateBasedRollupFunction;

public class RollupPhaseTwoJob extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(RollupPhaseTwoJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  public RollupPhaseTwoJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class RollupPhaseTwoMapper extends
      Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseTwoConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private List<String> rollupOrder;
    RollupThresholdFunc thresholdFunc;
    Map<String, Integer> dimensionNameToIndexMapping;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("RollupPhaseOneJob.RollupPhaseOneMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE2_CONFIG_PATH
          .toString()));
      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            RollupPhaseTwoConfig.class);
        dimensionNames = config.getDimensionNames();
        dimensionNameToIndexMapping = new HashMap<String, Integer>();

        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }
        metricNames = config.getMetricNames();
        metricTypes = Lists.newArrayList();
        for (String type : config.getMetricTypes()) {
          metricTypes.add(MetricType.valueOf(type));
        }
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        // TODO: get this form config
        thresholdFunc = new TotalAggregateBasedRollupFunction(
            "numberOfMemberConnectionsSent", 5000);
        rollupOrder = config.getRollupOrder();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable dimensionKeyWritable,
        BytesWritable metricTimeSeriesWritable, Context context)
        throws IOException, InterruptedException {
      
      DimensionKey rawDimensionKey;
      rawDimensionKey = DimensionKey.fromBytes(dimensionKeyWritable.getBytes());
      MetricTimeSeries timeSeries;
      byte[] bytes = metricTimeSeriesWritable.getBytes();
      timeSeries = MetricTimeSeries.fromBytes(bytes, metricSchema);
      // generate all combinations from the original dimension.
      RollupPhaseTwoMapOutput wrapper;
      wrapper = new RollupPhaseTwoMapOutput(rawDimensionKey, timeSeries);
      BytesWritable wrapperBytesWritable = new BytesWritable(wrapper.toBytes());
      LOG.info("Map.key {}", rawDimensionKey);
      List<DimensionKey> combinations = generateCombinations(rawDimensionKey);
      LOG.info("combinations:{}", combinations);
      BytesWritable combinationBytesWritable;
      combinationBytesWritable = new BytesWritable();
      for (DimensionKey combination : combinations) {
        byte[] combinationBytes = combination.toBytes();
        combinationBytesWritable.set(combinationBytes, 0, combinationBytes.length);
        LOG.info("Map.combination:{}", combination);
        LOG.info("Map.raw Dimension:{}", wrapper.dimensionKey);
        context.write(combinationBytesWritable, wrapperBytesWritable);
      }

    }

    private List<DimensionKey> generateCombinations(DimensionKey dimensionKey) {
      String[] dimensionsValues = dimensionKey.getDimensionsValues();
      List<DimensionKey> combinations = new ArrayList<DimensionKey>();
      String[] comb = Arrays.copyOf(dimensionsValues, dimensionsValues.length);
      for (String dimensionToRollup : rollupOrder) {
        comb = Arrays.copyOf(comb, comb.length);
        comb[dimensionNameToIndexMapping.get(dimensionToRollup)] = "?";
        combinations.add(new DimensionKey(comb));
      }
      return combinations;
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
    }

  }

  public static class RollupPhaseTwoReducer extends
      Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseTwoConfig config;
    private TimeUnit sourceTimeUnit;
    private TimeUnit aggregationTimeUnit;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE2_CONFIG_PATH
          .toString()));
      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            RollupPhaseTwoConfig.class);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = Lists.newArrayList();
        for (String type : config.getMetricTypes()) {
          metricTypes.add(MetricType.valueOf(type));
        }
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable rollupDimensionKeyWritable,
        Iterable<BytesWritable> rollupMapOutputWritableIterable, Context context)
        throws IOException, InterruptedException {
      DimensionKey rollupDimensionKey = DimensionKey
          .fromBytes(rollupDimensionKeyWritable.getBytes());
      MetricTimeSeries rollupTimeSeries = new MetricTimeSeries(metricSchema);
      Map<DimensionKey, MetricTimeSeries> map = new HashMap<DimensionKey, MetricTimeSeries>();
      //LOG.info("rollup Dimension:{}", rollupDimensionKey);
      for (BytesWritable writable : rollupMapOutputWritableIterable) {
        RollupPhaseTwoMapOutput temp;
        temp = RollupPhaseTwoMapOutput.fromBytes(writable.getBytes(),
            metricSchema);
        //LOG.info("temp.dimensionKey:{}", temp.dimensionKey);
        map.put(temp.dimensionKey, temp.getTimeSeries());
        rollupTimeSeries.aggregate(temp.getTimeSeries());
      }
      for (Entry<DimensionKey, MetricTimeSeries> entry : map.entrySet()) {
        RollupPhaseTwoReduceOutput output;
        output = new RollupPhaseTwoReduceOutput(rollupDimensionKey,
            rollupTimeSeries, entry.getKey(), entry.getValue());
        context.write(new BytesWritable(entry.getKey().toMD5()),
            new BytesWritable(output.toBytes()));
       // LOG.info("Phase 2 raw dimension:{}, raw dimension timeseries:{}", entry.getKey(), entry.getValue());
       // LOG.info("Phase 2 Reduce output value rollup dimension {}, timeseries:{}", rollupDimensionKey,rollupTimeSeries );

      }
    }
  }

  public void run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(RollupPhaseTwoJob.class);

    // Map config
    job.setMapperClass(RollupPhaseTwoMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    //job.setCombinerClass(RollupPhaseTwoReducer.class);
    job.setReducerClass(RollupPhaseTwoReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    // rollup phase 2 config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration,
        ROLLUP_PHASE2_INPUT_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE2_CONFIG_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE2_OUTPUT_PATH);
    LOG.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOG.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat.setOutputPath(job, new Path(
        getAndCheck(ROLLUP_PHASE2_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);
  }

  private String getAndSetConfiguration(Configuration configuration,
      RollupPhaseTwoConstants constant) {
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
