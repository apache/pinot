package com.linkedin.thirdeye.bootstrap.topkrollup.phase2;

import static com.linkedin.thirdeye.bootstrap.topkrollup.phase2.TopKRollupPhaseTwoConstants.TOPK_ROLLUP_PHASE2_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase2.TopKRollupPhaseTwoConstants.TOPK_ROLLUP_PHASE2_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase2.TopKRollupPhaseTwoConstants.TOPK_ROLLUP_PHASE2_OUTPUT_PATH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.thirdeye.api.StarTreeConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TopKDimensionSpec;
import com.linkedin.thirdeye.bootstrap.topkrollup.phase1.TopKRollupPhaseOneMapOutputKey;

/**
 * Map Input = Key:(dimensionName, dimensionValue) Value:(metricTimeSeries)
 * Map Output = Key:(dimensionName), Value:(dimensionValue, metricTimeSeries)
 * Reduce Output = Key:(dimensionName) Value: (dimensionValue)
 * Reduce picks top k dimension values for that dimension,
 * according to metric specified in the config
 */
public class TopKRollupPhaseTwoJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopKRollupPhaseTwoJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;

  private Properties props;

  /**
   * @param name
   * @param props
   */
  public TopKRollupPhaseTwoJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class TopKRollupPhaseTwoMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private TopKRollupPhaseTwoConfig config;
    StarTreeConfig starTreeConfig;
    List<MetricType> metricTypes;
    MetricSchema metricSchema;
    BytesWritable keyWritable;
    BytesWritable valWritable;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("TopKRollupPhaseTwoJob.TopKRollupPhaseTwoMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE2_CONFIG_PATH.toString()));
      try {
        starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = TopKRollupPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException {

      TopKRollupPhaseOneMapOutputKey phase1KeyWrapper;
      phase1KeyWrapper = TopKRollupPhaseOneMapOutputKey.fromBytes(key.getBytes());
      String dimensionName = phase1KeyWrapper.getDimensionName();
      String dimensionValue = phase1KeyWrapper.getDimensionValue();
      LOGGER.info("Dim name {} value {}", dimensionName, dimensionValue);

      MetricTimeSeries series = MetricTimeSeries.fromBytes(value.getBytes(), metricSchema);
      TopKRollupPhaseTwoMapOutputValue valueWrapper =
          new TopKRollupPhaseTwoMapOutputValue(dimensionValue, series);

      TopKRollupPhaseTwoMapOutputKey phase2Key = new TopKRollupPhaseTwoMapOutputKey(dimensionName);
      byte[] keyBytes = phase2Key.toBytes();
      keyWritable.set(keyBytes, 0, keyBytes.length);

      byte[] valueBytes = valueWrapper.toBytes();
      valWritable.set(valueBytes, 0, valueBytes.length);

      context.write(keyWritable, valWritable);

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    }
  }

  public static class TopKRollupPhaseTwoReducer
      extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private StarTreeConfig starTreeConfig;
    private TopKRollupPhaseTwoConfig config;
    private List<MetricType> metricTypes;
    private List<TopKDimensionSpec> rollupDimensionConfig;
    private MetricSchema metricSchema;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    TopKDimensionValues topKDimensionValues;
    Map<String, List<String>> dimensionExceptions;

    FileSystem fileSystem;
    Configuration configuration;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("TopKRollupPhaseTwoJob.TopKRollupPhaseTwoReducer.setup()");

      configuration = context.getConfiguration();
      fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE2_CONFIG_PATH.toString()));
      try {
        starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = TopKRollupPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        rollupDimensionConfig = config.getRollupDimensionConfig();
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();
        topKDimensionValues = new TopKDimensionValues();
        dimensionExceptions = config.getDimensionExceptions();

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable dimensionNameKey, Iterable<BytesWritable> topKRollupValues,
        Context context) throws IOException, InterruptedException {

      // find topk rollup config for this dimension
      String dimensionName = new String(dimensionNameKey.getBytes()).trim();
      LOGGER.info("Dimension name {}: ", dimensionName);

      // all dimension values
      List<TopKRollupPhaseTwoMapOutputValue> mapOutputValues =
          new ArrayList<TopKRollupPhaseTwoMapOutputValue>();
      for (BytesWritable writable : topKRollupValues) {
        TopKRollupPhaseTwoMapOutputValue valueWrapper;
        valueWrapper =
            TopKRollupPhaseTwoMapOutputValue.fromBytes(writable.getBytes(), metricSchema);
        mapOutputValues.add(valueWrapper);
      }

      TopKDimensionSpec topkDimensionSpec = null;
      for (TopKDimensionSpec spec : rollupDimensionConfig) {
        if (spec.getDimensionName().equals(dimensionName)) {
          topkDimensionSpec = spec;
        }
      }

      if (topkDimensionSpec == null || mapOutputValues.size() <= topkDimensionSpec.getTop()) {
        LOGGER.info("Taking all dimension values");
        for (TopKRollupPhaseTwoMapOutputValue mapOutput : mapOutputValues) {
          String dimensionValue = mapOutput.getDimensionValue();
          topKDimensionValues.addValue(dimensionName, dimensionValue);
        }
      } else {

        final TopKDimensionSpec topKDimension = topkDimensionSpec;

        // sort based on comparator
        Collections.sort(mapOutputValues, new Comparator<TopKRollupPhaseTwoMapOutputValue>() {

          @Override
          public int compare(TopKRollupPhaseTwoMapOutputValue o1,
              TopKRollupPhaseTwoMapOutputValue o2) {

            MetricTimeSeries series1 = o1.getSeries();
            long series1Sum = 0;
            for (Long time : series1.getTimeWindowSet()) {
              Number metricValue = series1.get(time, topKDimension.getMetricName());
              series1Sum += metricValue.longValue();
            }
            MetricTimeSeries series2 = o2.getSeries();
            long series2Sum = 0;
            for (Long time : series2.getTimeWindowSet()) {
              Number metricValue = series2.get(time, topKDimension.getMetricName());
              series2Sum += metricValue.longValue();
            }
            return (int) (series2Sum - series1Sum);
          }
        });

        // emit topk and exceptions
        List<String> dimensionExceptionsList = dimensionExceptions.get(dimensionName);
        for (int k = 0; k < mapOutputValues.size(); k++) {
          TopKRollupPhaseTwoMapOutputValue valueWrapper = mapOutputValues.get(k);
          String dimensionValue = valueWrapper.getDimensionValue();
          if (k < topkDimensionSpec.getTop()) {
            LOGGER.info("K : {} dimensionvalue : {}", k, dimensionValue);
            topKDimensionValues.addValue(dimensionName, dimensionValue);
          } else {
            if (dimensionExceptionsList != null && dimensionExceptionsList.contains(dimensionValue)) {
              LOGGER.info("K : {} Adding dimension exception : {}", k, dimensionValue);
              topKDimensionValues.addValue(dimensionName, dimensionValue);
            }
          }
        }

      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (topKDimensionValues.getTopKDimensions().size() > 0) {
        FSDataOutputStream topKDimensionValuesOutputStream =
            fileSystem.create(new Path(configuration.get(TOPK_ROLLUP_PHASE2_OUTPUT_PATH.toString())
                + "/" + context.getTaskAttemptID() + ".stat"));
        OBJECT_MAPPER.writeValue(topKDimensionValuesOutputStream, topKDimensionValues);
        topKDimensionValuesOutputStream.close();
      }
    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(TopKRollupPhaseTwoJob.class);

    // Map config
    job.setMapperClass(TopKRollupPhaseTwoMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(TopKRollupPhaseTwoReducer.class);

    String numReducers = props.getProperty("num.reducers");
    if (numReducers != null) {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());

    // topk_rollup_phase2 phase config
    Configuration configuration = job.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);
    String inputPathDir = getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE2_INPUT_PATH);
    getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE2_CONFIG_PATH);
    Path outputPath = new Path(getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE2_OUTPUT_PATH));
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
      TopKRollupPhaseTwoConstants constant) {
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
