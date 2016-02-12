package com.linkedin.thirdeye.bootstrap.topkrollup.phase3;

import static com.linkedin.thirdeye.bootstrap.topkrollup.phase3.TopKRollupPhaseThreeConstants.TOPK_ROLLUP_PHASE3_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase3.TopKRollupPhaseThreeConstants.TOPK_ROLLUP_PHASE3_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase3.TopKRollupPhaseThreeConstants.TOPK_ROLLUP_PHASE3_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.topkrollup.phase3.TopKRollupPhaseThreeConstants.TOPK_DIMENSIONS_PATH;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.linkedin.thirdeye.api.StarTreeConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.bootstrap.topkrollup.phase2.TopKDimensionValues;

/**
 * Map Input = Key:(Serialized dimensionKey) Value:(Serialized metricTimeSeries) from
 * aggregation phase
 * Map Output = Key:(Serialized dimensionKey) Value:(Serialized metricTimeSeries)
 * Map replaces every dimension value which is not in top k by ?
 * Reduce Output = Key:(Serialized dimensionKey) Value:(Serialized metricTimeSeries)
 */
public class TopKRollupPhaseThreeJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopKRollupPhaseThreeJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;

  private Properties props;

  /**
   * @param name
   * @param props
   */
  public TopKRollupPhaseThreeJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class TopKRollupPhaseThreeMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private TopKRollupPhaseThreeConfig config;
    StarTreeConfig starTreeConfig;
    List<MetricType> metricTypes;
    List<String> dimensionNames;
    MetricSchema metricSchema;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    TopKDimensionValues topKDimensionValues;
    Map<String, Set<String>> topKDimensionValuesSet;
    Map<String, Integer> dimensionNameToIndexMapping;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("TopKRollupPhaseThreeJob.TopKRollupPhaseThreeMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE3_CONFIG_PATH.toString()));
      Path topKDimensionsPath = new Path(configuration.get(TOPK_DIMENSIONS_PATH.toString()));
      try {
        starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = TopKRollupPhaseThreeConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        dimensionNameToIndexMapping = new HashMap<String, Integer>();
        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();
        topKDimensionValues = new TopKDimensionValues();

        FileStatus[] fileStatuses = fileSystem.listStatus(topKDimensionsPath, new PathFilter() {

          @Override
          public boolean accept(Path p) {
            return p.getName().startsWith("attempt");
          }
        });
        for (FileStatus fileStatus : fileStatuses) {
          TopKDimensionValues valuesFile = OBJECT_MAPPER
              .readValue(fileSystem.open(fileStatus.getPath()), TopKDimensionValues.class);
          topKDimensionValues.addMap(valuesFile);
        }
        topKDimensionValuesSet = topKDimensionValues.getTopKDimensions();
        for (Entry<String, Set<String>> entry : topKDimensionValuesSet.entrySet()) {
          LOGGER.info("entry = {} {}", entry.getKey(), entry.getValue());
        }

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable dimensionKeyBytes, BytesWritable seriesBytes, Context context)
        throws IOException, InterruptedException {

      DimensionKey dimensionKey;
      dimensionKey = DimensionKey.fromBytes(dimensionKeyBytes.getBytes());
      LOGGER.info("dimension key {}", dimensionKey);

      String[] dimensionValues = dimensionKey.getDimensionValues();
      for (String dimension : dimensionNames) {
        int index = dimensionNameToIndexMapping.get(dimension);
        String value = dimensionValues[index];
        if (!topKDimensionValuesSet.get(dimension).contains(value)) {
          LOGGER.info("Missing value {} for dimension {}", value, dimension);
          dimensionValues[index] = "?";
        }
      }
      DimensionKey newDimensionKey = new DimensionKey(dimensionValues);
      LOGGER.info("New Dimension Key {}", newDimensionKey);
      byte[] keyBytes = newDimensionKey.toBytes();
      keyWritable.set(keyBytes, 0, keyBytes.length);

      context.write(keyWritable, seriesBytes);

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    }
  }

  public static class TopKRollupPhaseThreeReducer
      extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private StarTreeConfig starTreeConfig;
    private TopKRollupPhaseThreeConfig config;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    BytesWritable keyWritable;
    BytesWritable valWritable;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("TopKRollupPhaseThreeJob.TopKRollupPhaseThreeReducer.setup()");

      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(TOPK_ROLLUP_PHASE3_CONFIG_PATH.toString()));
      try {
        starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = TopKRollupPhaseThreeConfig.fromStarTreeConfig(starTreeConfig);
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable dimensionNameKey, Iterable<BytesWritable> timeSeriesIterable,
        Context context) throws IOException, InterruptedException {

      MetricTimeSeries aggregateSeries = new MetricTimeSeries(metricSchema);
      for (BytesWritable writable : timeSeriesIterable) {
        MetricTimeSeries series = MetricTimeSeries.fromBytes(writable.copyBytes(), metricSchema);
        aggregateSeries.aggregate(series);
      }

      byte[] seriesBytes = aggregateSeries.toBytes();
      valWritable.set(seriesBytes, 0, seriesBytes.length);
      context.write(dimensionNameKey, valWritable);

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(TopKRollupPhaseThreeJob.class);

    // Map config
    job.setMapperClass(TopKRollupPhaseThreeMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(TopKRollupPhaseThreeReducer.class);
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

    // topk_rollup_phase2 phase config
    Configuration configuration = job.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);
    String inputPathDir = getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE3_INPUT_PATH);
    getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE3_CONFIG_PATH);
    Path outputPath = new Path(getAndSetConfiguration(configuration, TOPK_ROLLUP_PHASE3_OUTPUT_PATH));
    getAndSetConfiguration(configuration, TOPK_DIMENSIONS_PATH);
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
      TopKRollupPhaseThreeConstants constant) {
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
