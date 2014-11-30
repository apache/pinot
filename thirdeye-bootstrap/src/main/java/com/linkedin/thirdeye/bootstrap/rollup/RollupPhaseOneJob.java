package com.linkedin.thirdeye.bootstrap.rollup;

import static com.linkedin.thirdeye.bootstrap.rollup.RollupPhaseOneConstants.ROLLUP_PHASE1_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.RollupPhaseOneConstants.ROLLUP_PHASE1_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.RollupPhaseOneConstants.ROLLUP_PHASE1_OUTPUT_PATH;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricSchema;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
import com.linkedin.thirdeye.bootstrap.MetricType;

public class RollupPhaseOneJob extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(RollupPhaseOneJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  enum Constants {

  }

  public RollupPhaseOneJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class RollupPhaseOneMapper extends
      Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseOneConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private String[] dimensionValues;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("RollupPhaseOneJob.RollupPhaseOneMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE1_CONFIG_PATH
          .toString()));
      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            RollupPhaseOneConfig.class);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = Lists.newArrayList();
        for (String type : config.getMetricTypes()) {
          metricTypes.add(MetricType.valueOf(type));
        }
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        dimensionValues = new String[dimensionNames.size()];
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable dimensionKeyBytes,
        BytesWritable metricTimeSeriesBytes, Context context)
        throws IOException, InterruptedException {

      if (Math.random() > 0.95) {
        DimensionKey dimensionKey;
        dimensionKey = DimensionKey.fromBytes(dimensionKeyBytes.getBytes());
        LOG.info("dimension key {}", dimensionKey);
        MetricTimeSeries timeSeries;
        byte[] bytes = metricTimeSeriesBytes.getBytes();
        timeSeries = MetricTimeSeries.fromBytes(bytes, metricSchema);
        LOG.info("time series  {}", timeSeries);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {

    }

  }

  public static class RollupPhaseOneReducer extends
      Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseOneConfig config;
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
      Path configPath = new Path(configuration.get(ROLLUP_PHASE1_CONFIG_PATH
          .toString()));
      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            RollupPhaseOneConfig.class);
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
    public void reduce(BytesWritable aggregationKey,
        Iterable<BytesWritable> timeSeriesIterable, Context context)
        throws IOException, InterruptedException {
      MetricTimeSeries out = new MetricTimeSeries(metricSchema);
      // AggregationKey key =
      // AggregationKey.fromBytes(aggregationKey.getBytes());
      for (BytesWritable writable : timeSeriesIterable) {
        MetricTimeSeries series = MetricTimeSeries.fromBytes(
            writable.getBytes(), metricSchema);
        out.aggregate(series);
      }
      byte[] serializedBytes = out.toBytes();
      context.write(aggregationKey, new BytesWritable(serializedBytes));
    }
  }

  public void run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(RollupPhaseOneJob.class);

    // Map config
    job.setMapperClass(RollupPhaseOneMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);
    // AvroJob.setMapOutputKeySchema(job,
    // Schema.create(Schema.Type.STRING));
    // AvroJob.setMapOutputValueSchema(job, schema);

    // Reduce config
    job.setReducerClass(RollupPhaseOneReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);

    // aggregation phase config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration,
        ROLLUP_PHASE1_INPUT_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE1_CONFIG_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE1_OUTPUT_PATH);
    LOG.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOG.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat.setOutputPath(job, new Path(
        getAndCheck(ROLLUP_PHASE1_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);
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

}
