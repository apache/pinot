package com.linkedin.thirdeye.bootstrap.aggregation;

import static com.linkedin.thirdeye.bootstrap.aggregation.AggregationJobConstants.AGG_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.aggregation.AggregationJobConstants.AGG_INPUT_AVRO_SCHEMA;
import static com.linkedin.thirdeye.bootstrap.aggregation.AggregationJobConstants.AGG_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.aggregation.AggregationJobConstants.AGG_OUTPUT_PATH;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * @author kgopalak INPUT: RAW DATA FILES. EACH RECORD OF THE FORMAT DIMENSION,
 *         TIME, RECORD MAP OUTPUT: DIMENSION KEY, TIME, METRIC REDUCE OUTPUT:
 *         DIMENSIO KEY: SET{TIME_BUCKET, METRIC}
 */
public class AggregatePhaseJob extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(AggregatePhaseJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  enum Constants {

  }

  public AggregatePhaseJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class AggregationMapper
      extends
      Mapper<AvroKey<GenericRecord>, NullWritable, Text, AggregationTimeSeries> {
    private AggregationJobConfig config;
    private TimeUnit sourceTimeUnit;
    private TimeUnit aggregationTimeUnit;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<String> metricTypes;
    private MessageDigest md5;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("AggregatePhaseJob.AggregationMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(AGG_CONFIG_PATH.toString()));
      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            AggregationJobConfig.class);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        sourceTimeUnit = TimeUnit.valueOf(config.getTimeUnit());
        aggregationTimeUnit = TimeUnit.valueOf(config
            .getAggregationGranularity());
        md5 = MessageDigest.getInstance("MD5");

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(AvroKey<GenericRecord> record, NullWritable value,
        Context context) throws IOException, InterruptedException {

      List<String> dimensionValues = new ArrayList<String>();

      for (String dimensionName : dimensionNames) {
        dimensionValues.add(record.datum().get(dimensionName).toString());
      }
      AggregationKey key = new AggregationKey(dimensionValues);
      if(Math.random() > 0.9){
        LOG.info("{}",key);
      }
      String sourceTimeWindow = record.datum().get(config.getTimeColumnName())
          .toString();

      long aggregationTimeWindow = aggregationTimeUnit.convert(
          Long.parseLong(sourceTimeWindow), sourceTimeUnit);
      AggregationTimeSeries series = new AggregationTimeSeries(metricTypes);
      for (int i = 0; i < metricNames.size(); i++) {
        String metricName = metricNames.get(i);
        Object object = record.datum().get(metricName);
        String metricValueStr ="0";
        if (object != null) {
          metricValueStr = object.toString();
        }
        Number metricValue = MetricUnit.valueOf(metricTypes.get(i)).toNumber(
            metricValueStr);
        series.set(aggregationTimeWindow, i, metricValue);
      }
      byte[] digest = md5.digest(dimensionValues.toString().getBytes());
      
      context.write(new Text(digest), series);
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {

    }

  }

  public static class AggregationReducer
      extends
      Reducer<AggregationKey, AggregationTimeSeries, AggregationKey, AggregationTimeSeries> {
    private AggregationJobConfig config;
    private TimeUnit sourceTimeUnit;
    private TimeUnit aggregationTimeUnit;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<String> metricTypes;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(AGG_CONFIG_PATH.toString()));
      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            AggregationJobConfig.class);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        sourceTimeUnit = TimeUnit.valueOf(config.getTimeUnit());
        aggregationTimeUnit = TimeUnit.valueOf(config
            .getAggregationGranularity());

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(AggregationKey aggregationKey,
        Iterable<AggregationTimeSeries> timeSeriesIterable, Context context)
        throws IOException, InterruptedException {
      AggregationTimeSeries out = new AggregationTimeSeries(metricTypes);
      for (AggregationTimeSeries series : timeSeriesIterable) {
        out.aggregate(series);
      }
      context.write(aggregationKey, out);
    }
  }

  public void run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(AggregatePhaseJob.class);

    // Avro schema
    Schema schema = new Schema.Parser().parse(FileSystem.get(getConf()).open(
        new Path(getAndCheck(AggregationJobConstants.AGG_INPUT_AVRO_SCHEMA
            .toString()))));
    LOG.info("{}", schema);

    // Map config
    job.setMapperClass(AggregationMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AggregationTimeSeries.class);
    // AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    // AvroJob.setMapOutputValueSchema(job, schema);

    // Reduce config
    job.setReducerClass(AggregationReducer.class);
    job.setOutputKeyClass(AggregationKey.class);
    job.setOutputValueClass(AggregationTimeSeries.class);

    // aggregation phase config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration, AGG_INPUT_PATH);
    getAndSetConfiguration(configuration, AGG_CONFIG_PATH);
    getAndSetConfiguration(configuration, AGG_OUTPUT_PATH);
    getAndSetConfiguration(configuration, AGG_INPUT_AVRO_SCHEMA);
    LOG.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOG.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat.setOutputPath(job,
        new Path(getAndCheck(AGG_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);
  }

  private String getAndSetConfiguration(Configuration configuration,
      AggregationJobConstants constant) {
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

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: config.properties");
    }

    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));

    AggregatePhaseJob job = new AggregatePhaseJob("aggregate_avro_job", props);
    job.run();
  }

}
