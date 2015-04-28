package com.linkedin.thirdeye.bootstrap.analysis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;

public class AnalysisPhaseJob extends Configured
{
  private static final Logger LOG = LoggerFactory.getLogger(AnalysisPhaseJob.class);

  private final String name;
  private final Properties props;

  public AnalysisPhaseJob(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class AnalyzeMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable>
  {
    private StarTreeConfig config;
    private BytesWritable taskKey;
    private List<String> dimensionNames;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException
    {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      Path configPath = new Path(context.getConfiguration().get(AnalysisJobConstants.ANALYSIS_CONFIG_PATH.toString()));
      this.config = StarTreeConfig.decode(fileSystem.open(configPath));
      this.taskKey = new BytesWritable(context.getTaskAttemptID().getTaskID().toString().getBytes());
      this.dimensionNames = new ArrayList<String>();
      for (DimensionSpec dimensionSpec : config.getDimensions())
      {
        this.dimensionNames.add(dimensionSpec.getName());
      }
    }

    @Override
    public void map(AvroKey<GenericRecord> record, NullWritable value, Context context)
            throws IOException, InterruptedException
    {
      Object timeObj = record.datum().get(config.getTime().getColumnName());
      if (timeObj == null)
      {
        throw new IllegalStateException("Record has null value for time column "
                                                + config.getTime().getColumnName() + ": " + record.datum());
      }

      // check if the time column has appropriate values.
      try{
        Long time = Long.parseLong(record.datum().get(config.getTime().getColumnName()).toString());
        if(time < 0)
          throw new IllegalStateException("Value for dimension " + config.getTime().getColumnName() + " in " + record.datum() + " cannot be parsed");
      }catch(NumberFormatException ex){

        throw new IllegalStateException("Value for dimension "+ config.getTime().getColumnName() + " in " + record.datum() + " cannot be parsed");
      }


      Map<String, Set<String>> dimensionValues = new HashMap<String, Set<String>>();
      for(String dimension : dimensionNames){
        Set<String> val = new HashSet<String>();
        String dimVal = record.datum().get(dimension) == null ? "" : record.datum().get(dimension).toString();
        val.add(dimVal);
        dimensionValues.put(dimension, val);
      }
      Long time = ((Number) timeObj).longValue();

      // TODO: More analysis of record

      AnalysisPhaseStats stats = new AnalysisPhaseStats();
      stats.setMaxTime(time);
      stats.setMinTime(time);
      stats.setDimensionValues(dimensionValues);
      context.write(taskKey, new BytesWritable(stats.toBytes()));
    }
  }

  public static class AnalyzeCombiner extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable>
  {
    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException
    {
      AnalysisPhaseStats aggregatedStats = new AnalysisPhaseStats();

      for (BytesWritable value : values)
      {
        AnalysisPhaseStats stats = AnalysisPhaseStats.fromBytes(value.copyBytes());
        aggregatedStats.update(stats);
      }

      context.write(key, new BytesWritable(aggregatedStats.toBytes()));
    }
  }

  public static class AnalyzeReducer extends Reducer<BytesWritable, BytesWritable, BytesWritable, NullWritable>
  {
    private final AnalysisPhaseStats aggregatedStats = new AnalysisPhaseStats();

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException
    {
      aggregatedStats.setInputPath(context.getConfiguration().get(AnalysisJobConstants.ANALYSIS_INPUT_PATH.toString()));
    }

    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException
    {
      for (BytesWritable value : values)
      {
        AnalysisPhaseStats stats = AnalysisPhaseStats.fromBytes(value.copyBytes());
        aggregatedStats.update(stats);
      }
      context.write(new BytesWritable(aggregatedStats.toBytes()), NullWritable.get());
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException
    {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      Path outputPath = new Path(context.getConfiguration().get(
              AnalysisJobConstants.ANALYSIS_OUTPUT_PATH.toString()), AnalysisJobConstants.ANALYSIS_FILE_NAME.toString());
      OutputStream os = fileSystem.create(outputPath, true);
      os.write(aggregatedStats.toBytes());
      os.flush();
      os.close();
    }
  }

  public Job run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(AnalysisPhaseJob.class);

    FileSystem fs = FileSystem.get(getConf());
    // Avro schema
    Schema schema = new Schema.Parser()
            .parse(fs.open(new Path(
                    getAndCheck(AnalysisJobConstants.ANALYSIS_INPUT_AVRO_SCHEMA.toString()))));
    LOG.info("{}", schema);

    // Map config
    job.setMapperClass(AnalyzeMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Combiner config
    job.setCombinerClass(AnalyzeCombiner.class);

    // Reduce config
    job.setReducerClass(AnalyzeReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration, AnalysisJobConstants.ANALYSIS_INPUT_PATH);
    getAndSetConfiguration(configuration, AnalysisJobConstants.ANALYSIS_CONFIG_PATH);
    getAndSetConfiguration(configuration, AnalysisJobConstants.ANALYSIS_INPUT_AVRO_SCHEMA);
    getAndSetConfiguration(configuration, AnalysisJobConstants.ANALYSIS_OUTPUT_PATH);
    LOG.info("Input path dir: " + inputPathDir);

    for (String inputPath : inputPathDir.split(",")) {
      Path input = new Path(inputPath);
      FileStatus[] listFiles = fs.listStatus(input);
      boolean isNested = false;
      for (FileStatus fileStatus : listFiles) {
        if (fileStatus.isDirectory()) {
          isNested = true;
          LOG.info("Adding input:" + fileStatus.getPath());
          FileInputFormat.addInputPath(job, fileStatus.getPath());
        }
      }
      if (!isNested) {
        LOG.info("Adding input:" + inputPath);
        FileInputFormat.addInputPath(job, input);
      }

    }

    // Remove previous output path and set output path
    Path outputPath = new Path(getAndCheck(AnalysisJobConstants.ANALYSIS_OUTPUT_PATH.toString()));
    fs.delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);


    return job;
  }

  private String getAndSetConfiguration(Configuration configuration, AnalysisJobConstants constant)
  {
    String value = getAndCheck(constant.toString());
    configuration.set(constant.toString(), value);
    return value;
  }

  private String getAndCheck(String propName)
  {
    String propValue = props.getProperty(propName);
    if (propValue == null)
    {
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

    AnalysisPhaseJob job = new AnalysisPhaseJob("analyze_avro_job", props);
    job.run();
  }
}
