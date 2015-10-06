package com.linkedin.thirdeye.bootstrap.transform;

import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_INPUT_AVRO_SCHEMA;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_OUTPUT_AVRO_SCHEMA;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_UDF;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_CONFIG_UDF;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_NUM_REDUCERS;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Transform job to transform input files from one schema to another
 *
 * Required properties:
 * transform.input.schema=<path to input schema on hdfs>
 * transform.output.schema=<path to output schema on hdfs>
 * transform.input.path=<path to input data files on hdfs>
 * transform.output.path=<output data path on hdfs>
 * transform.udf.class=<UDF class to perform transformation>
 *
 */
public class TransformPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformPhaseJob.class);

  private String name;
  private Properties props;

  public TransformPhaseJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class GenericTransformMapper extends
      Mapper<AvroKey<GenericRecord>, NullWritable, IntWritable, AvroValue<GenericRecord>> {

    TransformUDF transformUDF;
    int numReducers;
    int reducerKey;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("GenericAvroTransformJob.GenericTransformMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fs = FileSystem.get(configuration);

      String numTransformReducers = configuration.get(TRANSFORM_NUM_REDUCERS.toString());
      numReducers = Integer.parseInt(numTransformReducers);
      reducerKey = 1;
      try {

        String transformUDFClass = configuration.get(TRANSFORM_UDF.toString());
        LOGGER.info("Initializing TransformUDFClass:{} with params:{}", transformUDFClass);
        Constructor<?> constructor = Class.forName(transformUDFClass).getConstructor();
        transformUDF = (TransformUDF) constructor.newInstance();

        String outputSchemaPath = configuration.get(TRANSFORM_OUTPUT_AVRO_SCHEMA.toString());
        Schema.Parser parser = new Schema.Parser();
        Schema outputSchema = parser.parse(fs.open(new Path(outputSchemaPath)));

        transformUDF.init(outputSchema);
      } catch (Exception e) {
        throw new IOException(e);
      }

    }

    @Override
    public void map(AvroKey<GenericRecord> recordWrapper, NullWritable value, Context context)
        throws IOException, InterruptedException {
      GenericRecord record = recordWrapper.datum();
      GenericRecord outputRecord = transformUDF.transformRecord(record);

      if (outputRecord != null) {

        IntWritable key = new IntWritable(reducerKey);
        reducerKey = (reducerKey == numReducers) ? (1) : (reducerKey + 1);
        context.write(key, new AvroValue<GenericRecord>(outputRecord));
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

    }

  }

  public static class GenericTransformReducer extends Reducer<IntWritable, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>
  {
    @Override
    public void reduce(IntWritable key, Iterable<AvroValue<GenericRecord>> values, Context context)
            throws IOException, InterruptedException
    {
      for (AvroValue<GenericRecord> value : values)
      {
        GenericRecord record = value.datum();
        context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
      }
    }
  }



  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    Configuration configuration = job.getConfiguration();
    job.setJobName(name);
    job.setJarByClass(TransformPhaseJob.class);

    // Set custom config like adding distributed caches
    String transformConfigUDFClass = getAndSetConfiguration(configuration, TRANSFORM_CONFIG_UDF);
    LOGGER.info("Initializing TransformUDFClass:{} with params:{}", transformConfigUDFClass);
    Constructor<?> constructor = Class.forName(transformConfigUDFClass).getConstructor();
    TransformConfigUDF transformConfigUDF = (TransformConfigUDF) constructor.newInstance();
    transformConfigUDF.setTransformConfig(job);

    FileSystem fs = FileSystem.get(configuration);

    String outputSchemaPath =
        getAndSetConfiguration(configuration, TRANSFORM_OUTPUT_AVRO_SCHEMA);
    Schema.Parser parser = new Schema.Parser();
    Schema outputSchema = parser.parse(fs.open(new Path(outputSchemaPath)));
    LOGGER.info("{}", outputSchema);

    String inputSchemaPath =
        getAndSetConfiguration(configuration, TRANSFORM_INPUT_AVRO_SCHEMA);
    parser = new Schema.Parser();
    Schema inputSchema = parser.parse(fs.open(new Path(inputSchemaPath)));
    LOGGER.info("{}", inputSchema);


    // Map config
    job.setMapperClass(GenericTransformMapper.class);
    AvroJob.setInputKeySchema(job, inputSchema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setMapOutputValueSchema(job, outputSchema);

    // Reducer config
    job.setReducerClass(GenericTransformReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    AvroJob.setOutputKeySchema(job, outputSchema);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    // transform phase config
    String inputPathDir = getAndSetConfiguration(configuration, TRANSFORM_INPUT_PATH);
    String outputPathDir = getAndSetConfiguration(configuration, TRANSFORM_OUTPUT_PATH);
    getAndSetConfiguration(configuration, TRANSFORM_UDF);

    LOGGER.info("Input path dir: " + inputPathDir);

    FileInputFormat.setInputDirRecursive(job, true);

    for (String inputPath : inputPathDir.split(",")) {
      Path input = new Path(inputPath);
      FileStatus[] listFiles = fs.listStatus(input);
      boolean isNested = false;
      for (FileStatus fileStatus : listFiles) {
        if (fileStatus.isDirectory()) {
          isNested = true;
          LOGGER.info("Adding input:" + fileStatus.getPath());
          FileInputFormat.addInputPath(job, fileStatus.getPath());
        }
      }
      if (!isNested) {
        LOGGER.info("Adding input:" + inputPath);
        FileInputFormat.addInputPath(job, input);
      }
    }
    FileOutputFormat.setOutputPath(job, new Path(outputPathDir));

    String numReducers = getAndSetConfiguration(configuration, TRANSFORM_NUM_REDUCERS);
    if (numReducers != null) {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());

    job.waitForCompletion(true);

    return job;
  }


  private String getAndSetConfiguration(Configuration configuration, TransformPhaseJobConstants constant) {
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

    TransformPhaseJob job = new TransformPhaseJob("transform_phase_job", props);
    job.run();
  }

}
