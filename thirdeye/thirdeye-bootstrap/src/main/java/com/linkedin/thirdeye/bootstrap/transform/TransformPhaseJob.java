package com.linkedin.thirdeye.bootstrap.transform;

import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_INPUT_AVRO_SCHEMA;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_OUTPUT_AVRO_SCHEMA;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.transform.TransformPhaseJobConstants.TRANSFORM_UDF;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
      Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

    TransformUDF transformUDF;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("GenericAvroTransformJob.GenericTransformMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fs = FileSystem.get(configuration);
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
        context.write(new AvroKey<GenericRecord>(outputRecord), NullWritable.get());
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

    }

  }



  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    Configuration conf = job.getConfiguration();
    job.setJobName(name);
    job.setJarByClass(TransformPhaseJob.class);
    FileSystem fs = FileSystem.get(conf);


    String outputSchemaPath =
        getAndSetConfiguration(conf, TRANSFORM_OUTPUT_AVRO_SCHEMA);
    Schema.Parser parser = new Schema.Parser();
    Schema outputSchema = parser.parse(fs.open(new Path(outputSchemaPath)));
    LOGGER.info("{}", outputSchema);

    String inputSchemaPath =
        getAndSetConfiguration(conf, TRANSFORM_INPUT_AVRO_SCHEMA);
    parser = new Schema.Parser();
    Schema inputSchema = parser.parse(fs.open(new Path(inputSchemaPath)));
    LOGGER.info("{}", inputSchema);
    job.setNumReduceTasks(0);

    // Map config
    job.setMapperClass(GenericTransformMapper.class);
    AvroJob.setInputKeySchema(job, inputSchema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setOutputKeySchema(job, outputSchema);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(NullWritable.class);

    // transform phase config
    Configuration configuration = job.getConfiguration();
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
