/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.hadoop.transform;

import static com.linkedin.thirdeye.hadoop.transform.TransformPhaseJobConstants.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Transform job to transform input files from one schema to another
 * Required properties:
 * transform.input.schema=<path to input schema on hdfs>
 * transform.output.schema=<path to output schema on hdfs>
 * transform.input.path=<path to input data files on hdfs>
 * transform.output.path=<output data path on hdfs>
 * transform.udf.class=<UDF class to perform transformation>
 */
public class TransformPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformPhaseJob.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  public TransformPhaseJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class GenericTransformMapper
      extends Mapper<AvroKey<GenericRecord>, NullWritable, IntWritable, AvroValue<GenericRecord>> {

    TransformUDF transformUDF;
    int numReducers;
    int reducerKey;
    String sourceName;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("GenericAvroTransformJob.GenericTransformMapper.setup()");

      Configuration configuration = context.getConfiguration();
      FileSystem fs = FileSystem.get(configuration);

      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      LOGGER.info("split name:" + fileSplit.toString());
      sourceName = DelegatingAvroKeyInputFormat.getSourceNameFromPath(fileSplit, configuration);
      LOGGER.info("Input: {} belongs to Source:{}", fileSplit, sourceName);

      String numTransformReducers = configuration.get(TRANSFORM_NUM_REDUCERS.toString());
      numReducers = Integer.parseInt(numTransformReducers);
      reducerKey = 1;
      try {

        String transformUDFClass = configuration.get(TRANSFORM_UDF.toString());
        LOGGER.info("Initializing TransformUDFClass:{} with params:{}", transformUDFClass);
        Constructor<?> constructor = Class.forName(transformUDFClass).getConstructor();
        transformUDF = (TransformUDF) constructor.newInstance();

        String outputSchemaPath = configuration.get(TRANSFORM_OUTPUT_SCHEMA.toString());
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
      GenericRecord outputRecord = transformUDF.transformRecord(sourceName, record);

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

  public static class GenericTransformReducer
      extends Reducer<IntWritable, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<AvroValue<GenericRecord>> values, Context context)
        throws IOException, InterruptedException {
      for (AvroValue<GenericRecord> value : values) {
        GenericRecord record = value.datum();
        context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
      }
    }
  }

  public Job run() throws Exception {

    // Set job config
    Job job = Job.getInstance(getConf());
    Configuration configuration = job.getConfiguration();
    job.setJobName(name);
    job.setJarByClass(TransformPhaseJob.class);

    // Set custom config like adding distributed caches
    String transformConfigUDFClass = getAndSetConfiguration(configuration, TRANSFORM_CONFIG_UDF);
    LOGGER.info("Initializing TransformConfigUDFClass:{} with params:{}", transformConfigUDFClass);
    Constructor<?> constructor = Class.forName(transformConfigUDFClass).getConstructor();
    TransformConfigUDF transformConfigUDF = (TransformConfigUDF) constructor.newInstance();
    transformConfigUDF.setTransformConfig(job);

    FileSystem fs = FileSystem.get(configuration);

    // Set outputSchema, output path
    String outputSchemaPath = getAndSetConfiguration(configuration, TRANSFORM_OUTPUT_SCHEMA);
    Schema.Parser parser = new Schema.Parser();
    Schema outputSchema = parser.parse(fs.open(new Path(outputSchemaPath)));
    LOGGER.info("{}", outputSchema);

    String outputPathDir = getAndSetConfiguration(configuration, TRANSFORM_OUTPUT_PATH);
    Path outputPath = new Path(outputPathDir);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, new Path(outputPathDir));

    // Set input schema, input path for every source
    String sources = getAndSetConfiguration(configuration, TRANSFORM_SOURCE_NAMES);
    List<String> sourceNames = Arrays.asList(sources.split(","));
    Map<String, String> schemaMap = new HashMap<String, String>();
    Map<String, String> schemaPathMapping = new HashMap<String, String>();

    for (String sourceName : sourceNames) {

      // load schema for each source
      LOGGER.info("Loading Schema for {}", sourceName);
      FSDataInputStream schemaStream =
          fs.open(new Path(getAndCheck(sourceName + "." + TRANSFORM_INPUT_SCHEMA.toString())));
      Schema schema = new Schema.Parser().parse(schemaStream);
      schemaMap.put(sourceName, schema.toString());
      LOGGER.info("Schema for {}:  \n{}", sourceName, schema);

      // configure input data for each source
      String inputPathDir = getAndCheck(sourceName + "." + TRANSFORM_INPUT_PATH.toString());
      LOGGER.info("Input path dir for " + sourceName + ": " + inputPathDir);
      for (String inputPath : inputPathDir.split(",")) {
        Path input = new Path(inputPath);
        FileStatus[] listFiles = fs.listStatus(input);
        boolean isNested = false;
        for (FileStatus fileStatus : listFiles) {
          if (fileStatus.isDirectory()) {
            isNested = true;
            Path path = fileStatus.getPath();
            LOGGER.info("Adding input:" + path);
            FileInputFormat.addInputPath(job, path);
            schemaPathMapping.put(path.toString(), sourceName);
          }
        }
        if (!isNested) {
          LOGGER.info("Adding input:" + inputPath);
          FileInputFormat.addInputPath(job, input);
          schemaPathMapping.put(input.toString(), sourceName);
        }
      }
    }
    StringWriter temp = new StringWriter();
    OBJECT_MAPPER.writeValue(temp, schemaPathMapping);
    job.getConfiguration().set("schema.path.mapping", temp.toString());

    temp = new StringWriter();
    OBJECT_MAPPER.writeValue(temp, schemaMap);
    job.getConfiguration().set("schema.json.mapping", temp.toString());

    // set transform UDF class
    getAndSetConfiguration(configuration, TRANSFORM_UDF);

    // set reducers
    String numReducers = getAndSetConfiguration(configuration, TRANSFORM_NUM_REDUCERS);
    if (numReducers != null) {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());

    // Map config
    job.setMapperClass(GenericTransformMapper.class);
    // AvroJob.setInputKeySchema(job, inputSchema);
    job.setInputFormatClass(DelegatingAvroKeyInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setMapOutputValueSchema(job, outputSchema);

    // Reducer config
    job.setReducerClass(GenericTransformReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    AvroJob.setOutputKeySchema(job, outputSchema);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    job.waitForCompletion(true);

    return job;
  }

  private String getAndSetConfiguration(Configuration configuration,
      TransformPhaseJobConstants constant) {
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
