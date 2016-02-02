/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.bootstrap.segment.create;

import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_SCHEMA_PATH;
import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_SEGMENT_TABLE_NAME;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;


public class SegmentCreationPhaseJob extends Configured {

  private static final String TEMP = "temp";

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreationPhaseJob.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String name;
  private final Properties props;


  public SegmentCreationPhaseJob(String jobName, Properties properties) throws Exception {
    super(new Configuration());
    getConf().set("mapreduce.job.user.classpath.first", "true");
    name = jobName;
    props = properties;

  }

  public Job run() throws Exception {

    Job job = Job.getInstance(getConf());

    job.setJarByClass(SegmentCreationPhaseJob.class);
    job.setJobName(name);

    FileSystem fs = FileSystem.get(getConf());

    Configuration configuration = job.getConfiguration();

    String schemaPath = getAndSetConfiguration(configuration, SEGMENT_CREATION_SCHEMA_PATH);
    LOGGER.info("Schema path : {}", schemaPath);
    String configPath = getAndSetConfiguration(configuration, SEGMENT_CREATION_CONFIG_PATH);
    LOGGER.info("Config path : {}", configPath);
    Schema dataSchema = createSchema(configPath);
    LOGGER.info("Data schema : {}", dataSchema);
    String inputSegmentDir = getAndSetConfiguration(configuration, SEGMENT_CREATION_INPUT_PATH);
    LOGGER.info("Input path : {}", inputSegmentDir);
    String outputDir = getAndSetConfiguration(configuration, SEGMENT_CREATION_OUTPUT_PATH);
    LOGGER.info("Output path : {}", outputDir);
    String stagingDir = new File(outputDir, TEMP).getAbsolutePath();
    LOGGER.info("Staging dir : {}", stagingDir);
    String tableName = getAndSetConfiguration(configuration, SEGMENT_CREATION_SEGMENT_TABLE_NAME);
    LOGGER.info("Segment table name : {}", tableName);

    if (fs.exists(new Path(stagingDir))) {
      LOGGER.warn("Found the temp folder, deleting it");
      fs.delete(new Path(stagingDir), true);
    }
    fs.mkdirs(new Path(stagingDir));
    fs.mkdirs(new Path(stagingDir + "/input/"));

    if (fs.exists(new Path(outputDir))) {
      LOGGER.warn("Found the output folder deleting it");
      fs.delete(new Path(outputDir), true);
    }
    fs.mkdirs(new Path(outputDir));

    Path inputPathPattern = new Path(inputSegmentDir);
    List<FileStatus> inputDataFiles = Arrays.asList(fs.listStatus(inputPathPattern));
    LOGGER.info("size {}", inputDataFiles.size());

    try {
    for (int seqId = 0; seqId < inputDataFiles.size(); ++seqId) {
      FileStatus file = inputDataFiles.get(seqId);
      String completeFilePath = " " + file.getPath().toString() + " " + seqId;
      Path newOutPutFile = new Path((stagingDir + "/input/" + file.getPath().toString().replace('.', '_').replace('/', '_').replace(':', '_') + ".txt"));
      FSDataOutputStream stream = fs.create(newOutPutFile);
      LOGGER.info("wrote {}", completeFilePath);
      stream.writeUTF(completeFilePath);
      stream.flush();
      stream.close();
    }
    } catch (Exception e) {
      LOGGER.error("Exception ", e);
    }


    job.setMapperClass(SegmentCreationPhaseMapReduceJob.SegmentCreationMapper.class);

    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      job.getConfiguration().set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(stagingDir + "/input/"));
    FileOutputFormat.setOutputPath(job, new Path(stagingDir + "/output/"));

    job.getConfiguration().setInt(JobContext.NUM_MAPS, inputDataFiles.size());
    job.getConfiguration().set("data.schema", OBJECT_MAPPER.writeValueAsString(dataSchema));
    if (!fs.exists(new Path(schemaPath))) {
      OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(fs.create(new Path(schemaPath), false), dataSchema);
    }


    job.setMaxReduceAttempts(1);
    job.setMaxMapAttempts(0);
    job.setNumReduceTasks(0);
    for (Object key : props.keySet()) {
      job.getConfiguration().set(key.toString(), props.getProperty(key.toString()));
    }

    // Submit the job for execution.
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed : " + job);
    }

    LOGGER.info("Moving Segment Tar files from {} to: {}", stagingDir + "/output/segmentTar", outputDir);
    FileStatus[] segmentArr = fs.listStatus(new Path(stagingDir + "/output/segmentTar"));
    for (FileStatus segment : segmentArr) {
      fs.rename(segment.getPath(), new Path(outputDir, segment.getPath().getName()));
    }

    // Delete temporary directory.
    LOGGER.info("Cleanup the working directory.");
    LOGGER.info("Deleting the dir: {}", stagingDir);
    fs.delete(new Path(stagingDir), true);

    return job;
  }

  private Schema createSchema(String configPath) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());

    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fs.open(new Path(configPath)));
    LOGGER.info("{}", starTreeConfig);

    Schema schema = new Schema();
    for (DimensionSpec dimensionSpec : starTreeConfig.getDimensions()) {
      FieldSpec spec = new DimensionFieldSpec();
      spec.setName(dimensionSpec.getName());
      spec.setDataType(DataType.STRING);
      spec.setSingleValueField(true);
      schema.addSchema(dimensionSpec.getName(), spec);
    }
    for (MetricSpec metricSpec : starTreeConfig.getMetrics()) {
      FieldSpec spec = new MetricFieldSpec();
      spec.setName(metricSpec.getName());
      spec.setDataType(DataType.valueOf(metricSpec.getType().toString()));
      spec.setSingleValueField(true);
      schema.addSchema(metricSpec.getName(), spec);
    }
    TimeGranularitySpec incoming =
        new TimeGranularitySpec(DataType.LONG, starTreeConfig.getTime().getBucket().getUnit(), starTreeConfig.getTime().getColumnName());
    TimeGranularitySpec outgoing =
        new TimeGranularitySpec(DataType.LONG, starTreeConfig.getTime().getBucket().getUnit(), starTreeConfig.getTime().getColumnName());
    schema.addSchema(starTreeConfig.getTime().getColumnName(),
        new TimeFieldSpec(incoming, outgoing));

    schema.setSchemaName(starTreeConfig.getCollection());

    return schema;
  }


  private String getAndSetConfiguration(Configuration configuration,
      SegmentCreationPhaseConstants constant) {
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

    SegmentCreationPhaseJob job = new SegmentCreationPhaseJob("segment_creation_job", props);
    job.run();
  }

}
