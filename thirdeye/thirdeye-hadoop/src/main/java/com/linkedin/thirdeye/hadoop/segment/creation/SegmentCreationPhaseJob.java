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
package com.linkedin.thirdeye.hadoop.segment.creation;

import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.SEGMENT_CREATION_INPUT_PATH;
import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.SEGMENT_CREATION_OUTPUT_PATH;
import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.SEGMENT_CREATION_SCHEDULE;
import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.SEGMENT_CREATION_THIRDEYE_CONFIG;
import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.SEGMENT_CREATION_WALLCLOCK_END_TIME;
import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.SEGMENT_CREATION_WALLCLOCK_START_TIME;
import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.SEGMENT_CREATION_BACKFILL;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
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
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfigProperties;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAvroUtils;

/**
 * This class contains the job that generates pinot segments with star tree index
 */
public class SegmentCreationPhaseJob extends Configured {

  private static final String TEMP = "temp";
  private static final String DEFAULT_BACKFILL = "false";

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

    String inputSegmentDir = getAndSetConfiguration(configuration, SEGMENT_CREATION_INPUT_PATH);
    LOGGER.info("Input path : {}", inputSegmentDir);
    Schema avroSchema = ThirdeyeAvroUtils.getSchema(inputSegmentDir);
    LOGGER.info("Schema : {}", avroSchema);
    String dimensionTypesProperty = ThirdeyeAvroUtils.getDimensionTypesProperty(
        props.getProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_NAMES.toString()), avroSchema);
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_DIMENSION_TYPES.toString(), dimensionTypesProperty);
    String metricTypesProperty = ThirdeyeAvroUtils.getMetricTypesProperty(
        props.getProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_NAMES.toString()),
        props.getProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString()), avroSchema);
    props.setProperty(ThirdEyeConfigProperties.THIRDEYE_METRIC_TYPES.toString(), metricTypesProperty);
    ThirdEyeConfig thirdeyeConfig = ThirdEyeConfig.fromProperties(props);
    LOGGER.info("ThirdEyeConfig {}", thirdeyeConfig.encode());
    String outputDir = getAndSetConfiguration(configuration, SEGMENT_CREATION_OUTPUT_PATH);
    LOGGER.info("Output path : {}", outputDir);
    Path stagingDir = new Path(outputDir, TEMP);
    LOGGER.info("Staging dir : {}", stagingDir);
    String segmentWallClockStart = getAndSetConfiguration(configuration, SEGMENT_CREATION_WALLCLOCK_START_TIME);
    LOGGER.info("Segment wallclock start time : {}", segmentWallClockStart);
    String segmentWallClockEnd = getAndSetConfiguration(configuration, SEGMENT_CREATION_WALLCLOCK_END_TIME);
    LOGGER.info("Segment wallclock end time : {}", segmentWallClockEnd);
    String schedule = getAndSetConfiguration(configuration, SEGMENT_CREATION_SCHEDULE);
    LOGGER.info("Segment schedule : {}", schedule);
    String isBackfill = props.getProperty(SEGMENT_CREATION_BACKFILL.toString(), DEFAULT_BACKFILL);
    configuration.set(SEGMENT_CREATION_BACKFILL.toString(), isBackfill);
    LOGGER.info("Is Backfill : {}", configuration.get(SEGMENT_CREATION_BACKFILL.toString()));

    // Create temporary directory
    if (fs.exists(stagingDir)) {
      LOGGER.warn("Found the temp folder, deleting it");
      fs.delete(stagingDir, true);
    }
    fs.mkdirs(stagingDir);
    fs.mkdirs(new Path(stagingDir + "/input/"));

    // Create output directory
    if (fs.exists(new Path(outputDir))) {
      LOGGER.warn("Found the output folder deleting it");
      fs.delete(new Path(outputDir), true);
    }
    fs.mkdirs(new Path(outputDir));

    // Read input files
    List<FileStatus> inputDataFiles = new ArrayList<>();
    for (String input : inputSegmentDir.split(",")) {
      Path inputPathPattern = new Path(input);
      inputDataFiles.addAll(Arrays.asList(fs.listStatus(inputPathPattern)));
    }
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
      LOGGER.error("Exception while reading input files ", e);
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
    job.getConfiguration().set(SEGMENT_CREATION_THIRDEYE_CONFIG.toString(), OBJECT_MAPPER.writeValueAsString(thirdeyeConfig));

    job.setMaxReduceAttempts(1);
    job.setMaxMapAttempts(0);
    job.setNumReduceTasks(0);
    for (Object key : props.keySet()) {
      job.getConfiguration().set(key.toString(), props.getProperty(key.toString()));
    }

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
    fs.delete(stagingDir, true);

    return job;
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
