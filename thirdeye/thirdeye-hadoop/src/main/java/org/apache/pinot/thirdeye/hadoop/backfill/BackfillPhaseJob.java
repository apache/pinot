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
package com.linkedin.thirdeye.hadoop.backfill;

import static com.linkedin.thirdeye.hadoop.backfill.BackfillPhaseConstants.*;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This phase is for backfilling segments which are already present on pinot, with star tree and topk information
 * The pinot segments are downloaded from the table, and converted to avro files
 * These avro files are then passed on to the rest of the thirdeye-hadoop segment generation pipeline
 */
public class BackfillPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackfillPhaseJob.class);
  private static final String DOWNLOAD = "download";
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";

  private String name;
  private Properties props;

  /**
   * @param name
   * @param props
   */
  public BackfillPhaseJob(String name, Properties props) {
    super(new Configuration());
    getConf().set("mapreduce.job.user.classpath.first", "true");
    this.name = name;
    this.props = props;
  }

  public Job run() throws Exception {

    Job job = Job.getInstance(getConf());
    job.setJarByClass(BackfillPhaseJob.class);
    job.setJobName(name);

    FileSystem fs = FileSystem.get(getConf());
    Configuration configuration = job.getConfiguration();

    LOGGER.info("*******************************************************************************");
    String controllerHost = getAndSetConfiguration(configuration, BACKFILL_PHASE_CONTROLLER_HOST);
    String controllerPort = getAndSetConfiguration(configuration, BACKFILL_PHASE_CONTROLLER_PORT);
    LOGGER.info("Controller Host : {} Controller Port : {}", controllerHost, controllerPort);
    String segmentStartTime = getAndSetConfiguration(configuration, BACKFILL_PHASE_START_TIME);
    String segmentEndTime = getAndSetConfiguration(configuration, BACKFILL_PHASE_END_TIME);
    long startTime = Long.valueOf(segmentStartTime);
    long endTime = Long.valueOf(segmentEndTime);
    if (Long.valueOf(segmentStartTime) > Long.valueOf(segmentEndTime)) {
      throw new IllegalStateException("Start time cannot be greater than end time");
    }
    String tableName = getAndSetConfiguration(configuration, BACKFILL_PHASE_TABLE_NAME);
    LOGGER.info("Start time : {} End time : {} Table name : {}", segmentStartTime, segmentEndTime, tableName);

    String outputPath = getAndSetConfiguration(configuration, BACKFILL_PHASE_OUTPUT_PATH);
    LOGGER.info("Output path : {}", outputPath);
    Path backfillDir = new Path(outputPath);
    if (fs.exists(backfillDir)) {
      LOGGER.warn("Found the output folder deleting it");
      fs.delete(backfillDir, true);
    }
    Path downloadDir = new Path(backfillDir, DOWNLOAD);
    LOGGER.info("Creating download dir : {}", downloadDir);
    fs.mkdirs(downloadDir);
    Path inputDir = new Path(backfillDir, INPUT);
    LOGGER.info("Creating input dir : {}", inputDir);
    fs.mkdirs(inputDir);
    Path outputDir = new Path(backfillDir, OUTPUT);
    LOGGER.info("Creating output dir : {}", outputDir);

    BackfillControllerAPIs backfillControllerAPIs = new BackfillControllerAPIs(controllerHost,
        Integer.valueOf(controllerPort), tableName);

    LOGGER.info("Downloading segments in range {} to {}", startTime, endTime);
    List<String> allSegments = backfillControllerAPIs.getAllSegments(tableName);
    List<String> segmentsToDownload = backfillControllerAPIs.findSegmentsInRange(tableName, allSegments, startTime, endTime);
    for (String segmentName : segmentsToDownload) {
      backfillControllerAPIs.downloadSegment(segmentName, downloadDir);
    }

    LOGGER.info("Reading downloaded segment input files");
    List<FileStatus> inputDataFiles = new ArrayList<>();
    inputDataFiles.addAll(Lists.newArrayList(fs.listStatus(downloadDir)));
    LOGGER.info("size {}", inputDataFiles.size());

    try {
      LOGGER.info("Creating input files at {} for segment input files", inputDir);
      for (int seqId = 0; seqId < inputDataFiles.size(); ++seqId) {
        FileStatus file = inputDataFiles.get(seqId);
        String completeFilePath = " " + file.getPath().toString() + " " + seqId;
        Path newOutPutFile = new Path((inputDir + "/" + file.getPath().toString().replace('.', '_').replace('/', '_').replace(':', '_') + ".txt"));
        FSDataOutputStream stream = fs.create(newOutPutFile);
        LOGGER.info("wrote {}", completeFilePath);
        stream.writeUTF(completeFilePath);
        stream.flush();
        stream.close();
      }
    } catch (Exception e) {
      LOGGER.error("Exception while reading input files ", e);
    }

    job.setMapperClass(BackfillPhaseMapJob.BackfillMapper.class);

    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      job.getConfiguration().set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);

    job.getConfiguration().setInt(JobContext.NUM_MAPS, inputDataFiles.size());
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

    LOGGER.info("Cleanup the working directory");
    LOGGER.info("Deleting the dir: {}", downloadDir);
    fs.delete(downloadDir, true);
    LOGGER.info("Deleting the dir: {}", inputDir);
    fs.delete(inputDir, true);
    LOGGER.info("Deleting the dir: {}", outputDir);
    fs.delete(outputDir, true);

    return job;
  }

  private String getAndCheck(String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

  private String getAndSetConfiguration(Configuration configuration, BackfillPhaseConstants constant) {
    String value = getAndCheck(constant.toString());
    configuration.set(constant.toString(), value);
    return value;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: config.properties");
    }
    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));
    BackfillPhaseJob job = new BackfillPhaseJob("backfill_job", props);
    job.run();
  }

}
