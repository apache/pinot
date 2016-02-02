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

import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.segment.create.SegmentCreationPhaseConstants.SEGMENT_CREATION_SEGMENT_TABLE_NAME;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;


public class SegmentCreationPhaseMapReduceJob {

  public static class SegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static Logger LOGGER = LoggerFactory.getLogger(SegmentCreationPhaseMapReduceJob.class);
    private Configuration properties;

    private String inputFilePath;
    private String outputPath;
    private String tableName;
    private String postfix;

    private Path currentHdfsWorkDir;
    private String currentDiskWorkDir;

    // Temporary HDFS path for local machine
    private String localHdfsSegmentTarPath;

    private String localDiskSegmentDirectory;
    private String localDiskSegmentTarPath;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      currentHdfsWorkDir = FileOutputFormat.getWorkOutputPath(context);
      currentDiskWorkDir = "pinot_hadoop_tmp";

      // Temporary HDFS path for local machine
      localHdfsSegmentTarPath = currentHdfsWorkDir + "/segmentTar";

      // Temporary DISK path for local machine
      localDiskSegmentDirectory = currentDiskWorkDir + "/segments/";
      localDiskSegmentTarPath = currentDiskWorkDir + "/segmentsTar/";
      new File(localDiskSegmentTarPath).mkdirs();

      LOGGER.info("*********************************************************************");
      LOGGER.info("Configurations : {}", context.getConfiguration().toString());
      LOGGER.info("*********************************************************************");
      LOGGER.info("Current HDFS working dir : {}", currentHdfsWorkDir);
      LOGGER.info("Current DISK working dir : {}", new File(currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");
      properties = context.getConfiguration();

      outputPath = properties.get(SEGMENT_CREATION_OUTPUT_PATH.toString());
      tableName = properties.get(SEGMENT_CREATION_SEGMENT_TABLE_NAME.toString());
      postfix = properties.get("segment.name.postfix", null);
      if (outputPath == null || tableName == null) {
        throw new RuntimeException(
            "Missing configs: " +
                "\n\toutputPath: " +
                properties.get("path.to.output") +
                "\n\ttableName: " +
                properties.get("segment.table.name"));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      FileUtils.deleteQuietly(new File(currentDiskWorkDir));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();
      String[] lineSplits = line.split(" ");

      LOGGER.info("*********************************************************************");
      LOGGER.info("mapper input : {}", value);
      LOGGER.info("PATH_TO_OUTPUT : {}", outputPath);
      LOGGER.info("TABLE_NAME : {}", tableName);
      LOGGER.info("num lines : {}", lineSplits.length);

      for (String split : lineSplits) {
        LOGGER.info("Command line : {}", split);
      }
      LOGGER.info("*********************************************************************");

      if (lineSplits.length != 3) {
        throw new RuntimeException("Input to the mapper is malformed, please contact the pinot team");
      }
      inputFilePath = lineSplits[1].trim();
      Schema schema = new ObjectMapper().readValue(context.getConfiguration().get("data.schema"), Schema.class);

      LOGGER.info("*********************************************************************");
      LOGGER.info("input data file path : {}", inputFilePath);
      LOGGER.info("local hdfs segment tar path: {}", localHdfsSegmentTarPath);
      LOGGER.info("local disk segment path: {}", localDiskSegmentDirectory);
      LOGGER.info("local disk segment tar path: {}", localDiskSegmentTarPath);
      LOGGER.info("data schema: {}", localDiskSegmentTarPath);
      LOGGER.info("*********************************************************************");

      try {
        createSegment(inputFilePath, schema, lineSplits[2]);
        LOGGER.info("finished segment creation job successfully");
      } catch (Exception e) {
        LOGGER.error("Got exceptions during creating segments!", e);
      }

      context.write(new LongWritable(Long.parseLong(lineSplits[2])),
          new Text(FileSystem.get(new Configuration()).listStatus(new Path(localHdfsSegmentTarPath + "/"))[0].getPath().getName()));
      LOGGER.info("finished the job successfully");
    }

    private String createSegment(String dataFilePath, Schema schema, String seqId) throws Exception {
      final FileSystem fs = FileSystem.get(new Configuration());
      final Path hdfsDataPath = new Path(dataFilePath);
      final File dataPath = new File(currentDiskWorkDir, "data");
      if (dataPath.exists()) {
        dataPath.delete();
      }
      dataPath.mkdir();
      final Path localAvroPath = new Path(dataPath + "/" + hdfsDataPath.getName());
      fs.copyToLocalFile(hdfsDataPath, localAvroPath);

      LOGGER.info("Data schema is : {}", schema);
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
      segmentGeneratorConfig.setTableName(tableName);

      segmentGeneratorConfig.setInputFilePath(new File(dataPath, hdfsDataPath.getName()).getAbsolutePath());

      if (null != postfix) {
        segmentGeneratorConfig.setSegmentNamePostfix(String.format("%s-%s", postfix, seqId));
      } else {
        segmentGeneratorConfig.setSegmentNamePostfix(seqId);
      }

      segmentGeneratorConfig.setIndexOutputDir(localDiskSegmentDirectory);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      ThirdeyeRecordReader thirdeyeRecordReader =
          new ThirdeyeRecordReader(segmentGeneratorConfig.getInputFilePath(), schema, properties.get(SEGMENT_CREATION_CONFIG_PATH.toString()));
      driver.init(segmentGeneratorConfig, thirdeyeRecordReader);
      driver.build();
      // Tar the segment directory into file.
      String segmentName = (new File(localDiskSegmentDirectory).listFiles()[0]).getName();
      String localSegmentPath = new File(localDiskSegmentDirectory, segmentName).getAbsolutePath();

      String localTarPath = localDiskSegmentTarPath + "/" + segmentName + ".tar.gz";
      LOGGER.info("Trying to tar the segment to: {}", localTarPath);
      TarGzCompressionUtils.createTarGzOfDirectory(localSegmentPath, localTarPath);
      String hdfsTarPath = localHdfsSegmentTarPath + "/" + segmentName + ".tar.gz";

      LOGGER.info("*********************************************************************");
      LOGGER.info("Copy from : {} to {}", localTarPath, hdfsTarPath);
      LOGGER.info("*********************************************************************");
      fs.copyFromLocalFile(true, true, new Path(localTarPath), new Path(hdfsTarPath));
      return segmentName;
    }


  }
}
