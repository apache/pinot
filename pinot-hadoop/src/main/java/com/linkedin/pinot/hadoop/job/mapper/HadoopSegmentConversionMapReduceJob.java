/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.hadoop.job.mapper;

import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.hadoop.io.PinotSegmentToCsvConverter;

import java.io.File;
import java.io.IOException;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HadoopSegmentConversionMapReduceJob {

  public static class HadoopSegmentConversionMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentConversionMapper.class);
    private Configuration _properties;

    private String _inputFilePath;
    private String _outputPath;
    private String _tableName;

    private Path _currentHdfsWorkDir;
    private String _currentDiskWorkDir;

    private String _localDiskSegmentDirectory;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      _currentHdfsWorkDir = FileOutputFormat.getWorkOutputPath(context);
      _currentDiskWorkDir = "pinot_hadoop_tmp";

      LOGGER.info("*********************************************************************");
      LOGGER.info("Configurations : {}", context.getConfiguration().toString());
      LOGGER.info("*********************************************************************");
      LOGGER.info("Current HDFS working dir : {}", _currentHdfsWorkDir);
      LOGGER.info("Current DISK working dir : {}", new File(_currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");
      _properties = context.getConfiguration();

      _outputPath = _properties.get("path.to.output");
      _tableName = _properties.get("segment.table.name");

      if (_outputPath == null || _tableName == null) {
        throw new RuntimeException(
            "Missing configs: " +
                "\n\toutputPath: " +
                _properties.get("path.to.output") +
                "\n\ttableName: " +
                _properties.get("segment.table.name"));
      }
    }

    protected String getTableName() {
      return _tableName;
    }

    @Override
    public void cleanup(Context context) {
      FileUtils.deleteQuietly(new File(_currentDiskWorkDir));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException {

      String line = value.toString();
      String[] lineSplits = line.split(" ");

      LOGGER.info("*********************************************************************");
      LOGGER.info("mapper input : {}", value);
      LOGGER.info("PATH_TO_OUTPUT : {}", _outputPath);
      LOGGER.info("TABLE_NAME : {}", _tableName);
      LOGGER.info("num lines : {}", lineSplits.length);

      for (String split : lineSplits) {
        LOGGER.info("Command line : {}", split);
      }
      LOGGER.info("*********************************************************************");

      if (lineSplits.length != 3) {
        throw new RuntimeException("Input to the mapper is malformed, please contact the pinot team");
      }
      _inputFilePath = lineSplits[1].trim();

      _localDiskSegmentDirectory = _currentDiskWorkDir + "/segments/";

      final FileSystem fs = FileSystem.get(new Configuration());
      final File dataPath = new File(_currentDiskWorkDir, "data");
      try {
        if (dataPath.exists()) {
          dataPath.delete();
        }
        dataPath.mkdir();
      } catch (Exception e) {
        LOGGER.error("Could not create data dir: " + e);
        throw new RuntimeException(e);
      }

      LOGGER.info("*********************************************************************");
      LOGGER.info("input data file path : {}", _inputFilePath);
      LOGGER.info("local disk segment path: {}", _localDiskSegmentDirectory);
      LOGGER.info("*********************************************************************");

      LOGGER.info("Copying pinot segment to local dir...");
      final Path hdfsSegmentPath = new Path(_inputFilePath);
      final Path localPath = new Path(dataPath + "/");
      fs.copyToLocalFile(hdfsSegmentPath, localPath);

      LOGGER.info("Unpacking tar file...");
      File segment;
      try {
        segment = TarGzCompressionUtils.unTar(new File(localPath.toString() + "/" + lineSplits[2]), new File(_localDiskSegmentDirectory)).get(0);
      } catch (ArchiveException e) {
        e.printStackTrace();
        throw new RuntimeException("Error when untarring!!!", e);
      }
      LOGGER.info("+++++++++++++++++++++++++++++++++++++");
      LOGGER.info("Untared Segment path: {}", segment.getPath());
      LOGGER.info("Untared Segment parent: {}", segment.getParent());
      LOGGER.info("Converted CSV Data path: {}", dataPath);
      LOGGER.info("Segment name: {}", lineSplits[2]);
      LOGGER.info("_outputPath: {}", _outputPath);
      LOGGER.info("+++++++++++++++++++++++++++++++++++++");

      LOGGER.info("Converting to CSV file...");
      String outputPath = new File(dataPath.toString(), lineSplits[2]).getAbsolutePath();
      PinotSegmentToCsvConverter csvConverter = new PinotSegmentToCsvConverter(segment.getPath(), outputPath + ".csv", '|', '|', false);
      try {
        csvConverter.convert();
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error when converting!!!", e);
      }

      LOGGER.info("Uploading CSV file {} to HDFS...", lineSplits[2]);
      fs.copyFromLocalFile(true, true, new Path(dataPath.toString() + "/" + lineSplits[2] + ".csv"), new Path(_outputPath + "/"));

      LOGGER.info("finished the job successfully");
    }
  }
}
