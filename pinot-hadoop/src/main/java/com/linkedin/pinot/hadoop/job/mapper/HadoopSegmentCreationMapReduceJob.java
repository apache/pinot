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
package com.linkedin.pinot.hadoop.job.mapper;

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.readers.CSVRecordReaderConfig;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderConfig;
import com.linkedin.pinot.core.data.readers.ThriftRecordReaderConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.hadoop.job.JobConfigConstants;
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
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;

public class HadoopSegmentCreationMapReduceJob {

  public static class HadoopSegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentCreationMapper.class);

    private static final String PINOT_HADOOP_TMP = "pinot_hadoop_tmp";
    private static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";
    private static final String SEGMENT_TAR = "/segmentTar";

    private Configuration _properties;

    private String _inputFilePath;
    private String _outputPath;
    private String _tableName;
    private String _postfix;
    private String _readerConfigFile;

    // Temporary local disk path for current working directory
    private String _currentDiskWorkDir;

    // Temporary hdfs path for segment tar file
    private String _localHdfsSegmentTarPath;

    // Temporary local disk path for segment tar file
    private String _localDiskSegmentTarPath;

    // Temporary local disk path for output segment directory
    private String _localDiskOutputSegmentDir;

    private TableConfig _tableConfig = null;

    private FileSystem _fileSystem = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      // Compute current working HDFS directory
      Path currentHdfsWorkDir = FileOutputFormat.getWorkOutputPath(context);
      _localHdfsSegmentTarPath = currentHdfsWorkDir + SEGMENT_TAR;

      // Compute current working LOCAL DISK directory
      _currentDiskWorkDir = PINOT_HADOOP_TMP;
      _localDiskSegmentTarPath = _currentDiskWorkDir + SEGMENT_TAR;

      _fileSystem = FileSystem.get(context.getConfiguration());

      // Create directory
      new File(_localDiskSegmentTarPath).mkdirs();

      LOGGER.info("*********************************************************************");
      LOGGER.info("Configurations : {}", context.getConfiguration().toString());
      LOGGER.info("*********************************************************************");
      LOGGER.info("Current HDFS working dir(setup) : {}", currentHdfsWorkDir);
      LOGGER.info("Current DISK working dir(setup) : {}", new File(_currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");

      _properties = context.getConfiguration();
      _outputPath = _properties.get(JobConfigConstants.PATH_TO_OUTPUT);
      _tableName = _properties.get(JobConfigConstants.SEGMENT_TABLE_NAME);
      _postfix = _properties.get(SEGMENT_NAME_POSTFIX, null);
      _readerConfigFile = _properties.get(JobConfigConstants.PATH_TO_READER_CONFIG);
      if (_outputPath == null || _tableName == null) {
        throw new RuntimeException(
            "Missing configs: " + "\n\toutputPath: " + _properties.get(JobConfigConstants.PATH_TO_OUTPUT)
                + "\n\ttableName: " + _properties.get(JobConfigConstants.SEGMENT_TABLE_NAME));
      }

        String tableConfigString = _properties.get(JobConfigConstants.TABLE_CONFIG);
        if (tableConfigString != null) {
          try {
            _tableConfig = TableConfig.init(tableConfigString);
          } catch (JSONException e) {
            // Though we get table config directly from the controller of hosts and port of push location are set,
            // it is possible for the user to pass in a table config as a parameter
            LOGGER.error("Exception when parsing table config: {}", tableConfigString);
          }
        }
    }

    protected String getTableName() {
      return _tableName;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      File currentDiskWorkDir = new File(_currentDiskWorkDir);
      LOGGER.info("Clean up directory: {}", currentDiskWorkDir.getAbsolutePath());
      FileUtils.deleteQuietly(currentDiskWorkDir);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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

      LOGGER.info("Current DISK working dir(mapper): {}", new File(_currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");

      if (lineSplits.length != 3) {
        throw new RuntimeException("Input to the mapper is malformed, please contact the pinot team");
      }
      _inputFilePath = lineSplits[1].trim();

      String segmentDirectoryName = _tableName + "_" + Integer.parseInt(lineSplits[2]);
      _localDiskOutputSegmentDir = _currentDiskWorkDir + "/segments/" + segmentDirectoryName;

      // To inherit from from the Hadoop Mapper class, you can't directly throw a general exception.
      Schema schema;
      Configuration conf = context.getConfiguration();
      final FileSystem fs = FileSystem.get(conf);
      final Path hdfsInputFilePath = new Path(_inputFilePath);

      final File localInputDataDir = new File(_currentDiskWorkDir, "inputData");
      try {
        if (localInputDataDir.exists()) {
          localInputDataDir.delete();
        }
        localInputDataDir.mkdir();

        final Path localInputFilePath =
            new Path(localInputDataDir.getAbsolutePath() + "/" + hdfsInputFilePath.getName());
        LOGGER.info("Copy from " + hdfsInputFilePath + " to " + localInputFilePath);
        fs.copyToLocalFile(hdfsInputFilePath, localInputFilePath);

        String schemaString = context.getConfiguration().get("data.schema");
        try {
          schema = Schema.fromString(schemaString);
        } catch (Exception e) {
          LOGGER.error("Could not get schema from string for value: " + schemaString);
          throw new RuntimeException(e);
        }
      } catch (Exception e) {
        LOGGER.error("Could not get schema: " + e);
        throw new RuntimeException(e);
      }

      LOGGER.info("*********************************************************************");
      LOGGER.info("input data file path : {}", _inputFilePath);
      LOGGER.info("local hdfs segment tar path: {}", _localHdfsSegmentTarPath);
      LOGGER.info("local disk output segment path: {}", _localDiskOutputSegmentDir);
      LOGGER.info("local disk segment tar path: {}", _localDiskSegmentTarPath);
      LOGGER.info("data schema: {}", schema);
      LOGGER.info("*********************************************************************");

      try {
        String segmentName =
            createSegment(_inputFilePath, schema, Integer.parseInt(lineSplits[2]), hdfsInputFilePath, localInputDataDir,
                fs);
        LOGGER.info(segmentName);
        LOGGER.info("Finished segment creation job successfully");
      } catch (Exception e) {
        LOGGER.error("Got exceptions during creating segments!", e);
      }

      context.write(new LongWritable(Long.parseLong(lineSplits[2])), new Text(
          FileSystem.get(_properties).listStatus(new Path(_localHdfsSegmentTarPath + "/"))[0].getPath().getName()));
      LOGGER.info("Finished the job successfully");
    }

    protected void setSegmentNameGenerator(SegmentGeneratorConfig segmentGeneratorConfig, Integer seqId, Path hdfsAvroPath, File dataPath) {
    }

    protected String createSegment(String dataFilePath, Schema schema, Integer seqId, Path hdfsInputFilePath,
        File localInputDataDir, FileSystem fs) throws Exception {
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, schema);

      segmentGeneratorConfig.setTableName(_tableName);
      setSegmentNameGenerator(segmentGeneratorConfig, seqId, hdfsInputFilePath, localInputDataDir);

      String inputFilePath = new File(localInputDataDir, hdfsInputFilePath.getName()).getAbsolutePath();
      LOGGER.info("Create segment input path: {}", inputFilePath);
      segmentGeneratorConfig.setInputFilePath(inputFilePath);

      FileFormat fileFormat = getFileFormat(dataFilePath);
      segmentGeneratorConfig.setFormat(fileFormat);
      segmentGeneratorConfig.setOnHeap(true);

      if (null != _postfix) {
        segmentGeneratorConfig.setSegmentNamePostfix(String.format("%s-%s", _postfix, seqId));
      } else {
        segmentGeneratorConfig.setSequenceId(seqId);
      }
      segmentGeneratorConfig.setReaderConfig(getReaderConfig(fileFormat));

      segmentGeneratorConfig.setOutDir(_localDiskOutputSegmentDir);

      // Add the current java package version to the segment metadata
      // properties file.
      Package objPackage = this.getClass().getPackage();
      if (null != objPackage) {
        String packageVersion = objPackage.getSpecificationVersion();
        if (null != packageVersion) {
          LOGGER.info("Pinot Hadoop Package version {}", packageVersion);
          segmentGeneratorConfig.setCreatorVersion(packageVersion);
        }
      }

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig);
      driver.build();

      // Tar the segment directory into file.
      String segmentName = driver.getSegmentName();

      File localDiskOutputSegmentDir = new File(_localDiskOutputSegmentDir, segmentName);
      String localDiskOutputSegmentDirAbsolutePath = localDiskOutputSegmentDir.getAbsolutePath();
      String localDiskSegmentTarFileAbsolutePath =
          new File(_localDiskSegmentTarPath).getAbsolutePath() + "/" + segmentName + JobConfigConstants.TARGZ;

      LOGGER.info("Trying to tar the segment to: {}", localDiskSegmentTarFileAbsolutePath);
      TarGzCompressionUtils.createTarGzOfDirectory(localDiskOutputSegmentDirAbsolutePath,
          localDiskSegmentTarFileAbsolutePath);
      String hdfsSegmentTarFilePath = _localHdfsSegmentTarPath + "/" + segmentName + JobConfigConstants.TARGZ;

      // Log segment size.
      long uncompressedSegmentSize = FileUtils.sizeOfDirectory(localDiskOutputSegmentDir);
      long compressedSegmentSize = new File(localDiskSegmentTarFileAbsolutePath).length();
      LOGGER.info(String.format("Segment %s uncompressed size: %s, compressed size: %s", segmentName,
          DataSize.fromBytes(uncompressedSegmentSize), DataSize.fromBytes(compressedSegmentSize)));

      LOGGER.info("*********************************************************************");
      LOGGER.info("Copy from : {} to {}", localDiskSegmentTarFileAbsolutePath, hdfsSegmentTarFilePath);
      LOGGER.info("*********************************************************************");
      fs.copyFromLocalFile(true, true, new Path(localDiskSegmentTarFileAbsolutePath), new Path(hdfsSegmentTarFilePath));
      return segmentName;
    }

    private RecordReaderConfig getReaderConfig(FileFormat fileFormat) throws IOException {
      RecordReaderConfig readerConfig = null;
      switch (fileFormat) {
        case CSV:
          if(_readerConfigFile == null) {
            readerConfig = new CSVRecordReaderConfig();
          }
          else {
            LOGGER.info("Reading CSV Record Reader Config from: {}", _readerConfigFile);
            Path readerConfigPath = new Path(_readerConfigFile);
            readerConfig = new ObjectMapper().readValue(_fileSystem.open(readerConfigPath), CSVRecordReaderConfig.class);
            LOGGER.info("CSV Record Reader Config: {}", readerConfig.toString());
          }
          break;
        case AVRO:
          break;
        case JSON:
          break;
        case THRIFT:
          readerConfig = new ThriftRecordReaderConfig();
        default:
          break;
      }
      return readerConfig;
    }

    private FileFormat getFileFormat(String dataFilePath) {
      if (dataFilePath.endsWith(".json")) {
        return FileFormat.JSON;
      }
      if (dataFilePath.endsWith(".csv")) {
        return FileFormat.CSV;
      }
      if (dataFilePath.endsWith(".avro")) {
        return FileFormat.AVRO;
      }
      if (dataFilePath.endsWith(".thrift")) {
        return FileFormat.THRIFT;
      }
      throw new RuntimeException("Not support file format - " + dataFilePath);
    }
  }
}