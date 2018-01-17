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

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.readers.CSVRecordReaderConfig;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderConfig;
import com.linkedin.pinot.core.data.readers.ThriftRecordReaderConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.hadoop.job.JobConfigConstants;

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

import java.io.File;
import java.io.IOException;


public class HadoopSegmentCreationMapReduceJob {

  public static class HadoopSegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentCreationMapper.class);
    private Configuration _properties;

    private String _inputFilePath;
    private String _outputPath;
    private String _tableName;
    private String _postfix;

    private Path _currentHdfsWorkDir;
    private String _currentDiskWorkDir;

    // Temporary HDFS path for local machine
    private String _localHdfsSegmentTarPath;

    private String _localDiskSegmentDirectory;
    private String _localDiskSegmentTarPath;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {


      _currentHdfsWorkDir = FileOutputFormat.getWorkOutputPath(context);
      _currentDiskWorkDir = "pinot_hadoop_tmp";

      // Temporary HDFS path for local machine
      _localHdfsSegmentTarPath =  _currentHdfsWorkDir + "/segmentTar";
      _localDiskSegmentTarPath = _currentDiskWorkDir + "/segmentsTar";



      new File(_localDiskSegmentTarPath).mkdirs();

      LOGGER.info("*********************************************************************");
      LOGGER.info("Configurations : {}", context.getConfiguration().toString());
      LOGGER.info("*********************************************************************");
      LOGGER.info("Current HDFS working dir : {}", _currentHdfsWorkDir);
      LOGGER.info("Current DISK working dir : {}", new File(_currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");
      _properties = context.getConfiguration();

      _outputPath = _properties.get("path.to.output");
      _tableName = _properties.get("segment.table.name");
      _postfix = _properties.get("segment.name.postfix", null);

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
    public void cleanup(Context context) throws IOException, InterruptedException {
      FileUtils.deleteQuietly(new File(_currentDiskWorkDir));
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
      LOGGER.info("*********************************************************************");

      if (lineSplits.length != 3) {
        throw new RuntimeException("Input to the mapper is malformed, please contact the pinot team");
      }
      _inputFilePath = lineSplits[1].trim();

      String segmentDirectory = _tableName + "_" + Integer.parseInt(lineSplits[2]);
      _localDiskSegmentDirectory = _currentDiskWorkDir + "/segments/" + segmentDirectory;

      // To inherit from from the Hadoop Mapper class, you can't directly throw a general exception.
      Schema schema;
      final FileSystem fs = FileSystem.get(new Configuration());
      final Path hdfsAvroPath = new Path(_inputFilePath);
      final File dataPath = new File(_currentDiskWorkDir, "data");
      try {
        if (dataPath.exists()) {
          dataPath.delete();
        }
        dataPath.mkdir();

        final Path localAvroPath = new Path(dataPath + "/" + hdfsAvroPath.getName());
        LOGGER.info("Copy from " + hdfsAvroPath + " to " + localAvroPath);
        fs.copyToLocalFile(hdfsAvroPath, localAvroPath);

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
      LOGGER.info("local disk segment path: {}", _localDiskSegmentDirectory);
      LOGGER.info("local disk segment tar path: {}", _localDiskSegmentTarPath);
      LOGGER.info("data schema: {}", _localDiskSegmentTarPath);
      LOGGER.info("*********************************************************************");

      try {
        String segmentName = createSegment(_inputFilePath, schema, Integer.parseInt(lineSplits[2]), hdfsAvroPath, dataPath, fs);
        LOGGER.info(segmentName);
        LOGGER.info("finished segment creation job successfully");
      } catch (Exception e) {
        LOGGER.error("Got exceptions during creating segments!", e);
      }

      context.write(new LongWritable(Long.parseLong(lineSplits[2])),
          new Text(FileSystem.get(_properties).listStatus(new Path(_localHdfsSegmentTarPath + "/"))[0].getPath().getName()));
      LOGGER.info("finished the job successfully");
    }

    protected void setSegmentNameGenerator(SegmentGeneratorConfig segmentGeneratorConfig, Integer seqId, Path hdfsAvroPath, File dataPath) {

    }

    protected String createSegment(String dataFilePath, Schema schema, Integer seqId, Path hdfsDataPath, File dataPath, FileSystem fs) throws Exception {
      LOGGER.info("Data schema is : {}", schema);
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
      segmentGeneratorConfig.setTableName(_tableName);
      setSegmentNameGenerator(segmentGeneratorConfig, seqId, hdfsDataPath, dataPath);

      segmentGeneratorConfig.setInputFilePath(new File(dataPath, hdfsDataPath.getName()).getAbsolutePath());

      FileFormat fileFormat = getFileFormat(dataFilePath);
      segmentGeneratorConfig.setFormat(fileFormat);
      segmentGeneratorConfig.setOnHeap(true);
      
      if (null != _postfix) {
        segmentGeneratorConfig.setSegmentNamePostfix(String.format("%s-%s", _postfix, seqId));
      } else {
        segmentGeneratorConfig.setSequenceId(seqId);
      }
      segmentGeneratorConfig.setReaderConfig(getReaderConfig(fileFormat));

      segmentGeneratorConfig.setOutDir(_localDiskSegmentDirectory);

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
      String localSegmentPath = new File(_localDiskSegmentDirectory, segmentName).getAbsolutePath();

      String localTarPath = _localDiskSegmentTarPath + "/" + segmentName + JobConfigConstants.TARGZ;
      LOGGER.info("Trying to tar the segment to: {}", localTarPath);
      TarGzCompressionUtils.createTarGzOfDirectory(localSegmentPath, localTarPath);
      String hdfsTarPath = _localHdfsSegmentTarPath + "/" + segmentName + JobConfigConstants.TARGZ;

      LOGGER.info("*********************************************************************");
      LOGGER.info("Copy from : {} to {}", localTarPath, hdfsTarPath);
      LOGGER.info("*********************************************************************");
      fs.copyFromLocalFile(true, true, new Path(localTarPath), new Path(hdfsTarPath));
      return segmentName;
    }

    private RecordReaderConfig getReaderConfig(FileFormat fileFormat) {
      RecordReaderConfig readerConfig = null;
      switch (fileFormat) {
        case CSV:
          readerConfig = new CSVRecordReaderConfig();
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
