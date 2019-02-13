/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.hadoop.job.mapper;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.DataSize;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.readers.CSVRecordReaderConfig;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.data.readers.RecordReaderConfig;
import org.apache.pinot.core.data.readers.ThriftRecordReaderConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.hadoop.job.JobConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  protected static final String LOCAL_TEMP_DIR = "pinot_hadoop_tmp";

  protected final Logger _logger = LoggerFactory.getLogger(getClass());

  protected Configuration _jobConf;
  protected String _rawTableName;
  protected Schema _schema;

  // Optional
  protected TableConfig _tableConfig;
  protected String _segmentNamePostfix;
  protected Path _readerConfigFile;

  // HDFS segment tar directory
  protected Path _hdfsSegmentTarDir;

  // Temporary local directories
  protected File _localStagingDir;
  protected File _localInputDir;
  protected File _localSegmentDir;
  protected File _localSegmentTarDir;

  protected FileSystem _fileSystem;

  @Override
  public void setup(Context context)
      throws IOException, InterruptedException {
    _jobConf = context.getConfiguration();
    _logger.info("*********************************************************************");
    _logger.info("Job Configurations: {}", _jobConf);
    _logger.info("*********************************************************************");

    _rawTableName = Preconditions.checkNotNull(_jobConf.get(JobConfigConstants.SEGMENT_TABLE_NAME));
    _schema = Schema.fromString(_jobConf.get(JobConfigConstants.SCHEMA));

    // Optional
    String tableConfigString = _jobConf.get(JobConfigConstants.TABLE_CONFIG);
    if (tableConfigString != null) {
      _tableConfig = TableConfig.fromJsonString(tableConfigString);
    }
    _segmentNamePostfix = _jobConf.get(JobConfigConstants.SEGMENT_NAME_POSTFIX);
    String readerConfigFile = _jobConf.get(JobConfigConstants.PATH_TO_READER_CONFIG);
    if (readerConfigFile != null) {
      _readerConfigFile = new Path(readerConfigFile);
    }

    // Working directories
    _hdfsSegmentTarDir = new Path(FileOutputFormat.getWorkOutputPath(context), JobConfigConstants.SEGMENT_TAR_DIR);
    _localStagingDir = new File(LOCAL_TEMP_DIR);
    _localInputDir = new File(_localStagingDir, "inputData");
    _localSegmentDir = new File(_localStagingDir, "segments");
    _localSegmentTarDir = new File(_localStagingDir, JobConfigConstants.SEGMENT_TAR_DIR);

    if (_localStagingDir.exists()) {
      _logger.warn("Deleting existing file: {}", _localStagingDir);
      FileUtils.forceDelete(_localStagingDir);
    }
    _logger
        .info("Making local temporary directories: {}, {}, {}", _localStagingDir, _localInputDir, _localSegmentTarDir);
    Preconditions.checkState(_localStagingDir.mkdirs());
    Preconditions.checkState(_localInputDir.mkdir());
    Preconditions.checkState(_localSegmentDir.mkdir());
    Preconditions.checkState(_localSegmentTarDir.mkdir());

    _fileSystem = FileSystem.get(context.getConfiguration());

    _logger.info("*********************************************************************");
    _logger.info("Raw Table Name: {}", _rawTableName);
    _logger.info("Schema: {}", _schema);
    _logger.info("Table Config: {}", _tableConfig);
    _logger.info("Segment Name Postfix: {}", _segmentNamePostfix);
    _logger.info("Reader Config File: {}", _readerConfigFile);
    _logger.info("*********************************************************************");
    _logger.info("HDFS Segment Tar Directory: {}", _hdfsSegmentTarDir);
    _logger.info("Local Staging Directory: {}", _localStagingDir);
    _logger.info("Local Input Directory: {}", _localInputDir);
    _logger.info("Local Segment Tar Directory: {}", _localSegmentTarDir);
    _logger.info("*********************************************************************");
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] splits = StringUtils.split(value.toString(), ' ');
    Preconditions.checkState(splits.length == 2, "Illegal input value: {}", value);

    Path hdfsInputFile = new Path(splits[0]);
    int sequenceId = Integer.parseInt(splits[1]);
    _logger.info("Generating segment with HDFS input file: {}, sequence id: {}", hdfsInputFile, sequenceId);

    String inputFileName = hdfsInputFile.getName();
    File localInputFile = new File(_localInputDir, inputFileName);
    _logger.info("Copying input file from: {} to: {}", hdfsInputFile, localInputFile);
    _fileSystem.copyToLocalFile(hdfsInputFile, new Path(localInputFile.getAbsolutePath()));

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, _schema);
    segmentGeneratorConfig.setTableName(_rawTableName);
    segmentGeneratorConfig.setInputFilePath(localInputFile.getPath());
    segmentGeneratorConfig.setOutDir(_localSegmentDir.getPath());
    segmentGeneratorConfig.setSequenceId(sequenceId);
    if (_segmentNamePostfix != null) {
      segmentGeneratorConfig.setSegmentNamePostfix(_segmentNamePostfix);
    }
    FileFormat fileFormat = getFileFormat(inputFileName);
    segmentGeneratorConfig.setFormat(fileFormat);
    segmentGeneratorConfig.setReaderConfig(getReaderConfig(fileFormat));
    segmentGeneratorConfig.setOnHeap(true);

    addAdditionalSegmentGeneratorConfigs(segmentGeneratorConfig, hdfsInputFile, sequenceId);

    _logger.info("Start creating segment with sequence id: {}", sequenceId);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    try {
      driver.init(segmentGeneratorConfig);
      driver.build();
    } catch (Exception e) {
      _logger.error("Caught exception while creating segment with HDFS input file: {}, sequence id: {}", hdfsInputFile,
          sequenceId, e);
      throw new RuntimeException(e);
    }
    String segmentName = driver.getSegmentName();
    _logger.info("Finish creating segment: {} with sequence id: {}", segmentName, sequenceId);

    File localSegmentDir = new File(_localSegmentDir, segmentName);
    String segmentTarFileName = segmentName + JobConfigConstants.TAR_GZ_FILE_EXT;
    File localSegmentTarFile = new File(_localSegmentTarDir, segmentTarFileName);
    _logger.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
    TarGzCompressionUtils.createTarGzOfDirectory(localSegmentDir.getPath(), localSegmentTarFile.getPath());

    long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
    long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
    _logger.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
        DataSize.fromBytes(uncompressedSegmentSize), DataSize.fromBytes(compressedSegmentSize));

    Path hdfsSegmentTarFile = new Path(_hdfsSegmentTarDir, segmentTarFileName);
    _logger.info("Copying segment tar file from: {} to: {}", localSegmentTarFile, hdfsSegmentTarFile);
    _fileSystem.copyFromLocalFile(true, true, new Path(localSegmentTarFile.getAbsolutePath()), hdfsSegmentTarFile);

    context.write(new LongWritable(sequenceId), new Text(segmentTarFileName));
    _logger.info("Finish generating segment: {} with HDFS input file: {}, sequence id: {}", segmentName, hdfsInputFile,
        sequenceId);
  }

  protected FileFormat getFileFormat(String fileName) {
    if (fileName.endsWith(".avro")) {
      return FileFormat.AVRO;
    }
    if (fileName.endsWith(".csv")) {
      return FileFormat.CSV;
    }
    if (fileName.endsWith(".json")) {
      return FileFormat.JSON;
    }
    if (fileName.endsWith(".thrift")) {
      return FileFormat.THRIFT;
    }
    throw new IllegalArgumentException("Unsupported file format: {}" + fileName);
  }

  @Nullable
  protected RecordReaderConfig getReaderConfig(FileFormat fileFormat)
      throws IOException {
    if (_readerConfigFile != null) {
      if (fileFormat == FileFormat.CSV) {
        try (InputStream inputStream = _fileSystem.open(_readerConfigFile)) {
          CSVRecordReaderConfig readerConfig = JsonUtils.inputStreamToObject(inputStream, CSVRecordReaderConfig.class);
          _logger.info("Using CSV record reader config: {}", readerConfig);
          return readerConfig;
        }
      }
      if (fileFormat == FileFormat.THRIFT) {
        try (InputStream inputStream = _fileSystem.open(_readerConfigFile)) {
          ThriftRecordReaderConfig readerConfig =
              JsonUtils.inputStreamToObject(inputStream, ThriftRecordReaderConfig.class);
          _logger.info("Using Thrift record reader config: {}", readerConfig);
          return readerConfig;
        }
      }
    }
    return null;
  }

  /**
   * Can be overridden to set additional segment generator configs.
   */
  @SuppressWarnings("unused")
  protected void addAdditionalSegmentGeneratorConfigs(SegmentGeneratorConfig segmentGeneratorConfig, Path hdfsInputFile,
      int sequenceId) {
  }

  @Override
  public void cleanup(Context context) {
    _logger.info("Deleting local temporary directory: {}", _localStagingDir);
    FileUtils.deleteQuietly(_localStagingDir);
  }
}
