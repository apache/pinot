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
package org.apache.pinot.hadoop.io;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic Single Threaded {@link RecordWriter}
 */
public class PinotRecordWriter<T> extends RecordWriter<NullWritable, T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRecordWriter.class);
  private static final long MAX_FILE_SIZE = 64 * 1000000L;

  private final SegmentGeneratorConfig _segmentGeneratorConfig;
  private final FieldExtractor<T> _fieldExtractor;
  private final File _tempSegmentDir;
  private final File _dataFileDir;
  private final File _segmentTarDir;
  private final FileHandler _handler;
  private final FileSystem _fileSystem;
  private final Path _outputDir;

  public PinotRecordWriter(TaskAttemptContext job, SegmentGeneratorConfig segmentGeneratorConfig, FieldExtractor<T> fieldExtractor)
      throws IOException {
    _segmentGeneratorConfig = segmentGeneratorConfig;
    _fieldExtractor = fieldExtractor;

    _tempSegmentDir = new File(PinotOutputFormat.getTempSegmentDir(job));
    if (_tempSegmentDir.exists()) {
      FileUtils.cleanDirectory(_tempSegmentDir);
    }
    _dataFileDir = new File(_tempSegmentDir, "dataFile");
    FileUtils.forceMkdir(_dataFileDir);
    _segmentTarDir = new File(_tempSegmentDir, "segmentTar");
    FileUtils.forceMkdir(_segmentTarDir);

    _handler = new FileHandler(_dataFileDir.getPath(), "data", ".json", MAX_FILE_SIZE);
    _handler.open(true);

    _fileSystem = FileSystem.get(job.getConfiguration());
    _outputDir = FileOutputFormat.getOutputPath(job);
  }

  @Override
  public void write(NullWritable key, T value)
      throws IOException {
    _handler.write(JsonUtils.objectToBytes(_fieldExtractor.extractFields(value)));
  }

  @Override
  public void close(TaskAttemptContext context)
      throws IOException {
    _handler.close();

    File[] dataFiles = _dataFileDir.listFiles();
    assert dataFiles != null;
    int numDataFiles = dataFiles.length;
    for (int i = 0; i < numDataFiles; i++) {
      createSegment(dataFiles[i], i);
    }

    FileUtils.deleteDirectory(_tempSegmentDir);
  }

  private void createSegment(File dataFile, int sequenceId)
      throws IOException {
    LOGGER.info("Creating segment from data file: {} of sequence id: {}", dataFile, sequenceId);
    _segmentGeneratorConfig.setInputFilePath(dataFile.getPath());
    _segmentGeneratorConfig.setSequenceId(sequenceId);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    try {
      driver.init(_segmentGeneratorConfig);
      driver.build();
    } catch (Exception e) {
      throw new IllegalStateException("Caught exception while creating segment from data file: " + dataFile);
    }
    String segmentName = driver.getSegmentName();
    File indexDir = driver.getOutputDirectory();
    LOGGER.info("Created segment: {} from data file: {} into directory: {}", segmentName, dataFile, indexDir);

    File segmentTarFile = new File(_segmentTarDir, segmentName + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    LOGGER.info("Tarring segment: {} from directory: {} to: {}", segmentName, indexDir, segmentTarFile);
    TarGzCompressionUtils.createTarGzFile(indexDir, segmentTarFile);

    Path hdfsSegmentTarPath = new Path(_outputDir, segmentTarFile.getName());
    LOGGER.info("Copying segment tar file from local: {} to HDFS: {}", segmentTarFile, hdfsSegmentTarPath);
    _fileSystem.copyFromLocalFile(true, new Path(segmentTarFile.getPath()), hdfsSegmentTarPath);

    LOGGER.info("Finish creating segment: {} from data file: {} of sequence id: {} into HDFS: {}", segmentName, dataFile, sequenceId, hdfsSegmentTarPath);
  }
}
