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
package org.apache.pinot.core.segment.processing.framework;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A framework to process "m" given segments and convert them into "n" segments
 * The phases of the Segment Processor are
 * 1. Map - record transformation, partitioning, partition filtering
 * 2. Reduce - rollup, concat, split etc
 * 3. Segment generation
 *
 * This will typically be used by minion tasks, which want to perform some processing on segments
 * (eg task which merges segments, tasks which aligns segments per time boundaries etc)
 */
public class SegmentProcessorFramework {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentProcessorFramework.class);

  private final File _inputSegmentsDir;
  private final File _outputSegmentsDir;
  private final SegmentProcessorConfig _segmentProcessorConfig;

  private final Schema _pinotSchema;
  private final TableConfig _tableConfig;

  private final File _baseDir;
  private final File _mapperInputDir;
  private final File _mapperOutputDir;
  private final File _reducerOutputDir;

  /**
   * Initializes the Segment Processor framework with input segments, output path and processing config
   * @param inputSegmentsDir directory containing the input segments. These can be tarred or untarred.
   * @param segmentProcessorConfig config for segment processing
   * @param outputSegmentsDir directory for placing the resulting segments. This should already exist.
   */
  public SegmentProcessorFramework(File inputSegmentsDir, SegmentProcessorConfig segmentProcessorConfig,
      File outputSegmentsDir) {

    LOGGER.info(
        "Initializing SegmentProcessorFramework with input segments dir: {}, output segments dir: {} and segment processor config: {}",
        inputSegmentsDir.getAbsolutePath(), outputSegmentsDir.getAbsolutePath(), segmentProcessorConfig.toString());

    _inputSegmentsDir = inputSegmentsDir;
    Preconditions.checkState(_inputSegmentsDir.exists() && _inputSegmentsDir.isDirectory(),
        "Input path: %s must be a directory with Pinot segments", _inputSegmentsDir.getAbsolutePath());
    _outputSegmentsDir = outputSegmentsDir;
    Preconditions.checkState(
        _outputSegmentsDir.exists() && _outputSegmentsDir.isDirectory() && (_outputSegmentsDir.list().length == 0),
        "Must provide existing empty output directory: %s", _outputSegmentsDir.getAbsolutePath());

    _segmentProcessorConfig = segmentProcessorConfig;
    _pinotSchema = segmentProcessorConfig.getSchema();
    _tableConfig = segmentProcessorConfig.getTableConfig();

    _baseDir = new File(FileUtils.getTempDirectory(), "segment_processor_" + System.currentTimeMillis());
    FileUtils.deleteQuietly(_baseDir);
    Preconditions.checkState(_baseDir.mkdirs(), "Failed to create base directory: %s for SegmentProcessor", _baseDir);
    _mapperInputDir = new File(_baseDir, "mapper_input");
    Preconditions
        .checkState(_mapperInputDir.mkdirs(), "Failed to create mapper input directory: %s for SegmentProcessor",
            _mapperInputDir);
    _mapperOutputDir = new File(_baseDir, "mapper_output");
    Preconditions
        .checkState(_mapperOutputDir.mkdirs(), "Failed to create mapper output directory: %s for SegmentProcessor",
            _mapperOutputDir);
    _reducerOutputDir = new File(_baseDir, "reducer_output");
    Preconditions
        .checkState(_reducerOutputDir.mkdirs(), "Failed to create reducer output directory: %s for SegmentProcessor",
            _reducerOutputDir);
  }

  /**
   * Processes segments from the input directory as per the provided configs, then puts resulting segments into the output directory
   */
  public void processSegments()
      throws Exception {

    // Check for input segments
    File[] segmentFiles = _inputSegmentsDir.listFiles();
    if (segmentFiles.length == 0) {
      throw new IllegalStateException("No segments found in input dir: " + _inputSegmentsDir.getAbsolutePath()
          + ". Exiting SegmentProcessorFramework.");
    }

    // Mapper phase.
    LOGGER.info("Beginning mapper phase. Processing segments: {}", Arrays.toString(_inputSegmentsDir.list()));
    for (File segment : segmentFiles) {

      String fileName = segment.getName();
      File mapperInput = segment;

      // Untar the segments if needed
      if (!segment.isDirectory()) {
        if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
          mapperInput = TarGzCompressionUtils.untar(segment, _mapperInputDir).get(0);
        } else {
          throw new IllegalStateException("Unsupported segment format: " + segment.getAbsolutePath());
        }
      }

      // Set mapperId as the name of the segment
      SegmentMapperConfig mapperConfig =
          new SegmentMapperConfig(_pinotSchema, _segmentProcessorConfig.getRecordTransformerConfig(),
              _segmentProcessorConfig.getRecordFilterConfig(), _segmentProcessorConfig.getPartitionerConfigs());
      SegmentMapper mapper = new SegmentMapper(mapperInput.getName(), mapperInput, mapperConfig, _mapperOutputDir);
      mapper.map();
      mapper.cleanup();
    }

    // Check for mapper output files
    File[] mapperOutputFiles = _mapperOutputDir.listFiles();
    if (mapperOutputFiles.length == 0) {
      throw new IllegalStateException("No files found in mapper output directory: " + _mapperOutputDir.getAbsolutePath()
          + ". Exiting SegmentProcessorFramework.");
    }

    // Reducer phase.
    LOGGER.info("Beginning reducer phase. Processing files: {}", Arrays.toString(_mapperOutputDir.list()));
    // Mapper output directory has 1 directory per partition, named after the partition. Each directory contains 1 or more avro files.
    for (File partDir : mapperOutputFiles) {

      // Set partition as reducerId
      SegmentReducerConfig reducerConfig =
          new SegmentReducerConfig(_pinotSchema, _segmentProcessorConfig.getCollectorConfig(),
              _segmentProcessorConfig.getSegmentConfig().getMaxNumRecordsPerSegment());
      SegmentReducer reducer = new SegmentReducer(partDir.getName(), partDir, reducerConfig, _reducerOutputDir);
      reducer.reduce();
      reducer.cleanup();
    }

    // Check for reducer output files
    File[] reducerOutputFiles = _reducerOutputDir.listFiles();
    if (reducerOutputFiles.length == 0) {
      throw new IllegalStateException(
          "No files found in reducer output directory: " + _reducerOutputDir.getAbsolutePath()
              + ". Exiting SegmentProcessorFramework.");
    }

    // Segment generation phase.
    LOGGER.info("Beginning segment generation phase. Processing files: {}", Arrays.toString(_reducerOutputDir.list()));
    // Reducer output directory will have 1 or more avro files
    int segmentNum = 0;
    for (File resultFile : reducerOutputFiles) {
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, _pinotSchema);
      segmentGeneratorConfig.setTableName(_tableConfig.getTableName());
      segmentGeneratorConfig.setOutDir(_outputSegmentsDir.getAbsolutePath());
      segmentGeneratorConfig.setInputFilePath(resultFile.getAbsolutePath());
      segmentGeneratorConfig.setFormat(FileFormat.AVRO);
      segmentGeneratorConfig.setSequenceId(segmentNum ++);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig);
      driver.build();
    }

    LOGGER.info("Successfully converted segments from: {} to {}", _inputSegmentsDir,
        Arrays.toString(_outputSegmentsDir.list()));
  }

  /**
   * Cleans up the Segment Processor Framework state
   */
  public void cleanup() {
    FileUtils.deleteQuietly(_baseDir);
  }
}
