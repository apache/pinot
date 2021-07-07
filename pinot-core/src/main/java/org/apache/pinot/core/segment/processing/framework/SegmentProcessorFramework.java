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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.mapper.SegmentMapper;
import org.apache.pinot.core.segment.processing.reducer.Reducer;
import org.apache.pinot.core.segment.processing.reducer.ReducerFactory;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
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

  private final TableConfig _tableConfig;
  private final Schema _schema;

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
    _tableConfig = segmentProcessorConfig.getTableConfig();
    _schema = segmentProcessorConfig.getSchema();

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
    Preconditions
        .checkState(segmentFiles != null && segmentFiles.length > 0, "Failed to find segments under input dir: %s",
            _inputSegmentsDir.getAbsolutePath());

    // Map phase
    LOGGER.info("Beginning map phase on segments: {}", Arrays.toString(_inputSegmentsDir.list()));
    List<RecordReader> recordReaders = new ArrayList<>(segmentFiles.length);
    for (File indexDir : segmentFiles) {
      String fileName = indexDir.getName();

      // Untar the segments if needed
      if (!indexDir.isDirectory()) {
        if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
          indexDir = TarGzCompressionUtils.untar(indexDir, _mapperInputDir).get(0);
        } else {
          throw new IllegalStateException("Unsupported segment format: " + indexDir.getAbsolutePath());
        }
      }

      PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader();
      // NOTE: Do not fill null field with default value to be consistent with other record readers
      recordReader.init(indexDir, null, null, true);
      recordReaders.add(recordReader);
    }
    SegmentMapper mapper = new SegmentMapper(recordReaders, _segmentProcessorConfig, _mapperOutputDir);
    Map<String, GenericRowFileManager> partitionToFileManagerMap = mapper.map();
    for (RecordReader recordReader : recordReaders) {
      recordReader.close();
    }
    FileUtils.deleteDirectory(_mapperInputDir);

    // Check for mapper output files
    if (partitionToFileManagerMap.isEmpty()) {
      LOGGER.info("No partition generated from mapper phase, skipping the reducer phase");
      return;
    }

    // Reduce phase
    LOGGER.info("Beginning reduce phase on partitions: {}", partitionToFileManagerMap.keySet());
    for (Map.Entry<String, GenericRowFileManager> entry : partitionToFileManagerMap.entrySet()) {
      String partitionId = entry.getKey();
      GenericRowFileManager fileManager = entry.getValue();
      Reducer reducer = ReducerFactory.getReducer(partitionId, fileManager, _segmentProcessorConfig, _reducerOutputDir);
      entry.setValue(reducer.reduce());
    }

    // Segment creation phase
    LOGGER.info("Beginning segment creation phase on partitions: {}", partitionToFileManagerMap.keySet());
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(_tableConfig, _schema);
    generatorConfig.setOutDir(_outputSegmentsDir.getPath());
    // TODO: Use NormalizedDateSegmentNameGenerator
    generatorConfig.setSegmentNamePrefix(_segmentProcessorConfig.getSegmentConfig().getSegmentNamePrefix());
    int maxNumRecordsPerSegment = _segmentProcessorConfig.getSegmentConfig().getMaxNumRecordsPerSegment();
    CompositeTransformer passThroughTransformer = CompositeTransformer.getPassThroughTransformer();
    int sequenceId = 0;
    for (Map.Entry<String, GenericRowFileManager> entry : partitionToFileManagerMap.entrySet()) {
      String partitionId = entry.getKey();
      GenericRowFileManager fileManager = entry.getValue();
      GenericRowFileReader fileReader = fileManager.getFileReader();
      int numRows = fileReader.getNumRows();
      int numSortFields = fileReader.getNumSortFields();
      LOGGER.info("Start creating segments on partition: {}, numRows: {}, numSortFields: {}", partitionId, numRows,
          numSortFields);
      GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
      for (int startRowId = 0; startRowId < numRows; startRowId += maxNumRecordsPerSegment, sequenceId++) {
        int endRowId = Math.min(startRowId + maxNumRecordsPerSegment, numRows);
        LOGGER.info("Start creating segment of sequenceId: {} with row range: {} to {}", sequenceId, startRowId,
            endRowId);
        generatorConfig.setSequenceId(sequenceId);
        GenericRowFileRecordReader recordReaderForRange = recordReader.getRecordReaderForRange(startRowId, endRowId);
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(generatorConfig, new RecordReaderSegmentCreationDataSource(recordReaderForRange),
            passThroughTransformer, null);
        driver.build();
      }
      fileManager.cleanUp();
    }

    LOGGER.info("Successfully converted segments from: {} to {}", Arrays.toString(_inputSegmentsDir.list()),
        Arrays.toString(_outputSegmentsDir.list()));
  }

  /**
   * Cleans up the Segment Processor Framework state
   */
  public void cleanup() {
    FileUtils.deleteQuietly(_baseDir);
  }
}
