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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.mapper.SegmentMapper;
import org.apache.pinot.core.segment.processing.reducer.Reducer;
import org.apache.pinot.core.segment.processing.reducer.ReducerFactory;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGeneratorFactory;
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

  private final List<RecordReader> _recordReaders;
  private final SegmentProcessorConfig _segmentProcessorConfig;
  private final File _mapperOutputDir;
  private final File _reducerOutputDir;
  private final File _segmentsOutputDir;

  /**
   * Initializes the SegmentProcessorFramework with record readers, config and working directory.
   */
  public SegmentProcessorFramework(List<RecordReader> recordReaders, SegmentProcessorConfig segmentProcessorConfig,
      File workingDir)
      throws IOException {
    Preconditions.checkState(!recordReaders.isEmpty(), "No record reader is provided");

    LOGGER.info("Initializing SegmentProcessorFramework with {} record readers, config: {}, working dir: {}",
        recordReaders.size(), segmentProcessorConfig, workingDir.getAbsolutePath());

    _recordReaders = recordReaders;
    _segmentProcessorConfig = segmentProcessorConfig;

    _mapperOutputDir = new File(workingDir, "mapper_output");
    FileUtils.forceMkdir(_mapperOutputDir);
    _reducerOutputDir = new File(workingDir, "reducer_output");
    FileUtils.forceMkdir(_reducerOutputDir);
    _segmentsOutputDir = new File(workingDir, "segments_output");
    FileUtils.forceMkdir(_segmentsOutputDir);
  }

  /**
   * Processes records from record readers per the provided config, returns the directories for the generated segments.
   */
  public List<File> process()
      throws Exception {
    // Map phase
    LOGGER.info("Beginning map phase on {} record readers", _recordReaders.size());
    SegmentMapper mapper = new SegmentMapper(_recordReaders, _segmentProcessorConfig, _mapperOutputDir);
    Map<String, GenericRowFileManager> partitionToFileManagerMap = mapper.map();

    // Check for mapper output files
    if (partitionToFileManagerMap.isEmpty()) {
      LOGGER.info("No partition generated from mapper phase, skipping the reducer phase");
      return Collections.emptyList();
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
    List<File> outputSegmentDirs = new ArrayList<>();
    TableConfig tableConfig = _segmentProcessorConfig.getTableConfig();
    Schema schema = _segmentProcessorConfig.getSchema();
    String segmentNamePrefix = _segmentProcessorConfig.getSegmentConfig().getSegmentNamePrefix();
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    generatorConfig.setOutDir(_segmentsOutputDir.getPath());

    if (tableConfig.getIndexingConfig().getSegmentNameGeneratorType() != null) {
      generatorConfig.setSegmentNameGenerator(
          SegmentNameGeneratorFactory.createSegmentNameGenerator(tableConfig, schema, segmentNamePrefix, null, false));
    } else {
      generatorConfig.setSegmentNamePrefix(segmentNamePrefix);
    }

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
        outputSegmentDirs.add(driver.getOutputDirectory());
      }
      fileManager.cleanUp();
    }
    FileUtils.deleteDirectory(_mapperOutputDir);
    FileUtils.deleteDirectory(_reducerOutputDir);

    LOGGER.info("Successfully created segments: {}", outputSegmentDirs);
    return outputSegmentDirs;
  }
}
