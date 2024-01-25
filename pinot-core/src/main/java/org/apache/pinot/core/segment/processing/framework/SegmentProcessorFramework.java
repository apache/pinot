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
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.mapper.SegmentMapper;
import org.apache.pinot.core.segment.processing.reducer.Reducer;
import org.apache.pinot.core.segment.processing.reducer.ReducerFactory;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGeneratorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;
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

  private final List<RecordReaderFileConfig> _recordReaderFileConfigs;
  private final List<RecordTransformer> _customRecordTransformers;
  private final SegmentProcessorConfig _segmentProcessorConfig;
  private final File _mapperOutputDir;
  private final File _reducerOutputDir;
  private final File _segmentsOutputDir;
  private final SegmentNumRowProvider _segmentNumRowProvider;
  private int _segmentSequenceId = 0;

  /**
   * Initializes the SegmentProcessorFramework with record readers, config and working directory. We will now rely on
   * users passing RecordReaderFileConfig, since that also allows us to do lazy initialization of RecordReaders.
   * Please use the other constructor that uses RecordReaderFileConfig.
   */
  @Deprecated
  public SegmentProcessorFramework(List<RecordReader> recordReaders, SegmentProcessorConfig segmentProcessorConfig,
      File workingDir)
      throws IOException {
    this(segmentProcessorConfig, workingDir, convertRecordReadersToRecordReaderFileConfig(recordReaders),
        Collections.emptyList(), null);
  }

  public SegmentProcessorFramework(SegmentProcessorConfig segmentProcessorConfig, File workingDir,
      List<RecordReaderFileConfig> recordReaderFileConfigs, List<RecordTransformer> customRecordTransformers,
      SegmentNumRowProvider segmentNumRowProvider)
      throws IOException {

    Preconditions.checkState(!recordReaderFileConfigs.isEmpty(), "No recordReaderFileConfigs provided");
    LOGGER.info("Initializing SegmentProcessorFramework with {} record readers, config: {}, working dir: {}",
        recordReaderFileConfigs.size(), segmentProcessorConfig, workingDir.getAbsolutePath());
    _recordReaderFileConfigs = recordReaderFileConfigs;
    _customRecordTransformers = customRecordTransformers;

    _segmentProcessorConfig = segmentProcessorConfig;

    _mapperOutputDir = new File(workingDir, "mapper_output");
    FileUtils.forceMkdir(_mapperOutputDir);
    _reducerOutputDir = new File(workingDir, "reducer_output");
    FileUtils.forceMkdir(_reducerOutputDir);
    _segmentsOutputDir = new File(workingDir, "segments_output");
    FileUtils.forceMkdir(_segmentsOutputDir);

    _segmentNumRowProvider = (segmentNumRowProvider == null) ? new DefaultSegmentNumRowProvider(
        segmentProcessorConfig.getSegmentConfig().getMaxNumRecordsPerSegment()) : segmentNumRowProvider;
  }

  private static List<RecordReaderFileConfig> convertRecordReadersToRecordReaderFileConfig(
      List<RecordReader> recordReaders) {
    Preconditions.checkState(!recordReaders.isEmpty(), "No record reader is provided");
    List<RecordReaderFileConfig> recordReaderFileConfigs = new ArrayList<>();
    for (RecordReader recordReader : recordReaders) {
      recordReaderFileConfigs.add(new RecordReaderFileConfig(recordReader));
    }
    return recordReaderFileConfigs;
  }

  /**
   * Processes records from record readers per the provided config, returns the directories for the generated segments.
   */
  public List<File> process()
      throws Exception {
    try {
      return doProcess();
    } catch (Exception e) {
      // Cleaning up output dir as processing has failed. file managers left from map or reduce phase will be cleaned
      // up in the respective phases.
      FileUtils.deleteQuietly(_segmentsOutputDir);
      throw e;
    } finally {
      FileUtils.deleteDirectory(_mapperOutputDir);
      FileUtils.deleteDirectory(_reducerOutputDir);
    }
  }

  private List<File> doProcess()
      throws Exception {
    List<File> outputSegmentDirs = new ArrayList<>();
    int numRecordReaders = _recordReaderFileConfigs.size();
    int nextRecordReaderIndexToBeProcessed = 0;
    int iterationCount = 1;
    Consumer<Object> observer = _segmentProcessorConfig.getProgressObserver();
    boolean isMapperOutputSizeThresholdEnabled =
        _segmentProcessorConfig.getSegmentConfig().getIntermediateFileSizeThreshold() != Long.MAX_VALUE;

    while (nextRecordReaderIndexToBeProcessed < numRecordReaders) {
      // Initialise the mapper. Eliminate the record readers that have been processed in the previous iterations.
      SegmentMapper mapper =
          new SegmentMapper(_recordReaderFileConfigs.subList(nextRecordReaderIndexToBeProcessed, numRecordReaders),
              _customRecordTransformers, _segmentProcessorConfig, _mapperOutputDir);

      // Log start of iteration details only if intermediate file size threshold is set.
      if (isMapperOutputSizeThresholdEnabled) {
        String logMessage =
            String.format("Starting iteration %d with %d record readers. Starting index = %d, end index = %d",
                iterationCount,
                _recordReaderFileConfigs.subList(nextRecordReaderIndexToBeProcessed, numRecordReaders).size(),
                nextRecordReaderIndexToBeProcessed + 1, numRecordReaders);
        LOGGER.info(logMessage);
        observer.accept(logMessage);
      }

      // Map phase.
      long mapStartTimeInMs = System.currentTimeMillis();
      Map<String, GenericRowFileManager> partitionToFileManagerMap = mapper.map();

      // Log the time taken to map.
      LOGGER.info("Finished iteration {} in {}ms", iterationCount, System.currentTimeMillis() - mapStartTimeInMs);

      // Check for mapper output files, if no files are generated, skip the reducer phase and move on to the next
      // iteration.
      if (partitionToFileManagerMap.isEmpty()) {
        LOGGER.info("No mapper output files generated, skipping reduce phase");
        nextRecordReaderIndexToBeProcessed = getNextRecordReaderIndexToBeProcessed(nextRecordReaderIndexToBeProcessed);
        continue;
      }

      // Reduce phase.
      doReduce(partitionToFileManagerMap);

      // Segment creation phase. Add the created segments to the final list.
      outputSegmentDirs.addAll(generateSegment(partitionToFileManagerMap));

      // Store the starting index of the record readers that were processed in this iteration for logging purposes.
      int startingProcessedRecordReaderIndex = nextRecordReaderIndexToBeProcessed;

      // Update next record reader index to be processed.
      nextRecordReaderIndexToBeProcessed = getNextRecordReaderIndexToBeProcessed(nextRecordReaderIndexToBeProcessed);

      // Log the details between iteration only if intermediate file size threshold is set.
      if (isMapperOutputSizeThresholdEnabled) {
        // Take care of logging the proper RecordReader index in case of the last iteration.
        int boundaryIndexToLog =
            nextRecordReaderIndexToBeProcessed == numRecordReaders ? nextRecordReaderIndexToBeProcessed
                : nextRecordReaderIndexToBeProcessed + 1;

        // We are sure that the last RecordReader is completely processed in the last iteration else it may or may not
        // have completed processing. Log it accordingly.
        String logMessage;
        if (nextRecordReaderIndexToBeProcessed == numRecordReaders) {
          logMessage = String.format("Finished processing all of %d RecordReaders", numRecordReaders);
        } else {
          logMessage = String.format(
              "Finished processing RecordReaders %d to %d (RecordReader %d might be partially processed) out of %d in "
                  + "iteration %d", startingProcessedRecordReaderIndex + 1, boundaryIndexToLog,
              nextRecordReaderIndexToBeProcessed + 1, numRecordReaders, iterationCount);
        }

        observer.accept(logMessage);
        LOGGER.info(logMessage);
      }

      iterationCount++;
    }
    return outputSegmentDirs;
  }

  private int getNextRecordReaderIndexToBeProcessed(int currentRecordIndex) {
    for (int i = currentRecordIndex; i < _recordReaderFileConfigs.size(); i++) {
      RecordReaderFileConfig recordReaderFileConfig = _recordReaderFileConfigs.get(i);
      if (!recordReaderFileConfig.isRecordReaderDone()) {
        return i;
      }
    }
    return _recordReaderFileConfigs.size();
  }

  private void doReduce(Map<String, GenericRowFileManager> partitionToFileManagerMap)
      throws Exception {
    LOGGER.info("Beginning reduce phase on partitions: {}", partitionToFileManagerMap.keySet());
    Consumer<Object> observer = _segmentProcessorConfig.getProgressObserver();
    int totalCount = partitionToFileManagerMap.keySet().size();
    int count = 1;
    for (Map.Entry<String, GenericRowFileManager> entry : partitionToFileManagerMap.entrySet()) {
      String partitionId = entry.getKey();
      observer.accept(
          String.format("Doing reduce phase on data from partition: %s (%d out of %d)", partitionId, count++,
              totalCount));
      GenericRowFileManager fileManager = entry.getValue();
      Reducer reducer = ReducerFactory.getReducer(partitionId, fileManager, _segmentProcessorConfig, _reducerOutputDir);
      entry.setValue(reducer.reduce());
    }
  }

  private List<File> generateSegment(Map<String, GenericRowFileManager> partitionToFileManagerMap)
      throws Exception {
    LOGGER.info("Beginning segment creation phase on partitions: {}", partitionToFileManagerMap.keySet());
    List<File> outputSegmentDirs = new ArrayList<>();
    TableConfig tableConfig = _segmentProcessorConfig.getTableConfig();
    Schema schema = _segmentProcessorConfig.getSchema();
    String segmentNamePrefix = _segmentProcessorConfig.getSegmentConfig().getSegmentNamePrefix();
    String segmentNamePostfix = _segmentProcessorConfig.getSegmentConfig().getSegmentNamePostfix();
    String fixedSegmentName = _segmentProcessorConfig.getSegmentConfig().getFixedSegmentName();
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    generatorConfig.setOutDir(_segmentsOutputDir.getPath());
    Consumer<Object> observer = _segmentProcessorConfig.getProgressObserver();

    if (tableConfig.getIndexingConfig().getSegmentNameGeneratorType() != null) {
      generatorConfig.setSegmentNameGenerator(
          SegmentNameGeneratorFactory.createSegmentNameGenerator(tableConfig, schema, segmentNamePrefix,
              segmentNamePostfix, fixedSegmentName, false));
    } else {
      // SegmentNameGenerator will be inferred by the SegmentGeneratorConfig.
      generatorConfig.setSegmentNamePrefix(segmentNamePrefix);
      generatorConfig.setSegmentNamePostfix(segmentNamePostfix);
    }

    for (Map.Entry<String, GenericRowFileManager> entry : partitionToFileManagerMap.entrySet()) {
      String partitionId = entry.getKey();
      GenericRowFileManager fileManager = entry.getValue();
      try {
        GenericRowFileReader fileReader = fileManager.getFileReader();
        int numRows = fileReader.getNumRows();
        int numSortFields = fileReader.getNumSortFields();
        LOGGER.info("Start creating segments on partition: {}, numRows: {}, numSortFields: {}", partitionId, numRows,
            numSortFields);
        GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
        int maxNumRecordsPerSegment;
        for (int startRowId = 0; startRowId < numRows; startRowId += maxNumRecordsPerSegment, _segmentSequenceId++) {
          maxNumRecordsPerSegment = _segmentNumRowProvider.getNumRows();
          int endRowId = Math.min(startRowId + maxNumRecordsPerSegment, numRows);
          LOGGER.info("Start creating segment of sequenceId: {} with row range: {} to {}", _segmentSequenceId,
              startRowId, endRowId);
          observer.accept(String.format(
              "Creating segment of sequentId: %d with data from partition: %s and row range: [%d, %d) out of [0, %d)",
              _segmentSequenceId, partitionId, startRowId, endRowId, numRows));
          generatorConfig.setSequenceId(_segmentSequenceId);
          GenericRowFileRecordReader recordReaderForRange = recordReader.getRecordReaderForRange(startRowId, endRowId);
          SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
          driver.init(generatorConfig, new RecordReaderSegmentCreationDataSource(recordReaderForRange),
              TransformPipeline.getPassThroughPipeline());
          driver.build();
          outputSegmentDirs.add(driver.getOutputDirectory());
          _segmentNumRowProvider.updateSegmentInfo(driver.getSegmentStats().getTotalDocCount(),
              FileUtils.sizeOfDirectory(driver.getOutputDirectory()));
        }
      } finally {
        fileManager.cleanUp();
      }
    }
    LOGGER.info("Successfully created segments: {}", outputSegmentDirs);
    return outputSegmentDirs;
  }
}
