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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.pinot.segment.local.recordtransformer.BasicFilterTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.name.SegmentNameGeneratorFactory;
import org.apache.pinot.spi.config.instance.InstanceType;
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
public class MaterializedViewProcessorFramework extends SegmentProcessorFramework {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewProcessorFramework.class);
  MaterializedViewProcessorConfig _mvProcessorConfig;

  /**
   * Initializes the MaterializedViewProcessorFramework with record readers, config and working directory.
   */
  public MaterializedViewProcessorFramework(List<RecordReader> recordReaders,
      SegmentProcessorConfig segmentProcessorConfig, File workingDir) throws IOException {
    super(recordReaders, segmentProcessorConfig, workingDir);
    _mvProcessorConfig = segmentProcessorConfig.getMaterializedViewProcessorConfig();
    if (_mvProcessorConfig.getFilterConfig() != null) {
      _customRecordTransformers = List.of(new BasicFilterTransformer(_mvProcessorConfig.getFilterConfig()));
    }
  }

  /**
   * Processes records from record readers per the provided config, returns the directories for the generated segments.
   */
  @Override
  public List<File> process()
      throws Exception {
    List<File> outputSegmentDirs = new ArrayList<>();
    int numRecordReaders = _recordReaderFileConfigs.size();
    int nextRecordReaderIndexToBeProcessed = 0;
    int iterationCount = 1;
    boolean canMapperBeEarlyTerminated =
        _segmentProcessorConfig.getSegmentConfig().getIntermediateFileSizeThreshold() != Long.MAX_VALUE
            || _segmentProcessorConfig.getSegmentConfig().getMaxDiskUsagePercentage() < 100;
    String logMessage;

    while (nextRecordReaderIndexToBeProcessed < numRecordReaders) {
      // Initialise the mapper. Eliminate the record readers that have been processed in the previous iterations.
      SegmentMapper mapper = getSegmentMapper(_recordReaderFileConfigs.subList(nextRecordReaderIndexToBeProcessed,
          numRecordReaders));

      // Log start of iteration details only if intermediate file size threshold is set.
      if (canMapperBeEarlyTerminated) {
        logMessage = String.format("Starting iteration %d with %d record readers. "
                + "Starting index = %d, end index = %d",
            iterationCount,
            _recordReaderFileConfigs
                .subList(nextRecordReaderIndexToBeProcessed, numRecordReaders).size(),
            nextRecordReaderIndexToBeProcessed + 1, numRecordReaders);
        LOGGER.info(logMessage);
        logToObserver(MAP_STAGE, logMessage);
      }

      // Map phase.
      long mapStartTimeInMs = System.currentTimeMillis();
      logToObserver(MAP_STAGE, "Starting Map phase for iteration " + iterationCount);
      Map<String, GenericRowFileManager> partitionToFileManagerMap = mapper.map();
      _incompleteRowsFound += mapper.getIncompleteRowsFound();
      _skippedRowsFound += mapper.getSkippedRowsFound();
      _sanitizedRowsFound += mapper.getSanitizedRowsFound();

      // Log the time taken to map.
      logMessage = "Finished Map phase for iteration " + iterationCount + " in "
          + (System.currentTimeMillis() - mapStartTimeInMs) + "ms";
      LOGGER.info(logMessage);
      logToObserver(MAP_STAGE, logMessage);

      // Check for mapper output files, if no files are generated, skip the reducer phase and move on to the next
      // iteration.
      if (partitionToFileManagerMap.isEmpty()) {
        logMessage = "No mapper output files generated, skipping reduce phase";
        LOGGER.info(logMessage);
        logToObserver(MAP_STAGE, logMessage);
        nextRecordReaderIndexToBeProcessed =
            getNextRecordReaderIndexToBeProcessed(nextRecordReaderIndexToBeProcessed);
        continue;
      }

      // Reduce phase.
      logToObserver(REDUCE_STAGE, "Starting Reduce phase for iteration " + iterationCount);
      doReduce(partitionToFileManagerMap);

      // Segment creation phase. Add the created segments to the final list.
      logToObserver(GENERATE_STAGE, "Generating segments for iteration " + iterationCount);
      outputSegmentDirs.addAll(generateSegment(partitionToFileManagerMap));

      // Store the starting index of the record readers that
      // were processed in this iteration for logging purposes.
      int startingProcessedRecordReaderIndex = nextRecordReaderIndexToBeProcessed;

      // Update next record reader index to be processed.
      nextRecordReaderIndexToBeProcessed =
          getNextRecordReaderIndexToBeProcessed(nextRecordReaderIndexToBeProcessed);

      // Log the details between iteration only if intermediate file size threshold is set.
      if (canMapperBeEarlyTerminated) {
        // Take care of logging the proper RecordReader index in case of the last iteration.
        int boundaryIndexToLog =
            nextRecordReaderIndexToBeProcessed == numRecordReaders ? nextRecordReaderIndexToBeProcessed
                : nextRecordReaderIndexToBeProcessed + 1;

        // We are sure that the last RecordReader is completely processed in the last iteration else
        // it may or may not
        // have completed processing. Log it accordingly.
        if (nextRecordReaderIndexToBeProcessed == numRecordReaders) {
          logMessage = String.format("Finished processing all of %d RecordReaders", numRecordReaders);
        } else {
          logMessage = String.format("Finished processing RecordReaders %d to %d "
                  + "(RecordReader %d might be partially processed) out of %d in "
                  + "iteration %d", startingProcessedRecordReaderIndex + 1, boundaryIndexToLog,
              nextRecordReaderIndexToBeProcessed + 1, numRecordReaders, iterationCount);
        }
        LOGGER.info(logMessage);
        logToObserver(GENERATE_STAGE, logMessage);
      }
      iterationCount++;
    }
    return outputSegmentDirs;
  }

  @Override
  protected void doReduce(Map<String, GenericRowFileManager> partitionToFileManagerMap)
      throws Exception {
    LOGGER.info("Beginning reduce phase on partitions: {}", partitionToFileManagerMap.keySet());
    Consumer<Object> observer = _segmentProcessorConfig.getProgressObserver();
    int totalCount = partitionToFileManagerMap.size();
    int count = 1;
    for (Map.Entry<String, GenericRowFileManager> entry : partitionToFileManagerMap.entrySet()) {
      String partitionId = entry.getKey();
      observer.accept(
          String.format("Doing reduce phase on data from partition: %s (%d out of %d)", partitionId, count++,
              totalCount));
      GenericRowFileManager fileManager = entry.getValue();
      Reducer reducer = ReducerFactory.getReducer(partitionId, fileManager, _segmentProcessorConfig,
          _reducerOutputDir);
      entry.setValue(reducer.reduce());
    }
  }

  @Override
  protected List<File> generateSegment(Map<String, GenericRowFileManager> partitionToFileManagerMap)
      throws Exception {
    LOGGER.info("Beginning segment creation phase on partitions: {}", partitionToFileManagerMap.keySet());
    List<File> outputSegmentDirs = new ArrayList<>();
    TableConfig mvTableConfig = _mvProcessorConfig.getMvTableConfig();
    Schema mvSchema = _mvProcessorConfig.getMvSchema();
    String segmentNamePrefix = _segmentProcessorConfig.getSegmentConfig().getSegmentNamePrefix();
    String segmentNamePostfix = _segmentProcessorConfig.getSegmentConfig().getSegmentNamePostfix();
    String fixedSegmentName = _segmentProcessorConfig.getSegmentConfig().getFixedSegmentName();
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(mvTableConfig, mvSchema);
    generatorConfig.setOutDir(_segmentsOutputDir.getPath());
    Consumer<Object> observer = _segmentProcessorConfig.getProgressObserver();
    generatorConfig.setCreationTime(String.valueOf(_segmentProcessorConfig.getCustomCreationTime()));

    if (_segmentProcessorConfig.getSegmentNameGenerator() != null) {
      generatorConfig.setSegmentNameGenerator(_segmentProcessorConfig.getSegmentNameGenerator());
    } else if (mvTableConfig.getIndexingConfig().getSegmentNameGeneratorType() != null) {
      generatorConfig.setSegmentNameGenerator(
          SegmentNameGeneratorFactory.createSegmentNameGenerator(mvTableConfig, mvSchema, segmentNamePrefix,
              segmentNamePostfix, fixedSegmentName, false));
    } else {
      // SegmentNameGenerator will be inferred by the SegmentGeneratorConfig.
      generatorConfig.setSegmentNamePrefix(segmentNamePrefix);
      generatorConfig.setSegmentNamePostfix(segmentNamePostfix);
      generatorConfig.setSegmentName(fixedSegmentName);
    }

    for (Map.Entry<String, GenericRowFileManager> entry : partitionToFileManagerMap.entrySet()) {
      String partitionId = entry.getKey();
      GenericRowFileManager fileManager = entry.getValue();
      try {
        GenericRowFileReader fileReader = fileManager.getFileReader();
        int numRows = fileReader.getNumRows();
        int numSortFields = fileReader.getNumSortFields();
        LOGGER.info("Start creating segments on partition: {}, numRows: {}, numSortFields: {}",
            partitionId, numRows, numSortFields);
        GenericRowFileRecordReader recordReader = fileReader.getRecordReader();
        int maxNumRecordsPerSegment;
        for (int startRowId = 0; startRowId < numRows; startRowId += maxNumRecordsPerSegment,
            _segmentSequenceId++) {
          maxNumRecordsPerSegment = _segmentNumRowProvider.getNumRows();
          int endRowId = Math.min(startRowId + maxNumRecordsPerSegment, numRows);
          LOGGER.info("Start creating segment of sequenceId: {} with row range: {} to {}", _segmentSequenceId,
              startRowId, endRowId);
          observer.accept(String.format(
              "Creating segment of sequentId: %d with data from partition: %s and "
                  + "row range: [%d, %d) out of [0, %d)",
              _segmentSequenceId, partitionId, startRowId, endRowId, numRows));
          generatorConfig.setSequenceId(_segmentSequenceId);
          GenericRowFileRecordReader recordReaderForRange =
              recordReader.getRecordReaderForRange(startRowId, endRowId);
          SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
          driver.init(generatorConfig, new RecordReaderSegmentCreationDataSource(recordReaderForRange),
              TransformPipeline.getPassThroughPipeline(mvTableConfig.getTableName()), InstanceType.MINION);
          driver.build();
          _incompleteRowsFound += driver.getIncompleteRowsFound();
          _skippedRowsFound += driver.getSkippedRowsFound();
          _sanitizedRowsFound += driver.getSanitizedRowsFound();
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
