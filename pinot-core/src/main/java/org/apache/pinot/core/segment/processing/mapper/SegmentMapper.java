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
package org.apache.pinot.core.segment.processing.mapper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.core.segment.processing.genericrow.AdaptiveSizeBasedWriter;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.core.segment.processing.partitioner.Partitioner;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandler;
import org.apache.pinot.core.segment.processing.timehandler.TimeHandlerFactory;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFileConfig;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mapper phase of the SegmentProcessorFramework.
 * Reads the input records and creates partitioned generic row files.
 * Performs:
 * - record filtering
 * - column transformations
 * - partitioning
 */
public class SegmentMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMapper.class);
  private final SegmentProcessorConfig _processorConfig;
  private final File _mapperOutputDir;
  private final List<FieldSpec> _fieldSpecs;
  private final boolean _includeNullFields;
  private final int _numSortFields;
  private final CompositeTransformer _recordTransformer;
  private final TimeHandler _timeHandler;
  private final Partitioner[] _partitioners;
  private final String[] _partitionsBuffer;
  // NOTE: Use TreeMap so that the order is deterministic
  private final Map<String, GenericRowFileManager> _partitionToFileManagerMap = new TreeMap<>();
  private AdaptiveSizeBasedWriter _adaptiveSizeBasedWriter;
  private List<RecordReaderFileConfig> _recordReaderFileConfigs;
  private List<RecordTransformer> _customRecordTransformers;

  public SegmentMapper(List<RecordReaderFileConfig> recordReaderFileConfigs,
      List<RecordTransformer> customRecordTransformers, SegmentProcessorConfig processorConfig, File mapperOutputDir) {
    _recordReaderFileConfigs = recordReaderFileConfigs;
    _customRecordTransformers = customRecordTransformers;
    _processorConfig = processorConfig;
    _mapperOutputDir = mapperOutputDir;

    TableConfig tableConfig = processorConfig.getTableConfig();
    Schema schema = processorConfig.getSchema();
    Pair<List<FieldSpec>, Integer> pair = SegmentProcessorUtils.getFieldSpecs(schema, processorConfig.getMergeType(),
        tableConfig.getIndexingConfig().getSortedColumn());
    _fieldSpecs = pair.getLeft();
    _numSortFields = pair.getRight();
    _includeNullFields = tableConfig.getIndexingConfig().isNullHandlingEnabled();
    _recordTransformer = CompositeTransformer.composeAllTransformers(_customRecordTransformers, tableConfig, schema);
    _timeHandler = TimeHandlerFactory.getTimeHandler(processorConfig);
    List<PartitionerConfig> partitionerConfigs = processorConfig.getPartitionerConfigs();
    int numPartitioners = partitionerConfigs.size();
    _partitioners = new Partitioner[numPartitioners];
    for (int i = 0; i < numPartitioners; i++) {
      _partitioners[i] = PartitionerFactory.getPartitioner(partitionerConfigs.get(i));
    }
    // Time partition + partition from partitioners
    _partitionsBuffer = new String[numPartitioners + 1];

    LOGGER.info("Initialized mapper with {} record readers, output dir: {}, timeHandler: {}, partitioners: {}",
        _recordReaderFileConfigs.size(), _mapperOutputDir, _timeHandler.getClass(),
        Arrays.stream(_partitioners).map(p -> p.getClass().toString()).collect(Collectors.joining(",")));

    // Get the threshold for intermediate file size during map phase.
    long intermediateFileSizeThresholdPerTask = processorConfig.getSegmentConfig().getIntermediateFileSizeThreshold();

    // If the threshold is set, we distribute it across the number of concurrent tasks. We do this because we want to
    // limit the disk usage to the specified threshold across all the concurrent tasks. If we do not distribute it, all
    // the concurrent tasks can run into the mapper phase simultaneously and the total disk usage can be much higher
    // than the specified total threshold. We do not enforce the threshold per task if the threshold for mapper output
    // size is not set.
    if (intermediateFileSizeThresholdPerTask != Long.MAX_VALUE) {
      intermediateFileSizeThresholdPerTask =
          (intermediateFileSizeThresholdPerTask / processorConfig.getNumConcurrentTasksPerInstance());
    }

    // Initialize the adaptive writer with the distributed threshold.
    _adaptiveSizeBasedWriter = new AdaptiveSizeBasedWriter(intermediateFileSizeThresholdPerTask);
  }

  /**
   * Reads the input records and generates partitioned generic row files into the mapper output directory.
   * Records for each partition are put into a directory of the partition name within the mapper output directory.
   */
  public Map<String, GenericRowFileManager> map()
      throws Exception {
    try {
      return doMap();
    } catch (Exception e) {
      // Cleaning up resources created by the mapper.
      for (GenericRowFileManager fileManager : _partitionToFileManagerMap.values()) {
        fileManager.cleanUp();
      }
      throw e;
    }
  }

  private Map<String, GenericRowFileManager> doMap()
      throws Exception {
    Consumer<Object> observer = _processorConfig.getProgressObserver();
    int count = 1;
    int totalNumRecordReaders = _recordReaderFileConfigs.size();
    GenericRow reuse = new GenericRow();
    for (RecordReaderFileConfig recordReaderFileConfig : _recordReaderFileConfigs) {
      RecordReader recordReader = recordReaderFileConfig.getRecordReader();

      // Mapper can terminate midway of reading a file if the intermediate file size has crossed the configured
      // threshold. Map phase will continue in the next iteration right where we are leaving off in the current
      // iteration.
      boolean shouldMapperTerminate =
          !completeMapAndTransformRow(recordReader, reuse, observer, count, totalNumRecordReaders);

      // Terminate the map phase if intermediate file size has crossed the threshold.
      if (shouldMapperTerminate) {
        break;
      }
      recordReaderFileConfig.closeRecordReader();
      count++;
    }

    for (GenericRowFileManager fileManager : _partitionToFileManagerMap.values()) {
      fileManager.closeFileWriter();
    }
    return _partitionToFileManagerMap;
  }


//   Returns true if the map phase can continue, false if it should terminate based on the configured threshold for
//   intermediate file size during map phase.
  private boolean completeMapAndTransformRow(RecordReader recordReader, GenericRow reuse,
      Consumer<Object> observer, int count, int totalCount) throws Exception {
    observer.accept(String.format("Doing map phase on data from RecordReader (%d out of %d)", count, totalCount));
    while (recordReader.hasNext() && (_adaptiveSizeBasedWriter.canWrite())) {
      reuse = recordReader.next(reuse);

      // TODO: Add ComplexTypeTransformer here. Currently it is not idempotent so cannot add it

      if (reuse.getValue(GenericRow.MULTIPLE_RECORDS_KEY) != null) {
        //noinspection unchecked
        for (GenericRow row : (Collection<GenericRow>) reuse.getValue(GenericRow.MULTIPLE_RECORDS_KEY)) {
          GenericRow transformedRow = _recordTransformer.transform(row);
          if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow)) {
            writeRecord(transformedRow);
          }
        }
      } else {
        GenericRow transformedRow = _recordTransformer.transform(reuse);
        if (transformedRow != null && IngestionUtils.shouldIngestRow(transformedRow)) {
          writeRecord(transformedRow);
        }
      }
      reuse.clear();
    }
    if (recordReader.hasNext() && !_adaptiveSizeBasedWriter.canWrite()) {
      String logMessage = String.format(
          "Stopping record readers at index: %d out of %d passed to mapper as size limit reached, bytes written = %d,"
              + " bytes " + "limit = %d", count, totalCount, _adaptiveSizeBasedWriter.getNumBytesWritten(),
          _adaptiveSizeBasedWriter.getBytesLimit());
      observer.accept(logMessage);
      LOGGER.info(logMessage);
      return false;
    }
    return true;
  }

  private void writeRecord(GenericRow row)
      throws IOException {
    String timePartition = _timeHandler.handleTime(row);
    if (timePartition == null) {
      // Record not in the valid time range
      return;
    }
    _partitionsBuffer[0] = timePartition;

    // Partitioning
    int numPartitioners = _partitioners.length;
    for (int i = 0; i < numPartitioners; i++) {
      _partitionsBuffer[i + 1] = _partitioners[i].getPartition(row);
    }
    String partition = StringUtil.join("_", _partitionsBuffer);

    // Create writer for the partition if not exists
    GenericRowFileManager fileManager = _partitionToFileManagerMap.get(partition);
    if (fileManager == null) {
      File partitionOutputDir = new File(_mapperOutputDir, partition);
      FileUtils.forceMkdir(partitionOutputDir);
      fileManager = new GenericRowFileManager(partitionOutputDir, _fieldSpecs, _includeNullFields, _numSortFields);
      _partitionToFileManagerMap.put(partition, fileManager);
    }

    // Get the file writer.
    GenericRowFileWriter fileWriter = fileManager.getFileWriter();

    // Write the row.
    _adaptiveSizeBasedWriter.write(fileWriter, row);
  }
}
