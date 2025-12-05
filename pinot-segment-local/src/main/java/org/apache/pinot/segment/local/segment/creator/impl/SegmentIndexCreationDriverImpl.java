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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.realtime.converter.stats.RealtimeSegmentSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.ColumnarSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaValidatorFactory;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Implementation of an index segment creator.
 *
 */
// TODO: Check resource leaks
public class SegmentIndexCreationDriverImpl implements SegmentIndexCreationDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexCreationDriverImpl.class);

  private SegmentGeneratorConfig _config;
  @Nullable private RecordReader _recordReader;
  private SegmentPreIndexStatsContainer _segmentStats;
  // NOTE: Use TreeMap so that the columns are ordered alphabetically
  private TreeMap<String, ColumnIndexCreationInfo> _indexCreationInfoMap;
  private SegmentCreator _indexCreator;
  private SegmentIndexCreationInfo _segmentIndexCreationInfo;
  private SegmentCreationDataSource _dataSource;
  private Schema _dataSchema;
  @Nullable private TransformPipeline _transformPipeline;
  private IngestionSchemaValidator _ingestionSchemaValidator;
  private int _totalDocs = 0;
  private File _tempIndexDir;
  private String _segmentName;
  private File _outputSegmentDir;
  private long _totalRecordReadTimeNs = 0;
  private long _totalIndexTimeNs = 0;
  private long _totalStatsCollectorTimeNs = 0;
  private boolean _continueOnError;
  private int _incompleteRowsFound = 0;
  private int _skippedRowsFound = 0;
  private int _sanitizedRowsFound = 0;
  @Nullable private InstanceType _instanceType;

  @Override
  public void init(SegmentGeneratorConfig config)
      throws Exception {
    init(config, getRecordReader(config), null);
  }

  @Override
  public void init(SegmentGeneratorConfig config, @Nullable InstanceType instanceType)
      throws Exception {
    init(config, getRecordReader(config), instanceType);
  }

  private RecordReader getRecordReader(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    File dataFile = new File(segmentGeneratorConfig.getInputFilePath());
    Preconditions.checkState(dataFile.exists(), "Input file: " + dataFile.getAbsolutePath() + " does not exist");

    Schema schema = segmentGeneratorConfig.getSchema();
    TableConfig tableConfig = segmentGeneratorConfig.getTableConfig();
    FileFormat fileFormat = segmentGeneratorConfig.getFormat();
    String recordReaderClassName = segmentGeneratorConfig.getRecordReaderPath();
    Set<String> sourceFields =
        IngestionUtils.getFieldsForRecordExtractor(tableConfig, segmentGeneratorConfig.getSchema());

    // Allow for instantiation general record readers from a record reader path passed into segment generator config
    // If this is set, this will override the file format
    if (recordReaderClassName != null) {
      if (fileFormat != FileFormat.OTHER) {
        // NOTE: we currently have default file format set to AVRO inside segment generator config, do not want to break
        // this behavior for clients.
        LOGGER.warn("Using class: {} to read segment, ignoring configured file format: {}", recordReaderClassName,
            fileFormat);
      }
      return RecordReaderFactory.getRecordReaderByClass(recordReaderClassName, dataFile, sourceFields,
          segmentGeneratorConfig.getReaderConfig());
    }

    // NOTE: PinotSegmentRecordReader does not support time conversion (field spec must match)
    if (fileFormat == FileFormat.PINOT) {
      return new PinotSegmentRecordReader(dataFile, schema, segmentGeneratorConfig.getColumnSortOrder());
    } else {
      return RecordReaderFactory.getRecordReader(fileFormat, dataFile, sourceFields,
          segmentGeneratorConfig.getReaderConfig());
    }
  }

  public RecordReader getRecordReader() {
    return _recordReader;
  }

  public void init(SegmentGeneratorConfig config, RecordReader recordReader)
      throws Exception {
    init(config, new RecordReaderSegmentCreationDataSource(recordReader),
        new TransformPipeline(config.getTableConfig(), config.getSchema()), null);
  }

  public void init(SegmentGeneratorConfig config, RecordReader recordReader, @Nullable InstanceType instanceType)
      throws Exception {
    init(config, new RecordReaderSegmentCreationDataSource(recordReader),
        new TransformPipeline(config.getTableConfig(), config.getSchema()), instanceType);
  }

  /**
   * Initialize the driver for columnar segment building using a ColumnReaderFactory.
   * This method sets up the driver to use column-wise input data access instead of row-wise.
   *
   * @param config Segment generator configuration
   * @param columnReaderFactory Factory for creating column readers
   * @throws Exception if initialization fails
   */
  public void init(SegmentGeneratorConfig config, ColumnReaderFactory columnReaderFactory)
      throws Exception {
    // Initialize the column reader factory with target schema
    columnReaderFactory.init(config.getSchema());

    // Get all column readers for the target schema
    Map<String, ColumnReader> columnReaders = columnReaderFactory.getAllColumnReaders();

    // Create columnar data source
    ColumnarSegmentCreationDataSource columnarDataSource = new ColumnarSegmentCreationDataSource(columnReaders);

    // Use the existing init method with columnar data source and no transform pipeline
    init(config, columnarDataSource, null, null);

    LOGGER.info("Initialized SegmentIndexCreationDriverImpl for columnar data source building with {} columns",
        columnReaders.size());
  }

  public void init(SegmentGeneratorConfig config, SegmentCreationDataSource dataSource,
      TransformPipeline transformPipeline, @Nullable InstanceType instanceType)
      throws Exception {
    _config = config;
    _dataSchema = config.getSchema();
    _continueOnError = config.isContinueOnError();
    String readerClassName = null;
    Preconditions.checkState(instanceType == null || instanceType == InstanceType.SERVER
        || instanceType == InstanceType.MINION, "InstanceType passed must be for minion or server or null");
    _instanceType = instanceType;

    // Handle columnar data sources differently
    if (dataSource instanceof ColumnarSegmentCreationDataSource) {
      // For columnar data sources, we don't have a record reader
      _recordReader = null;
      _transformPipeline = null; // No transform pipeline for columnar mode
      _dataSource = dataSource;
    } else {
      // For record reader-based data sources
      _recordReader = dataSource.getRecordReader();

      if (config.isFailOnEmptySegment()) {
        Preconditions.checkState(_recordReader.hasNext(), "No record in data source");
      }
      _transformPipeline = transformPipeline;
      // Use the same transform pipeline if the data source is backed by a record reader
      if (dataSource instanceof RecordReaderSegmentCreationDataSource) {
        ((RecordReaderSegmentCreationDataSource) dataSource).setTransformPipeline(transformPipeline);
      }

      // Optimization for realtime segment conversion
      if (dataSource instanceof RealtimeSegmentSegmentCreationDataSource) {
        _config.setRealtimeConversion(true);
        _config.setConsumerDir(((RealtimeSegmentSegmentCreationDataSource) dataSource).getConsumerDir());
      }

      // For stats collection
      _dataSource = dataSource;

      // Initialize common components
      readerClassName = _recordReader.getClass().getName();
    }

    // Initialize index creation
    _segmentIndexCreationInfo = new SegmentIndexCreationInfo();
    _indexCreationInfoMap = new TreeMap<>();
    _indexCreator = new SegmentColumnarIndexCreator();

    // Ensure that the output directory exists
    final File indexDir = new File(config.getOutDir());
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }

    _ingestionSchemaValidator =
        SchemaValidatorFactory.getSchemaValidator(_dataSchema, readerClassName,
            config.getInputFilePath());

    // Create a temporary directory used in segment creation
    _tempIndexDir = new File(indexDir, "tmp-" + UUID.randomUUID());
    LOGGER.debug("tempIndexDir:{}", _tempIndexDir);
  }

  /**
   * Generate a mutable docId to immutable docId mapping from the sortedDocIds iteration order
   *
   * @param sortedDocIds used to map sortedDocIds[immutableId] = mutableId (based on RecordReader iteration order)
   * @return int[] used to map output[mutableId] = immutableId, or null if sortedDocIds is null
   */
  private int[] getImmutableToMutableIdMap(@Nullable int[] sortedDocIds) {
    if (sortedDocIds == null) {
      return null;
    }
    int[] res = new int[sortedDocIds.length];
    for (int i = 0; i < res.length; i++) {
      res[sortedDocIds[i]] = i;
    }
    return res;
  }

  /**
   * Get sorted document IDs from the record reader if it supports this functionality.
   * This method handles the fact that getSortedDocIds() was removed from the RecordReader interface
   * but is still available on specific implementations.
   *
   * @return sorted document IDs array, or null if not available
   */
  @Nullable
  private int[] getSortedDocIdsFromRecordReader() {
    if (_recordReader instanceof PinotSegmentRecordReader) {
      return ((PinotSegmentRecordReader) _recordReader).getSortedDocIds();
    } else if (_recordReader instanceof CompactedPinotSegmentRecordReader) {
      return ((CompactedPinotSegmentRecordReader) _recordReader).getSortedDocIds();
    }
    return null;
  }

  @Override
  public void build()
      throws Exception {
    // Check if we're using a columnar data source and switch to columnar mode
    if (_dataSource instanceof ColumnarSegmentCreationDataSource) {
      LOGGER.info("Detected columnar data source, using columnar building approach");
      buildColumnar();
      return;
    }

    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    collectStatsAndIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", _totalDocs);

    _incompleteRowsFound = 0;
    _skippedRowsFound = 0;
    _sanitizedRowsFound = 0;
    try {
      // TODO: Eventually pull the doc Id sorting logic out of Record Reader so that all row oriented logic can be
      //    removed from this code.
      int[] immutableToMutableIdMap = null;
      if (_recordReader instanceof PinotSegmentRecordReader) {
        immutableToMutableIdMap =
            getImmutableToMutableIdMap(((PinotSegmentRecordReader) _recordReader).getSortedDocIds());
      }

      // Initialize the index creation using the per-column statistics information
      // TODO: _indexCreationInfoMap holds the reference to all unique values on heap (ColumnIndexCreationInfo ->
      //       ColumnStatistics) throughout the segment creation. Find a way to release the memory early.
      _indexCreator.init(_config, _segmentIndexCreationInfo, _indexCreationInfoMap, _dataSchema, _tempIndexDir,
          immutableToMutableIdMap);

      // Build the index
      _recordReader.rewind();
      LOGGER.info("Start building IndexCreator!");
      GenericRow reuse = new GenericRow();
      while (_recordReader.hasNext()) {
        long recordReadStopTimeNs;
        reuse.clear();

        TransformPipeline.Result result;
        try {
          long recordReadStartTimeNs = System.nanoTime();
          GenericRow decodedRow = _recordReader.next(reuse);
          result = _transformPipeline.processRow(decodedRow);
          recordReadStopTimeNs = System.nanoTime();
          _totalRecordReadTimeNs += recordReadStopTimeNs - recordReadStartTimeNs;
        } catch (Exception e) {
          if (!_continueOnError) {
            throw new RuntimeException("Error occurred while reading row during indexing", e);
          } else {
            _incompleteRowsFound++;
            LOGGER.debug("Error occurred while reading row during indexing", e);
            continue;
          }
        }

        for (GenericRow row : result.getTransformedRows()) {
          _indexCreator.indexRow(row);
        }
        _totalIndexTimeNs += System.nanoTime() - recordReadStopTimeNs;
        _incompleteRowsFound += result.getIncompleteRowCount();
        _skippedRowsFound += result.getSkippedRowCount();
        _sanitizedRowsFound += result.getSanitizedRowCount();
      }
    } catch (Exception e) {
      _indexCreator.close();
      throw e;
    } finally {
      _recordReader.close();
    }

    if (_incompleteRowsFound > 0) {
      LOGGER.warn("Incomplete data found for {} records. This can be due to error during reader or transformations",
          _incompleteRowsFound);
    }
    if (_skippedRowsFound > 0) {
      LOGGER.info("Skipped {} records during transformation", _skippedRowsFound);
    }
    if (_sanitizedRowsFound > 0) {
      LOGGER.info("Sanitized {} records during transformation", _sanitizedRowsFound);
    }

    updateMetrics(_config.getTableConfig().getTableName());

    LOGGER.info("Finished records indexing in IndexCreator!");

    handlePostCreation();
  }

  private void updateMetrics(String tableNameWithType) {
    if (_instanceType == null) {
      return;
    }

    // Use appropriate metrics based on instance type
    if (_instanceType == InstanceType.MINION) {
      MinionMetrics metrics = MinionMetrics.get();
      if (_incompleteRowsFound > 0) {
        metrics.addMeteredTableValue(tableNameWithType, MinionMeter.TRANSFORMATION_ERROR_COUNT, _incompleteRowsFound);
      }
      if (_skippedRowsFound > 0) {
        metrics.addMeteredTableValue(tableNameWithType, MinionMeter.DROPPED_RECORD_COUNT, _skippedRowsFound);
      }
      if (_sanitizedRowsFound > 0) {
        metrics.addMeteredTableValue(tableNameWithType, MinionMeter.CORRUPTED_RECORD_COUNT, _sanitizedRowsFound);
      }
    } else if (_instanceType == InstanceType.SERVER) {
      ServerMetrics metrics = ServerMetrics.get();
      if (_incompleteRowsFound > 0) {
        metrics.addMeteredTableValue(tableNameWithType, ServerMeter.TRANSFORMATION_ERROR_COUNT, _incompleteRowsFound);
      }
      if (_skippedRowsFound > 0) {
        metrics.addMeteredTableValue(tableNameWithType, ServerMeter.DROPPED_RECORD_COUNT, _skippedRowsFound);
      }
      if (_sanitizedRowsFound > 0) {
        metrics.addMeteredTableValue(tableNameWithType, ServerMeter.CORRUPTED_RECORD_COUNT, _sanitizedRowsFound);
      }
    }
  }

  public void buildByColumn(IndexSegment indexSegment, ThreadSafeMutableRoaringBitmap validDocIds)
      throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    collectStatsAndIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", _totalDocs);

    try {
      // TODO: Eventually pull the doc Id sorting logic out of Record Reader so that all row oriented logic can be
      //    removed from this code.
      int[] sortedDocIds = getSortedDocIdsFromRecordReader();
      int[] immutableToMutableIdMap = getImmutableToMutableIdMap(sortedDocIds);

      // Initialize the index creation using the per-column statistics information
      // TODO: _indexCreationInfoMap holds the reference to all unique values on heap (ColumnIndexCreationInfo ->
      //       ColumnStatistics) throughout the segment creation. Find a way to release the memory early.
      _indexCreator.init(_config, _segmentIndexCreationInfo, _indexCreationInfoMap, _dataSchema, _tempIndexDir,
          immutableToMutableIdMap);

      // Build the indexes
      LOGGER.info("Start building Index by column");

      TreeSet<String> columns = _dataSchema.getPhysicalColumnNames();

      for (String col : columns) {
        _indexCreator.indexColumn(col, sortedDocIds, indexSegment, validDocIds);
      }
    } catch (Exception e) {
      _indexCreator.close();
      throw e;
    } finally {
      // The record reader is created by the `init` method and needs to be closed and
      // cleaned up even by the Column Mode builder.
      _recordReader.close();
    }

    // TODO: Using column oriented, we can't catch incomplete records.  Does that matter?

    LOGGER.info("Finished records indexing by column in IndexCreator!");

    handlePostCreation();
  }

  private void handlePostCreation()
      throws Exception {
    // Execute all post-creation operations directly on the index creator
    _outputSegmentDir = _indexCreator.createSegment(_instanceType);
    _segmentName = _indexCreator.getSegmentName();

    LOGGER.info("Driver, record read time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalRecordReadTimeNs));
    LOGGER.info("Driver, stats collector time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalStatsCollectorTimeNs));
    LOGGER.info("Driver, indexing time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalIndexTimeNs));
  }

  @Override
  public ColumnStatistics getColumnStatisticsCollector(final String columnName)
      throws Exception {
    return _segmentStats.getColumnProfileFor(columnName);
  }

  /**
   * Complete the stats gathering process and store the stats information in indexCreationInfoMap.
   */
  void collectStatsAndIndexCreationInfo()
      throws Exception {
    long statsCollectorStartTime = System.nanoTime();

    // Initialize stats collection
    _segmentStats = _dataSource.gatherStats(
        new StatsCollectorConfig(_config.getTableConfig(), _dataSchema, _config.getSegmentPartitionConfig()));
    _totalDocs = _segmentStats.getTotalDocCount();
    Map<String, FieldIndexConfigs> indexConfigsMap = _config.getIndexConfigsByColName();

    for (FieldSpec fieldSpec : _dataSchema.getAllFieldSpecs()) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String column = fieldSpec.getName();
      DataType storedType = fieldSpec.getDataType().getStoredType();
      ColumnStatistics columnProfile = _segmentStats.getColumnProfileFor(column);
      DictionaryIndexConfig dictionaryIndexConfig = indexConfigsMap.get(column).getConfig(StandardIndexes.dictionary());
      boolean createDictionary = dictionaryIndexConfig.isDisabled();
      boolean useVarLengthDictionary = dictionaryIndexConfig.getUseVarLengthDictionary()
          || DictionaryIndexType.optimizeTypeShouldUseVarLengthDictionary(storedType, columnProfile);
      Object defaultNullValue = fieldSpec.getDefaultNullValue();
      if (storedType == DataType.BYTES) {
        defaultNullValue = new ByteArray((byte[]) defaultNullValue);
      }
      _indexCreationInfoMap.put(column,
          new ColumnIndexCreationInfo(columnProfile, createDictionary, useVarLengthDictionary, false/*isAutoGenerated*/,
              defaultNullValue));
    }
    _segmentIndexCreationInfo.setTotalDocs(_totalDocs);
    _totalStatsCollectorTimeNs = System.nanoTime() - statsCollectorStartTime;
  }

  /**
   * Uses config and column properties like storedType and length of elements to determine if
   * varLengthDictionary should be used for a column
   * @deprecated Use
   * {@link DictionaryIndexType#shouldUseVarLengthDictionary(String, Set, DataType, ColumnStatistics)} instead.
   */
  @Deprecated
  public static boolean shouldUseVarLengthDictionary(String columnName, Set<String> varLengthDictColumns,
      DataType columnStoredType, ColumnStatistics columnProfile) {
    return DictionaryIndexType.shouldUseVarLengthDictionary(columnName, varLengthDictColumns, columnStoredType,
        columnProfile);
  }

  /**
   * Returns the name of the segment associated with this index creation driver.
   */
  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Returns the path of the output directory
   */
  @Override
  public File getOutputDirectory() {
    return _outputSegmentDir;
  }

  /**
   * Returns the schema validator.
   */
  @Override
  public IngestionSchemaValidator getIngestionSchemaValidator() {
    return _ingestionSchemaValidator;
  }

  public SegmentPreIndexStatsContainer getSegmentStats() {
    return _segmentStats;
  }

  public int getIncompleteRowsFound() {
    return _incompleteRowsFound;
  }

  public int getSkippedRowsFound() {
    return _skippedRowsFound;
  }

  public int getSanitizedRowsFound() {
    return _sanitizedRowsFound;
  }

  /**
   * Build segment using columnar approach.
   * This method builds the segment by processing data column-wise instead of row-wise.
   * Following is not supported:
   * <li> recort transformation
   * <li> sorted column change wrt to input data
   *
   * <p>Initialize the driver using {@link #init(SegmentGeneratorConfig, ColumnReaderFactory)}
   *
   * @throws Exception if segment building fails
   */
  private void buildColumnar()
      throws Exception {
    if (!(_dataSource instanceof ColumnarSegmentCreationDataSource)) {
      throw new IllegalStateException("buildColumnar() can only be called after initColumnar()");
    }

    ColumnarSegmentCreationDataSource columnarDataSource = (ColumnarSegmentCreationDataSource) _dataSource;

    try {
      Map<String, ColumnReader> columnReaders = columnarDataSource.getColumnReaders();

      LOGGER.info("Starting columnar segment building for {} columns", columnReaders.size());

      // Reuse existing stats collection and index creation info logic
      LOGGER.debug("Start building StatsCollector!");
      collectStatsAndIndexCreationInfo();
      LOGGER.info("Finished building StatsCollector!");
      LOGGER.info("Collected stats for {} documents", _totalDocs);

      if (_totalDocs == 0) {
        LOGGER.warn("No documents found in data source");
        handlePostCreation();
        return;
      }

      // Initialize the index creation using the per-column statistics information
      _indexCreator.init(_config, _segmentIndexCreationInfo, _indexCreationInfoMap, _dataSchema, _tempIndexDir, null);

      // Build the indexes column-wise (true column-major approach)
      LOGGER.info("Start building Index using columnar approach");
      long indexStartTime = System.nanoTime();

      TreeSet<String> columns = _dataSchema.getPhysicalColumnNames();
      for (String columnName : columns) {
        LOGGER.debug("Indexing column: {}", columnName);
        ColumnReader columnReader = columnReaders.get(columnName);
        if (columnReader == null) {
          throw new IllegalStateException("No column reader found for column: " + columnName);
        }

        // Index each column independently using true column-major approach
        // This is similar to how buildByColumn works but uses ColumnReader instead of IndexSegment
        _indexCreator.indexColumn(columnName, columnReader);
      }

      _totalIndexTimeNs = System.nanoTime() - indexStartTime;

      LOGGER.info("Finished indexing using columnar approach in IndexCreator!");
      handlePostCreation();
    } catch (Exception e) {
      try {
        _indexCreator.close();
      } catch (Exception closeException) {
        LOGGER.error("Error closing index creator", closeException);
        // Add the close exception as suppressed to preserve both exceptions
        e.addSuppressed(closeException);
      }
      throw e;
    } finally {
      // Always close the columnar data source to prevent resource leaks
      try {
        columnarDataSource.close();
      } catch (Exception closeException) {
        LOGGER.error("Error closing columnar data source", closeException);
      }
    }
  }
}
