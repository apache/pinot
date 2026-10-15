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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.segment.local.recordtransformer.DataTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.ExpressionTransformer;
import org.apache.pinot.segment.local.recordtransformer.NullValueTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerUtils;
import org.apache.pinot.segment.local.recordtransformer.SanitizationTransformer;
import org.apache.pinot.segment.local.recordtransformer.SpecialValueTransformer;
import org.apache.pinot.segment.local.recordtransformer.TimeValidationTransformer;
import org.apache.pinot.segment.local.segment.creator.ColumnarSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.EmptyColumnStatistics;
import org.apache.pinot.segment.local.segment.creator.impl.stats.NoDictColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StatsCollectorUtil;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.utils.ClusterConfigForTable;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
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
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/// Implementation of an index segment creator.
// TODO: Check resource leaks
public class SegmentIndexCreationDriverImpl implements SegmentIndexCreationDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexCreationDriverImpl.class);

  private SegmentGeneratorConfig _config;
  private Schema _schema;
  private boolean _continueOnError;
  @Nullable
  private InstanceType _instanceType;

  // For row major segment creation
  @Nullable
  private RecordReader _recordReader;
  @Nullable
  private TransformPipeline _transformPipeline;
  @Nullable
  private TransformPipeline _statsTransformPipeline;
  @Nullable
  private Schema _statsSchema;
  private final Map<String, DeferredRawColumnStatistics> _deferredRawStats = new TreeMap<>();
  @Nullable
  private Map<String, NormalizedColumnReader> _normalizedColumnReaders;
  @Nullable
  private ColumnReaderFactory _ownedColumnReaderFactory;
  private final BitSet _columnarSanitizedDocIds = new BitSet();

  private SegmentPreIndexStatsContainer _segmentStats;
  // NOTE: Use TreeMap so that the columns are ordered alphabetically
  private TreeMap<String, ColumnStatistics> _columnStatisticsMap;
  private SegmentCreator _indexCreator;
  private SegmentCreationDataSource _dataSource;
  private IngestionSchemaValidator _ingestionSchemaValidator;
  private int _totalDocs = 0;
  private File _tempIndexDir;
  private String _segmentName;
  private long _totalRecordReadTimeNs = 0;
  private long _totalIndexTimeNs = 0;
  private long _totalStatsCollectorTimeNs = 0;
  private int _incompleteRowsFound = 0;
  private int _skippedRowsFound = 0;
  private int _sanitizedRowsFound = 0;

  @Override
  public void init(SegmentGeneratorConfig config)
      throws Exception {
    init(config, getRecordReader(config));
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
    List<RecordTransformer> transformers =
        RecordTransformerUtils.getDefaultTransformers(config.getTableConfig(), config.getSchema());
    TransformPipeline pipeline = new TransformPipeline(config.getTableConfig().getTableName(), transformers);
    if (canUseNormalizedColumns(config, transformers)) {
      ColumnReaderFactory factory;
      try {
        factory = recordReader.tryCreateColumnReaderFactory(config.getSchema());
      } catch (Exception | Error e) {
        try {
          recordReader.close();
        } catch (Exception closeError) {
          e.addSuppressed(closeError);
        }
        throw e;
      }
      if (factory != null) {
        initNormalizedColumns(config, recordReader, factory);
        return;
      }
    }
    init(config, new RecordReaderSegmentCreationDataSource(recordReader), pipeline);
    prepareDeferredRawStats(transformers);
  }

  /// Select only pipelines whose required physical values already win over identifier aliases. Constructing the
  /// normal row transformers before this check still validates expression syntax, dependencies, and cycles.
  private static boolean canUseNormalizedColumns(SegmentGeneratorConfig config, List<RecordTransformer> transformers) {
    TableConfig table = config.getTableConfig();
    Schema schema = config.getSchema();
    IndexingConfig indexing = table.getIndexingConfig();
    if (table.getTableType() != TableType.OFFLINE || !indexing.isColumnMajorSegmentBuilderEnabled()
        || config.isRealtimeConversion() || config.isContinueOnError() || table.getUpsertConfig() != null
        || table.getDedupConfig() != null || schema.isEnableColumnBasedNullHandling()
        || config.isDefaultNullHandlingEnabled() || !isEmpty(config.getColumnSortOrder())
        || config.getSegmentPartitionConfig() != null || config.isOptimizeDictionary()
        || config.isOptimizeDictionaryForMetrics() || config.isOptimizeDictionaryType()
        || !isEmpty(config.getStarTreeIndexConfigs()) || config.isEnableDefaultStarTree()
        || config.getMultiColumnTextIndexConfig() != null || indexing.isAggregateMetrics()
        || schema.getPhysicalColumnNames().isEmpty()) {
      return false;
    }
    IngestionConfig ingestion = table.getIngestionConfig();
    if (ingestion != null && (ingestion.isContinueOnError() || ingestion.isRowTimeValueCheck()
        || ingestion.getFilterConfig() != null || ingestion.getComplexTypeConfig() != null
        || ingestion.getSchemaConformingTransformerConfig() != null || !isEmpty(ingestion.getSourceFieldConfigs())
        || !isEmpty(ingestion.getEnrichmentConfigs()) || !isEmpty(ingestion.getAggregationConfigs()))) {
      return false;
    }
    for (RecordTransformer transformer : transformers) {
      if (!(transformer instanceof ExpressionTransformer || transformer instanceof DataTypeTransformer
          || transformer instanceof TimeValidationTransformer || transformer instanceof SpecialValueTransformer
          || transformer instanceof NullValueTransformer || transformer instanceof SanitizationTransformer)) {
        return false;
      }
    }
    if (ingestion != null && ingestion.getTransformConfigs() != null) {
      for (TransformConfig transform : ingestion.getTransformConfigs()) {
        if (!isPhysicalAlias(schema, transform.getColumnName(), transform.getTransformFunction())) {
          return false;
        }
      }
    }
    Set<String> supportedIndexes = Set.of(StandardIndexes.FORWARD_ID, StandardIndexes.DICTIONARY_ID,
        StandardIndexes.INVERTED_ID, StandardIndexes.RANGE_ID, StandardIndexes.TEXT_ID,
        StandardIndexes.NULL_VALUE_VECTOR_ID, StandardIndexes.OPEN_STRUCT_ID);
    Set<DataType> supportedTypes = Set.of(DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
        DataType.BIG_DECIMAL, DataType.BOOLEAN, DataType.TIMESTAMP, DataType.STRING, DataType.JSON, DataType.BYTES,
        DataType.UUID);
    for (FieldSpec field : schema.getAllFieldSpecs()) {
      if (field.isVirtualColumn()) {
        continue;
      }
      // Legacy schema transforms must still be checked for compatibility.
      @SuppressWarnings("deprecation")
      String transformFunction = field.getTransformFunction();
      if (!field.isSingleValueField() || !supportedTypes.contains(field.getDataType())
          || transformFunction != null && !isPhysicalAlias(schema, field.getName(), transformFunction)
          || transformFunction == null && FunctionEvaluatorFactory.getExpressionEvaluator(field) != null) {
        return false;
      }
      FieldIndexConfigs indexes = config.getIndexConfigsByColName().get(field.getName());
      for (IndexType<?, ?, ?> index : IndexService.getInstance().getAllIndexes()) {
        if (!supportedIndexes.contains(index.getId()) && indexes.getConfig(index).isEnabled()) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean isEmpty(@Nullable Collection<?> values) {
    return values == null || values.isEmpty();
  }

  private static boolean isPhysicalAlias(Schema schema, String target, String expression) {
    FieldSpec field = schema.getFieldSpecFor(target);
    String source = expression.trim();
    // JSON's raw value may be a Map, which ExpressionTransformer can overwrite through its nested-value branch.
    return field != null && !field.isVirtualColumn() && field.getDataType() != DataType.JSON
        && source.matches("[A-Za-z_$][A-Za-z0-9_$]*")
        && FunctionEvaluatorFactory.getExpressionEvaluator(expression).getArguments().equals(List.of(source));
  }

  private void initNormalizedColumns(SegmentGeneratorConfig config, RecordReader rowReader, ColumnReaderFactory factory)
      throws Exception {
    boolean rowCloseAttempted = false;
    try {
      factory.init(config.getSchema());
      Map<String, ColumnReader> rawReaders = factory.getAllColumnReaders();
      Preconditions.checkState(rawReaders.keySet().equals(config.getSchema().getPhysicalColumnNames()),
          "Column factory must provide every physical target column exactly once");
      int totalDocs = -1;
      for (Map.Entry<String, ColumnReader> entry : rawReaders.entrySet()) {
        ColumnReader reader = entry.getValue();
        Preconditions.checkState(reader != null && entry.getKey().equals(reader.getColumnName()),
            "Missing or mismatched column reader: %s", entry.getKey());
        if (totalDocs < 0) {
          totalDocs = reader.getTotalDocs();
        }
        Preconditions.checkState(totalDocs >= 0 && reader.getTotalDocs() == totalDocs,
            "Invalid or inconsistent document count for column: %s", entry.getKey());
      }
      Preconditions.checkState(!config.isFailOnEmptySegment() || totalDocs > 0, "No record in data source");
      Map<String, NormalizedColumnReader> normalized = new TreeMap<>();
      Map<String, ColumnReader> readers = new TreeMap<>();
      for (Map.Entry<String, ColumnReader> entry : rawReaders.entrySet()) {
        NormalizedColumnReader reader = new NormalizedColumnReader(entry.getValue(), config.getTableConfig(),
            config.getSchema().getFieldSpecFor(entry.getKey()), config.getSchema(), _columnarSanitizedDocIds);
        normalized.put(entry.getKey(), reader);
        readers.put(entry.getKey(), reader);
      }
      init(config, new ColumnarSegmentCreationDataSource(readers), null);
      // Preserve the identity used by row ingestion validation, including custom reader subclasses.
      _ingestionSchemaValidator = SchemaValidatorFactory.getSchemaValidator(_schema, rowReader.getClass().getName(),
          config.getInputFilePath());
      _normalizedColumnReaders = normalized;
      _totalDocs = totalDocs;
      Set<String> deferred = findDeferredRawColumns();
      if (!deferred.isEmpty()) {
        _statsSchema = _schema.withoutVirtualColumns();
        deferred.forEach(_statsSchema::removeField);
      }
      rowCloseAttempted = true;
      rowReader.close();
      _ownedColumnReaderFactory = factory;
      LOGGER.info("Selected normalized column-major ingestion for {} physical columns", normalized.size());
    } catch (Exception | Error e) {
      try {
        factory.close();
      } catch (Exception closeError) {
        e.addSuppressed(closeError);
      }
      if (!rowCloseAttempted) {
        try {
          rowReader.close();
        } catch (Exception closeError) {
          e.addSuppressed(closeError);
        }
      }
      throw e;
    }
  }

  /// Initialize the driver for columnar segment building using a ColumnReaderFactory.
  /// This method sets up the driver to use column-wise input data access instead of row-wise.
  ///
  /// The column-major build this driver currently performs is two-pass — a statistics pass (which
  /// seals the dictionary) followed by an index pass — so each column's [ColumnReader] is read
  /// sequentially, `rewind()`ed, and read again. A factory supplied here must therefore return
  /// readers that support repeated `rewind()` and a full re-read; how it meets that (buffering all
  /// values, or re-reading a seekable/batch-bounded backing store) is the factory's own choice and sets
  /// the build's peak memory — this driver does not constrain it.
  ///
  /// @param config Segment generator configuration
  /// @param columnReaderFactory Factory for creating column readers
  /// @throws Exception if initialization fails
  public void init(SegmentGeneratorConfig config, ColumnReaderFactory columnReaderFactory)
      throws Exception {
    // Initialize the column reader factory with target schema
    columnReaderFactory.init(config.getSchema());

    // Get all column readers for the target schema
    Map<String, ColumnReader> columnReaders = columnReaderFactory.getAllColumnReaders();

    // Create columnar data source
    ColumnarSegmentCreationDataSource columnarDataSource = new ColumnarSegmentCreationDataSource(columnReaders);

    // Use the existing init method with columnar data source and no transform pipeline
    init(config, columnarDataSource, null);

    LOGGER.info("Initialized SegmentIndexCreationDriverImpl for columnar data source building with {} columns",
        columnReaders.size());
  }

  public void init(SegmentGeneratorConfig config, SegmentCreationDataSource dataSource,
      @Nullable TransformPipeline transformPipeline)
      throws Exception {
    _config = config;
    _schema = config.getSchema();
    _continueOnError = config.isContinueOnError();
    _instanceType = config.getInstanceType();
    _statsSchema = null;
    _statsTransformPipeline = null;
    _deferredRawStats.clear();
    _normalizedColumnReaders = null;
    _ownedColumnReaderFactory = null;
    _columnarSanitizedDocIds.clear();

    // Handle columnar data sources differently
    String readerClassName = null;
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

      // For stats collection
      _dataSource = dataSource;

      // Initialize common components
      readerClassName = _recordReader.getClass().getName();
    }

    // Initialize index creation
    _columnStatisticsMap = new TreeMap<>();
    _indexCreator = new SegmentColumnarIndexCreator();

    // Ensure that the output directory exists
    final File indexDir = new File(config.getOutDir());
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }

    _ingestionSchemaValidator =
        SchemaValidatorFactory.getSchemaValidator(_schema, readerClassName, config.getInputFilePath());

    // Create a temporary directory used in segment creation
    _tempIndexDir = new File(indexDir, "tmp-" + UUID.randomUUID());
    LOGGER.debug("tempIndexDir:{}", _tempIndexDir);
  }

  /// Get sorted document IDs from the record reader if it supports this functionality.
  /// This method handles the fact that getSortedDocIds() was removed from the RecordReader interface
  /// but is still available on specific implementations.
  ///
  /// @return sorted document IDs array, or null if not available
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
    boolean readerRewound = collectStatsAndIndexCreationInfo(true);
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", _totalDocs);

    _incompleteRowsFound = 0;
    _skippedRowsFound = 0;
    _sanitizedRowsFound = 0;
    try {
      // Initialize the index creation using the per-column statistics information
      // TODO: _columnStatisticsMap holds the reference to all unique values on heap (via ColumnStatistics)
      //       throughout the segment creation. Find a way to release the memory early.
      _indexCreator.init(_config, _totalDocs, _columnStatisticsMap, _tempIndexDir);

      // Build the index
      if (!readerRewound) {
        _recordReader.rewind();
      }
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
          for (Map.Entry<String, DeferredRawColumnStatistics> entry : _deferredRawStats.entrySet()) {
            entry.getValue().collect(row.getValue(entry.getKey()));
          }
          _indexCreator.indexRow(row);
        }
        _totalIndexTimeNs += System.nanoTime() - recordReadStopTimeNs;
        _incompleteRowsFound += result.getIncompleteRowCount();
        _skippedRowsFound += result.getSkippedRowCount();
        _sanitizedRowsFound += result.getSanitizedRowCount();
      }
      for (DeferredRawColumnStatistics stats : _deferredRawStats.values()) {
        stats.finish();
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

  public void buildByColumn(IndexSegment indexSegment, RoaringBitmap validDocIds)
      throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    collectStatsAndIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", _totalDocs);

    try {
      // Initialize the index creation using the per-column statistics information
      // TODO: _columnStatisticsMap holds the reference to all unique values on heap (via ColumnStatistics)
      //       throughout the segment creation. Find a way to release the memory early.
      _indexCreator.init(_config, _totalDocs, _columnStatisticsMap, _tempIndexDir);

      // Build the indexes
      LOGGER.info("Start building Index by column");

      int[] sortedDocIds = getSortedDocIdsFromRecordReader();
      TreeSet<String> columns = _schema.getPhysicalColumnNames();
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
    try {
      _indexCreator.seal();
    } finally {
      _indexCreator.close();
    }
    _segmentName = _indexCreator.getSegmentName();

    // Move the segment from the temporary directory to the final output directory
    File outputDir = new File(_config.getOutDir());
    File segmentOutputDir = new File(outputDir, _segmentName);
    if (segmentOutputDir.exists()) {
      FileUtils.deleteDirectory(segmentOutputDir);
    }
    FileUtils.moveDirectory(_tempIndexDir, segmentOutputDir);
    FileUtils.deleteQuietly(_tempIndexDir);

    LOGGER.info("Driver, record read time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalRecordReadTimeNs));
    LOGGER.info("Driver, stats collector time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalStatsCollectorTimeNs));
    LOGGER.info("Driver, indexing time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalIndexTimeNs));
  }

  @Override
  public ColumnStatistics getColumnStatisticsCollector(final String columnName)
      throws Exception {
    return _segmentStats.getColumnProfileFor(columnName);
  }

  /// Complete the stats gathering process and store the stats information in indexCreationInfoMap.
  void collectStatsAndIndexCreationInfo()
      throws Exception {
    collectStatsAndIndexCreationInfo(false);
  }

  private boolean collectStatsAndIndexCreationInfo(boolean deferRawStats)
      throws Exception {
    long statsCollectorStartTime = System.nanoTime();

    // Initialize stats collection
    _deferredRawStats.clear();
    boolean deferred = deferRawStats && _statsSchema != null;
    Schema statsSchema = deferred ? _statsSchema : _schema;
    RecordReaderSegmentCreationDataSource rowDataSource = deferred
        ? (RecordReaderSegmentCreationDataSource) _dataSource : null;
    boolean projected = false;
    try {
      if (deferred) {
        rowDataSource.setTransformPipeline(_statsTransformPipeline);
        Set<String> statsInputs = new HashSet<>(_statsTransformPipeline.getInputColumns());
        statsInputs.addAll(statsSchema.getPhysicalColumnNames());
        if (!statsInputs.isEmpty()) {
          // Restore the original fields even if a reader fails part-way through changing its projection.
          projected = true;
          projected = _recordReader.rewind(statsInputs);
        }
      }
      _segmentStats = _dataSource.gatherStats(
          new StatsCollectorConfig(_config.getTableConfig(), statsSchema, _config.getSegmentPartitionConfig()));
    } finally {
      if (deferred) {
        rowDataSource.setTransformPipeline(_transformPipeline);
        if (projected) {
          _recordReader.rewind();
        }
      }
    }
    _totalDocs = _segmentStats.getTotalDocCount();

    // Empty segments use the normal empty-column profiles, not an unsealed deferred collector.
    StatsCollectorConfig fullStatsConfig =
        new StatsCollectorConfig(_config.getTableConfig(), _schema, _config.getSegmentPartitionConfig());
    SegmentPreIndexStatsContainer collectedStats = _segmentStats;

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      // Ignore virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String column = fieldSpec.getName();
      ColumnStatistics columnProfile;
      if (deferred && statsSchema.getFieldSpecFor(column) == null) {
        if (_totalDocs == 0) {
          columnProfile = new EmptyColumnStatistics(fieldSpec, null, null);
        } else {
          DeferredRawColumnStatistics rawStats = new DeferredRawColumnStatistics(column, fullStatsConfig, _totalDocs);
          _deferredRawStats.put(column, rawStats);
          columnProfile = rawStats;
        }
      } else {
        columnProfile = collectedStats.getColumnProfileFor(column);
      }
      _columnStatisticsMap.put(column, columnProfile);
    }
    if (deferred) {
      _segmentStats = new SegmentPreIndexStatsContainer() {
        @Override
        public ColumnStatistics getColumnProfileFor(String column) {
          return _columnStatisticsMap.get(column);
        }

        @Override
        public int getTotalDocCount() {
          return _totalDocs;
        }
      };
      LOGGER.info("Deferring statistics and value transformations for {} raw columns to the index pass: {}",
          _deferredRawStats.size(), _deferredRawStats.keySet());
    }
    _totalStatsCollectorTimeNs = System.nanoTime() - statsCollectorStartTime;
    return projected;
  }

  /// For explicit raw STRING/JSON columns, fixed-chunk V4/V6 forward and text writers need no value distribution
  /// before writing. Requiring the existing fixed-chunk policy also preserves the forward-index chunk layout.
  /// Keep their expensive JSON conversion and statistics on the index pass, while dictionary columns retain the
  /// normal two-pass path. Reject pipelines whose row counts or retained values might depend on deferred columns.
  private Set<String> findDeferredRawColumns() {
    TableConfig tableConfig = _config.getTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (tableConfig.getTableType() != TableType.OFFLINE || _config.isRealtimeConversion()
        || !ClusterConfigForTable.useOptimizedNoDictCollector(tableConfig) || _continueOnError
        || indexingConfig.isAggregateMetrics() || indexingConfig.isEnableDefaultStarTree()
        || indexingConfig.getStarTreeIndexConfigs() != null && !indexingConfig.getStarTreeIndexConfigs().isEmpty()
        || _config.getColumnSortOrder() != null && !_config.getColumnSortOrder().isEmpty()) {
      return Set.of();
    }
    if (ingestionConfig != null && (ingestionConfig.getFilterConfig() != null
        || ingestionConfig.getComplexTypeConfig() != null
        || ingestionConfig.getSchemaConformingTransformerConfig() != null
        || ingestionConfig.getSourceFieldConfigs() != null && !ingestionConfig.getSourceFieldConfigs().isEmpty()
        || ingestionConfig.getEnrichmentConfigs() != null && !ingestionConfig.getEnrichmentConfigs().isEmpty()
        || ingestionConfig.getAggregationConfigs() != null && !ingestionConfig.getAggregationConfigs().isEmpty()
        || ingestionConfig.isRowTimeValueCheck())) {
      return Set.of();
    }

    Set<String> deferredColumns = new HashSet<>();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }
      if (!fieldSpec.isSingleValueField() || fieldSpec.getDataType() == DataType.MAP
          || fieldSpec.getDataType() == DataType.OPEN_STRUCT) {
        return Set.of();
      }
      if (fieldSpec.getFieldType() != FieldSpec.FieldType.DIMENSION
          || fieldSpec.getDataType() != DataType.STRING && fieldSpec.getDataType() != DataType.JSON) {
        continue;
      }
      String column = fieldSpec.getName();
      FieldIndexConfigs indexes = _config.getIndexConfigsByColName().get(column);
      ForwardIndexConfig forward = indexes.getConfig(StandardIndexes.forward());
      if (indexes.getConfig(StandardIndexes.dictionary()).isEnabled() || !forward.isEnabled()
          || forward.getEncodingType() != FieldConfig.EncodingType.RAW || forward.getCodecSpec() != null
          || forward.getTargetDocsPerChunk() >= 0
          || forward.getRawIndexWriterVersion() != 4 && forward.getRawIndexWriterVersion() != 6
          || forward.getCompressionCodec() == FieldConfig.CompressionCodec.CLP
          || forward.getCompressionCodec() == FieldConfig.CompressionCodec.CLPV2
          || forward.getCompressionCodec() == FieldConfig.CompressionCodec.CLPV2_ZSTD
          || forward.getCompressionCodec() == FieldConfig.CompressionCodec.CLPV2_LZ4
          || _config.getSegmentPartitionConfig() != null
          && _config.getSegmentPartitionConfig().getColumnPartitionConfig(column) != null) {
        continue;
      }
      boolean supported = true;
      for (IndexType<?, ?, ?> index : IndexService.getInstance().getAllIndexes()) {
        String id = index.getId();
        // The standard OPEN_STRUCT config defaults to enabled, but never creates an index for these scalar types.
        if (!id.equals(StandardIndexes.FORWARD_ID) && !id.equals(StandardIndexes.TEXT_ID)
            && !id.equals(StandardIndexes.NULL_VALUE_VECTOR_ID) && !id.equals(StandardIndexes.OPEN_STRUCT_ID)
            && indexes.getConfig(index).isEnabled()) {
          supported = false;
          break;
        }
      }
      if (supported) {
        deferredColumns.add(column);
      }
    }
    if (deferredColumns.isEmpty()) {
      return Set.of();
    }
    return deferredColumns;
  }

  private void prepareDeferredRawStats(List<RecordTransformer> transformers) {
    Set<String> deferredColumns = findDeferredRawColumns();
    if (deferredColumns.isEmpty()) {
      return;
    }
    TableConfig tableConfig = _config.getTableConfig();
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig != null && ingestionConfig.getTransformConfigs() != null) {
      for (TransformConfig transform : ingestionConfig.getTransformConfigs()) {
        if (!isIndependentAlias(transform.getColumnName(), transform.getTransformFunction(), deferredColumns)) {
          return;
        }
      }
    }
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      // Legacy schema transforms must still be checked for compatibility.
      @SuppressWarnings("deprecation")
      String transformFunction = fieldSpec.getTransformFunction();
      if (transformFunction != null
          && !isIndependentAlias(fieldSpec.getName(), transformFunction, deferredColumns)) {
        return;
      }
    }

    _statsSchema = _schema.withoutVirtualColumns();
    deferredColumns.forEach(_statsSchema::removeField);
    List<RecordTransformer> statsTransformers = new ArrayList<>(transformers.size());
    for (RecordTransformer transformer : transformers) {
      if (transformer instanceof DataTypeTransformer) {
        statsTransformers.add(new DataTypeTransformer(tableConfig, _statsSchema));
      } else if (transformer instanceof SanitizationTransformer) {
        statsTransformers.add(new SanitizationTransformer(_statsSchema));
      } else if (transformer instanceof SpecialValueTransformer) {
        statsTransformers.add(new SpecialValueTransformer(_statsSchema));
      } else {
        // In particular, share the original null transformer so ingestion-time time defaults are identical.
        statsTransformers.add(transformer);
      }
    }
    _statsTransformPipeline = new TransformPipeline(tableConfig.getTableName(), statsTransformers);
  }

  private static boolean isIndependentAlias(String column, String expression, Set<String> deferredColumns) {
    String source = expression.trim();
    return !deferredColumns.contains(column) && !deferredColumns.contains(source)
        && source.matches("[A-Za-z_$][A-Za-z0-9_$]*");
  }

  /// An exact collector populated on the index pass. The only provisional allocation hint is a saturated V4/V6
  /// chunk size; these writers handle arbitrary-length values with their huge-chunk path. Final metadata reads the
  /// actual sealed statistics, and never publishes the provisional shape.
  private static final class DeferredRawColumnStatistics extends NoDictColumnStatisticsCollector {
    private final int _expectedDocs;
    private boolean _finished;

    private DeferredRawColumnStatistics(String column, StatsCollectorConfig config, int expectedDocs) {
      super(column, config);
      _expectedDocs = expectedDocs;
    }

    private void finish() {
      Preconditions.checkState(super.getTotalDocs() == _expectedDocs,
          "Raw column %s index-pass row count %s differs from stats-pass count %s", getColumnName(),
          super.getTotalDocs(), _expectedDocs);
      seal();
      _finished = true;
    }

    @Override
    public int getTotalDocs() {
      return _finished ? super.getTotalDocs() : _expectedDocs;
    }

    @Override
    public int getTotalNumberOfEntries() {
      return _finished ? super.getTotalNumberOfEntries() : _expectedDocs;
    }

    @Override
    public int getLengthOfLongestElement() {
      return _finished ? super.getLengthOfLongestElement() : Integer.MAX_VALUE;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return getLengthOfLongestElement();
    }

    @Override
    public boolean isSorted() {
      return _finished && super.isSorted();
    }

    @Override
    @Nullable
    public Comparable<?> getMinValue() {
      return _finished ? super.getMinValue() : null;
    }

    @Override
    @Nullable
    public Comparable<?> getMaxValue() {
      return _finished ? super.getMaxValue() : null;
    }
  }

  /// Returns the name of the segment associated with this index creation driver.
  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  /// Returns the path of the output directory
  @Override
  public File getOutputDirectory() {
    return new File(new File(_config.getOutDir()), _segmentName);
  }

  /// Returns the schema validator.
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

  /// Statistics consume already normalized cells. Keeping the reduced schema separate from the segment schema
  /// permits every eligible RAW column to skip this pass, including an all-RAW schema with no stats columns.
  private void collectNormalizedColumnStats()
      throws Exception {
    long start = System.nanoTime();
    _deferredRawStats.clear();
    Schema statsSchema = _statsSchema != null ? _statsSchema : _schema;
    StatsCollectorConfig statsConfig = new StatsCollectorConfig(_config.getTableConfig(), statsSchema, null);
    StatsCollectorConfig fullConfig = new StatsCollectorConfig(_config.getTableConfig(), _schema, null);
    for (String column : _schema.getPhysicalColumnNames()) {
      FieldSpec field = _schema.getFieldSpecFor(column);
      ColumnStatistics statistics;
      if (_totalDocs == 0) {
        statistics = new EmptyColumnStatistics(field, null, null);
      } else if (statsSchema.getFieldSpecFor(column) == null) {
        DeferredRawColumnStatistics deferred = new DeferredRawColumnStatistics(column, fullConfig, _totalDocs);
        _deferredRawStats.put(column, deferred);
        statistics = deferred;
      } else {
        AbstractColumnStatisticsCollector collector = StatsCollectorUtil.createStatsCollector(column, field,
            _config.getIndexConfigsByColName().get(column), statsConfig);
        NormalizedColumnReader reader = _normalizedColumnReaders.get(column);
        for (int doc = 0; doc < _totalDocs; doc++) {
          collector.collect(reader.getValue(doc));
        }
        collector.seal();
        statistics = collector;
      }
      _columnStatisticsMap.put(column, statistics);
    }
    _segmentStats = new SegmentPreIndexStatsContainer() {
      @Override
      public ColumnStatistics getColumnProfileFor(String column) {
        return _columnStatisticsMap.get(column);
      }

      @Override
      public int getTotalDocCount() {
        return _totalDocs;
      }
    };
    _totalStatsCollectorTimeNs = System.nanoTime() - start;
    LOGGER.info("Collected normalized column statistics for {} documents; deferred RAW columns: {}", _totalDocs,
        _deferredRawStats.keySet());
  }

  private void buildNormalizedColumns()
      throws Exception {
    // The provider factory owns every raw reader. Normalized wrappers release only their own cache state.
    try (ColumnReaderFactory ignored = _ownedColumnReaderFactory) {
      try {
        collectNormalizedColumnStats();
        _incompleteRowsFound = 0;
        _skippedRowsFound = 0;
        _sanitizedRowsFound = 0;
        _indexCreator.init(_config, _totalDocs, _columnStatisticsMap, _tempIndexDir);
        long start = System.nanoTime();
        SegmentColumnarIndexCreator creator = (SegmentColumnarIndexCreator) _indexCreator;
        for (Map.Entry<String, NormalizedColumnReader> entry : _normalizedColumnReaders.entrySet()) {
          NormalizedColumnReader reader = entry.getValue();
          reader.enableSanitizationTracking();
          DeferredRawColumnStatistics deferred = _deferredRawStats.get(entry.getKey());
          creator.indexNormalizedColumn(entry.getKey(), reader, deferred != null ? deferred::collect : null);
          if (deferred != null) {
            deferred.finish();
          }
          reader.releaseCache();
        }
        _totalIndexTimeNs = System.nanoTime() - start;
        _sanitizedRowsFound = _columnarSanitizedDocIds.cardinality();
        updateMetrics(_config.getTableConfig().getTableName());
        handlePostCreation();
      } catch (Exception | Error e) {
        try {
          _indexCreator.close();
        } catch (Exception closeError) {
          e.addSuppressed(closeError);
        }
        throw e;
      } finally {
        for (NormalizedColumnReader reader : _normalizedColumnReaders.values()) {
          reader.close();
        }
      }
    } finally {
      _ownedColumnReaderFactory = null;
    }
  }

  /// Build segment using columnar approach.
  /// This method builds the segment by processing data column-wise instead of row-wise.
  /// Following is not supported:
  ///
  /// - recort transformation
  /// - sorted column change wrt to input data
  ///
  ///   @throws Exception if segment building fails
  private void buildColumnar()
      throws Exception {
    if (_normalizedColumnReaders != null) {
      buildNormalizedColumns();
      return;
    }
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
      _indexCreator.init(_config, _totalDocs, _columnStatisticsMap, _tempIndexDir);

      // Build the indexes column-wise (true column-major approach)
      LOGGER.info("Start building Index using columnar approach");
      long indexStartTime = System.nanoTime();

      TreeSet<String> columns = _schema.getPhysicalColumnNames();
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

      if (_indexCreator instanceof SegmentColumnarIndexCreator) {
        // Logged after seal so the final segment name is available; only reads accumulated counters.
        ((SegmentColumnarIndexCreator) _indexCreator).logColumnMajorBuildPathSummary();
      }
    } catch (Exception e) {
      try {
        _indexCreator.close();
      } catch (Exception closeException) {
        LOGGER.error("Error closing index creator", closeException);
        // Add the close exception as suppressed to preserve both exceptions
        ExceptionUtils.suppress(closeException, e);
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
