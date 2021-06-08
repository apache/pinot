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
package org.apache.pinot.segment.local.indexsegment.mutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.geospatial.MutableH3Index;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeInvertedIndexReader;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexReader;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndex;
import org.apache.pinot.segment.local.realtime.impl.nullvalue.MutableNullValueVector;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.utils.FixedIntArrayOffHeapIdMap;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.IdMap;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.MutableDictionary;
import org.apache.pinot.segment.spi.index.reader.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.roaringbitmap.IntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DataType.BYTES;
import static org.apache.pinot.spi.data.FieldSpec.DataType.INT;
import static org.apache.pinot.spi.data.FieldSpec.DataType.STRING;


@SuppressWarnings({"rawtypes", "unchecked"})
public class MutableSegmentImpl implements MutableSegment {
  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final String RECORD_ID_MAP = "__recordIdMap__";
  private static final int EXPECTED_COMPRESSION = 1000;
  private static final int MIN_ROWS_TO_INDEX = 1000_000; // Min size of recordIdMap for updatable metrics.
  private static final int MIN_RECORD_ID_MAP_CACHE_SIZE = 10000; // Min overflow map size for updatable metrics.

  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT = 100;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT = 100_000;

  private final Logger _logger;
  private final long _startTimeMillis = System.currentTimeMillis();
  private final ServerMetrics _serverMetrics;

  private final String _tableNameWithType;
  private final String _segmentName;
  private final Schema _schema;
  private final String _timeColumnName;
  private final int _capacity;
  private final SegmentMetadata _segmentMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final String _partitionColumn;
  private final PartitionFunction _partitionFunction;
  private final boolean _nullHandlingEnabled;

  private final Map<String, IndexContainer> _indexContainerMap = new HashMap<>();

  private final IdMap<FixedIntArray> _recordIdMap;
  private boolean _aggregateMetrics;

  private volatile int _numDocsIndexed = 0;
  private final int _numKeyColumns;

  // Cache the physical (non-virtual) field specs
  private final Collection<FieldSpec> _physicalFieldSpecs;
  private final Collection<DimensionFieldSpec> _physicalDimensionFieldSpecs;
  private final Collection<MetricFieldSpec> _physicalMetricFieldSpecs;
  private final Collection<String> _physicalTimeColumnNames;

  // default message metadata
  private volatile long _lastIndexedTimeMs = Long.MIN_VALUE;
  private volatile long _latestIngestionTimeMs = Long.MIN_VALUE;

  private RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders _realtimeLuceneReaders;
  // If the table schema is changed before the consuming segment is committed, newly added columns would appear in _newlyAddedColumnsFieldMap.
  private final Map<String, FieldSpec> _newlyAddedColumnsFieldMap = new ConcurrentHashMap();
  private final Map<String, FieldSpec> _newlyAddedPhysicalColumnsFieldMap = new ConcurrentHashMap();

  private final UpsertConfig.Mode _upsertMode;
  private final PartitionUpsertMetadataManager _partitionUpsertMetadataManager;
  // The valid doc ids are maintained locally instead of in the upsert metadata manager because:
  // 1. There is only one consuming segment per partition, the committed segments do not need to modify the valid doc
  //    ids for the consuming segment.
  // 2. During the segment commitment, when loading the immutable version of this segment, in order to keep the result
  //    correct, the valid doc ids should not be changed, only the record location should be changed.
  // FIXME: There is a corner case for this approach which could cause inconsistency. When there is segment load during
  //        consumption with newer timestamp (late event in consuming segment), the record location will be updated, but
  //        the valid doc ids won't be updated.
  private final ThreadSafeMutableRoaringBitmap _validDocIds;

  public MutableSegmentImpl(RealtimeSegmentConfig config, @Nullable ServerMetrics serverMetrics) {
    _serverMetrics = serverMetrics;
    _tableNameWithType = config.getTableNameWithType();
    _segmentName = config.getSegmentName();
    _schema = config.getSchema();
    _timeColumnName = config.getTimeColumnName();
    _capacity = config.getCapacity();
    final RealtimeSegmentZKMetadata realtimeSegmentZKMetadata = config.getRealtimeSegmentZKMetadata();
    _segmentMetadata =
        new SegmentMetadataImpl(realtimeSegmentZKMetadata.getTableName(), realtimeSegmentZKMetadata.getSegmentName(),
            realtimeSegmentZKMetadata.getCreationTime(), realtimeSegmentZKMetadata.getStartTime(),
            realtimeSegmentZKMetadata.getEndTime(), realtimeSegmentZKMetadata.getTimeUnit(),
            realtimeSegmentZKMetadata.getTotalDocs(), realtimeSegmentZKMetadata.getCrc(), _schema) {
          @Override
          public int getTotalDocs() {
            return _numDocsIndexed;
          }

          @Override
          public long getLastIndexedTimestamp() {
            return _lastIndexedTimeMs;
          }

          @Override
          public long getLatestIngestionTimestamp() {
            return _latestIngestionTimeMs;
          }
        };

    _offHeap = config.isOffHeap();
    _memoryManager = config.getMemoryManager();
    _statsHistory = config.getStatsHistory();
    _partitionColumn = config.getPartitionColumn();
    _partitionFunction = config.getPartitionFunction();
    _nullHandlingEnabled = config.isNullHandlingEnabled();
    _aggregateMetrics = config.aggregateMetrics();

    Collection<FieldSpec> allFieldSpecs = _schema.getAllFieldSpecs();
    List<FieldSpec> physicalFieldSpecs = new ArrayList<>(allFieldSpecs.size());
    List<DimensionFieldSpec> physicalDimensionFieldSpecs = new ArrayList<>(_schema.getDimensionNames().size());
    List<MetricFieldSpec> physicalMetricFieldSpecs = new ArrayList<>(_schema.getMetricNames().size());
    List<String> physicalTimeColumnNames = new ArrayList<>();

    for (FieldSpec fieldSpec : allFieldSpecs) {
      if (!fieldSpec.isVirtualColumn()) {
        physicalFieldSpecs.add(fieldSpec);
        FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
        if (fieldType == FieldSpec.FieldType.DIMENSION) {
          physicalDimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        } else if (fieldType == FieldSpec.FieldType.METRIC) {
          physicalMetricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        } else if (fieldType == FieldSpec.FieldType.DATE_TIME || fieldType == FieldSpec.FieldType.TIME) {
          physicalTimeColumnNames.add(fieldSpec.getName());
        }
      }
    }
    _physicalFieldSpecs = Collections.unmodifiableCollection(physicalFieldSpecs);
    _physicalDimensionFieldSpecs = Collections.unmodifiableCollection(physicalDimensionFieldSpecs);
    _physicalMetricFieldSpecs = Collections.unmodifiableCollection(physicalMetricFieldSpecs);
    _physicalTimeColumnNames = Collections.unmodifiableCollection(physicalTimeColumnNames);

    _numKeyColumns = _physicalDimensionFieldSpecs.size() + _physicalTimeColumnNames.size();

    _logger =
        LoggerFactory.getLogger(MutableSegmentImpl.class.getName() + "_" + _segmentName + "_" + config.getStreamName());

    Set<String> noDictionaryColumns = config.getNoDictionaryColumns();
    Set<String> invertedIndexColumns = config.getInvertedIndexColumns();
    Set<String> textIndexColumns = config.getTextIndexColumns();
    Set<String> fstIndexColumns = config.getFSTIndexColumns();
    Set<String> jsonIndexColumns = config.getJsonIndexColumns();
    Map<String, H3IndexConfig> h3IndexConfigs = config.getH3IndexConfigs();

    int avgNumMultiValues = config.getAvgNumMultiValues();

    // Initialize for each column
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();

      // Partition info
      PartitionFunction partitionFunction = null;
      Set<Integer> partitions = null;
      if (column.equals(_partitionColumn)) {
        partitionFunction = _partitionFunction;

        // NOTE: Use a concurrent set because the partitions can be updated when the partition of the ingested record
        //       does not match the stream partition. This could happen when stream partition changes, or the records
        //       are not properly partitioned from the stream. Log an warning and emit a metric if it happens, then add
        //       the new partition into this set.
        partitions = ConcurrentHashMap.newKeySet();
        partitions.add(config.getPartitionId());
      }

      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index
      DataType storedType = fieldSpec.getDataType().getStoredType();
      boolean isFixedWidthColumn = storedType.isFixedWidth();
      MutableForwardIndex forwardIndex;
      MutableDictionary dictionary;
      if (isNoDictionaryColumn(noDictionaryColumns, invertedIndexColumns, fieldSpec, column)) {
        // No dictionary column (always single-valued)
        assert fieldSpec.isSingleValueField();

        dictionary = null;
        String allocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
        if (isFixedWidthColumn) {
          forwardIndex =
              new FixedByteSVMutableForwardIndex(false, storedType, _capacity, _memoryManager, allocationContext);
        } else {
          // RealtimeSegmentStatsHistory does not have the stats for no-dictionary columns from previous consuming
          // segments
          // TODO: Add support for updating RealtimeSegmentStatsHistory with average column value size for no dictionary
          //       columns as well
          // TODO: Use the stats to get estimated average length
          // Use a smaller capacity as opposed to segment flush size
          int initialCapacity = Math.min(_capacity, NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
          forwardIndex =
              new VarByteSVMutableForwardIndex(storedType, _memoryManager, allocationContext, initialCapacity,
                  NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT);
        }
      } else {
        // Dictionary-encoded column

        int dictionaryColumnSize;
        if (isFixedWidthColumn) {
          dictionaryColumnSize = storedType.size();
        } else {
          dictionaryColumnSize = _statsHistory.getEstimatedAvgColSize(column);
        }
        // NOTE: preserve 10% buffer for cardinality to reduce the chance of re-sizing the dictionary
        int estimatedCardinality = (int) (_statsHistory.getEstimatedCardinality(column) * 1.1);
        String dictionaryAllocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Dict.FILE_EXTENSION);
        dictionary = MutableDictionaryFactory
            .getMutableDictionary(storedType, _offHeap, _memoryManager, dictionaryColumnSize,
                Math.min(estimatedCardinality, _capacity), dictionaryAllocationContext);

        if (fieldSpec.isSingleValueField()) {
          // Single-value dictionary-encoded forward index
          String allocationContext = buildAllocationContext(_segmentName, column,
              V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
          forwardIndex = new FixedByteSVMutableForwardIndex(true, INT, _capacity, _memoryManager, allocationContext);
        } else {
          // Multi-value dictionary-encoded forward index
          String allocationContext = buildAllocationContext(_segmentName, column,
              V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
          // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
          forwardIndex =
              new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, avgNumMultiValues, _capacity, Integer.BYTES,
                  _memoryManager, allocationContext);
        }

        // Even though the column is defined as 'no-dictionary' in the config, we did create dictionary for consuming segment.
        noDictionaryColumns.remove(column);
      }

      // Inverted index
      RealtimeInvertedIndexReader invertedIndexReader =
          invertedIndexColumns.contains(column) ? new RealtimeInvertedIndexReader() : null;

      // Text index
      RealtimeLuceneTextIndexReader textIndex;
      if (textIndexColumns.contains(column)) {
        textIndex = new RealtimeLuceneTextIndexReader(column, new File(config.getConsumerDir()), _segmentName);
        if (_realtimeLuceneReaders == null) {
          _realtimeLuceneReaders = new RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders(_segmentName);
        }
        _realtimeLuceneReaders.addReader(textIndex);
      } else {
        textIndex = null;
      }

      // Json index
      MutableJsonIndex jsonIndex = jsonIndexColumns.contains(column) ? new MutableJsonIndex() : null;

      // H3 index
      MutableH3Index h3Index;
      try {
        H3IndexConfig h3IndexConfig = h3IndexConfigs.get(column);
        h3Index = h3IndexConfig != null ? new MutableH3Index(h3IndexConfig.getResolution()) : null;
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to initiate H3 index for column: %s", column), e);
      }

      // Null value vector
      MutableNullValueVector nullValueVector = _nullHandlingEnabled ? new MutableNullValueVector() : null;

      // TODO: Support range index and bloom filter for mutable segment
      _indexContainerMap.put(column,
          new IndexContainer(fieldSpec, partitionFunction, partitions, new NumValuesInfo(), forwardIndex, dictionary,
              invertedIndexReader, null, textIndex, fstIndexColumns.contains(column), jsonIndex, h3Index, null,
              nullValueVector));
    }

    if (_realtimeLuceneReaders != null) {
      // add the realtime lucene index readers to the global queue for refresh task to pick up
      RealtimeLuceneIndexRefreshState realtimeLuceneIndexRefreshState = RealtimeLuceneIndexRefreshState.getInstance();
      realtimeLuceneIndexRefreshState.addRealtimeReadersToQueue(_realtimeLuceneReaders);
    }

    // Metric aggregation can be enabled only if config is specified, and all dimensions have dictionary,
    // and no metrics have dictionary. If not enabled, the map returned is null.
    _recordIdMap = enableMetricsAggregationIfPossible(config, noDictionaryColumns);

    // init upsert-related data structure
    _upsertMode = config.getUpsertMode();
    if (isUpsertEnabled()) {
      Preconditions.checkState(!_aggregateMetrics, "Metrics aggregation and upsert cannot be enabled together");
      _partitionUpsertMetadataManager = config.getPartitionUpsertMetadataManager();
      _validDocIds = new ThreadSafeMutableRoaringBitmap();
    } else {
      _partitionUpsertMetadataManager = null;
      _validDocIds = null;
    }
  }

  /**
   * Decide whether a given column should be dictionary encoded or not
   * @param noDictionaryColumns no dictionary column set
   * @param invertedIndexColumns inverted index column set
   * @param fieldSpec field spec of column
   * @param column column name
   * @return true if column is no-dictionary, false if dictionary encoded
   */
  private boolean isNoDictionaryColumn(Set<String> noDictionaryColumns, Set<String> invertedIndexColumns,
      FieldSpec fieldSpec, String column) {
    DataType dataType = fieldSpec.getDataType();
    if (noDictionaryColumns.contains(column)) {
      // Earlier we didn't support noDict in consuming segments for STRING and BYTES columns.
      // So even if the user had the column in noDictionaryColumns set in table config, we still
      // created dictionary in consuming segments.
      // Later on we added this support. There is a particular impact of this change on the use cases
      // that have set noDict on their STRING dimension columns for other performance
      // reasons and also want metricsAggregation. These use cases don't get to
      // aggregateMetrics because the new implementation is able to honor their table config setting
      // of noDict on STRING/BYTES. Without metrics aggregation, memory pressure increases.
      // So to continue aggregating metrics for such cases, we will create dictionary even
      // if the column is part of noDictionary set from table config
      if (fieldSpec instanceof DimensionFieldSpec && _aggregateMetrics && (dataType == STRING || dataType == BYTES)) {
        _logger
            .info("Aggregate metrics is enabled. Will create dictionary in consuming segment for column {} of type {}",
                column, dataType.toString());
        return false;
      }
      // So don't create dictionary if the column is member of noDictionary, is single-value
      // and doesn't have an inverted index
      return fieldSpec.isSingleValueField() && !invertedIndexColumns.contains(column);
    }
    // column is not a part of noDictionary set, so create dictionary
    return false;
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    if (_partitionColumn != null) {
      return new SegmentPartitionConfig(Collections.singletonMap(_partitionColumn,
          new ColumnPartitionConfig(_partitionFunction.toString(), _partitionFunction.getNumPartitions())));
    } else {
      return null;
    }
  }

  /**
   * Get min time from the segment, based on the time column, only used by Kafka HLC.
   */
  @Deprecated
  public long getMinTime() {
    Long minTime = IngestionUtils.extractTimeValue(_indexContainerMap.get(_timeColumnName)._minValue);
    if (minTime != null) {
      return minTime;
    }
    return Long.MAX_VALUE;
  }

  /**
   * Get max time from the segment, based on the time column, only used by Kafka HLC.
   */
  @Deprecated
  public long getMaxTime() {
    Long maxTime = IngestionUtils.extractTimeValue(_indexContainerMap.get(_timeColumnName)._maxValue);
    if (maxTime != null) {
      return maxTime;
    }
    return Long.MIN_VALUE;
  }

  public void addExtraColumns(Schema newSchema) {
    for (String columnName : newSchema.getColumnNames()) {
      if (!_schema.getColumnNames().contains(columnName)) {
        FieldSpec fieldSpec = newSchema.getFieldSpecFor(columnName);
        _newlyAddedColumnsFieldMap.put(columnName, fieldSpec);
        if (!fieldSpec.isVirtualColumn()) {
          _newlyAddedPhysicalColumnsFieldMap.put(columnName, fieldSpec);
        }
      }
    }
    _logger.info("Newly added columns: " + _newlyAddedColumnsFieldMap.toString());
  }

  // NOTE: Okay for single-writer
  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  @Override
  public boolean index(GenericRow row, @Nullable RowMetadata rowMetadata)
      throws IOException {
    boolean canTakeMore;
    if (isUpsertEnabled()) {
      row = handleUpsert(row, _numDocsIndexed);

      updateDictionary(row);
      addNewRow(row);
      // Update number of documents indexed at last to make the latest row queryable
      canTakeMore = _numDocsIndexed++ < _capacity;
    } else {
      // Update dictionary first
      updateDictionary(row);

      // If metrics aggregation is enabled and if the dimension values were already seen, this will return existing
      // docId, else this will return a new docId.
      int docId = getOrCreateDocId();

      if (docId == _numDocsIndexed) {
        // New row
        addNewRow(row);
        // Update number of documents indexed at last to make the latest row queryable
        canTakeMore = _numDocsIndexed++ < _capacity;
      } else {
        assert _aggregateMetrics;
        aggregateMetrics(row, docId);
        canTakeMore = true;
      }
    }

    // Update last indexed time and latest ingestion time
    _lastIndexedTimeMs = System.currentTimeMillis();
    if (rowMetadata != null) {
      _latestIngestionTimeMs = Math.max(_latestIngestionTimeMs, rowMetadata.getIngestionTimeMs());
    }

    return canTakeMore;
  }

  private boolean isUpsertEnabled() {
    return _upsertMode != UpsertConfig.Mode.NONE;
  }

  private GenericRow handleUpsert(GenericRow row, int docId) {
    PrimaryKey primaryKey = row.getPrimaryKey(_schema.getPrimaryKeyColumns());
    Object timeValue = row.getValue(_timeColumnName);
    Preconditions.checkArgument(timeValue instanceof Comparable, "time column shall be comparable");
    long timestamp = IngestionUtils.extractTimeValue((Comparable) timeValue);
    return _partitionUpsertMetadataManager
        .updateRecord(this, new PartitionUpsertMetadataManager.RecordInfo(primaryKey, docId, timestamp), row);
  }

  private void updateDictionary(GenericRow row) {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IndexContainer indexContainer = entry.getValue();
      Object value = row.getValue(column);
      MutableDictionary dictionary = indexContainer._dictionary;
      if (dictionary != null) {
        if (indexContainer._fieldSpec.isSingleValueField()) {
          indexContainer._dictId = dictionary.index(value);
        } else {
          indexContainer._dictIds = dictionary.index((Object[]) value);
        }

        // Update min/max value from dictionary
        indexContainer._minValue = dictionary.getMinVal();
        indexContainer._maxValue = dictionary.getMaxVal();
      }
    }
  }

  private void addNewRow(GenericRow row)
      throws IOException {
    int docId = _numDocsIndexed;
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IndexContainer indexContainer = entry.getValue();
      Object value = row.getValue(column);
      FieldSpec fieldSpec = indexContainer._fieldSpec;
      if (fieldSpec.isSingleValueField()) {
        // Single-value column

        // Check partitions
        if (column.equals(_partitionColumn)) {
          int partition = _partitionFunction.getPartition(value);
          if (indexContainer._partitions.add(partition)) {
            _logger.warn("Found new partition: {} from partition column: {}, value: {}", partition, column, value);
            if (_serverMetrics != null) {
              _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.REALTIME_PARTITION_MISMATCH, 1);
            }
          }
        }

        // Update numValues info
        indexContainer._numValuesInfo.updateSVEntry();

        // Update indexes
        MutableForwardIndex forwardIndex = indexContainer._forwardIndex;
        int dictId = indexContainer._dictId;
        if (dictId >= 0) {
          // Dictionary-encoded single-value column

          // Update forward index
          forwardIndex.setDictId(docId, dictId);

          // Update inverted index
          RealtimeInvertedIndexReader invertedIndex = indexContainer._invertedIndex;
          if (invertedIndex != null) {
            invertedIndex.add(dictId, docId);
          }
        } else {
          // Single-value column with raw index

          // Update forward index
          DataType dataType = fieldSpec.getDataType();
          switch (dataType) {
            case INT:
              forwardIndex.setInt(docId, (Integer) value);
              break;
            case LONG:
              forwardIndex.setLong(docId, (Long) value);
              break;
            case FLOAT:
              forwardIndex.setFloat(docId, (Float) value);
              break;
            case DOUBLE:
              forwardIndex.setDouble(docId, (Double) value);
              break;
            case STRING:
              forwardIndex.setString(docId, (String) value);
              break;
            case BYTES:
              forwardIndex.setBytes(docId, (byte[]) value);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
          }

          // Update min/max value from raw value
          // NOTE: Skip updating min/max value for aggregated metrics because the value will change over time.
          if (!_aggregateMetrics || fieldSpec.getFieldType() != FieldSpec.FieldType.METRIC) {
            Comparable comparable;
            if (dataType == BYTES) {
              comparable = new ByteArray((byte[]) value);
            } else {
              comparable = (Comparable) value;
            }
            if (indexContainer._minValue == null) {
              indexContainer._minValue = comparable;
              indexContainer._maxValue = comparable;
            } else {
              if (comparable.compareTo(indexContainer._minValue) < 0) {
                indexContainer._minValue = comparable;
              }
              if (comparable.compareTo(indexContainer._maxValue) > 0) {
                indexContainer._maxValue = comparable;
              }
            }
          }
        }

        // Update text index
        RealtimeLuceneTextIndexReader textIndex = indexContainer._textIndex;
        if (textIndex != null) {
          textIndex.add((String) value);
        }

        // Update json index
        MutableJsonIndex jsonIndex = indexContainer._jsonIndex;
        if (jsonIndex != null) {
          jsonIndex.add((String) value);
        }

        // Update H3 index
        MutableH3Index h3Index = indexContainer._h3Index;
        if (h3Index != null) {
          h3Index.add(GeometrySerializer.deserialize((byte[]) value));
        }
      } else {
        // Multi-value column (always dictionary-encoded)

        int[] dictIds = indexContainer._dictIds;

        // Update numValues info
        indexContainer._numValuesInfo.updateMVEntry(dictIds.length);

        // Update forward index
        indexContainer._forwardIndex.setDictIdMV(docId, dictIds);

        // Update inverted index
        RealtimeInvertedIndexReader invertedIndex = indexContainer._invertedIndex;
        if (invertedIndex != null) {
          for (int dictId : dictIds) {
            invertedIndex.add(dictId, docId);
          }
        }
      }

      // Update null value vector
      if (_nullHandlingEnabled && row.isNullValue(column)) {
        indexContainer._nullValueVector.setNull(docId);
      }
    }
  }

  private void aggregateMetrics(GenericRow row, int docId) {
    for (MetricFieldSpec metricFieldSpec : _physicalMetricFieldSpecs) {
      String column = metricFieldSpec.getName();
      Object value = row.getValue(column);
      MutableForwardIndex forwardIndex = _indexContainerMap.get(column)._forwardIndex;
      DataType dataType = metricFieldSpec.getDataType();
      switch (dataType) {
        case INT:
          forwardIndex.setInt(docId, (Integer) value + forwardIndex.getInt(docId));
          break;
        case LONG:
          forwardIndex.setLong(docId, (Long) value + forwardIndex.getLong(docId));
          break;
        case FLOAT:
          forwardIndex.setFloat(docId, (Float) value + forwardIndex.getFloat(docId));
          break;
        case DOUBLE:
          forwardIndex.setDouble(docId, (Double) value + forwardIndex.getDouble(docId));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported data type: " + dataType + " for aggregate metric column: " + column);
      }
    }
  }

  @Override
  public int getNumDocsIndexed() {
    return _numDocsIndexed;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public Set<String> getColumnNames() {
    // Return all column names, virtual and physical.
    return Sets.union(_schema.getColumnNames(), _newlyAddedColumnsFieldMap.keySet());
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    HashSet<String> physicalColumnNames = new HashSet<>();

    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      physicalColumnNames.add(fieldSpec.getName());
    }
    // We should include newly added columns in the physical columns
    return Sets.union(physicalColumnNames, _newlyAddedPhysicalColumnsFieldMap.keySet());
  }

  @Override
  public DataSource getDataSource(String column) {
    FieldSpec fieldSpec = _schema.getFieldSpecFor(column);
    if (fieldSpec == null || fieldSpec.isVirtualColumn()) {
      // Column is either added during ingestion, or was initiated with a virtual column provider
      if (fieldSpec == null) {
        // If the column was added during ingestion, we will construct the column provider based on its fieldSpec to provide values
        fieldSpec = _newlyAddedColumnsFieldMap.get(column);
        Preconditions.checkNotNull(fieldSpec,
            "FieldSpec for " + column + " should not be null. " + "Potentially invalid column name specified.");
      }
      // TODO: Refactor virtual column provider to directly generate data source
      VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, _numDocsIndexed);
      VirtualColumnProvider virtualColumnProvider = VirtualColumnProviderFactory.buildProvider(virtualColumnContext);
      return new ImmutableDataSource(virtualColumnProvider.buildMetadata(virtualColumnContext),
          virtualColumnProvider.buildColumnIndexContainer(virtualColumnContext));
    } else {
      return _indexContainerMap.get(column).toDataSource();
    }
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return _validDocIds;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
      String column = entry.getKey();
      IndexContainer indexContainer = entry.getValue();
      Object value = getValue(docId, indexContainer._forwardIndex, indexContainer._dictionary,
          indexContainer._numValuesInfo._maxNumValuesPerMVEntry);
      if (_nullHandlingEnabled && indexContainer._nullValueVector.isNull(docId)) {
        reuse.putDefaultNullValue(column, value);
      } else {
        reuse.putValue(column, value);
      }
    }
    return reuse;
  }

  /**
   * Helper method to read the value for the given document id.
   */
  private static Object getValue(int docId, MutableForwardIndex forwardIndex, @Nullable MutableDictionary dictionary,
      int maxNumMultiValues) {
    if (dictionary != null) {
      // Dictionary based
      if (forwardIndex.isSingleValue()) {
        int dictId = forwardIndex.getDictId(docId);
        return dictionary.get(dictId);
      } else {
        int[] dictIds = new int[maxNumMultiValues];
        int numValues = forwardIndex.getDictIdMV(docId, dictIds);
        Object[] value = new Object[numValues];
        for (int i = 0; i < numValues; i++) {
          value[i] = dictionary.get(dictIds[i]);
        }
        return value;
      }
    } else {
      // Raw index based
      // TODO: support multi-valued column
      switch (forwardIndex.getValueType()) {
        case INT:
          return forwardIndex.getInt(docId);
        case LONG:
          return forwardIndex.getLong(docId);
        case FLOAT:
          return forwardIndex.getFloat(docId);
        case DOUBLE:
          return forwardIndex.getDouble(docId);
        case STRING:
          return forwardIndex.getString(docId);
        case BYTES:
          return forwardIndex.getBytes(docId);
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  public void destroy() {
    _logger.info("Trying to close RealtimeSegmentImpl : {}", _segmentName);

    // Gather statistics for off-heap mode
    if (_offHeap) {
      if (_numDocsIndexed > 0) {
        int numSeconds = (int) ((System.currentTimeMillis() - _startTimeMillis) / 1000);
        long totalMemBytes = _memoryManager.getTotalAllocatedBytes();
        _logger
            .info("Segment used {} bytes of memory for {} rows consumed in {} seconds", totalMemBytes, _numDocsIndexed,
                numSeconds);

        RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
        for (Map.Entry<String, IndexContainer> entry : _indexContainerMap.entrySet()) {
          String column = entry.getKey();
          BaseOffHeapMutableDictionary dictionary = (BaseOffHeapMutableDictionary) entry.getValue()._dictionary;
          if (dictionary != null) {
            RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
            columnStats.setCardinality(dictionary.length());
            columnStats.setAvgColumnSize(dictionary.getAvgValueSize());
            segmentStats.setColumnStats(column, columnStats);
          }
        }
        segmentStats.setNumRowsConsumed(_numDocsIndexed);
        segmentStats.setNumRowsIndexed(_numDocsIndexed);
        segmentStats.setMemUsedBytes(totalMemBytes);
        segmentStats.setNumSeconds(numSeconds);
        _statsHistory.addSegmentStats(segmentStats);
      }
    }

    // Stop the text index refresh before closing the indexes
    if (_realtimeLuceneReaders != null) {
      // set this to true as a way of signalling the refresh task thread to
      // not attempt refresh on this segment here onwards
      _realtimeLuceneReaders.getLock().lock();
      try {
        _realtimeLuceneReaders.setSegmentDestroyed();
        _realtimeLuceneReaders.clearRealtimeReaderList();
      } finally {
        _realtimeLuceneReaders.getLock().unlock();
      }
    }

    // Close the indexes
    for (IndexContainer indexContainer : _indexContainerMap.values()) {
      indexContainer.close();
    }

    if (_recordIdMap != null) {
      try {
        _recordIdMap.close();
      } catch (IOException e) {
        _logger.error("Failed to close the record id map. Continuing with error.", e);
      }
    }

    _segmentMetadata.close();

    // NOTE: Close the memory manager as the last step. It will release all the PinotDataBuffers allocated.
    try {
      _memoryManager.close();
    } catch (IOException e) {
      _logger.error("Failed to close the memory manager", e);
    }
  }

  /**
   * Returns the docIds to use for iteration when the data is sorted by the given column.
   * <p>Called only by realtime record reader.
   *
   * @param column The column to use for sorting
   * @return The docIds to use for iteration
   */
  public int[] getSortedDocIdIterationOrderWithSortedColumn(String column) {
    IndexContainer indexContainer = _indexContainerMap.get(column);
    MutableDictionary dictionary = indexContainer._dictionary;

    // Sort all values in the dictionary
    int numValues = dictionary.length();
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      dictIds[i] = i;
    }
    IntArrays.quickSort(dictIds, dictionary::compare);

    // Re-order documents using the inverted index
    RealtimeInvertedIndexReader invertedIndex = indexContainer._invertedIndex;
    int[] docIds = new int[_numDocsIndexed];
    int docIdIndex = 0;
    for (int dictId : dictIds) {
      IntIterator intIterator = invertedIndex.getDocIds(dictId).getIntIterator();
      while (intIterator.hasNext()) {
        docIds[docIdIndex++] = intIterator.next();
      }
    }

    // Sanity check
    Preconditions.checkState(_numDocsIndexed == docIdIndex,
        "The number of documents indexed: %s is not equal to the number of sorted documents: %s", _numDocsIndexed,
        docIdIndex);

    return docIds;
  }

  /**
   * Helper method that builds allocation context that includes segment name, column name, and index type.
   *
   * @param segmentName Name of segment.
   * @param columnName Name of column.
   * @param indexType Index type.
   * @return Allocation context built from segment name, column name and index type.
   */
  private String buildAllocationContext(String segmentName, String columnName, String indexType) {
    return segmentName + ":" + columnName + indexType;
  }

  private int getOrCreateDocId() {
    if (!_aggregateMetrics) {
      return _numDocsIndexed;
    }

    int i = 0;
    int[] dictIds = new int[_numKeyColumns]; // dimensions + date time columns + time column.

    // FIXME: this for loop breaks for multi value dimensions. https://github.com/apache/incubator-pinot/issues/3867
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      dictIds[i++] = _indexContainerMap.get(fieldSpec.getName())._dictId;
    }
    for (String timeColumnName : _physicalTimeColumnNames) {
      dictIds[i++] = _indexContainerMap.get(timeColumnName)._dictId;
    }
    return _recordIdMap.put(new FixedIntArray(dictIds));
  }

  /**
   * Helper method to enable/initialize aggregation of metrics, based on following conditions:
   * <ul>
   *   <li> Config to enable aggregation of metrics is specified. </li>
   *   <li> All dimensions and time are dictionary encoded. This is because an integer array containing dictionary id's
   *        is used as key for dimensions to record Id map. </li>
   *   <li> None of the metrics are dictionary encoded. </li>
   *   <li> All columns should be single-valued (see https://github.com/apache/incubator-pinot/issues/3867)</li>
   * </ul>
   *
   * TODO: Eliminate the requirement on dictionary encoding for dimension and metric columns.
   *
   * @param config Segment config.
   * @param noDictionaryColumns Set of no dictionary columns.
   *
   * @return Map from dictionary id array to doc id, null if metrics aggregation cannot be enabled.
   */
  private IdMap<FixedIntArray> enableMetricsAggregationIfPossible(RealtimeSegmentConfig config,
      Set<String> noDictionaryColumns) {
    if (!_aggregateMetrics) {
      _logger.info("Metrics aggregation is disabled.");
      return null;
    }

    // All metric columns should have no-dictionary index.
    // All metric columns must be single value
    for (FieldSpec fieldSpec : _physicalMetricFieldSpecs) {
      String metric = fieldSpec.getName();
      if (!noDictionaryColumns.contains(metric)) {
        _logger
            .warn("Metrics aggregation cannot be turned ON in presence of dictionary encoded metrics, eg: {}", metric);
        _aggregateMetrics = false;
        break;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger
            .warn("Metrics aggregation cannot be turned ON in presence of multi-value metric columns, eg: {}", metric);
        _aggregateMetrics = false;
        break;
      }
    }

    // All dimension columns should be dictionary encoded.
    // All dimension columns must be single value
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      String dimension = fieldSpec.getName();
      if (noDictionaryColumns.contains(dimension)) {
        _logger
            .warn("Metrics aggregation cannot be turned ON in presence of no-dictionary dimensions, eg: {}", dimension);
        _aggregateMetrics = false;
        break;
      }

      if (!fieldSpec.isSingleValueField()) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of multi-value dimension columns, eg: {}",
            dimension);
        _aggregateMetrics = false;
        break;
      }
    }

    // Time columns should be dictionary encoded.
    for (String timeColumnName : _physicalTimeColumnNames) {
      if (noDictionaryColumns.contains(timeColumnName)) {
        _logger
            .warn("Metrics aggregation cannot be turned ON in presence of no-dictionary datetime/time columns, eg: {}",
                timeColumnName);
        _aggregateMetrics = false;
        break;
      }
    }

    if (!_aggregateMetrics) {
      return null;
    }

    int estimatedRowsToIndex;
    if (_statsHistory.isEmpty()) {
      // Choose estimated rows to index as maxNumRowsPerSegment / EXPECTED_COMPRESSION (1000, to be conservative in size).
      // These are just heuristics at the moment, and can be refined based on experimental results.
      estimatedRowsToIndex = Math.max(config.getCapacity() / EXPECTED_COMPRESSION, MIN_ROWS_TO_INDEX);
    } else {
      estimatedRowsToIndex = Math.max(_statsHistory.getEstimatedRowsToIndex(), MIN_ROWS_TO_INDEX);
    }

    // Compute size of overflow map.
    int maxOverFlowHashSize = Math.max(estimatedRowsToIndex / 1000, MIN_RECORD_ID_MAP_CACHE_SIZE);

    _logger.info("Initializing metrics update: estimatedRowsToIndex:{}, cacheSize:{}", estimatedRowsToIndex,
        maxOverFlowHashSize);
    return new FixedIntArrayOffHeapIdMap(estimatedRowsToIndex, maxOverFlowHashSize, _numKeyColumns, _memoryManager,
        RECORD_ID_MAP);
  }

  // NOTE: Okay for single-writer
  @SuppressWarnings("NonAtomicOperationOnVolatileField")
  private static class NumValuesInfo {
    volatile int _numValues = 0;
    volatile int _maxNumValuesPerMVEntry = -1;

    void updateSVEntry() {
      _numValues++;
    }

    void updateMVEntry(int numValuesInMVEntry) {
      _numValues += numValuesInMVEntry;
      _maxNumValuesPerMVEntry = Math.max(_maxNumValuesPerMVEntry, numValuesInMVEntry);
    }
  }

  private class IndexContainer implements Closeable {
    final FieldSpec _fieldSpec;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final NumValuesInfo _numValuesInfo;
    final MutableForwardIndex _forwardIndex;
    final MutableDictionary _dictionary;
    final RealtimeInvertedIndexReader _invertedIndex;
    final InvertedIndexReader _rangeIndex;
    final MutableH3Index _h3Index;
    final RealtimeLuceneTextIndexReader _textIndex;
    final boolean _enableFST;
    final MutableJsonIndex _jsonIndex;
    final BloomFilterReader _bloomFilter;
    final MutableNullValueVector _nullValueVector;

    volatile Comparable _minValue;
    volatile Comparable _maxValue;

    // Hold the dictionary id for the latest record
    int _dictId = Integer.MIN_VALUE;
    int[] _dictIds;

    IndexContainer(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
        @Nullable Set<Integer> partitions, NumValuesInfo numValuesInfo, MutableForwardIndex forwardIndex,
        @Nullable MutableDictionary dictionary, @Nullable RealtimeInvertedIndexReader invertedIndex,
        @Nullable InvertedIndexReader rangeIndex, @Nullable RealtimeLuceneTextIndexReader textIndex, boolean enableFST,
        @Nullable MutableJsonIndex jsonIndex, @Nullable MutableH3Index h3Index, @Nullable BloomFilterReader bloomFilter,
        @Nullable MutableNullValueVector nullValueVector) {
      _fieldSpec = fieldSpec;
      _partitionFunction = partitionFunction;
      _partitions = partitions;
      _numValuesInfo = numValuesInfo;
      _forwardIndex = forwardIndex;
      _dictionary = dictionary;
      _invertedIndex = invertedIndex;
      _rangeIndex = rangeIndex;
      _h3Index = h3Index;

      _textIndex = textIndex;
      _enableFST = enableFST;
      _jsonIndex = jsonIndex;
      _bloomFilter = bloomFilter;
      _nullValueVector = nullValueVector;
    }

    DataSource toDataSource() {
      return new MutableDataSource(_fieldSpec, _numDocsIndexed, _numValuesInfo._numValues,
          _numValuesInfo._maxNumValuesPerMVEntry, _partitionFunction, _partitions, _minValue, _maxValue, _forwardIndex,
          _dictionary, _invertedIndex, _rangeIndex, _textIndex, _enableFST, _jsonIndex, _h3Index, _bloomFilter,
          _nullValueVector);
    }

    @Override
    public void close() {
      String column = _fieldSpec.getName();
      try {
        _forwardIndex.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing forward index for column: {}, continuing with error", column, e);
      }
      if (_dictionary != null) {
        try {
          _dictionary.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing dictionary for column: {}, continuing with error", column, e);
        }
      }
      if (_invertedIndex != null) {
        try {
          _invertedIndex.close();
        } catch (Exception e) {
          _logger
              .error("Caught exception while closing inverted index for column: {}, continuing with error", column, e);
        }
      }
      if (_rangeIndex != null) {
        try {
          _rangeIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing range index for column: {}, continuing with error", column, e);
        }
      }
      if (_textIndex != null) {
        try {
          _textIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing text index for column: {}, continuing with error", column, e);
        }
      }
      if (_jsonIndex != null) {
        try {
          _jsonIndex.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing json index for column: {}, continuing with error", column, e);
        }
      }
      if (_h3Index != null) {
        try {
          _h3Index.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing H3 index for column: {}, continuing with error", column, e);
        }
      }
      if (_bloomFilter != null) {
        try {
          _bloomFilter.close();
        } catch (Exception e) {
          _logger.error("Caught exception while closing bloom filter for column: {}, continuing with error", column, e);
        }
      }
    }
  }
}
