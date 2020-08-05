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
package org.apache.pinot.core.indexsegment.mutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntArrays;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.core.realtime.impl.dictionary.BaseMutableDictionary;
import org.apache.pinot.core.realtime.impl.dictionary.BaseOffHeapMutableDictionary;
import org.apache.pinot.core.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.core.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.core.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.core.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeInvertedIndexReader;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders;
import org.apache.pinot.core.realtime.impl.invertedindex.RealtimeLuceneTextIndexReader;
import org.apache.pinot.core.realtime.impl.nullvalue.RealtimeNullValueVectorReaderWriter;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.core.segment.index.datasource.MutableDataSource;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.MutableForwardIndex;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.core.startree.v2.StarTreeV2;
import org.apache.pinot.core.util.FixedIntArray;
import org.apache.pinot.core.util.FixedIntArrayOffHeapIdMap;
import org.apache.pinot.core.util.IdMap;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RowMetadata;
import org.roaringbitmap.IntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private final int _partitionId;
  private final boolean _nullHandlingEnabled;

  // TODO: Keep one map to store all these info
  private final Map<String, NumValuesInfo> _numValuesInfoMap = new HashMap<>();
  private final Map<String, BaseMutableDictionary> _dictionaryMap = new HashMap<>();
  private final Map<String, MutableForwardIndex> _forwardIndexMap = new HashMap<>();
  private final Map<String, InvertedIndexReader> _invertedIndexMap = new HashMap<>();
  private final Map<String, InvertedIndexReader> _rangeIndexMap = new HashMap<>();
  private final Map<String, BloomFilterReader> _bloomFilterMap = new HashMap<>();
  // Only store min/max for non-dictionary fields
  private final Map<String, Comparable> _minValueMap = new HashMap<>();
  private final Map<String, Comparable> _maxValueMap = new HashMap<>();

  private final Map<String, RealtimeNullValueVectorReaderWriter> _nullValueVectorMap = new HashMap<>();
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

  private RealtimeLuceneReaders _realtimeLuceneReaders;
  // If the table schema is changed before the consuming segment is committed, newly added columns would appear in _newlyAddedColumnsFieldMap.
  private final Map<String, FieldSpec> _newlyAddedColumnsFieldMap = new ConcurrentHashMap();
  private final Map<String, FieldSpec> _newlyAddedPhysicalColumnsFieldMap = new ConcurrentHashMap();

  public MutableSegmentImpl(RealtimeSegmentConfig config) {
    _segmentName = config.getSegmentName();
    _schema = config.getSchema();
    _timeColumnName = config.getTimeColumnName();
    _capacity = config.getCapacity();
    _segmentMetadata = new SegmentMetadataImpl(config.getRealtimeSegmentZKMetadata(), _schema) {
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
    _partitionId = config.getPartitionId();
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

    int avgNumMultiValues = config.getAvgNumMultiValues();

    // Initialize for each column
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      _numValuesInfoMap.put(column, new NumValuesInfo());

      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index
      DataType dataType = fieldSpec.getDataType();
      boolean isFixedWidthColumn = dataType.isFixedWidth();
      if (isNoDictionaryColumn(noDictionaryColumns, invertedIndexColumns, fieldSpec, column)) {
        // No dictionary column (always single-valued)
        assert fieldSpec.isSingleValueField();

        String allocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
        if (isFixedWidthColumn) {
          _forwardIndexMap.put(column,
              new FixedByteSVMutableForwardIndex(false, dataType, _capacity, _memoryManager, allocationContext));
        } else {
          // RealtimeSegmentStatsHistory does not have the stats for no-dictionary columns from previous consuming
          // segments
          // TODO: Add support for updating RealtimeSegmentStatsHistory with average column value size for no dictionary
          //       columns as well
          // TODO: Use the stats to get estimated average length
          // Use a smaller capacity as opposed to segment flush size
          int initialCapacity = Math.min(_capacity, NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
          _forwardIndexMap.put(column,
              new VarByteSVMutableForwardIndex(dataType, _memoryManager, allocationContext, initialCapacity,
                  NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT));
        }
        // Init min/max value map to avoid potential thread safety issue (expanding hashMap while reading).
        _minValueMap.put(column, null);
        _maxValueMap.put(column, null);
      } else {
        // Dictionary-encoded column

        int dictionaryColumnSize;
        if (isFixedWidthColumn) {
          dictionaryColumnSize = dataType.size();
        } else {
          dictionaryColumnSize = _statsHistory.getEstimatedAvgColSize(column);
        }
        // NOTE: preserve 10% buffer for cardinality to reduce the chance of re-sizing the dictionary
        int estimatedCardinality = (int) (_statsHistory.getEstimatedCardinality(column) * 1.1);
        String dictionaryAllocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Dict.FILE_EXTENSION);
        _dictionaryMap.put(column, MutableDictionaryFactory
            .getMutableDictionary(dataType, _offHeap, _memoryManager, dictionaryColumnSize,
                Math.min(estimatedCardinality, _capacity), dictionaryAllocationContext));

        if (fieldSpec.isSingleValueField()) {
          // Single-value dictionary-encoded forward index
          String allocationContext = buildAllocationContext(_segmentName, column,
              V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
          _forwardIndexMap.put(column,
              new FixedByteSVMutableForwardIndex(true, DataType.INT, _capacity, _memoryManager, allocationContext));
        } else {
          // Multi-value dictionary-encoded forward index
          String allocationContext = buildAllocationContext(_segmentName, column,
              V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
          // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
          _forwardIndexMap.put(column,
              new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, avgNumMultiValues, _capacity, Integer.BYTES,
                  _memoryManager, allocationContext));
        }

        // Even though the column is defined as 'no-dictionary' in the config, we did create dictionary for consuming segment.
        noDictionaryColumns.remove(column);
      }

      if (invertedIndexColumns.contains(column)) {
        _invertedIndexMap.put(column, new RealtimeInvertedIndexReader());
      }

      if (_nullHandlingEnabled) {
        _nullValueVectorMap.put(column, new RealtimeNullValueVectorReaderWriter());
      }

      if (textIndexColumns.contains(column)) {
        RealtimeLuceneTextIndexReader realtimeLuceneIndexReader =
            new RealtimeLuceneTextIndexReader(column, new File(config.getConsumerDir()), _segmentName);
        _invertedIndexMap.put(column, realtimeLuceneIndexReader);
        if (_realtimeLuceneReaders == null) {
          _realtimeLuceneReaders = new RealtimeLuceneReaders(_segmentName);
        }
        _realtimeLuceneReaders.addReader(realtimeLuceneIndexReader);
      }
    }

    if (_realtimeLuceneReaders != null) {
      // add the realtime lucene index readers to the global queue for refresh task to pick up
      RealtimeLuceneIndexRefreshState realtimeLuceneIndexRefreshState = RealtimeLuceneIndexRefreshState.getInstance();
      realtimeLuceneIndexRefreshState.addRealtimeReadersToQueue(_realtimeLuceneReaders);
    }

    // Metric aggregation can be enabled only if config is specified, and all dimensions have dictionary,
    // and no metrics have dictionary. If not enabled, the map returned is null.
    _recordIdMap = enableMetricsAggregationIfPossible(config, noDictionaryColumns);
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
      if (fieldSpec instanceof DimensionFieldSpec && _aggregateMetrics && (dataType == DataType.STRING
          || dataType == DataType.BYTES)) {
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
    Long minTime = extractTimeValue(getMinVal(_timeColumnName));
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
    Long maxTime = extractTimeValue(getMaxVal(_timeColumnName));
    if (maxTime != null) {
      return maxTime;
    }
    return Long.MIN_VALUE;
  }

  private Long extractTimeValue(Comparable time) {
    if (time != null) {
      if (time instanceof Number) {
        return ((Number) time).longValue();
      } else {
        String stringValue = time.toString();
        if (StringUtils.isNumeric(stringValue)) {
          return Long.parseLong(stringValue);
        }
      }
    }
    return null;
  }

  /**
   * Get min value of a column in the segment.
   * @param column
   * @return min value
   */
  public Comparable getMinVal(String column) {
    BaseMutableDictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      return dictionary.getMinVal();
    }
    return _minValueMap.get(column);
  }

  /**
   * Get max value of a column in the segment.
   * @param column
   * @return max value
   */
  public Comparable getMaxVal(String column) {
    BaseMutableDictionary dictionary = _dictionaryMap.get(column);
    if (dictionary != null) {
      return dictionary.getMaxVal();
    }
    return _maxValueMap.get(column);
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

  @Override
  public boolean index(GenericRow row, @Nullable RowMetadata rowMetadata) {
    boolean canTakeMore;
    // Update dictionary first
    Map<String, Object> dictIdMap = updateDictionary(row);

    int numDocs = _numDocsIndexed;

    // If metrics aggregation is enabled and if the dimension values were already seen, this will return existing docId,
    // else this will return a new docId.
    int docId = getOrCreateDocId(dictIdMap);

    // docId == numDocs implies new docId.
    if (docId == numDocs) {
      // Add forward and inverted indices for new document.
      addForwardIndex(row, docId, dictIdMap);
      addInvertedIndex(row, docId, dictIdMap);
      if (_nullHandlingEnabled) {
        handleNullValues(row, docId);
      }

      // Update number of document indexed at last to make the latest record queryable
      canTakeMore = _numDocsIndexed++ < _capacity;
    } else {
      Preconditions
          .checkState(_aggregateMetrics, "Invalid document-id during indexing: " + docId + " expected: " + numDocs);
      // Update metrics for existing document.
      canTakeMore = aggregateMetrics(row, docId);
    }

    _lastIndexedTimeMs = System.currentTimeMillis();

    if (rowMetadata != null && rowMetadata.getIngestionTimeMs() != Long.MIN_VALUE) {
      _latestIngestionTimeMs = Math.max(_latestIngestionTimeMs, rowMetadata.getIngestionTimeMs());
    }
    return canTakeMore;
  }

  private Map<String, Object> updateDictionary(GenericRow row) {
    Map<String, Object> dictIdMap = new HashMap<>();
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);

      BaseMutableDictionary dictionary = _dictionaryMap.get(column);
      if (dictionary != null) {
        if (fieldSpec.isSingleValueField()) {
          dictIdMap.put(column, dictionary.index(value));
        } else {
          int[] dictIds = dictionary.index((Object[]) value);
          dictIdMap.put(column, dictIds);

          // No need to update min/max time value as time column cannot be multi-valued
          continue;
        }
      }
    }
    return dictIdMap;
  }

  private void addForwardIndex(GenericRow row, int docId, Map<String, Object> dictIdMap) {
    // Store dictionary Id(s) for columns with dictionary
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);
      NumValuesInfo numValuesInfo = _numValuesInfoMap.get(column);
      if (fieldSpec.isSingleValueField()) {
        // SV column
        MutableForwardIndex mutableForwardIndex = _forwardIndexMap.get(column);
        Integer dictId = (Integer) dictIdMap.get(column);
        if (dictId != null) {
          // SV Column with dictionary
          mutableForwardIndex.setDictId(docId, dictId);
        } else {
          // No-dictionary SV column
          DataType dataType = fieldSpec.getDataType();
          switch (dataType) {
            case INT:
              mutableForwardIndex.setInt(docId, (Integer) value);
              break;
            case LONG:
              mutableForwardIndex.setLong(docId, (Long) value);
              break;
            case FLOAT:
              mutableForwardIndex.setFloat(docId, (Float) value);
              break;
            case DOUBLE:
              mutableForwardIndex.setDouble(docId, (Double) value);
              break;
            case STRING:
              mutableForwardIndex.setString(docId, (String) value);
              break;
            case BYTES:
              mutableForwardIndex.setBytes(docId, (byte[]) value);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
          }
        }

        numValuesInfo.updateSVEntry();
      } else {
        // MV column: always dictionary encoded
        int[] dictIds = (int[]) dictIdMap.get(column);
        _forwardIndexMap.get(column).setDictIdMV(docId, dictIds);

        numValuesInfo.updateMVEntry(dictIds.length);
      }

      // Update min/max value for no dictionary columns
      if ((_dictionaryMap.get(column) != null) || !(value instanceof Comparable)) {
        continue;
      }
      Comparable comparableValue = (Comparable) value;
      Comparable currentMinValue = _minValueMap.get(column);
      if (currentMinValue == null) {
        _minValueMap.put(column, comparableValue);
        _maxValueMap.put(column, comparableValue);
        continue;
      }
      if (comparableValue.compareTo(currentMinValue) < 0) {
        _minValueMap.put(column, comparableValue);
      }
      if (comparableValue.compareTo(_maxValueMap.get(column)) > 0) {
        _maxValueMap.put(column, comparableValue);
      }
    }
  }

  private void addInvertedIndex(GenericRow row, int docId, Map<String, Object> dictIdMap) {
    // Update inverted index at last
    // NOTE: inverted index have to be updated at last because once it gets updated, the latest record will become
    // queryable
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      InvertedIndexReader invertedIndex = _invertedIndexMap.get(column);
      if (invertedIndex != null) {
        if (invertedIndex instanceof RealtimeLuceneTextIndexReader) {
          ((RealtimeLuceneTextIndexReader) invertedIndex).addDoc(row.getValue(column), docId);
        } else {
          RealtimeInvertedIndexReader realtimeInvertedIndexReader = (RealtimeInvertedIndexReader) invertedIndex;
          if (fieldSpec.isSingleValueField()) {
            realtimeInvertedIndexReader.add(((Integer) dictIdMap.get(column)), docId);
          } else {
            int[] dictIds = (int[]) dictIdMap.get(column);
            for (int dictId : dictIds) {
              realtimeInvertedIndexReader.add(dictId, docId);
            }
          }
        }
      }
    }
  }

  /**
   * Check if the row has any null fields and update the
   * column null value vectors accordingly
   * @param row specifies row being ingested
   * @param docId specified docId for this row
   */
  private void handleNullValues(GenericRow row, int docId) {
    if (!row.hasNullValues()) {
      return;
    }

    for (String columnName : row.getNullValueFields()) {
      _nullValueVectorMap.get(columnName).setNull(docId);
    }
  }

  private boolean aggregateMetrics(GenericRow row, int docId) {
    for (MetricFieldSpec metricFieldSpec : _physicalMetricFieldSpecs) {
      String column = metricFieldSpec.getName();
      Object value = row.getValue(column);
      MutableForwardIndex mutableForwardIndex = _forwardIndexMap.get(column);
      DataType dataType = metricFieldSpec.getDataType();
      switch (dataType) {
        case INT:
          mutableForwardIndex.setInt(docId, (Integer) value + mutableForwardIndex.getInt(docId));
          break;
        case LONG:
          mutableForwardIndex.setLong(docId, (Long) value + mutableForwardIndex.getLong(docId));
          break;
        case FLOAT:
          mutableForwardIndex.setFloat(docId, (Float) value + mutableForwardIndex.getFloat(docId));
          break;
        case DOUBLE:
          mutableForwardIndex.setDouble(docId, (Double) value + mutableForwardIndex.getDouble(docId));
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
      }
    }
    return true;
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
        Preconditions.checkNotNull(fieldSpec, "FieldSpec for " + column + " should not be null");
      }
      // TODO: Refactor virtual column provider to directly generate data source
      VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, _numDocsIndexed);
      VirtualColumnProvider virtualColumnProvider = VirtualColumnProviderFactory.buildProvider(virtualColumnContext);
      return new ImmutableDataSource(virtualColumnProvider.buildMetadata(virtualColumnContext),
          virtualColumnProvider.buildColumnIndexContainer(virtualColumnContext));
    } else {
      PartitionFunction partitionFunction = null;
      int partitionId = 0;
      if (column.equals(_partitionColumn)) {
        partitionFunction = _partitionFunction;
        partitionId = _partitionId;
      }
      NumValuesInfo numValuesInfo = _numValuesInfoMap.get(column);
      MutableForwardIndex forwardIndex = _forwardIndexMap.get(column);
      BaseMutableDictionary dictionary = _dictionaryMap.get(column);
      InvertedIndexReader invertedIndex = _invertedIndexMap.get(column);
      InvertedIndexReader rangeIndex = _rangeIndexMap.get(column);
      BloomFilterReader bloomFilter = _bloomFilterMap.get(column);
      Comparable minValue = getMinVal(column);
      Comparable maxValue = getMaxVal(column);
      RealtimeNullValueVectorReaderWriter nullValueVector = _nullValueVectorMap.get(column);
      return new MutableDataSource(fieldSpec, _numDocsIndexed, numValuesInfo.getNumValues(),
          numValuesInfo.getMaxNumValuesPerMVEntry(), partitionFunction, partitionId, minValue, maxValue, forwardIndex,
          dictionary, invertedIndex, rangeIndex, bloomFilter, nullValueVector);
    }
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  /**
   * Returns a record that contains only physical columns
   * @param docId document ID
   * @param reuse a GenericRow object that will be re-used if provided. Otherwise, this method will allocate a new one
   * @return Generic row with physical columns of the specified row.
   */
  public GenericRow getRecord(int docId, GenericRow reuse) {
    for (FieldSpec fieldSpec : _physicalFieldSpecs) {
      String column = fieldSpec.getName();
      Object value = getValue(docId, _forwardIndexMap.get(column), _dictionaryMap.get(column),
          _numValuesInfoMap.get(column).getMaxNumValuesPerMVEntry());
      reuse.putValue(column, value);

      if (_nullHandlingEnabled) {
        NullValueVectorReader reader = _nullValueVectorMap.get(column);
        // If column has null value for this docId, set that accordingly in GenericRow
        if (reader.isNull(docId)) {
          reuse.putDefaultNullValue(column, value);
        }
      }
    }
    return reuse;
  }

  /**
   * Helper method to read the value for the given document id.
   */
  private static Object getValue(int docId, MutableForwardIndex forwardIndex,
      @Nullable BaseMutableDictionary dictionary, int maxNumMultiValues) {
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
        for (Map.Entry<String, BaseMutableDictionary> entry : _dictionaryMap.entrySet()) {
          String columnName = entry.getKey();
          BaseOffHeapMutableDictionary dictionary = (BaseOffHeapMutableDictionary) entry.getValue();
          RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
          columnStats.setCardinality(dictionary.length());
          columnStats.setAvgColumnSize(dictionary.getAvgValueSize());
          segmentStats.setColumnStats(columnName, columnStats);
        }
        segmentStats.setNumRowsConsumed(_numDocsIndexed);
        segmentStats.setNumRowsIndexed(_numDocsIndexed);
        segmentStats.setMemUsedBytes(totalMemBytes);
        segmentStats.setNumSeconds(numSeconds);
        _statsHistory.addSegmentStats(segmentStats);
      }
    }

    for (MutableForwardIndex mutableForwardIndex : _forwardIndexMap.values()) {
      try {
        mutableForwardIndex.close();
      } catch (IOException e) {
        _logger.error("Failed to close index. Service will continue with potential memory leak, error: ", e);
        // fall through to close other segments
      }
    }
    // clear map now that index is closed to prevent accidental usage
    _forwardIndexMap.clear();

    for (InvertedIndexReader index : _invertedIndexMap.values()) {
      if (index instanceof RealtimeInvertedIndexReader) {
        ((RealtimeInvertedIndexReader) index).close();
      }
    }
    _invertedIndexMap.clear();

    if (_realtimeLuceneReaders != null) {
      // set this to true as a way of signalling the refresh task thread to
      // not attempt refresh on this segment here onwards
      _realtimeLuceneReaders.getLock().lock();
      _realtimeLuceneReaders.setSegmentDestroyed();
      try {
        for (RealtimeLuceneTextIndexReader realtimeLuceneReader : _realtimeLuceneReaders.getRealtimeLuceneReaders()) {
          // close each realtime lucene reader for this segment
          realtimeLuceneReader.close();
        }
        // clear the list.
        _realtimeLuceneReaders.clearRealtimeReaderList();
      } finally {
        _realtimeLuceneReaders.getLock().unlock();
      }
    }

    for (Map.Entry<String, BaseMutableDictionary> entry : _dictionaryMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        _logger.error("Failed to close the dictionary for column: {}. Continuing with error.", entry.getKey(), e);
      }
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
    BaseMutableDictionary dictionary = _dictionaryMap.get(column);
    int numValues = dictionary.length();

    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      dictIds[i] = i;
    }

    IntArrays.quickSort(dictIds, (dictId1, dictId2) -> dictionary.compare(dictId1, dictId2));
    RealtimeInvertedIndexReader invertedIndex = (RealtimeInvertedIndexReader) _invertedIndexMap.get(column);
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

  private int getOrCreateDocId(Map<String, Object> dictIdMap) {
    if (!_aggregateMetrics) {
      return _numDocsIndexed;
    }

    int i = 0;
    int[] dictIds = new int[_numKeyColumns]; // dimensions + date time columns + time column.

    // FIXME: this for loop breaks for multi value dimensions. https://github.com/apache/incubator-pinot/issues/3867
    for (FieldSpec fieldSpec : _physicalDimensionFieldSpecs) {
      dictIds[i++] = (Integer) dictIdMap.get(fieldSpec.getName());
    }
    for (String timeColumnName : _physicalTimeColumnNames) {
      dictIds[i++] = (Integer) dictIdMap.get(timeColumnName);
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

    int getNumValues() {
      return _numValues;
    }

    int getMaxNumValuesPerMVEntry() {
      return _maxNumValuesPerMVEntry;
    }
  }
}
