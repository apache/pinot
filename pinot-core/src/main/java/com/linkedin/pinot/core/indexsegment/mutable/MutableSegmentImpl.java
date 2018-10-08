/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.indexsegment.mutable;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegmentUtils;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentConfig;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentStatsHistory;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryFactory;
import com.linkedin.pinot.core.realtime.impl.invertedindex.RealtimeInvertedIndexReader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import com.linkedin.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import com.linkedin.pinot.core.segment.virtualcolumn.VirtualColumnProvider;
import com.linkedin.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import com.linkedin.pinot.core.util.FixedIntArray;
import com.linkedin.pinot.core.util.FixedIntArrayOffHeapIdMap;
import com.linkedin.pinot.core.util.IdMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private final Logger _logger;
  private final long _startTimeMillis = System.currentTimeMillis();

  private final String _segmentName;
  private final Schema _schema;
  private final int _capacity;
  private final SegmentMetadata _segmentMetadata;
  private final boolean _offHeap;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final RealtimeSegmentStatsHistory _statsHistory;
  private final SegmentPartitionConfig _segmentPartitionConfig;

  private final Map<String, MutableDictionary> _dictionaryMap = new HashMap<>();
  private final Map<String, DataFileReader> _indexReaderWriterMap = new HashMap<>();
  private final Map<String, Integer> _maxNumValuesMap = new HashMap<>();
  private final Map<String, RealtimeInvertedIndexReader> _invertedIndexMap = new HashMap<>();
  private final IdMap<FixedIntArray> _recordIdMap;
  private boolean _aggregateMetrics;

  private volatile int _numDocsIndexed = 0;

  // to compute the rolling interval
  private volatile long _minTime = Long.MAX_VALUE;
  private volatile long _maxTime = Long.MIN_VALUE;
  private final int _numKeyColumns;

  public MutableSegmentImpl(RealtimeSegmentConfig config) {
    _segmentName = config.getSegmentName();
    _schema = config.getSchema();
    _capacity = config.getCapacity();
    _segmentMetadata = new SegmentMetadataImpl(config.getRealtimeSegmentZKMetadata(), _schema) {
      @Override
      public int getTotalDocs() {
        return _numDocsIndexed;
      }

      @Override
      public int getTotalRawDocs() {
        // In realtime total docs and total raw docs are the same currently.
        return _numDocsIndexed;
      }
    };

    _offHeap = config.isOffHeap();
    _memoryManager = config.getMemoryManager();
    _statsHistory = config.getStatsHistory();
    _segmentPartitionConfig = config.getSegmentPartitionConfig();
    _numKeyColumns = _schema.getDimensionNames().size() + 1;

    _logger = LoggerFactory.getLogger(
        MutableSegmentImpl.class.getName() + "_" + _segmentName + "_" + config.getStreamName());

    Set<String> noDictionaryColumns = config.getNoDictionaryColumns();

    Set<String> invertedIndexColumns = config.getInvertedIndexColumns();
    int avgNumMultiValues = config.getAvgNumMultiValues();

    // Initialize for each column
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      _maxNumValuesMap.put(column, 0);

      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value non-string columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index
      FieldSpec.DataType dataType = fieldSpec.getDataType();
      int indexColumnSize = FieldSpec.DataType.INT.size();
      if (noDictionaryColumns.contains(column) && fieldSpec.isSingleValueField()
          && dataType != FieldSpec.DataType.STRING && !invertedIndexColumns.contains(column)) {
        // No dictionary
        indexColumnSize = dataType.size();
      } else {
        int dictionaryColumnSize;
        if (dataType == FieldSpec.DataType.STRING) {
          dictionaryColumnSize = _statsHistory.getEstimatedAvgColSize(column);
        } else {
          dictionaryColumnSize = dataType.size();
        }
        String allocationContext = buildAllocationContext(_segmentName, column, V1Constants.Dict.FILE_EXTENSION);
        MutableDictionary dictionary =
            MutableDictionaryFactory.getMutableDictionary(dataType, _offHeap, _memoryManager, dictionaryColumnSize,
                Math.min(_statsHistory.getEstimatedCardinality(column), _capacity), allocationContext);
        _dictionaryMap.put(column, dictionary);

        // Even though the column is defined as 'no-dictionary' in the config, we did create dictionary for consuming segment.
        noDictionaryColumns.remove(column);
      }

      DataFileReader indexReaderWriter;
      if (fieldSpec.isSingleValueField()) {
        String allocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        indexReaderWriter = new FixedByteSingleColumnSingleValueReaderWriter(_capacity, indexColumnSize, _memoryManager,
            allocationContext);
      } else {
        // TODO: Start with a smaller capacity on FixedByteSingleColumnMultiValueReaderWriter and let it expand
        String allocationContext =
            buildAllocationContext(_segmentName, column, V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
        indexReaderWriter =
            new FixedByteSingleColumnMultiValueReaderWriter(MAX_MULTI_VALUES_PER_ROW, avgNumMultiValues, _capacity,
                indexColumnSize, _memoryManager, allocationContext);
      }
      _indexReaderWriterMap.put(column, indexReaderWriter);

      if (invertedIndexColumns.contains(column)) {
        _invertedIndexMap.put(column, new RealtimeInvertedIndexReader());
      }
    }

    // Metric aggregation can be enabled only if config is specified, and all dimensions have dictionary,
    // and no metrics have dictionary. If not enabled, the map returned is null.
    _recordIdMap = enableMetricsAggregationIfPossible(config, _schema, noDictionaryColumns);
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    return _segmentPartitionConfig;
  }

  public long getMinTime() {
    return _minTime;
  }

  public long getMaxTime() {
    return _maxTime;
  }

  @Override
  public boolean index(GenericRow row) {
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
      addInvertedIndex(docId, dictIdMap);
      // Update number of document indexed at last to make the latest record queryable
      return _numDocsIndexed++ < _capacity;
    } else {
      Preconditions.checkState(_aggregateMetrics,
          "Invalid document-id during indexing: " + docId + " expected: " + numDocs);
      // Update metrics for existing document.
      return aggregateMetrics(row, docId);
    }
  }

  private Map<String, Object> updateDictionary(GenericRow row) {
    Map<String, Object> dictIdMap = new HashMap<>();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);
      MutableDictionary dictionary = _dictionaryMap.get(column);
      if (dictionary != null) {
        dictionary.index(value);
      }
      // Update max number of values for multi-value column
      if (fieldSpec.isSingleValueField()) {
        if (dictionary != null) {
          dictIdMap.put(column, dictionary.indexOf(value));
        }
      } else {
        // No-dictionary not supported for multi-valued columns.
        int i = 0;
        Object[] values = (Object[]) value;
        int[] dictIds = new int[values.length];
        for (Object object : values) {
          dictIds[i++] = dictionary.indexOf(object);
        }
        dictIdMap.put(column, dictIds);
        int numValues = ((Object[]) value).length;
        if (_maxNumValuesMap.get(column) < numValues) {
          _maxNumValuesMap.put(column, numValues);
        }
        continue;
      }
      // Update min/max value for time column
      if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.TIME)) {
        long timeValue;
        if (value instanceof Number) {
          timeValue = ((Number) value).longValue();
        } else {
          timeValue = Long.valueOf(value.toString());
        }
        _minTime = Math.min(_minTime, timeValue);
        _maxTime = Math.max(_maxTime, timeValue);
      }
    }
    return dictIdMap;
  }

  private void addForwardIndex(GenericRow row, int docId, Map<String, Object> dictIdMap) {
    // Store dictionary Id(s) for columns with dictionary
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);
      if (fieldSpec.isSingleValueField()) {
        FixedByteSingleColumnSingleValueReaderWriter indexReaderWriter =
            (FixedByteSingleColumnSingleValueReaderWriter) _indexReaderWriterMap.get(column);
        Integer dictId = (Integer) dictIdMap.get(column);
        if (dictId != null) {
          // Column with dictionary
          indexReaderWriter.setInt(docId, dictId);
        } else {
          // No-dictionary column
          FieldSpec.DataType dataType = fieldSpec.getDataType();
          switch (dataType) {
            case INT:
              indexReaderWriter.setInt(docId, (Integer) value);
              break;
            case LONG:
              indexReaderWriter.setLong(docId, (Long) value);
              break;
            case FLOAT:
              indexReaderWriter.setFloat(docId, (Float) value);
              break;
            case DOUBLE:
              indexReaderWriter.setDouble(docId, (Double) value);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
          }
        }
      } else {
        int[] dictIds = (int[]) dictIdMap.get(column);
        ((FixedByteSingleColumnMultiValueReaderWriter) _indexReaderWriterMap.get(column)).setIntArray(docId, dictIds);
      }
    }
  }

  private void addInvertedIndex(int docId, Map<String, Object> dictIdMap) {
    // Update inverted index at last
    // NOTE: inverted index have to be updated at last because once it gets updated, the latest record will become
    // queryable
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      RealtimeInvertedIndexReader invertedIndex = _invertedIndexMap.get(column);
      if (invertedIndex != null) {
        if (fieldSpec.isSingleValueField()) {
          invertedIndex.add(((Integer) dictIdMap.get(column)), docId);
        } else {
          int[] dictIds = (int[]) dictIdMap.get(column);
          for (int dictId : dictIds) {
            invertedIndex.add(dictId, docId);
          }
        }
      }
    }
  }

  private boolean aggregateMetrics(GenericRow row, int docId) {
    for (FieldSpec metricSpec : _schema.getMetricFieldSpecs()) {
      String column = metricSpec.getName();
      Object value = row.getValue(column);
      Preconditions.checkState(metricSpec.isSingleValueField(), "Multivalued metrics cannot be updated.");
      FixedByteSingleColumnSingleValueReaderWriter indexReaderWriter =
          (FixedByteSingleColumnSingleValueReaderWriter) _indexReaderWriterMap.get(column);
      Preconditions.checkState(_dictionaryMap.get(column) == null, "Updating metrics not supported with dictionary.");
      FieldSpec.DataType dataType = metricSpec.getDataType();
      switch (dataType) {
        case INT:
          indexReaderWriter.setInt(docId, (Integer) value + indexReaderWriter.getInt(docId));
          break;
        case LONG:
          indexReaderWriter.setLong(docId, (Long) value + indexReaderWriter.getLong(docId));
          break;
        case FLOAT:
          indexReaderWriter.setFloat(docId, indexReaderWriter.getFloat(docId) + indexReaderWriter.getFloat(docId));
          break;
        case DOUBLE:
          indexReaderWriter.setDouble(docId, indexReaderWriter.getDouble(docId) + indexReaderWriter.getDouble(docId));
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
    return _schema.getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    HashSet<String> physicalColumnNames = new HashSet<>();

    for (String columnName : getColumnNames()) {
      if (!_segmentMetadata.getSchema().isVirtualColumn(columnName)) {
        physicalColumnNames.add(columnName);
      }
    }

    return physicalColumnNames;
  }

  @Override
  public ColumnDataSource getDataSource(String columnName) {
    if (!_schema.isVirtualColumn(columnName)) {
      return new ColumnDataSource(_schema.getFieldSpecFor(columnName), _numDocsIndexed, _maxNumValuesMap.get(columnName),
          _indexReaderWriterMap.get(columnName), _invertedIndexMap.get(columnName), _dictionaryMap.get(columnName));
    } else {
      return getVirtualDataSource(columnName);
    }
  }

  private ColumnDataSource getVirtualDataSource(String column) {
    VirtualColumnContext virtualColumnContext = new VirtualColumnContext(NetUtil.getHostnameOrAddress(), _segmentMetadata.getTableName(), getSegmentName(),
        column, _numDocsIndexed + 1);
    VirtualColumnProvider provider = VirtualColumnProviderFactory.buildProvider(_schema.getFieldSpecFor(column).getVirtualColumnProvider());
    return new ColumnDataSource(provider.buildColumnIndexContainer(virtualColumnContext), provider.buildMetadata(virtualColumnContext));
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      reuse.putField(column,
          IndexSegmentUtils.getValue(docId, fieldSpec, _indexReaderWriterMap.get(column), _dictionaryMap.get(column),
              _maxNumValuesMap.getOrDefault(column, 0)));
    }
    return reuse;
  }

  @Override
  public void destroy() {
    _logger.info("Trying to close RealtimeSegmentImpl : {}", _segmentName);

    // Gather statistics for off-heap mode
    if (_offHeap) {
      if (_numDocsIndexed > 0) {
        int numSeconds = (int) ((System.currentTimeMillis() - _startTimeMillis) / 1000);
        long totalMemBytes = _memoryManager.getTotalAllocatedBytes();
        _logger.info("Segment used {} bytes of memory for {} rows consumed in {} seconds", totalMemBytes,
            _numDocsIndexed, numSeconds);

        RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
        for (Map.Entry<String, MutableDictionary> entry : _dictionaryMap.entrySet()) {
          RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
          columnStats.setCardinality(entry.getValue().length());
          columnStats.setAvgColumnSize(entry.getValue().getAvgValueSize());
          segmentStats.setColumnStats(entry.getKey(), columnStats);
        }
        segmentStats.setNumRowsConsumed(_numDocsIndexed);
        segmentStats.setNumRowsIndexed(_numDocsIndexed);
        segmentStats.setMemUsedBytes(totalMemBytes);
        segmentStats.setNumSeconds(numSeconds);
        _statsHistory.addSegmentStats(segmentStats);
      }
    }

    for (DataFileReader dfReader : _indexReaderWriterMap.values()) {
      try {
        dfReader.close();
      } catch (IOException e) {
        _logger.error("Failed to close index. Service will continue with potential memory leak, error: ", e);
        // fall through to close other segments
      }
    }
    // clear map now that index is closed to prevent accidental usage
    _indexReaderWriterMap.clear();

    for (RealtimeInvertedIndexReader index : _invertedIndexMap.values()) {
      index.close();
    }

    for (Map.Entry<String, MutableDictionary> entry : _dictionaryMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        _logger.error("Could not close dictionary for column {}", entry.getKey());
      }
    }
    _invertedIndexMap.clear();
    _segmentMetadata.close();
    try {
      _memoryManager.close();
    } catch (IOException e) {
      _logger.error("Could not close memory manager", e);
    }

    // Clear the recordId map.
    if (_recordIdMap != null) {
      _recordIdMap.clear();
    }
  }

  private IntIterator[] getSortedBitmapIntIteratorsForIntColumn(String column) {
    MutableDictionary dictionary = _dictionaryMap.get(column);
    int numValues = dictionary.length();
    IntIterator[] intIterators = new IntIterator[numValues];
    RealtimeInvertedIndexReader invertedIndex = _invertedIndexMap.get(column);

    int[] values = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (Integer) dictionary.get(i);
    }

    long start = System.currentTimeMillis();
    Arrays.sort(values);
    _logger.info("Spent {}ms sorting int column: {} with cardinality: {}", System.currentTimeMillis() - start, column,
        numValues);

    for (int i = 0; i < numValues; i++) {
      intIterators[i] = invertedIndex.getDocIds(dictionary.indexOf(values[i])).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForLongColumn(String column) {
    MutableDictionary dictionary = _dictionaryMap.get(column);
    int numValues = dictionary.length();
    IntIterator[] intIterators = new IntIterator[numValues];
    RealtimeInvertedIndexReader invertedIndex = _invertedIndexMap.get(column);

    long[] values = new long[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (Long) dictionary.get(i);
    }

    long start = System.currentTimeMillis();
    Arrays.sort(values);
    _logger.info("Spent {}ms sorting long column: {} with cardinality: {}", System.currentTimeMillis() - start, column,
        numValues);

    for (int i = 0; i < numValues; i++) {
      intIterators[i] = invertedIndex.getDocIds(dictionary.indexOf(values[i])).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForFloatColumn(String column) {
    MutableDictionary dictionary = _dictionaryMap.get(column);
    int numValues = dictionary.length();
    IntIterator[] intIterators = new IntIterator[numValues];
    RealtimeInvertedIndexReader invertedIndex = _invertedIndexMap.get(column);

    float[] values = new float[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (Float) dictionary.get(i);
    }

    long start = System.currentTimeMillis();
    Arrays.sort(values);
    _logger.info("Spent {}ms sorting float column: {} with cardinality: {}", System.currentTimeMillis() - start, column,
        numValues);

    for (int i = 0; i < numValues; i++) {
      intIterators[i] = invertedIndex.getDocIds(dictionary.indexOf(values[i])).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForDoubleColumn(String column) {
    MutableDictionary dictionary = _dictionaryMap.get(column);
    int numValues = dictionary.length();
    IntIterator[] intIterators = new IntIterator[numValues];
    RealtimeInvertedIndexReader invertedIndex = _invertedIndexMap.get(column);

    double[] values = new double[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (Double) dictionary.get(i);
    }

    long start = System.currentTimeMillis();
    Arrays.sort(values);
    _logger.info("Spent {}ms sorting double column: {} with cardinality: {}", System.currentTimeMillis() - start,
        column, numValues);

    for (int i = 0; i < numValues; i++) {
      intIterators[i] = invertedIndex.getDocIds(dictionary.indexOf(values[i])).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForStringColumn(String column) {
    MutableDictionary dictionary = _dictionaryMap.get(column);
    int numValues = dictionary.length();
    IntIterator[] intIterators = new IntIterator[numValues];
    RealtimeInvertedIndexReader invertedIndex = _invertedIndexMap.get(column);

    String[] values = new String[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (String) dictionary.get(i);
    }

    long start = System.currentTimeMillis();
    Arrays.sort(values);
    _logger.info("Spent {}ms sorting string column: {} with cardinality: {}", System.currentTimeMillis() - start,
        column, numValues);

    for (int i = 0; i < numValues; i++) {
      intIterators[i] = invertedIndex.getDocIds(dictionary.indexOf(values[i])).getIntIterator();
    }
    return intIterators;
  }

  /**
   * Returns the docIds to use for iteration when the data is sorted by the given column.
   * <p>Called only by realtime record reader.
   *
   * @param column The column to use for sorting
   * @return The docIds to use for iteration
   */
  public int[] getSortedDocIdIterationOrderWithSortedColumn(String column) {
    int[] docIds = new int[_numDocsIndexed];

    // Get docId iterators that iterate in order on the data
    IntIterator[] iterators;
    FieldSpec.DataType dataType = _schema.getFieldSpecFor(column).getDataType();
    switch (dataType) {
      case INT:
        iterators = getSortedBitmapIntIteratorsForIntColumn(column);
        break;
      case LONG:
        iterators = getSortedBitmapIntIteratorsForLongColumn(column);
        break;
      case FLOAT:
        iterators = getSortedBitmapIntIteratorsForFloatColumn(column);
        break;
      case DOUBLE:
        iterators = getSortedBitmapIntIteratorsForDoubleColumn(column);
        break;
      case STRING:
        iterators = getSortedBitmapIntIteratorsForStringColumn(column);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType + " for sorted column: " + column);
    }

    // Drain the iterators into the docIds array
    int i = 0;
    for (IntIterator iterator : iterators) {
      while (iterator.hasNext()) {
        docIds[i++] = iterator.next();
      }
    }

    // Sanity check
    Preconditions.checkState(_numDocsIndexed == i,
        "The number of docs indexed: %s is not equal to the number of sorted documents: %s", _numDocsIndexed, i);

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
    int[] dictIds = new int[_numKeyColumns]; // dimensions + time column.

    for (String column : _schema.getDimensionNames()) {
      dictIds[i++] = (Integer) dictIdMap.get(column);
    }

    String timeColumnName = _schema.getTimeColumnName();
    if (timeColumnName != null) {
      dictIds[i] = (Integer) dictIdMap.get(timeColumnName);
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
   * </ul>
   *
   * TODO: Eliminate the requirement on dictionary encoding for dimension and metric columns.
   *
   * @param config Segment config.
   * @param schema Schema for the table.
   * @param noDictionaryColumns Set of no dictionary columns.
   *
   * @return Map from dictionary id array to doc id, null if metrics aggregation cannot be enabled.
   */
  private IdMap<FixedIntArray> enableMetricsAggregationIfPossible(RealtimeSegmentConfig config, Schema schema,
      Set<String> noDictionaryColumns) {
    _aggregateMetrics = config.aggregateMetrics();
    if (!_aggregateMetrics) {
      _logger.info("Metrics aggregation is disabled.");
      return null;
    }

    // All metric columns should have no-dictionary index.
    for (String metric : schema.getMetricNames()) {
      if (!noDictionaryColumns.contains(metric)) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of dictionary encoded metrics, eg: {}",
            metric);
        _aggregateMetrics = false;
        break;
      }
    }

    // All dimension columns should be dictionary encoded.
    for (String dimension : schema.getDimensionNames()) {
      if (noDictionaryColumns.contains(dimension)) {
        _logger.warn("Metrics aggregation cannot be turned ON in presence of no-dictionary dimensions, eg: {}",
            dimension);
        _aggregateMetrics = false;
        break;
      }
    }

    // Time column should be dictionary encoded.
    String timeColumn = schema.getTimeColumnName();
    if (noDictionaryColumns.contains(timeColumn)) {
      _logger.warn("Metrics aggregation cannot be turned ON in presence of no-dictionary time column, eg: {}",
          timeColumn);
      _aggregateMetrics = false;
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
}
