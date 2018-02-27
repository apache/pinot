/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.realtime.impl;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.realtime.RealtimeSegment;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryFactory;
import com.linkedin.pinot.core.realtime.impl.invertedindex.RealtimeInvertedIndexReader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import com.linkedin.pinot.core.startree.StarTree;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.roaringbitmap.IntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeSegmentImpl implements RealtimeSegment {
  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;

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

  private volatile int _numDocsIndexed = 0;

  // to compute the rolling interval
  private volatile long _minTime = Long.MAX_VALUE;
  private volatile long _maxTime = Long.MIN_VALUE;

  public RealtimeSegmentImpl(RealtimeSegmentConfig config) {
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

    _logger = LoggerFactory.getLogger(
        RealtimeSegmentImpl.class.getName() + "_" + _segmentName + "_" + config.getStreamName());

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
                _statsHistory.getEstimatedCardinality(column), allocationContext);
        _dictionaryMap.put(column, dictionary);
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
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);
      MutableDictionary dictionary = _dictionaryMap.get(column);
      if (dictionary != null) {
        dictionary.index(value);
      }
      // Update max number of values for multi-value column
      if (!fieldSpec.isSingleValueField()) {
        int numValues = ((Object[]) value).length;
        if (_maxNumValuesMap.get(column) < numValues) {
          _maxNumValuesMap.put(column, numValues);
        }
        continue;
      }
      // Update min/max value for time column
      if (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) {
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

    // Update forward index
    int docId = _numDocsIndexed;
    // Store dictionary Id(s) for columns with dictionary
    Map<String, Object> dictIdMap = new HashMap<>();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      Object value = row.getValue(column);
      if (fieldSpec.isSingleValueField()) {
        FixedByteSingleColumnSingleValueReaderWriter indexReaderWriter =
            (FixedByteSingleColumnSingleValueReaderWriter) _indexReaderWriterMap.get(column);
        MutableDictionary dictionary = _dictionaryMap.get(column);
        if (dictionary != null) {
          // Column with dictionary
          int dictId = dictionary.indexOf(value);
          indexReaderWriter.setInt(docId, dictId);
          dictIdMap.put(column, dictId);
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
        Object[] values = (Object[]) value;
        int numValues = values.length;
        int[] dictIds = new int[numValues];
        for (int i = 0; i < numValues; i++) {
          dictIds[i] = _dictionaryMap.get(column).indexOf(values[i]);
        }
        ((FixedByteSingleColumnMultiValueReaderWriter) _indexReaderWriterMap.get(column)).setIntArray(docId, dictIds);
        dictIdMap.put(column, dictIds);
      }
    }

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

    // Update number of document indexed at last to make the latest record queryable
    return _numDocsIndexed++ < _capacity;
  }

  @Override
  public int getNumDocsIndexed() {
    return _numDocsIndexed;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      DataFileReader indexReaderWriter = _indexReaderWriterMap.get(column);
      MutableDictionary dictionary = _dictionaryMap.get(column);
      if (dictionary != null) {
        // Column with dictionary
        if (fieldSpec.isSingleValueField()) {
          int dictId = ((FixedByteSingleColumnSingleValueReaderWriter) indexReaderWriter).getInt(docId);
          reuse.putField(column, dictionary.get(dictId));
        } else {
          int[] dictIds = new int[_maxNumValuesMap.get(column)];
          int numValues = ((FixedByteSingleColumnMultiValueReaderWriter) indexReaderWriter).getIntArray(docId, dictIds);
          Object[] values = new Object[numValues];
          for (int i = 0; i < numValues; i++) {
            values[i] = dictionary.get(dictIds[i]);
          }
          reuse.putField(column, values);
        }
      } else {
        // No-dictionary column
        FixedByteSingleColumnSingleValueReaderWriter singleValueReaderWriter =
            (FixedByteSingleColumnSingleValueReaderWriter) indexReaderWriter;
        FieldSpec.DataType dataType = fieldSpec.getDataType();
        switch (dataType) {
          case INT:
            reuse.putField(column, singleValueReaderWriter.getInt(docId));
            break;
          case LONG:
            reuse.putField(column, singleValueReaderWriter.getLong(docId));
            break;
          case FLOAT:
            reuse.putField(column, singleValueReaderWriter.getFloat(docId));
            break;
          case DOUBLE:
            reuse.putField(column, singleValueReaderWriter.getDouble(docId));
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported data type: " + dataType + " for no-dictionary column: " + column);
        }
      }
    }
    return reuse;
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
  public ColumnDataSource getDataSource(String columnName) {
    return new ColumnDataSource(_schema.getFieldSpecFor(columnName), _numDocsIndexed, _maxNumValuesMap.get(columnName),
        _indexReaderWriterMap.get(columnName), _invertedIndexMap.get(columnName), _dictionaryMap.get(columnName));
  }

  @Override
  public StarTree getStarTree() {
    return null;
  }

  @Override
  public long getDiskSizeBytes() {
    return 0;
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
        segmentStats.setMemUsedBytes(totalMemBytes);
        segmentStats.setNumSeconds(numSeconds);
        _statsHistory.addSegmentStats(segmentStats);
        _statsHistory.save();
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
}
