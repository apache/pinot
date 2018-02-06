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

import com.linkedin.pinot.common.config.SegmentPartitionConfig;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.readerwriter.RealtimeIndexOffHeapMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnMultiValueReaderWriter;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.realtime.RealtimeSegment;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryFactory;
import com.linkedin.pinot.core.realtime.impl.invertedindex.RealtimeInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.startree.StarTree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.roaringbitmap.IntIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RealtimeSegmentImpl implements RealtimeSegment {
  private final Logger LOGGER;
  public static final int[] EMPTY_DICTIONARY_IDS_ARRAY = new int[0];

  private static final String MMGR_DICTIONARY_SUFFIX = ".dict";
  private static final String MMGR_FWDINDEX_SUFFIX = ".fwd";

  private final String _segmentName;
  private final Schema _schema;
  private final String _timeColumn;

  private final Map<String, DataFileReader> columnIndexReaderWriterMap = new HashMap<>();
  private final Map<String, Integer> maxNumberOfMultivaluesMap = new HashMap<>();
  private final Map<String, RealtimeInvertedIndexReader> invertedIndexMap = new HashMap<>();
  private final Map<String, MutableDictionary> dictionaryMap = new HashMap<>();

  private volatile SegmentMetadata _segmentMetadata;
  private volatile SegmentPartitionConfig _segmentPartitionConfig;
  private volatile int _numDocsIndexed = 0;

  // to compute the rolling interval
  private volatile long minTimeVal = Long.MAX_VALUE;
  private volatile long maxTimeVal = Long.MIN_VALUE;

  private final int capacity;
  private final ServerMetrics serverMetrics;
  private final String tableAndStreamName;
  private final List<String> consumingNoDictionaryColumns = new ArrayList<>();
  private final RealtimeIndexOffHeapMemoryManager memoryManager;
  private final boolean isOffHeapAllocation;
  private final RealtimeSegmentStatsHistory statsHistory;
  private final long startTimeMillis = System.currentTimeMillis();

  // TODO Dynamcally adjust these variables, maybe on a per column basis

  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;

  public RealtimeSegmentImpl(ServerMetrics serverMetrics, RealtimeSegmentDataManager segmentDataManager,
      IndexLoadingConfig indexLoadingConfig, int capacity, String streamName) {
    // initial variable setup
    _segmentName = segmentDataManager.getSegmentName();
    this.serverMetrics = serverMetrics;
    LOGGER = LoggerFactory.getLogger(RealtimeSegmentImpl.class.getName() + "_" + _segmentName + "_" + streamName);
    _schema = segmentDataManager.getSchema();
    _timeColumn = _schema.getTimeColumnName();
    final List<String> noDictionaryColumns = segmentDataManager.getNoDictionaryColumns();
    final List<String> invertedIndexColumns = segmentDataManager.getInvertedIndexColumns();
    final String tableName = segmentDataManager.getTableName();
    final int avgMultiValueCount = indexLoadingConfig.getRealtimeAvgMultiValueCount();
    statsHistory = segmentDataManager.getStatsHistory();
    isOffHeapAllocation = indexLoadingConfig.isRealtimeOffheapAllocation();
    memoryManager = segmentDataManager.getMemoryManager();
    this.capacity = capacity;
    tableAndStreamName = tableName + "-" + streamName;

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String columnName = fieldSpec.getName();
      maxNumberOfMultivaluesMap.put(columnName, 0);

      // Check whether to generate raw index for the column while consuming
      // Only support generating raw index on single-value non-string columns that do not have inverted index while
      // consuming. After consumption completes and the segment is built, all single-value columns can have raw index
      FieldSpec.DataType dataType = fieldSpec.getDataType();
      int indexColumnSize = FieldSpec.DataType.INT.size();
      if (noDictionaryColumns.contains(columnName) && fieldSpec.isSingleValueField()
          && dataType != FieldSpec.DataType.STRING && !invertedIndexColumns.contains(columnName)) {
        consumingNoDictionaryColumns.add(columnName);
        indexColumnSize = dataType.size();
      } else {
        int dictionaryColumnSize;
        if (dataType == FieldSpec.DataType.STRING) {
          dictionaryColumnSize = statsHistory.getEstimatedAvgColSize(columnName);
        } else {
          dictionaryColumnSize = dataType.size();
        }
        MutableDictionary dictionary =
            MutableDictionaryFactory.getMutableDictionary(dataType, isOffHeapAllocation, memoryManager,
                dictionaryColumnSize, statsHistory.getEstimatedCardinality(columnName),
                columnName + MMGR_DICTIONARY_SUFFIX);
        dictionaryMap.put(columnName, dictionary);
      }

      DataFileReader dataFileReader;
      if (fieldSpec.isSingleValueField()) {
        dataFileReader = new FixedByteSingleColumnSingleValueReaderWriter(capacity, indexColumnSize, memoryManager,
            columnName + MMGR_FWDINDEX_SUFFIX);
      } else {
        // TODO: Start with a smaller capacity on FixedByteSingleColumnMultiValueReaderWriter and let it expand
        dataFileReader =
            new FixedByteSingleColumnMultiValueReaderWriter(MAX_MULTI_VALUES_PER_ROW, avgMultiValueCount, capacity,
                indexColumnSize, memoryManager, columnName + MMGR_FWDINDEX_SUFFIX);
      }
      columnIndexReaderWriterMap.put(columnName, dataFileReader);

      if (invertedIndexColumns.contains(columnName)) {
        invertedIndexMap.put(columnName, new RealtimeInvertedIndexReader());
      }
    }
  }

  public long getMinTime() {
    return minTimeVal;
  }

  public long getMaxTime() {
    return maxTimeVal;
  }

  @Override
  public boolean index(GenericRow row) {
    // Validate row prior to indexing it
    StringBuilder invalidColumns = null;

    for (String column : _schema.getColumnNames()) {
      if (row.getValue(column) == null) {
        if (invalidColumns == null) {
          invalidColumns = new StringBuilder(column);
        } else {
          invalidColumns.append(", ").append(column);
        }
      }
    }

    if (invalidColumns != null) {
      LOGGER.warn("Dropping invalid row {} with null values for column(s) {}", row, invalidColumns);
      serverMetrics.addMeteredTableValue(tableAndStreamName, ServerMeter.INVALID_REALTIME_ROWS_DROPPED, 1L);
      return true;
    }

    // updating dictionary for dimensions only
    // its ok to insert this first
    // since filtering won't return back anything unless a new entry is made in the inverted index
    for (String dimension : _schema.getDimensionNames()) {
      if (!consumingNoDictionaryColumns.contains(dimension)) {
        dictionaryMap.get(dimension).index(row.getValue(dimension));
      }
      if (!_schema.getFieldSpecFor(dimension).isSingleValueField()) {
        Object[] entries = (Object[]) row.getValue(dimension);
        if ((entries != null) && (maxNumberOfMultivaluesMap.get(dimension) < entries.length)) {
          maxNumberOfMultivaluesMap.put(dimension, entries.length);
        }
      }
    }

    for (String metric : _schema.getMetricNames()) {
      if (!consumingNoDictionaryColumns.contains(metric)) {
        dictionaryMap.get(metric).index(row.getValue(metric));
      }
    }

    // Conversion already happens in PlainFieldExtractor
    Object timeValueObj = row.getValue(_timeColumn);

    long timeValue;
    if (timeValueObj instanceof Number) {
      timeValue = ((Number) timeValueObj).longValue();
    } else {
      timeValue = Long.valueOf(timeValueObj.toString());
    }

    dictionaryMap.get(_timeColumn).index(timeValueObj);

    // update the min max time values
    minTimeVal = Math.min(minTimeVal, timeValue);
    maxTimeVal = Math.max(maxTimeVal, timeValue);

    // also lets collect all dicIds to update inverted index later
    Map<String, Object> rawRowToDicIdMap = new HashMap<String, Object>();

    // lets update forward index now
    int docId = _numDocsIndexed;

    for (String dimension : _schema.getDimensionNames()) {
      if (_schema.getFieldSpecFor(dimension).isSingleValueField()) {
        storeIncomingColumnValue(row, rawRowToDicIdMap, docId, dimension);
      } else {
        Object[] mValues = (Object[]) row.getValue(dimension);
        int[] dicIds;

        if (mValues != null) {
          dicIds = new int[mValues.length];
          for (int i = 0; i < dicIds.length; i++) {
            dicIds[i] = dictionaryMap.get(dimension).indexOf(mValues[i]);
          }
        } else {
          dicIds = EMPTY_DICTIONARY_IDS_ARRAY;
        }

        ((FixedByteSingleColumnMultiValueReaderWriter) columnIndexReaderWriterMap.get(dimension)).setIntArray(docId,
            dicIds);
        rawRowToDicIdMap.put(dimension, dicIds);
      }
    }

    for (String metric : _schema.getMetricNames()) {
      storeIncomingColumnValue(row, rawRowToDicIdMap, docId, metric);
    }

    int timeDicId = dictionaryMap.get(_timeColumn).indexOf(timeValueObj);

    ((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(_timeColumn)).setInt(
        docId, timeDicId);
    rawRowToDicIdMap.put(_timeColumn, timeDicId);

    // lets update the inverted index now
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      RealtimeInvertedIndexReader invertedIndex = invertedIndexMap.get(column);
      if (invertedIndex != null) {
        if (fieldSpec.isSingleValueField()) {
          invertedIndex.add(((Integer) rawRowToDicIdMap.get(column)), docId);
        } else {
          int[] dictIds = (int[]) rawRowToDicIdMap.get(column);
          for (int dictId : dictIds) {
            invertedIndex.add(dictId, docId);
          }
        }
      }
    }

    return _numDocsIndexed++ < capacity;
  }

  private void storeIncomingColumnValue(GenericRow row, Map<String, Object> rawRowToDicIdMap, int docId,
      String columnName) {
    FixedByteSingleColumnSingleValueReaderWriter readerWriter =
        (FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(columnName);
    if (consumingNoDictionaryColumns.contains(columnName)) {
      switch (_schema.getFieldSpecFor(columnName).getDataType()) {
        case INT:
          readerWriter.setInt(docId, (int) row.getValue(columnName));
          break;
        case LONG:
          readerWriter.setLong(docId, (long) row.getValue(columnName));
          break;
        case FLOAT:
          readerWriter.setFloat(docId, (float) row.getValue(columnName));
          break;
        case DOUBLE:
          readerWriter.setDouble(docId, (double) row.getValue(columnName));
          break;
      }
    } else {
      int dicId = dictionaryMap.get(columnName).indexOf(row.getValue(columnName));
      readerWriter.setInt(docId, dicId);
      rawRowToDicIdMap.put(columnName, dicId);
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
  public ColumnDataSource getDataSource(String columnName) {
    return new ColumnDataSource(_schema.getFieldSpecFor(columnName), _numDocsIndexed,
        maxNumberOfMultivaluesMap.get(columnName), columnIndexReaderWriterMap.get(columnName),
        invertedIndexMap.get(columnName), dictionaryMap.get(columnName));
  }

  @Override
  public Set<String> getColumnNames() {
    return _schema.getColumnNames();
  }

  @Override
  public void destroy() {
    LOGGER.info("Trying to close RealtimeSegmentImpl : {}", this.getSegmentName());

    // If offHeapAllocation, then gather some statistics before  destroying the segment.
    if (isOffHeapAllocation) {
      if (_numDocsIndexed > 0) {
        RealtimeSegmentStatsHistory.SegmentStats segmentStats = new RealtimeSegmentStatsHistory.SegmentStats();
        for (Map.Entry<String, MutableDictionary> entry : dictionaryMap.entrySet()) {
          RealtimeSegmentStatsHistory.ColumnStats columnStats = new RealtimeSegmentStatsHistory.ColumnStats();
          columnStats.setCardinality(entry.getValue().length());
          columnStats.setAvgColumnSize(entry.getValue().getAvgValueSize());
          segmentStats.setColumnStats(entry.getKey(), columnStats);
        }
        segmentStats.setNumRowsConsumed(_numDocsIndexed);
        segmentStats.setMemUsedBytes(memoryManager.getTotalMemBytes());
        final long now = System.currentTimeMillis();
        final int numSeconds = (int) ((now - startTimeMillis) / 1000);
        segmentStats.setNumSeconds(numSeconds);
        statsHistory.addSegmentStats(segmentStats);
        LOGGER.info("Segment used {} bytes of memory for {} rows consumed in {} seconds",
            memoryManager.getTotalMemBytes(), _numDocsIndexed, numSeconds);
        statsHistory.save();
      }
    }

    for (DataFileReader dfReader : columnIndexReaderWriterMap.values()) {
      try {
        dfReader.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close index. Service will continue with potential memory leak, error: ", e);
        // fall through to close other segments
      }
    }
    // clear map now that index is closed to prevent accidental usage
    columnIndexReaderWriterMap.clear();

    for (RealtimeInvertedIndexReader index : invertedIndexMap.values()) {
      index.close();
    }

    for (Map.Entry<String, MutableDictionary> entry : dictionaryMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOGGER.error("Could not close dictionary for column {}", entry.getKey());
      }
    }
    invertedIndexMap.clear();
    _segmentMetadata.close();
    try {
      memoryManager.close();
    } catch (IOException e) {
      LOGGER.error("Could not close memory manager", e);
    }
  }

  private IntIterator[] getSortedBitmapIntIteratorsForStringColumn(final String columnToSortOn) {
    final RealtimeInvertedIndexReader index = invertedIndexMap.get(columnToSortOn);
    final MutableDictionary dictionary = dictionaryMap.get(columnToSortOn);
    final IntIterator[] intIterators = new IntIterator[dictionary.length()];

    final List<String> rawValues = new ArrayList<String>();
    for (int i = 0; i < dictionary.length(); i++) {
      rawValues.add((String) dictionary.get(i));
    }

    long start = System.currentTimeMillis();
    Collections.sort(rawValues);

    LOGGER.info("Column {}, dictionary len : {}, time to sort : {} ", columnToSortOn, dictionary.length(),
        (System.currentTimeMillis() - start));

    for (int i = 0; i < rawValues.size(); i++) {
      intIterators[i] = index.getDocIds(dictionary.indexOf(rawValues.get(i))).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForIntegerColumn(final String columnToSortOn) {
    final RealtimeInvertedIndexReader index = invertedIndexMap.get(columnToSortOn);
    final MutableDictionary dictionary = dictionaryMap.get(columnToSortOn);
    final IntIterator[] intIterators = new IntIterator[dictionary.length()];

    int[] rawValuesArr = new int[dictionary.length()];

    for (int i = 0; i < dictionary.length(); i++) {
      rawValuesArr[i] = (Integer) dictionary.get(i);
    }

    Arrays.sort(rawValuesArr);

    long start = System.currentTimeMillis();

    LOGGER.info("Column {}, dictionary len : {}, time to sort : {} ", columnToSortOn, dictionary.length(),
        (System.currentTimeMillis() - start));

    for (int i = 0; i < rawValuesArr.length; i++) {
      intIterators[i] = index.getDocIds(dictionary.indexOf(rawValuesArr[i])).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForLongColumn(final String columnToSortOn) {
    final RealtimeInvertedIndexReader index = invertedIndexMap.get(columnToSortOn);
    final MutableDictionary dictionary = dictionaryMap.get(columnToSortOn);
    final IntIterator[] intIterators = new IntIterator[dictionary.length()];

    final List<Long> rawValues = new ArrayList<Long>();
    for (int i = 0; i < dictionary.length(); i++) {
      rawValues.add((Long) dictionary.get(i));
    }

    long start = System.currentTimeMillis();
    Collections.sort(rawValues);

    LOGGER.info("Column {}, dictionary len : {}, time to sort : {} ", columnToSortOn, dictionary.length(),
        (System.currentTimeMillis() - start));

    for (int i = 0; i < rawValues.size(); i++) {
      intIterators[i] = index.getDocIds(dictionary.indexOf(rawValues.get(i))).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForFloatColumn(final String columnToSortOn) {
    final RealtimeInvertedIndexReader index = invertedIndexMap.get(columnToSortOn);
    final MutableDictionary dictionary = dictionaryMap.get(columnToSortOn);
    final IntIterator[] intIterators = new IntIterator[dictionary.length()];

    final List<Float> rawValues = new ArrayList<Float>();
    for (int i = 0; i < dictionary.length(); i++) {
      rawValues.add((Float) dictionary.get(i));
    }

    long start = System.currentTimeMillis();
    Collections.sort(rawValues);

    LOGGER.info("Column {}, dictionary len : {}, time to sort : {} ", columnToSortOn, dictionary.length(),
        (System.currentTimeMillis() - start));

    for (int i = 0; i < rawValues.size(); i++) {
      intIterators[i] = index.getDocIds(dictionary.indexOf(rawValues.get(i))).getIntIterator();
    }
    return intIterators;
  }

  private IntIterator[] getSortedBitmapIntIteratorsForDoubleColumn(final String columnToSortOn) {
    final RealtimeInvertedIndexReader index = invertedIndexMap.get(columnToSortOn);
    final MutableDictionary dictionary = dictionaryMap.get(columnToSortOn);
    final IntIterator[] intIterators = new IntIterator[dictionary.length()];

    final List<Double> rawValues = new ArrayList<Double>();
    for (int i = 0; i < dictionary.length(); i++) {
      rawValues.add((Double) dictionary.get(i));
    }

    long start = System.currentTimeMillis();
    Collections.sort(rawValues);

    LOGGER.info("Column {}, dictionary len : {}, time to sort : {} ", columnToSortOn, dictionary.length(),
        (System.currentTimeMillis() - start));

    for (int i = 0; i < rawValues.size(); i++) {
      intIterators[i] = index.getDocIds(dictionary.indexOf(rawValues.get(i))).getIntIterator();
    }
    return intIterators;
  }

  /**
   * Returns the docIds to use for iteration when the data is sorted by <code>columnToSortOn</code>
   * Called only by realtime record reader
   * @param columnToSortOn The column to use for sorting
   * @return The docIds to use for iteration
   */
  public int[] getSortedDocIdIterationOrderWithSortedColumn(final String columnToSortOn) {
    int[] docIds = new int[_numDocsIndexed];
    final IntIterator[] iterators;

    // Get docId iterators that iterate in order on the data
    switch (_schema.getFieldSpecFor(columnToSortOn).getDataType()) {
      case INT:
        iterators = getSortedBitmapIntIteratorsForIntegerColumn(columnToSortOn);
        break;
      case LONG:
        iterators = getSortedBitmapIntIteratorsForLongColumn(columnToSortOn);
        break;
      case FLOAT:
        iterators = getSortedBitmapIntIteratorsForFloatColumn(columnToSortOn);
        break;
      case DOUBLE:
        iterators = getSortedBitmapIntIteratorsForDoubleColumn(columnToSortOn);
        break;
      case STRING:
      case BOOLEAN:
        iterators = getSortedBitmapIntIteratorsForStringColumn(columnToSortOn);
        break;
      default:
        iterators = null;
        break;
    }

    // Drain the iterators into the docIds array
    int i = 0;
    for (IntIterator iterator : iterators) {
      while (iterator.hasNext()) {
        docIds[i] = iterator.next();
        ++i;
      }
    }

    // Sanity check
    if (i != _numDocsIndexed) {
      throw new RuntimeException("The number of docs indexed is not equal to the number of sorted documents");
    }

    return docIds;
  }

  // Called by record reader.
  @Override
  public GenericRow getRecord(int docId, GenericRow row) {
    for (String dimension : _schema.getDimensionNames()) {
      final FieldSpec.DataType dataType = _schema.getFieldSpecFor(dimension).getDataType();
      if (_schema.getFieldSpecFor(dimension).isSingleValueField()) {
        if (consumingNoDictionaryColumns.contains(dimension)) {
          copyValueIntoGenericRow(docId, row, dimension, dataType);
        } else {
          int dicId =
              ((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(dimension)).getInt(docId);
          Object rawValue = dictionaryMap.get(dimension).get(dicId);
          row.putField(dimension, rawValue);
        }
      } else {
        int[] dicIds = new int[maxNumberOfMultivaluesMap.get(dimension)];
        int len =
            ((FixedByteSingleColumnMultiValueReaderWriter) columnIndexReaderWriterMap.get(dimension)).getIntArray(docId,
                dicIds);
        Object[] rawValues = new Object[len];
        for (int i = 0; i < len; i++) {
          rawValues[i] = dictionaryMap.get(dimension).get(dicIds[i]);
        }
        row.putField(dimension, rawValues);
      }
    }

    for (String metric : _schema.getMetricNames()) {
      final FieldSpec.DataType dataType = _schema.getFieldSpecFor(metric).getDataType();
      if (consumingNoDictionaryColumns.contains(metric)) {
        copyValueIntoGenericRow(docId, row, metric, dataType);
      } else {
        final int dicId =
            ((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(metric)).getInt(docId);
        switch (dataType) {
          case INT:
            int intValue = dictionaryMap.get(metric).getIntValue(dicId);
            row.putField(metric, intValue);
            break;
          case FLOAT:
            float floatValue = dictionaryMap.get(metric).getFloatValue(dicId);
            row.putField(metric, floatValue);
            break;
          case LONG:
            long longValue = dictionaryMap.get(metric).getLongValue(dicId);
            row.putField(metric, longValue);
            break;
          case DOUBLE:
            double doubleValue = dictionaryMap.get(metric).getDoubleValue(dicId);
            row.putField(metric, doubleValue);
            break;
          default:
            throw new UnsupportedOperationException("unsopported metric data type");
        }
      }
    }

    row.putField(_timeColumn, dictionaryMap.get(_timeColumn)
        .get(((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(_timeColumn)).getInt(
            docId)));

    return row;
  }

  private void copyValueIntoGenericRow(int docId, GenericRow row, String columnName, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        int intValue =
            ((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(columnName)).getInt(docId);
        row.putField(columnName, intValue);
        break;
      case LONG:
        long longValue =
            ((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(columnName)).getLong(docId);
        row.putField(columnName, longValue);
        break;
      case FLOAT:
        float floatValue =
            ((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(columnName)).getFloat(docId);
        row.putField(columnName, floatValue);
        break;
      case DOUBLE:
        double doubleValue =
            ((FixedByteSingleColumnSingleValueReaderWriter) columnIndexReaderWriterMap.get(columnName)).getDouble(
                docId);
        row.putField(columnName, doubleValue);
        break;
      default:
        throw new UnsupportedOperationException(
            "unsopported data type " + dataType.toString() + " for column " + columnName);
    }
  }

  public void setSegmentMetadata(RealtimeSegmentZKMetadata segmentMetadata, Schema schema) {
    _segmentMetadata = new SegmentMetadataImpl(segmentMetadata, schema) {
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
  }

  public boolean hasDictionary(String columnName) {
    return dictionaryMap.containsKey(columnName);
  }

  @Override
  public StarTree getStarTree() {
    return null;
  }

  @Override
  public long getDiskSizeBytes() {
    // all the data is in memory..disk size is 0
    return 0;
  }

  public void setSegmentPartitionConfig(SegmentPartitionConfig segmentPartitionConfig) {
    _segmentPartitionConfig = segmentPartitionConfig;
  }

  public SegmentPartitionConfig getSegmentPartitionConfig() {
    return _segmentPartitionConfig;
  }
}
