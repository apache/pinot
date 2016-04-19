/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.groupby;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import java.util.Map;


/**
 * Class for generating group-by keys for a given list of group-by columns.
 * Uses dictionary id's to generate group-by keys.
 *
 * If maximum number of possible group keys are less than a threshold (1M),
 * returns the group key generated based on dictionary id's for the group by columns.
 *
 * For larger values, maps the actual generated group by keys onto contiguous
 * indices, and returns the latter. Keeps a mapping between the two internally.
 *
 */
public class SingleValueGroupKeyGenerator implements GroupKeyGenerator {
  private static final int INVALID_ID = -1;

  public enum STORAGE_TYPE {
    ARRAY_BASED,
    MAP_BASED
  }

  private final String[] _groupByColumns;

  private final BlockSingleValIterator[] _singleValIterators;
  private final Dictionary[] _dictionaries;
  private final int[] _cardinalities;

  // A reusable array of number of group by columns size,
  // to avoid creating int[] objects with each call.
  private final int[] _reusableGroupByValuesArray;

  private final boolean[] _isSingleValueArray;
  private STORAGE_TYPE _storageType;
  private int _numUniqueGroupKeys;

  // For ARRAY_BASED storage type.
  private boolean[] _uniqueGroupKeysFlag;

  // For MAP_BASED storage type.
  private Long2IntOpenHashMap _groupKeyToId;

  /**
   * Constructor for the class. Initializes data members (reusable arrays).
   *
   * @param indexSegment
   * @param groupByColumns
   * @param maxNumGroupKeys
   */
  SingleValueGroupKeyGenerator(IndexSegment indexSegment, String[] groupByColumns, int maxNumGroupKeys) {
    _groupByColumns = groupByColumns;

    _singleValIterators = new BlockSingleValIterator[groupByColumns.length];
    _dictionaries = new Dictionary[groupByColumns.length];
    _cardinalities = new int[groupByColumns.length];
    _isSingleValueArray = new boolean[groupByColumns.length];

    for (int i = 0; i < groupByColumns.length; i++) {
      DataSource dataSource = indexSegment.getDataSource(_groupByColumns[i]);
      _singleValIterators[i] = (BlockSingleValIterator) dataSource.nextBlock().getBlockValueSet().iterator();

      _dictionaries[i] = dataSource.getDictionary();
      _cardinalities[i] = dataSource.getDataSourceMetadata().cardinality();
      _isSingleValueArray[i] = (dataSource.getDataSourceMetadata().isSingleValue()) ? true : false;
    }

    _numUniqueGroupKeys = 0;
    if (maxNumGroupKeys <= ResultHolderFactory.INITIAL_RESULT_HOLDER_CAPACITY) {
      _storageType = STORAGE_TYPE.ARRAY_BASED;
      _uniqueGroupKeysFlag = new boolean[maxNumGroupKeys];
    } else {
      _storageType = STORAGE_TYPE.MAP_BASED;
      _groupKeyToId = new Long2IntOpenHashMap();
      _groupKeyToId.defaultReturnValue(INVALID_ID);
    }

    _reusableGroupByValuesArray = new int[groupByColumns.length];
  }

  /**
   * Generate group-by key for a given array of column values, and corresponding
   * column cardinality.
   *
   *
   * @param values
   * @param cardinalities
   * @return
   */
  public static long generateRawKey(int[] values, int[] cardinalities) {
    long groupKey = 0;
    for (int i = 0; i < values.length; i++) {
      groupKey = groupKey * cardinalities[i] + values[i];
    }
    return groupKey;
  }

  /**
   * Decode the individual column values from a given group by key and the individual
   * column cardinalities.
   *
   * @param rawGroupKey
   * @param cardinalities
   * @param decoded
   */
  public static void decodeRawGroupKey(long rawGroupKey, int[] cardinalities, int[] decoded) {
    int length = cardinalities.length;

    for (int i = length - 1; i >= 0; --i) {
      decoded[i] = (int) (rawGroupKey % cardinalities[i]);
      rawGroupKey = rawGroupKey / cardinalities[i];
    }
  }

  /**
   * {@inheritDoc}
   *
   * The group-by key generated is always 'int'. For array-based storage, the rawKey is
   * guaranteed to be < 10K. And for map-based storage, we map the rawKey to an index,
   * which is also guaranteed to fit in 'int'.
   *
   * @param docId
   * @return
   */
  @Override
  public int generateKeyForDocId(int docId) {
    for (int i = 0; i < _groupByColumns.length; i++) {
      if (_isSingleValueArray[i]) {
        _singleValIterators[i].skipTo(docId);
        _reusableGroupByValuesArray[i] = _singleValIterators[i].nextIntVal();
      } else {
        throw new RuntimeException("GroupBy on multi-valued columns not supported.");
      }
    }

    long rawKey = generateRawKey(_reusableGroupByValuesArray, _cardinalities);
    return updateRawKeyToGroupKeyMapping(rawKey);
  }

  /**
   * {@inheritDoc}
   * @param docIdSet
   * @param startIndex
   * @param length
   * @param docIdToGroupKey
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[] docIdToGroupKey) {
    int endIndex = startIndex + length - 1;
    for (int i = startIndex; i <= endIndex; ++i) {
      int docId = docIdSet[i];
      docIdToGroupKey[i] = generateKeyForDocId(docId);
    }
  }

  /**
   * Convert group key from dictId based to string based, using actually values
   * corresponding to dictionary id's.
   *
   * @param groupKey
   * @return
   */
  private String dictIdToStringGroupKey(long groupKey) {
    decodeRawGroupKey(groupKey, _cardinalities, _reusableGroupByValuesArray);

    // Special case one group by column for performance.
    if (_groupByColumns.length == 1) {
      return _dictionaries[0].get(_reusableGroupByValuesArray[0]).toString();
    } else {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < _reusableGroupByValuesArray.length; i++) {
        String key = _dictionaries[i].get(_reusableGroupByValuesArray[i]).toString();

        if (i > 0) {
          builder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString());
        }
        builder.append(key);
      }

      return builder.toString();
    }
  }

  /**
   * Returns an iterator of group by key (dictionary based) and the
   * corresponding string group by key based on the actual column values.
   * @return
   */
  @Override
  public Iterator<Pair<Long, String>> getUniqueGroupKeys() {
    if (_storageType == STORAGE_TYPE.ARRAY_BASED) {
      return new ArrayBasedGroupKeyIterator();
    } else {
      final ObjectIterator<Map.Entry<Long, Integer>> iterator = _groupKeyToId.entrySet().iterator();
      return new MapBasedGroupKeyIterator(iterator);
    }
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int getNumGroupKeys() {
    return _numUniqueGroupKeys;
  }

  /**
   * Save the passed in group key:
   * - For ARRAY_BASED storage, we store unique keys in a boolean array
   *   (key is indexed into the array).
   * - For MAP_BASED storage, we generate a unique new key, which is the total
   *   number of unique keys, and use that as the group key. We also store the
   *   mapping between the original group by key and the newly generated index.
   *
   * @param rawKey
   * @return
   */
  private int updateRawKeyToGroupKeyMapping(long rawKey) {
    int groupKey;
    if (_storageType == STORAGE_TYPE.ARRAY_BASED) {
      Preconditions.checkState(rawKey < _uniqueGroupKeysFlag.length);

      int intRawKey = (int) rawKey;
      if (_uniqueGroupKeysFlag[intRawKey] == false) {
        _uniqueGroupKeysFlag[intRawKey] = true;
        _numUniqueGroupKeys++;
      }
      groupKey = intRawKey;

    } else {

      groupKey = _groupKeyToId.get(rawKey);
      if (groupKey == INVALID_ID) {
        groupKey = _groupKeyToId.size();
        _groupKeyToId.put(rawKey, groupKey);
        _numUniqueGroupKeys++;
      } else {
        groupKey = _groupKeyToId.get(rawKey);
      }
    }
    return groupKey;
  }

  /**
   * Inner class to implement group by key iterator for ARRAY_BASED storage.
   */
  private class ArrayBasedGroupKeyIterator implements Iterator<Pair<Long, String>> {
    int index = 0;
    Pair<Long, String> ret = new Pair<Long, String>(-1L, null);

    @Override
    public boolean hasNext() {
      while (index < _uniqueGroupKeysFlag.length) {
        if (_uniqueGroupKeysFlag[index]) {
          return true;
        }
        index++;
      }
      return false;
    }

    @Override
    public Pair<Long, String> next() {
      String stringGroupKey = dictIdToStringGroupKey(index);
      ret.setFirst((long) index++);
      ret.setSecond(stringGroupKey);
      return ret;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Unsupported remove method.");
    }
  }

  /**
   * Inner class to implement group by keys iterator for MAP_BASED storage.
   */
  private class MapBasedGroupKeyIterator implements Iterator<Pair<Long, String>> {
    private final ObjectIterator<Map.Entry<Long, Integer>> _iterator;
    Pair<Long, String> ret;

    public MapBasedGroupKeyIterator(ObjectIterator<Map.Entry<Long, Integer>> iterator) {
      _iterator = iterator;
      ret = new Pair<Long, String>(-1L, null);
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public Pair<Long, String> next() {
      Map.Entry<Long, Integer> entry = _iterator.next();

      long groupKey = entry.getKey().longValue();
      int groupId = entry.getValue().intValue();

      String stringGroupKey = dictIdToStringGroupKey(groupKey);
      ret.setFirst((long) groupId);
      ret.setSecond(stringGroupKey);
      return ret;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Unsupported remove method.");
    }
  }
}
