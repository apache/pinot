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
package com.linkedin.pinot.core.operator.aggregation.groupby;

import com.clearspring.analytics.util.Preconditions;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.ResultHolderFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Class for generating group-by keys for a given list of group-by columns.
 * Uses dictionary id's to generate group-by keys.
 *
 * If maximum number of possible group keys are less than a threshold (10K),
 * returns the group key generated based on dictionary id's for the group by columns.
 *
 * For larger values, maps the actual generated group by keys onto contiguous
 * indices, and returns the latter. Keeps a mapping between the two internally.
 *
 */
public class DefaultGroupKeyGenerator implements GroupKeyGenerator {
  private static final int INVALID_ID = -1;

  public enum STORAGE_TYPE {
    ARRAY_BASED,
    MAP_BASED
  }

  private final String[] _groupByColumns;
  private final int _numGroupByColumns;

  private final BlockValSet[] _singleBlockValSets;
  private final int[][] _singleDictIds;
  private final BlockMultiValIterator[] _multiValIterators;

  private final Dictionary[] _dictionaries;
  private final int[] _cardinalities;

  // A reusable array of number of group by columns size,
  // to avoid creating int[] objects with each call.
  private final int[] _reusableGroupByValuesArray;
  private final int[] _reusableMultiValDictIdArray;

  private final boolean[] _isSingleValueGroupByColumn;
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
  DefaultGroupKeyGenerator(IndexSegment indexSegment, String[] groupByColumns, int maxNumGroupKeys) {
    _groupByColumns = groupByColumns;
    _numGroupByColumns = groupByColumns.length;

    _singleBlockValSets = new BlockValSet[_numGroupByColumns];
    _singleDictIds = new int[_numGroupByColumns][];
    _multiValIterators = new BlockMultiValIterator[_numGroupByColumns];

    _dictionaries = new Dictionary[_numGroupByColumns];
    _cardinalities = new int[_numGroupByColumns];
    _isSingleValueGroupByColumn = new boolean[_numGroupByColumns];

    int maxNumMultiValues = 0;
    for (int i = 0; i < _numGroupByColumns; i++) {
      DataSource dataSource = indexSegment.getDataSource(_groupByColumns[i]);

      _dictionaries[i] = dataSource.getDictionary();
      _cardinalities[i] = dataSource.getDataSourceMetadata().cardinality();
      _isSingleValueGroupByColumn[i] = dataSource.getDataSourceMetadata().isSingleValue();

      Block block = dataSource.nextBlock();

      if (_isSingleValueGroupByColumn[i]) {
        BlockValSet blockValSet = block.getBlockValueSet();
        _singleBlockValSets[i] = blockValSet;
        _singleDictIds[i] = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      } else {
        maxNumMultiValues = Math.max(maxNumMultiValues, block.getMetadata().getMaxNumberOfMultiValues());
        _multiValIterators[i] = (BlockMultiValIterator) block.getBlockValueSet().iterator();
      }
    }

    _reusableMultiValDictIdArray = new int[maxNumMultiValues];

    _numUniqueGroupKeys = 0;
    if (maxNumGroupKeys <= ResultHolderFactory.MAX_INITIAL_RESULT_HOLDER_CAPACITY) {
      _storageType = STORAGE_TYPE.ARRAY_BASED;
      _uniqueGroupKeysFlag = new boolean[maxNumGroupKeys];
    } else {
      _storageType = STORAGE_TYPE.MAP_BASED;
      _groupKeyToId = new Long2IntOpenHashMap();
      _groupKeyToId.defaultReturnValue(INVALID_ID);
    }

    _reusableGroupByValuesArray = new int[_numGroupByColumns];
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
   * @param docIdSet
   * @param startIndex
   * @param length
   * @param docIdToGroupKey
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[] docIdToGroupKey) {
    Preconditions.checkArgument(startIndex == 0);
    for (int i = 0; i < _numGroupByColumns; i++) {
      _singleBlockValSets[i].readIntValues(docIdSet, 0, length, _singleDictIds[i], 0);
    }
    for (int i = 0; i < length; i++) {
      long rawKey = 0;
      for (int j = 0; j < _numGroupByColumns; j++) {
        rawKey = rawKey * _cardinalities[j] + _singleDictIds[j][i];
      }
      docIdToGroupKey[i] = updateRawKeyToGroupKeyMapping(rawKey);
    }
  }

  /**
   * Given an index and docId, generate and return an array of unique group by keys,
   * based on the values of group by columns of the record of given docId.
   *
   * @param index
   * @param docId
   * @return
   */
  private int[] generateKeysForDocId(int index, int docId) {
    List<Long> groupByKeys = new ArrayList<>();
    groupByKeys.add(0L);

    for (int i = 0; i < _numGroupByColumns; i++) {
      if (_isSingleValueGroupByColumn[i]) {
        int dictId = _singleDictIds[i][index];

        for (int j = 0; j < groupByKeys.size(); j++) {
          groupByKeys.set(j, groupByKeys.get(j) * _cardinalities[i] + dictId);
        }
      } else {
        BlockMultiValIterator blockValIterator = _multiValIterators[i];
        blockValIterator.skipTo(docId);
        int numMultiValues = blockValIterator.nextIntVal(_reusableMultiValDictIdArray);

        int originalSize = groupByKeys.size();
        for (int j = 0; j < numMultiValues - 1; ++j) {
          for (int k = 0; k < originalSize; k++) {
            groupByKeys.add(groupByKeys.get(k));
          }
        }

        for (int j = 0; j < numMultiValues; j++) {
          for (int k = 0; k < originalSize; k++) {
            int idx = j * originalSize + k;
            groupByKeys.set(idx, (groupByKeys.get(idx) * _cardinalities[i]) + _reusableMultiValDictIdArray[j]);
          }
        }
      }
    }

    int numGroupKeys = groupByKeys.size();
    int[] groupKeys = new int[numGroupKeys];
    for (int i = 0; i < numGroupKeys; i++) {
      int groupKey = updateRawKeyToGroupKeyMapping(groupByKeys.get(i));
      groupKeys[i] = groupKey;
    }
    return groupKeys;
  }

  /**
   * Given a docIdSet, return a map containing array of group by keys for each docId.
   * The key in the map returned is indexed from startIndex to startIndex + length.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   * @param docIdToGroupKeys
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[][] docIdToGroupKeys) {
    Preconditions.checkArgument(startIndex == 0);
    for (int i = 0; i < _numGroupByColumns; i++) {
      if (_isSingleValueGroupByColumn[i]) {
        _singleBlockValSets[i].readIntValues(docIdSet, 0, length, _singleDictIds[i], 0);
      }
    }
    for (int i = 0; i < length; i++) {
      docIdToGroupKeys[i] = generateKeysForDocId(i, docIdSet[i]);
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
    // Special case one group by column for performance.
    if (_groupByColumns.length == 1) {
      return _dictionaries[0].get((int) groupKey).toString();
    } else {
      decodeRawGroupKey(groupKey, _cardinalities, _reusableGroupByValuesArray);
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
  public Iterator<GroupKey> getUniqueGroupKeys() {
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
  private class ArrayBasedGroupKeyIterator implements Iterator<GroupKey> {
    int index = 0;
    GroupKey _groupKey = new GroupKey(INVALID_ID, null);

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
    public GroupKey next() {
      String stringGroupKey = dictIdToStringGroupKey(index);
      _groupKey.setFirst(index++);
      _groupKey.setSecond(stringGroupKey);
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Unsupported remove method.");
    }
  }

  /**
   * Inner class to implement group by keys iterator for MAP_BASED storage.
   */
  private class MapBasedGroupKeyIterator implements Iterator<GroupKey> {
    private final ObjectIterator<Map.Entry<Long, Integer>> _iterator;
    GroupKey _groupKey;

    public MapBasedGroupKeyIterator(ObjectIterator<Map.Entry<Long, Integer>> iterator) {
      _iterator = iterator;
      _groupKey = new GroupKey(INVALID_ID, null);
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Map.Entry<Long, Integer> entry = _iterator.next();

      long groupKey = entry.getKey().longValue();
      int groupId = entry.getValue().intValue();

      String stringGroupKey = dictIdToStringGroupKey(groupKey);
      _groupKey.setFirst(groupId);
      _groupKey.setSecond(stringGroupKey);
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new RuntimeException("Unsupported remove method.");
    }
  }
}
