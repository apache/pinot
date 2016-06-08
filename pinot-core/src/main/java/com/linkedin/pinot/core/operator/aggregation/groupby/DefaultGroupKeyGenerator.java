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

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.operator.aggregation.ResultHolderFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
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

  public enum StorageType {
    ARRAY_BASED,
    MAP_BASED
  }

  private final int _numGroupByColumns;
  private final Dictionary[] _dictionaries;
  private final BlockValSet[] _singleBlockValSets;
  private final BlockMultiValIterator[] _multiValIterators;
  private final int[] _cardinalities;
  private final boolean[] _isSingleValueGroupByColumn;

  // Reusable arrays to avoid creating objects with each call.
  private final int[][] _reusableSingleDictIds;
  private final int[] _reusableMultiValDictIdBuffer;

  private StorageType _storageType;
  private int _numUniqueGroupKeys;

  // For ARRAY_BASED storage type.
  private boolean[] _uniqueGroupKeysFlag;

  // For MAP_BASED storage type.
  private Long2IntOpenHashMap _groupKeyToId;

  /**
   * Constructor for the class. Initializes data members (reusable arrays).
   *
   * @param dataFetcher
   * @param groupByColumns
   * @param maxNumGroupKeys
   */
  DefaultGroupKeyGenerator(DataFetcher dataFetcher, String[] groupByColumns, int maxNumGroupKeys) {
    _numGroupByColumns = groupByColumns.length;
    _dictionaries = new Dictionary[_numGroupByColumns];
    _singleBlockValSets = new BlockValSet[_numGroupByColumns];
    _multiValIterators = new BlockMultiValIterator[_numGroupByColumns];
    _cardinalities = new int[_numGroupByColumns];
    _isSingleValueGroupByColumn = new boolean[_numGroupByColumns];
    _reusableSingleDictIds = new int[_numGroupByColumns][];

    int maxNumMultiValues = 0;
    for (int i = 0; i < _numGroupByColumns; i++) {
      DataSource dataSource = dataFetcher.getDataSourceForColumn(groupByColumns[i]);
      _dictionaries[i] = dataSource.getDictionary();
      _cardinalities[i] = dataSource.getDataSourceMetadata().cardinality();
      _isSingleValueGroupByColumn[i] = dataSource.getDataSourceMetadata().isSingleValue();

      Block block = dataSource.nextBlock();
      if (_isSingleValueGroupByColumn[i]) {
        _singleBlockValSets[i] = block.getBlockValueSet();
        _reusableSingleDictIds[i] = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      } else {
        maxNumMultiValues = Math.max(maxNumMultiValues, block.getMetadata().getMaxNumberOfMultiValues());
        _multiValIterators[i] = (BlockMultiValIterator) block.getBlockValueSet().iterator();
      }
    }
    _reusableMultiValDictIdBuffer = new int[maxNumMultiValues];

    _numUniqueGroupKeys = 0;
    if (maxNumGroupKeys <= ResultHolderFactory.MAX_INITIAL_RESULT_HOLDER_CAPACITY) {
      _storageType = StorageType.ARRAY_BASED;
      _uniqueGroupKeysFlag = new boolean[maxNumGroupKeys];
    } else {
      _storageType = StorageType.MAP_BASED;
      _groupKeyToId = new Long2IntOpenHashMap();
      _groupKeyToId.defaultReturnValue(INVALID_ID);
    }
  }

  /**
   * {@inheritDoc}
   * @param docIdSet
   * @param length
   * @param docIdToGroupKey
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[] docIdToGroupKey) {
    for (int i = 0; i < _numGroupByColumns; i++) {
      _singleBlockValSets[i].readIntValues(docIdSet, startIndex, length, _reusableSingleDictIds[i], 0);
    }
    int outIndex = 0;
    int endIndex = startIndex + length;
    for (int i = startIndex; i < endIndex; i++) {
      long rawKey = 0;
      for (int j = _numGroupByColumns - 1; j >= 0; j--) {
        rawKey = rawKey * _cardinalities[j] + _reusableSingleDictIds[j][i];
      }
      docIdToGroupKey[outIndex++] = updateRawKeyToGroupKeyMapping(rawKey);
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
    LongList groupKeysList = new LongArrayList();
    groupKeysList.add(0L);

    for (int i = _numGroupByColumns - 1; i >= 0; i--) {
      if (_isSingleValueGroupByColumn[i]) {
        int dictId = _reusableSingleDictIds[i][index];
        int size = groupKeysList.size();
        for (int j = 0; j < size; j++) {
          groupKeysList.set(j, groupKeysList.getLong(j) * _cardinalities[i] + dictId);
        }
      } else {
        BlockMultiValIterator blockValIterator = _multiValIterators[i];
        blockValIterator.skipTo(docId);
        int numMultiValues = blockValIterator.nextIntVal(_reusableMultiValDictIdBuffer);

        int originalSize = groupKeysList.size();
        for (int j = 0; j < numMultiValues - 1; j++) {
          for (int k = 0; k < originalSize; k++) {
            groupKeysList.add(groupKeysList.getLong(k));
          }
        }

        for (int j = 0; j < numMultiValues; j++) {
          for (int k = 0; k < originalSize; k++) {
            int idx = j * originalSize + k;
            groupKeysList.set(idx, (groupKeysList.getLong(idx) * _cardinalities[i]) + _reusableMultiValDictIdBuffer[j]);
          }
        }
      }
    }

    int numGroupKeys = groupKeysList.size();
    int[] groupKeys = new int[numGroupKeys];
    for (int i = 0; i < numGroupKeys; i++) {
      int groupKey = updateRawKeyToGroupKeyMapping(groupKeysList.getLong(i));
      groupKeys[i] = groupKey;
    }
    return groupKeys;
  }

  /**
   * Given a docIdSet, return a map containing array of group by keys for each docId.
   *
   * @param docIdSet
   * @param length
   * @param docIdToGroupKeys
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[][] docIdToGroupKeys) {
    for (int i = 0; i < _numGroupByColumns; i++) {
      if (_isSingleValueGroupByColumn[i]) {
        _singleBlockValSets[i].readIntValues(docIdSet, startIndex, length, _reusableSingleDictIds[i], 0);
      }
    }
    int outIndex = 0;
    int endIndex = startIndex + length;
    for (int i = startIndex; i < endIndex; i++) {
      docIdToGroupKeys[outIndex++] = generateKeysForDocId(outIndex, docIdSet[i]);
    }
  }

  /**
   * With an integer raw key, convert group key from dictId based to string based, using actually values corresponding
   * to dictionary id's.
   *
   * @param rawKey
   * @return
   */
  private String rawKeyToStringGroupKey(int rawKey) {
    // Special case one group by column for performance.
    if (_numGroupByColumns == 1) {
      return _dictionaries[0].get(rawKey).toString();
    } else {
      // Decode the rawKey.
      int cardinality = _cardinalities[0];
      StringBuilder builder = new StringBuilder(_dictionaries[0].get(rawKey % cardinality).toString());
      rawKey /= cardinality;
      for (int i = 1; i < _numGroupByColumns; i++) {
        builder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter);
        cardinality = _cardinalities[i];
        builder.append(_dictionaries[i].get(rawKey % cardinality));
        rawKey /= cardinality;
      }
      return builder.toString();
    }
  }

  /**
   * With a long raw key, convert group key from dictId based to string based, using actually values corresponding to
   * dictionary id's.
   *
   * @param rawKey
   * @return
   */
  private String rawKeyToStringGroupKey(long rawKey) {
    // Decode the rawKey.
    int cardinality = _cardinalities[0];
    StringBuilder builder = new StringBuilder(_dictionaries[0].get((int) (rawKey % cardinality)).toString());
    rawKey /= cardinality;
    for (int i = 1; i < _numGroupByColumns; i++) {
      builder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter);
      cardinality = _cardinalities[i];
      builder.append(_dictionaries[i].get((int) (rawKey % cardinality)));
      rawKey /= cardinality;
    }
    return builder.toString();
  }

  /**
   * Returns an iterator of group by key (dictionary based) and the
   * corresponding string group by key based on the actual column values.
   * @return
   */
  @Override
  public Iterator<GroupKey> getUniqueGroupKeys() {
    if (_storageType == StorageType.ARRAY_BASED) {
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
    if (_storageType == StorageType.ARRAY_BASED) {
      int intRawKey = (int) rawKey;
      if (!_uniqueGroupKeysFlag[intRawKey]) {
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
    int _index = 0;
    GroupKey _groupKey = new GroupKey(INVALID_ID, null);

    @Override
    public boolean hasNext() {
      while (_index < _uniqueGroupKeysFlag.length) {
        if (_uniqueGroupKeysFlag[_index]) {
          return true;
        }
        _index++;
      }
      return false;
    }

    @Override
    public GroupKey next() {
      String stringGroupKey = rawKeyToStringGroupKey(_index);
      _groupKey.setFirst(_index++);
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

      long groupKey = entry.getKey();
      int groupId = entry.getValue();

      String stringGroupKey = rawKeyToStringGroupKey(groupKey);
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
