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
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.util.FixedIntArray;
import com.linkedin.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import com.linkedin.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Implementation of {@link GroupKeyGenerator} interface using actual value based
 * group keys, instead of dictionary ids. This implementation is used for group-by key
 * generation when one or more of the group-by columns do not have dictionary.
 *
 * TODO:
 * 1. Add support for multi-valued group-by columns.
 * 2. Add support for trimming group-by results.
 */
public class NoDictionaryMultiColumnGroupKeyGenerator implements GroupKeyGenerator {

  private String[] _groupByColumns;
  private Map<FixedIntArray, Integer> _groupKeyMap;
  private int _numGroupKeys = 0;
  private boolean[] _hasDictionary;

  private Dictionary[] _dictionaries;
  private ValueToIdMap[] _onTheFlyDictionaries;

  /**
   * Constructor for the class.
   *
   * @param groupByColumns Columns for which to generate group-by keys
   */
  public NoDictionaryMultiColumnGroupKeyGenerator(TransformBlock transformBlock, String[] groupByColumns) {
    _groupByColumns = groupByColumns;
    _groupKeyMap = new HashMap<>();

    _hasDictionary = new boolean[groupByColumns.length];
    _dictionaries = new Dictionary[groupByColumns.length];
    _onTheFlyDictionaries = new ValueToIdMap[groupByColumns.length];

    for (int i = 0; i < groupByColumns.length; i++) {
      BlockMetadata blockMetadata = transformBlock.getBlockMetadata(groupByColumns[i]);
      if (blockMetadata.hasDictionary()) {
        _dictionaries[i] = blockMetadata.getDictionary();
        _hasDictionary[i] = true;
      } else {
        _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(blockMetadata.getDataType());
        _hasDictionary[i] = false;
      }
    }
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    // Since there's no dictionary, we cannot find the cardinality
    return Integer.MAX_VALUE;
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[] docIdToGroupKey) {
    int numGroupByColumns = _groupByColumns.length;
    int numDocs = transformBlock.getNumDocs();

    Object[] values = new Object[numGroupByColumns];
    boolean[] hasDictionary = new boolean[numGroupByColumns];
    FieldSpec.DataType[] dataTypes = new FieldSpec.DataType[numGroupByColumns];

    for (int i = 0; i < numGroupByColumns; i++) {
      BlockValSet blockValSet = transformBlock.getBlockValueSet(_groupByColumns[i]);
      dataTypes[i] = blockValSet.getValueType();
      BlockMetadata blockMetadata = transformBlock.getBlockMetadata(_groupByColumns[i]);

      if (blockMetadata.hasDictionary()) {
        hasDictionary[i] = true;
        values[i] = blockValSet.getDictionaryIds();
      } else {
        hasDictionary[i] = false;
        values[i] = getValuesFromBlockValSet(blockValSet, dataTypes[i]);
      }
    }

    for (int i = 0; i < numDocs; i++) {
      int[] keys = new int[numGroupByColumns];

      for (int j = 0; j < numGroupByColumns; j++) {

        if (hasDictionary[j]) {
          int[] dictIds = (int[]) values[j];
          keys[j] = dictIds[i];
        } else {
          // BlockValSet.getDoubleValuesSV() always returns double currently, as all aggregation functions assume
          // data type to be double.

          switch (dataTypes[j]) {
            case INT:
              int[] intValues = (int[]) values[j];
              keys[j] = _onTheFlyDictionaries[j].put(intValues[i]);
              break;

            case LONG:
              long[] longValues = (long[]) values[j];
              keys[j] = _onTheFlyDictionaries[j].put(longValues[i]);
              break;

            case FLOAT:
              float[] floatValues = (float[]) values[j];
              keys[j] = _onTheFlyDictionaries[j].put(floatValues[i]);
              break;

            case DOUBLE:
              double[] doubleValues = (double[]) values[j];
              keys[j] = _onTheFlyDictionaries[j].put(doubleValues[i]);
              break;

            case STRING:
              String[] stringValues = (String[]) values[j];
              keys[j] = _onTheFlyDictionaries[j].put(stringValues[i]);
              break;

            default:
              throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + dataTypes[j]);
          }
        }
      }

      docIdToGroupKey[i] = getGroupIdForKey(new FixedIntArray(keys));
    }
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[][] docIdToGroupKeys) {
    // TODO: Support generating keys for multi-valued columns.
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _groupKeyMap.size();
  }

  @Override
  public Iterator<GroupKey> getUniqueGroupKeys() {
    return new GroupKeyIterator(_groupKeyMap);
  }

  @Override
  public void purgeKeys(int[] keysToPurge) {
    // TODO: Implement purging.
    throw new UnsupportedOperationException("Purging keys not yet supported in GroupKeyGenerator without dictionary.");
  }

  /**
   * Helper method to get or create group-id for a group key.
   *
   * @param keyList Group key, that is a list of objects to be grouped
   * @return Group id
   */
  private int getGroupIdForKey(FixedIntArray keyList) {
    Integer groupId = _groupKeyMap.get(keyList);
    if (groupId == null) {
      groupId = _numGroupKeys;
      _groupKeyMap.put(keyList, _numGroupKeys++);
    }
    return groupId;
  }

  /**
   * Iterator for {Group-Key, Group-id) pair.
   */
  class GroupKeyIterator implements Iterator<GroupKey> {
    Iterator<Map.Entry<FixedIntArray, Integer>> _iterator;
    GroupKey _groupKey;

    public GroupKeyIterator(Map<FixedIntArray, Integer> map) {
      _iterator = map.entrySet().iterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Map.Entry<FixedIntArray, Integer> entry = _iterator.next();
      _groupKey._groupId = entry.getValue();
      _groupKey._stringKey = buildStringKeyFromIds(entry.getKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private String buildStringKeyFromIds(FixedIntArray keyList) {
    StringBuilder builder = new StringBuilder();
    int[] keys = keyList.elements();
    for (int i = 0; i < keyList.size(); i++) {
      String key;
      int dictId = keys[i];

      if (_hasDictionary[i]) {
        key = _dictionaries[i].get(dictId).toString();
      } else {
        key = _onTheFlyDictionaries[i].getString(dictId);
      }

      if (i > 0) {
        builder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
      }
      builder.append(key);
    }

    return builder.toString();
  }

  /**
   * Helper method to fetch values from BlockValSet
   * @param dataType Data type
   * @param blockValSet Block val set
   * @return Values from block val set
   */
  private Object getValuesFromBlockValSet(BlockValSet blockValSet, FieldSpec.DataType dataType) {
    Object values;

    switch (dataType) {
      case INT:
        values = blockValSet.getIntValuesSV();
        break;

      case LONG:
        values = blockValSet.getLongValuesSV();
        break;

      case FLOAT:
        values = blockValSet.getFloatValuesSV();
        break;

      case DOUBLE:
        values = blockValSet.getDoubleValuesSV();
        break;

      case STRING:
        values = blockValSet.getStringValuesSV();
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + dataType);
    }
    return values;
  }
}
