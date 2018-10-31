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
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.transform.TransformOperator;
import com.linkedin.pinot.core.operator.transform.TransformResultMetadata;
import com.linkedin.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import com.linkedin.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.util.FixedIntArray;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;


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
  private final TransformExpressionTree[] _groupByExpressions;
  private final int _numGroupByExpressions;
  private final FieldSpec.DataType[] _dataTypes;
  private final Dictionary[] _dictionaries;
  private final ValueToIdMap[] _onTheFlyDictionaries;
  private final Object2IntOpenHashMap<FixedIntArray> _groupKeyMap;
  private final int _globalGroupIdUpperBound;

  private int _numGroups = 0;

  public NoDictionaryMultiColumnGroupKeyGenerator(TransformOperator transformOperator,
      TransformExpressionTree[] groupByExpressions, int numGroupsLimit) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;
    _dataTypes = new FieldSpec.DataType[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _onTheFlyDictionaries = new ValueToIdMap[_numGroupByExpressions];

    for (int i = 0; i < _numGroupByExpressions; i++) {
      TransformExpressionTree groupByExpression = groupByExpressions[i];
      TransformResultMetadata transformResultMetadata = transformOperator.getResultMetadata(groupByExpression);
      _dataTypes[i] = transformResultMetadata.getDataType();
      if (transformResultMetadata.hasDictionary()) {
        _dictionaries[i] = transformOperator.getDictionary(groupByExpression);
      } else {
        _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(_dataTypes[i]);
      }
    }

    _groupKeyMap = new Object2IntOpenHashMap<>();
    _groupKeyMap.defaultReturnValue(INVALID_ID);
    _globalGroupIdUpperBound = numGroupsLimit;
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  @Override
  public void generateKeysForBlock(@Nonnull TransformBlock transformBlock, @Nonnull int[] groupKeys) {
    int numDocs = transformBlock.getNumDocs();
    Object[] values = new Object[_numGroupByExpressions];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValSet = transformBlock.getBlockValueSet(_groupByExpressions[i]);
      if (_dictionaries[i] != null) {
        values[i] = blockValSet.getDictionaryIdsSV();
      } else {
        values[i] = getValuesFromBlockValSet(blockValSet, _dataTypes[i]);
      }
    }

    for (int i = 0; i < numDocs; i++) {
      int[] keys = new int[_numGroupByExpressions];
      for (int j = 0; j < _numGroupByExpressions; j++) {
        if (_dictionaries[j] != null) {
          int[] dictIds = (int[]) values[j];
          keys[j] = dictIds[i];
        } else {
          FieldSpec.DataType dataType = _dataTypes[j];
          switch (dataType) {
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
              throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + dataType);
          }
        }
      }
      groupKeys[i] = getGroupIdForKey(new FixedIntArray(keys));
    }
  }

  @Override
  public void generateKeysForBlock(@Nonnull TransformBlock transformBlock, @Nonnull int[][] groupKeys) {
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

  /**
   * Helper method to get or create group-id for a group key.
   *
   * @param keyList Group key, that is a list of objects to be grouped
   * @return Group id
   */
  private int getGroupIdForKey(FixedIntArray keyList) {
    int groupId = _groupKeyMap.getInt(keyList);
    if (groupId == INVALID_ID) {
      if (_numGroups < _globalGroupIdUpperBound) {
        groupId = _numGroups;
        _groupKeyMap.put(keyList, _numGroups++);
      }
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

      if (_dictionaries[i] != null) {
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
