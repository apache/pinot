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
package org.apache.pinot.core.query.aggregation.groupby;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.util.FixedIntArray;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


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
  private final ExpressionContext[] _groupByExpressions;
  private final int _numGroupByExpressions;
  private final DataType[] _dataTypes;
  private final Dictionary[] _dictionaries;
  private final ValueToIdMap[] _onTheFlyDictionaries;
  private final Object2IntOpenHashMap<FixedIntArray> _groupKeyMap;
  private final int _globalGroupIdUpperBound;

  private int _numGroups = 0;

  public NoDictionaryMultiColumnGroupKeyGenerator(TransformOperator transformOperator,
      ExpressionContext[] groupByExpressions, int numGroupsLimit) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;
    _dataTypes = new DataType[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _onTheFlyDictionaries = new ValueToIdMap[_numGroupByExpressions];

    for (int i = 0; i < _numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = groupByExpressions[i];
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
  public void generateKeysForBlock(TransformBlock transformBlock, int[] groupKeys) {
    int numDocs = transformBlock.getNumDocs();
    int[][] keys = new int[numDocs][_numGroupByExpressions];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValSet = transformBlock.getBlockValueSet(_groupByExpressions[i]);
      if (_dictionaries[i] != null) {
        int[] dictIds = blockValSet.getDictionaryIdsSV();
        for (int j = 0; j < numDocs; j++) {
          keys[j][i] = dictIds[j];
        }
      } else {
        ValueToIdMap onTheFlyDictionary = _onTheFlyDictionaries[i];
        switch (_dataTypes[i]) {
          case INT:
            int[] intValues = blockValSet.getIntValuesSV();
            for (int j = 0; j < numDocs; j++) {
              keys[j][i] = onTheFlyDictionary.put(intValues[j]);
            }
            break;
          case LONG:
            long[] longValues = blockValSet.getLongValuesSV();
            for (int j = 0; j < numDocs; j++) {
              keys[j][i] = onTheFlyDictionary.put(longValues[j]);
            }
            break;
          case FLOAT:
            float[] floatValues = blockValSet.getFloatValuesSV();
            for (int j = 0; j < numDocs; j++) {
              keys[j][i] = onTheFlyDictionary.put(floatValues[j]);
            }
            break;
          case DOUBLE:
            double[] doubleValues = blockValSet.getDoubleValuesSV();
            for (int j = 0; j < numDocs; j++) {
              keys[j][i] = onTheFlyDictionary.put(doubleValues[j]);
            }
            break;
          case STRING:
            String[] stringValues = blockValSet.getStringValuesSV();
            for (int j = 0; j < numDocs; j++) {
              keys[j][i] = onTheFlyDictionary.put(stringValues[j]);
            }
            break;
          case BYTES:
            byte[][] bytesValues = blockValSet.getBytesValuesSV();
            for (int j = 0; j < numDocs; j++) {
              keys[j][i] = onTheFlyDictionary.put(new ByteArray(bytesValues[j]));
            }
            break;
          default:
            throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _dataTypes[i]);
        }
      }
    }
    for (int i = 0; i < numDocs; i++) {
      groupKeys[i] = getGroupIdForKey(new FixedIntArray(keys[i]));
    }
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[][] groupKeys) {
    // TODO: Support generating keys for multi-valued columns.
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _groupKeyMap.size();
  }

  @Override
  public Iterator<GroupKey> getUniqueGroupKeys() {
    return new GroupKeyIterator();
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
  private class GroupKeyIterator implements Iterator<GroupKey> {
    final ObjectIterator<Object2IntMap.Entry<FixedIntArray>> _iterator;
    final GroupKey _groupKey;

    public GroupKeyIterator() {
      _iterator = _groupKeyMap.object2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Object2IntMap.Entry<FixedIntArray> entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
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
    for (int i = 0; i < _numGroupByExpressions; i++) {
      if (i > 0) {
        builder.append(GroupKeyGenerator.DELIMITER);
      }
      if (_dictionaries[i] != null) {
        builder.append(_dictionaries[i].getStringValue(keys[i]));
      } else {
        builder.append(_onTheFlyDictionaries[i].getString(keys[i]));
      }
    }
    return builder.toString();
  }
}
