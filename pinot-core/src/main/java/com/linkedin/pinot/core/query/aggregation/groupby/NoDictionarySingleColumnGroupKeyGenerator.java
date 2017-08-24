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
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Implementation of {@link GroupKeyGenerator} interface for single group by column,
 * in absence of dictionary for the group by column.
 *
 */
public class NoDictionarySingleColumnGroupKeyGenerator implements GroupKeyGenerator {

  private final String _groupByColumn;
  private final Map _groupKeyMap;
  private int _numGroupKeys = 0;

  /**
   * Constructor for the class.
   *
   * @param groupByColumn Column for which to generate group-by keys
   */
  public NoDictionarySingleColumnGroupKeyGenerator(String groupByColumn, FieldSpec.DataType dataType) {
    _groupByColumn = groupByColumn;
    _groupKeyMap = createGroupKeyMap(dataType);
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    // Since there's no dictionary, we cannot find the cardinality
    return Integer.MAX_VALUE;
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[] docIdToGroupKey) {
    BlockValSet blockValSet = transformBlock.getBlockValueSet(_groupByColumn);
    FieldSpec.DataType dataType = blockValSet.getValueType();
    int numDocs = transformBlock.getNumDocs();

    switch (dataType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < numDocs; i++) {
          docIdToGroupKey[i] = getKeyForValue(intValues[i]);
        }
        break;

      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < numDocs; i++) {
          docIdToGroupKey[i] = getKeyForValue(longValues[i]);
        }
        break;

      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < numDocs; i++) {
          docIdToGroupKey[i] = getKeyForValue(floatValues[i]);
        }
        break;

      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < numDocs; i++) {
          docIdToGroupKey[i] = getKeyForValue(doubleValues[i]);
        }
        break;

      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < numDocs; i++) {
          docIdToGroupKey[i] = getKeyForValue(stringValues[i]);
        }
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + dataType);
    }
  }

  /**
   * Helper method to create the group-key map, depending on the data type.
   * Uses primitive maps when possible.
   *
   * @param keyType DataType for the key
   * @return Map
   */
  private Map createGroupKeyMap(FieldSpec.DataType keyType) {
    Map map;
    switch (keyType) {
      case INT:
        Int2IntMap intMap = new Int2IntOpenHashMap();
        intMap.defaultReturnValue(INVALID_ID);
        map = intMap;
        break;

      case LONG:
        Long2IntOpenHashMap longMap = new Long2IntOpenHashMap();
        longMap.defaultReturnValue(INVALID_ID);
        map = longMap;
        break;

      case FLOAT:
        Float2IntOpenHashMap floatMap = new Float2IntOpenHashMap();
        floatMap.defaultReturnValue(INVALID_ID);
        map = floatMap;
        break;

      case DOUBLE:
        Double2IntOpenHashMap doubleMap = new Double2IntOpenHashMap();
        doubleMap.defaultReturnValue(INVALID_ID);
        map = doubleMap;
        break;

      case STRING:
        Object2IntOpenHashMap<String> stringMap = new Object2IntOpenHashMap<>();
        stringMap.defaultReturnValue(INVALID_ID);
        map = stringMap;
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + keyType);
    }
    return map;
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

  @SuppressWarnings("unchecked")
  private int getKeyForValue(int value) {
    Int2IntMap map = (Int2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID) {
      groupId = _numGroupKeys;
      map.put(value, _numGroupKeys++);
    }
    return groupId;
  }

  @SuppressWarnings("unchecked")
  private int getKeyForValue(long value) {
    Long2IntMap map = (Long2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID) {
      groupId = _numGroupKeys;
      map.put(value, _numGroupKeys++);
    }
    return groupId;
  }

  @SuppressWarnings("unchecked")
  private int getKeyForValue(float value) {
    Float2IntMap map = (Float2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID) {
      groupId = _numGroupKeys;
      map.put(value, _numGroupKeys++);
    }
    return groupId;
  }

  @SuppressWarnings("unchecked")
  private int getKeyForValue(double value) {
    Double2IntMap map = (Double2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID) {
      groupId = _numGroupKeys;
      map.put(value, _numGroupKeys++);
    }
    return groupId;
  }

  @SuppressWarnings("unchecked")
  private int getKeyForValue(String value) {
    Object2IntMap<String> map = (Object2IntMap<String>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId == INVALID_ID) {
      groupId = _numGroupKeys;
      map.put(value, _numGroupKeys++);
    }
    return groupId;
  }

  /**
   * Iterator for {Group-Key, Group-id) pair.
   */
  class GroupKeyIterator implements Iterator<GroupKey> {
    Iterator<Map.Entry<Object, Integer>> _iterator;
    GroupKey _groupKey;

    @SuppressWarnings("unchecked")
    public GroupKeyIterator(Map map) {
      _iterator = (Iterator<Map.Entry<Object, Integer>>) map.entrySet().iterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Map.Entry<Object, Integer> entry = _iterator.next();
      _groupKey._groupId = entry.getValue();
      _groupKey._stringKey = entry.getKey().toString();
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
