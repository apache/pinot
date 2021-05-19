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
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Implementation of {@link GroupKeyGenerator} interface for single group by column,
 * in absence of dictionary for the group by column.
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NoDictionarySingleColumnGroupKeyGenerator implements GroupKeyGenerator {
  private final ExpressionContext _groupByExpression;
  private final DataType _dataType;
  private Map _groupKeyMap;
  private final int _globalGroupIdUpperBound;

  private int _numGroups = 0;

  public NoDictionarySingleColumnGroupKeyGenerator(TransformOperator transformOperator,
      ExpressionContext groupByExpression, int numGroupsLimit) {
    _groupByExpression = groupByExpression;
    _dataType = transformOperator.getResultMetadata(_groupByExpression).getDataType();
    _groupKeyMap = createGroupKeyMap(_dataType);
    _globalGroupIdUpperBound = numGroupsLimit;
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[] groupKeys) {
    BlockValSet blockValSet = transformBlock.getBlockValueSet(_groupByExpression);
    int numDocs = transformBlock.getNumDocs();

    switch (_dataType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = getKeyForValue(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = getKeyForValue(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = getKeyForValue(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = getKeyForValue(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = getKeyForValue(stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = getKeyForValue(new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _dataType);
    }
  }

  /**
   * Helper method to create the group-key map, depending on the data type.
   * Uses primitive maps when possible.
   *
   * @param keyType DataType for the key
   * @return Map
   */
  private Map createGroupKeyMap(DataType keyType) {
    switch (keyType) {
      case INT:
        Int2IntMap intMap = new Int2IntOpenHashMap();
        intMap.defaultReturnValue(INVALID_ID);
        return intMap;
      case LONG:
        Long2IntOpenHashMap longMap = new Long2IntOpenHashMap();
        longMap.defaultReturnValue(INVALID_ID);
        return longMap;
      case FLOAT:
        Float2IntOpenHashMap floatMap = new Float2IntOpenHashMap();
        floatMap.defaultReturnValue(INVALID_ID);
        return floatMap;
      case DOUBLE:
        Double2IntOpenHashMap doubleMap = new Double2IntOpenHashMap();
        doubleMap.defaultReturnValue(INVALID_ID);
        return doubleMap;
      case STRING:
        Object2IntOpenHashMap<String> stringMap = new Object2IntOpenHashMap<>();
        stringMap.defaultReturnValue(INVALID_ID);
        return stringMap;
      case BYTES:
        Object2IntOpenHashMap<ByteArray> bytesMap = new Object2IntOpenHashMap<>();
        bytesMap.defaultReturnValue(INVALID_ID);
        return bytesMap;
      default:
        throw new IllegalStateException("Illegal data type for no-dictionary key generator: " + keyType);
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
  public Iterator<GroupKey> getGroupKeys() {
    switch (_dataType) {
      case INT:
        return new IntGroupKeyIterator((Int2IntOpenHashMap) _groupKeyMap);
      case LONG:
        return new LongGroupKeyIterator((Long2IntOpenHashMap) _groupKeyMap);
      case FLOAT:
        return new FloatGroupKeyIterator((Float2IntOpenHashMap) _groupKeyMap);
      case DOUBLE:
        return new DoubleGroupKeyIterator((Double2IntOpenHashMap) _groupKeyMap);
      case STRING:
      case BYTES:
        return new ObjectGroupKeyIterator((Object2IntOpenHashMap) _groupKeyMap);
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public Iterator<StringGroupKey> getStringGroupKeys() {
    switch (_dataType) {
      case INT:
        return new IntStringGroupKeyIterator((Int2IntOpenHashMap) _groupKeyMap);
      case LONG:
        return new LongStringGroupKeyIterator((Long2IntOpenHashMap) _groupKeyMap);
      case FLOAT:
        return new FloatStringGroupKeyIterator((Float2IntOpenHashMap) _groupKeyMap);
      case DOUBLE:
        return new DoubleStringGroupKeyIterator((Double2IntOpenHashMap) _groupKeyMap);
      case STRING:
      case BYTES:
        return new ObjectStringGroupKeyIterator((Object2IntOpenHashMap) _groupKeyMap);
      default:
        throw new IllegalStateException();
    }
  }

  private int getKeyForValue(int value) {
    Int2IntMap map = (Int2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(long value) {
    Long2IntMap map = (Long2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(float value) {
    Float2IntMap map = (Float2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(double value) {
    Double2IntMap map = (Double2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(String value) {
    Object2IntMap<String> map = (Object2IntMap<String>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(ByteArray value) {
    Object2IntMap<ByteArray> map = (Object2IntMap<ByteArray>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private static class IntGroupKeyIterator implements Iterator<GroupKey> {
    final Iterator<Int2IntMap.Entry> _iterator;
    final GroupKey _groupKey;

    IntGroupKeyIterator(Int2IntOpenHashMap intMap) {
      _iterator = intMap.int2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Int2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._keys = new Object[]{entry.getIntKey()};
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class LongGroupKeyIterator implements Iterator<GroupKey> {
    final Iterator<Long2IntMap.Entry> _iterator;
    final GroupKey _groupKey;

    LongGroupKeyIterator(Long2IntOpenHashMap longMap) {
      _iterator = longMap.long2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Long2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._keys = new Object[]{entry.getLongKey()};
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FloatGroupKeyIterator implements Iterator<GroupKey> {
    final Iterator<Float2IntMap.Entry> _iterator;
    final GroupKey _groupKey;

    FloatGroupKeyIterator(Float2IntOpenHashMap floatMap) {
      _iterator = floatMap.float2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Float2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._keys = new Object[]{entry.getFloatKey()};
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class DoubleGroupKeyIterator implements Iterator<GroupKey> {
    final Iterator<Double2IntMap.Entry> _iterator;
    final GroupKey _groupKey;

    DoubleGroupKeyIterator(Double2IntOpenHashMap doubleMap) {
      _iterator = doubleMap.double2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Double2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._keys = new Object[]{entry.getDoubleKey()};
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class ObjectGroupKeyIterator implements Iterator<GroupKey> {
    final ObjectIterator<Object2IntMap.Entry> _iterator;
    final GroupKey _groupKey;

    ObjectGroupKeyIterator(Object2IntOpenHashMap objectMap) {
      _iterator = objectMap.object2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Object2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._keys = new Object[]{entry.getKey()};
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class IntStringGroupKeyIterator implements Iterator<StringGroupKey> {
    final Iterator<Int2IntMap.Entry> _iterator;
    final StringGroupKey _groupKey;

    IntStringGroupKeyIterator(Int2IntOpenHashMap intMap) {
      _iterator = intMap.int2IntEntrySet().fastIterator();
      _groupKey = new StringGroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public StringGroupKey next() {
      Int2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._stringKey = Integer.toString(entry.getIntKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class LongStringGroupKeyIterator implements Iterator<StringGroupKey> {
    final Iterator<Long2IntMap.Entry> _iterator;
    final StringGroupKey _groupKey;

    LongStringGroupKeyIterator(Long2IntOpenHashMap longMap) {
      _iterator = longMap.long2IntEntrySet().fastIterator();
      _groupKey = new StringGroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public StringGroupKey next() {
      Long2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._stringKey = Long.toString(entry.getLongKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FloatStringGroupKeyIterator implements Iterator<StringGroupKey> {
    final Iterator<Float2IntMap.Entry> _iterator;
    final StringGroupKey _groupKey;

    FloatStringGroupKeyIterator(Float2IntOpenHashMap floatMap) {
      _iterator = floatMap.float2IntEntrySet().fastIterator();
      _groupKey = new StringGroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public StringGroupKey next() {
      Float2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._stringKey = Float.toString(entry.getFloatKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class DoubleStringGroupKeyIterator implements Iterator<StringGroupKey> {
    final Iterator<Double2IntMap.Entry> _iterator;
    final StringGroupKey _groupKey;

    DoubleStringGroupKeyIterator(Double2IntOpenHashMap doubleMap) {
      _iterator = doubleMap.double2IntEntrySet().fastIterator();
      _groupKey = new StringGroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public StringGroupKey next() {
      Double2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._stringKey = Double.toString(entry.getDoubleKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class ObjectStringGroupKeyIterator implements Iterator<StringGroupKey> {
    final ObjectIterator<Object2IntMap.Entry> _iterator;
    final StringGroupKey _groupKey;

    ObjectStringGroupKeyIterator(Object2IntOpenHashMap objectMap) {
      _iterator = objectMap.object2IntEntrySet().fastIterator();
      _groupKey = new StringGroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public StringGroupKey next() {
      Object2IntMap.Entry entry = _iterator.next();
      _groupKey._groupId = entry.getIntValue();
      _groupKey._stringKey = entry.getKey().toString();
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

}
