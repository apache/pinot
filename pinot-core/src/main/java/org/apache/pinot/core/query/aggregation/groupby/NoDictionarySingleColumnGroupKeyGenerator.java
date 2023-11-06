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
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/**
 * Implementation of {@link GroupKeyGenerator} interface for single group by column,
 * in absence of dictionary for the group by column.
 *
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class NoDictionarySingleColumnGroupKeyGenerator implements GroupKeyGenerator {
  private final ExpressionContext _groupByExpression;
  private final DataType _storedType;
  private final Map _groupKeyMap;
  private final int _globalGroupIdUpperBound;
  // TODO(nhejazi): Most of the logic between _nullHandlingEnabled=true/false is not sharable, so consider making a
  //  base implementation, and 2 derived classes, one for null enabled, one for disabled.
  private final boolean _nullHandlingEnabled;

  private Integer _groupIdForNullValue = null;
  private final boolean _isSingleValueExpression;

  private int _numGroups = 0;
  private boolean _globalGroupKeyLimitReached;

  public NoDictionarySingleColumnGroupKeyGenerator(BaseProjectOperator<?> projectOperator,
      ExpressionContext groupByExpression, int numGroupsLimit, boolean nullHandlingEnabled) {
    _groupByExpression = groupByExpression;
    ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpression);
    _storedType = columnContext.getDataType().getStoredType();
    _groupKeyMap = createGroupKeyMap(_storedType);
    _globalGroupIdUpperBound = numGroupsLimit;
    _nullHandlingEnabled = nullHandlingEnabled;
    _isSingleValueExpression = columnContext.isSingleValue();
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  @Override
  public boolean globalGroupKeyLimitReached() { return _globalGroupKeyLimitReached; }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpression);
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        generateKeysForBlockNullHandlingEnabled(valueBlock, groupKeys, nullBitmap);
        return;
      }
    }
    int numDocs = valueBlock.getNumDocs();

    switch (_storedType) {
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
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = getKeyForValue(bigDecimalValues[i]);
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
        throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _storedType);
    }
  }

  public void generateKeysForBlockNullHandlingEnabled(ValueBlock valueBlock, int[] groupKeys,
      RoaringBitmap nullBitmap) {
    assert nullBitmap != null;
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpression);
    int numDocs = valueBlock.getNumDocs();

    switch (_storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        if (nullBitmap.getCardinality() < numDocs) {
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = nullBitmap.contains(i) ? getKeyForNullValue() : getKeyForValue(intValues[i]);
          }
        } else if (numDocs > 0) {
          Arrays.fill(groupKeys, 0, numDocs, getKeyForNullValue());
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        if (nullBitmap.getCardinality() < numDocs) {
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = nullBitmap.contains(i) ? getKeyForNullValue() : getKeyForValue(longValues[i]);
          }
        } else if (numDocs > 0) {
          Arrays.fill(groupKeys, 0, numDocs, getKeyForNullValue());
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        if (nullBitmap.getCardinality() < numDocs) {
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = nullBitmap.contains(i) ? getKeyForNullValue() : getKeyForValue(floatValues[i]);
          }
        } else if (numDocs > 0) {
          Arrays.fill(groupKeys, 0, numDocs, getKeyForNullValue());
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        if (nullBitmap.getCardinality() < numDocs) {
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = nullBitmap.contains(i) ? getKeyForNullValue() : getKeyForValue(doubleValues[i]);
          }
        } else if (numDocs > 0) {
          Arrays.fill(groupKeys, 0, numDocs, getKeyForNullValue());
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        if (nullBitmap.getCardinality() < numDocs) {
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = getKeyForValue(nullBitmap.contains(i) ? null : bigDecimalValues[i]);
          }
        } else if (numDocs > 0) {
          Arrays.fill(groupKeys, 0, numDocs, getKeyForValue((BigDecimal) null));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        if (nullBitmap.getCardinality() < numDocs) {
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = getKeyForValue(nullBitmap.contains(i) ? null : stringValues[i]);
          }
        } else if (numDocs > 0) {
          Arrays.fill(groupKeys, 0, numDocs, getKeyForValue((String) null));
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        if (nullBitmap.getCardinality() < numDocs) {
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = getKeyForValue(nullBitmap.contains(i) ? null : new ByteArray(bytesValues[i]));
          }
        } else if (numDocs > 0) {
          Arrays.fill(groupKeys, 0, numDocs, getKeyForValue((ByteArray) null));
        }
        break;
      default:
        throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _storedType);
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
      case BIG_DECIMAL:
        Object2IntOpenHashMap<BigDecimal> bigDecimalMap = new Object2IntOpenHashMap<BigDecimal>();
        bigDecimalMap.defaultReturnValue(INVALID_ID);
        return bigDecimalMap;
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
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    int numDocs = valueBlock.getNumDocs();
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpression);

    if (_isSingleValueExpression) {
      switch (_storedType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = new int[]{getKeyForValue(intValues[i])};
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = new int[]{getKeyForValue(longValues[i])};
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = new int[]{getKeyForValue(floatValues[i])};
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = new int[]{getKeyForValue(doubleValues[i])};
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = new int[]{getKeyForValue(stringValues[i])};
          }
          break;
        case BYTES:
          byte[][] byteValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < numDocs; i++) {
            groupKeys[i] = new int[]{getKeyForValue(new ByteArray(byteValues[i]))};
          }
          break;
        default:
          throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _storedType);
      }
    } else {
      switch (_storedType) {
        case INT:
          int[][] intValues = blockValSet.getIntValuesMV();
          for (int i = 0; i < numDocs; i++) {
            int mvSize = intValues[i].length;
            int[] mvKeys = new int[mvSize];
            for (int j = 0; j < mvSize; j++) {
              mvKeys[j] = getKeyForValue(intValues[i][j]);
            }
            groupKeys[i] = mvKeys;
          }
          break;
        case LONG:
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < numDocs; i++) {
            int mvSize = longValues[i].length;
            int[] mvKeys = new int[mvSize];
            for (int j = 0; j < mvSize; j++) {
              mvKeys[j] = getKeyForValue(longValues[i][j]);
            }
            groupKeys[i] = mvKeys;
          }
          break;
        case FLOAT:
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < numDocs; i++) {
            int mvSize = floatValues[i].length;
            int[] mvKeys = new int[mvSize];
            for (int j = 0; j < mvSize; j++) {
              mvKeys[j] = getKeyForValue(floatValues[i][j]);
            }
            groupKeys[i] = mvKeys;
          }
          break;
        case DOUBLE:
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < numDocs; i++) {
            int mvSize = doubleValues[i].length;
            int[] mvKeys = new int[mvSize];
            for (int j = 0; j < mvSize; j++) {
              mvKeys[j] = getKeyForValue(doubleValues[i][j]);
            }
            groupKeys[i] = mvKeys;
          }
          break;
        case STRING:
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < numDocs; i++) {
            int mvSize = stringValues[i].length;
            int[] mvKeys = new int[mvSize];
            for (int j = 0; j < mvSize; j++) {
              mvKeys[j] = getKeyForValue(stringValues[i][j]);
            }
            groupKeys[i] = mvKeys;
          }
          break;
        default:
          throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _storedType);
      }
    }
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _groupKeyMap.size();
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    switch (_storedType) {
      case INT:
        return new IntGroupKeyIterator((Int2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case LONG:
        return new LongGroupKeyIterator((Long2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case FLOAT:
        return new FloatGroupKeyIterator((Float2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case DOUBLE:
        return new DoubleGroupKeyIterator((Double2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case BIG_DECIMAL:
      case STRING:
      case BYTES:
        return new ObjectGroupKeyIterator((Object2IntOpenHashMap) _groupKeyMap);
      default:
        throw new IllegalStateException();
    }
  }

  private int getKeyForNullValue() {
    if (_groupIdForNullValue != null) {
      return _groupIdForNullValue;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }
    _groupIdForNullValue = _numGroups++;
    return _groupIdForNullValue;
  }

  @Override
  public int getNumKeys() {
    return _groupKeyMap.size();
  }

  private int getKeyForValue(int value) {
    Int2IntMap map = (Int2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }

    groupId = _numGroups++;
    map.put(value, groupId);
    return groupId;
  }

  private int getKeyForValue(long value) {
    Long2IntMap map = (Long2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }

    groupId = _numGroups++;
    map.put(value, groupId);
    return groupId;
  }

  private int getKeyForValue(float value) {
    Float2IntMap map = (Float2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }

    groupId = _numGroups++;
    map.put(value, groupId);
    return groupId;
  }

  private int getKeyForValue(double value) {
    Double2IntMap map = (Double2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }

    groupId = _numGroups++;
    map.put(value, groupId);
    return groupId;
  }

  private int getKeyForValue(BigDecimal value) {
    Object2IntMap<BigDecimal> map = (Object2IntMap<BigDecimal>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }

    groupId = _numGroups++;
    map.put(value, groupId);
    return groupId;
  }

  private int getKeyForValue(String value) {
    Object2IntMap<String> map = (Object2IntMap<String>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }

    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(ByteArray value) {
    Object2IntMap<ByteArray> map = (Object2IntMap<ByteArray>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId != INVALID_ID) {
      return groupId;
    }
    if (_numGroups >= _globalGroupIdUpperBound) {
      _globalGroupKeyLimitReached = true;
      return INVALID_ID;
    }

    groupId = _numGroups++;
    map.put(value, groupId);
    return groupId;
  }

  private static class IntGroupKeyIterator implements Iterator<GroupKey> {
    final Iterator<Int2IntMap.Entry> _iterator;
    final GroupKey _groupKey;
    Integer _groupKeyForNullValue;

    IntGroupKeyIterator(Int2IntOpenHashMap intMap, Integer groupKeyForNullValue) {
      _iterator = intMap.int2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
      _groupKeyForNullValue = groupKeyForNullValue;
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext() || _groupKeyForNullValue != null;
    }

    @Override
    public GroupKey next() {
      // TODO(nhejazi): revisit to avoid adding overhead to the regular case where null handling is not enabled.
      if (_groupKeyForNullValue != null) {
        _groupKey._groupId = _groupKeyForNullValue;
        _groupKey._keys = new Object[]{null};
        _groupKeyForNullValue = null;
        return _groupKey;
      }
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
    Integer _groupKeyForNullValue;

    LongGroupKeyIterator(Long2IntOpenHashMap longMap, Integer groupKeyForNullValue) {
      _iterator = longMap.long2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
      _groupKeyForNullValue = groupKeyForNullValue;
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext() || _groupKeyForNullValue != null;
    }

    @Override
    public GroupKey next() {
      if (_groupKeyForNullValue != null) {
        _groupKey._groupId = _groupKeyForNullValue;
        _groupKey._keys = new Object[]{null};
        _groupKeyForNullValue = null;
        return _groupKey;
      }
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
    Integer _groupKeyForNullValue;

    FloatGroupKeyIterator(Float2IntOpenHashMap floatMap, Integer groupKeyForNullValue) {
      _iterator = floatMap.float2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
      _groupKeyForNullValue = groupKeyForNullValue;
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext() || _groupKeyForNullValue != null;
    }

    @Override
    public GroupKey next() {
      if (_groupKeyForNullValue != null) {
        _groupKey._groupId = _groupKeyForNullValue;
        _groupKey._keys = new Object[]{null};
        _groupKeyForNullValue = null;
        return _groupKey;
      }
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
    Integer _groupKeyForNullValue;

    DoubleGroupKeyIterator(Double2IntOpenHashMap doubleMap, Integer groupKeyForNullValue) {
      _iterator = doubleMap.double2IntEntrySet().fastIterator();
      _groupKey = new GroupKey();
      _groupKeyForNullValue = groupKeyForNullValue;
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext() || _groupKeyForNullValue != null;
    }

    @Override
    public GroupKey next() {
      if (_groupKeyForNullValue != null) {
        _groupKey._groupId = _groupKeyForNullValue;
        _groupKey._keys = new Object[]{null};
        _groupKeyForNullValue = null;
        return _groupKey;
      }
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
}
