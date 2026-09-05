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
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapBytesGroupIdMap;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapGroupByUtils;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapIntGroupIdMap;
import org.apache.pinot.core.query.aggregation.groupby.offheap.OffHeapLongGroupIdMap;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


/// Implementation of [GroupKeyGenerator] interface for single group by column, in absence of dictionary for the group
/// by column.
///
/// With null handling enabled, a null row forms a group of its own instead of joining the group of the column's default
/// null value, which is what a null row is physically stored as. The object-keyed maps hold the null key directly,
/// while the primitive-keyed maps cannot, so the null group id is tracked beside them. Both the single-value and the
/// multi-value key paths recognize nulls.
///
/// Off-heap mode caveat for STRING keys: keys are grouped by their UTF-8 encoding (byte-identical to
/// `String#getBytes(UTF_8)`), so two strings that differ only in unpaired surrogates collapse into one group
/// (both encode to `'?'`), and the emitted group key is the re-decoded string — whereas the on-heap
/// `Object2IntOpenHashMap<String>` keeps such (malformed) strings distinct. Valid strings are unaffected.
@SuppressWarnings({"rawtypes", "unchecked"})
public class NoDictionarySingleColumnGroupKeyGenerator implements GroupKeyGenerator {
  private final ExpressionContext _groupByExpression;
  private final DataType _storedType;
  private final Map _groupKeyMap;
  private final int _globalGroupIdUpperBound;
  private final boolean _nullHandlingEnabled;
  private final boolean _isSingleValueExpression;

  private Integer _groupIdForNullValue;
  private int _numGroups;

  // Off-heap mode (see the offHeap constructor param): exactly one of the two maps below is non-null and
  // _groupKeyMap is null. Null keys never enter the off-heap maps: for every type the null group is tracked via
  // _groupIdForNullValue, with _nullGroupIdMapSize recording the map size at the moment the null group was
  // assigned, so map-internal ids at/after that point shift up by one to keep the global ids dense in assignment
  // order (matching the on-heap _numGroups counter semantics).
  private final OffHeapIntGroupIdMap _offHeapIntKeyMap;
  private final OffHeapLongGroupIdMap _offHeapLongKeyMap;
  private final OffHeapBytesGroupIdMap _offHeapBytesKeyMap;
  private int _nullGroupIdMapSize = -1;
  private byte[] _stringEncodeScratch = new byte[64];

  public NoDictionarySingleColumnGroupKeyGenerator(BaseProjectOperator<?> projectOperator,
      ExpressionContext groupByExpression, int numGroupsLimit, boolean nullHandlingEnabled,
      @Nullable Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates) {
    this(projectOperator, groupByExpression, numGroupsLimit, nullHandlingEnabled,
        groupByExpressionSizesFromPredicates, false);
  }

  public NoDictionarySingleColumnGroupKeyGenerator(BaseProjectOperator<?> projectOperator,
      ExpressionContext groupByExpression, int numGroupsLimit, boolean nullHandlingEnabled,
      @Nullable Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates, boolean offHeap) {
    _groupByExpression = groupByExpression;
    ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpression);
    _storedType = columnContext.getDataType().getStoredType();
    if (groupByExpressionSizesFromPredicates != null) {
      Integer size = groupByExpressionSizesFromPredicates.get(groupByExpression);
      _globalGroupIdUpperBound = size != null ? Math.min(size, numGroupsLimit) : numGroupsLimit;
    } else {
      _globalGroupIdUpperBound = numGroupsLimit;
    }
    if (offHeap) {
      _groupKeyMap = null;
      switch (_storedType) {
        case INT:
        case FLOAT:
          // 32-bit keys go to the 8-byte-slot int map for better probe locality (FLOAT keys are stored as
          // floatToIntBits, which never produces -1 — all NaNs canonicalize — and INT -1 is held out-of-band)
          _offHeapIntKeyMap = new OffHeapIntGroupIdMap(Math.min(_globalGroupIdUpperBound, 8192));
          _offHeapLongKeyMap = null;
          _offHeapBytesKeyMap = null;
          break;
        case LONG:
        case DOUBLE:
          _offHeapIntKeyMap = null;
          _offHeapLongKeyMap = new OffHeapLongGroupIdMap(Math.min(_globalGroupIdUpperBound, 8192));
          _offHeapBytesKeyMap = null;
          break;
        case BIG_DECIMAL:
        case STRING:
        case BYTES:
          _offHeapIntKeyMap = null;
          _offHeapLongKeyMap = null;
          _offHeapBytesKeyMap = new OffHeapBytesGroupIdMap(Math.min(_globalGroupIdUpperBound, 8192));
          break;
        default:
          throw new IllegalStateException("Illegal data type for no-dictionary key generator: " + _storedType);
      }
    } else {
      _groupKeyMap = createGroupKeyMap(_storedType);
      _offHeapIntKeyMap = null;
      _offHeapLongKeyMap = null;
      _offHeapBytesKeyMap = null;
    }
    _nullHandlingEnabled = nullHandlingEnabled;
    _isSingleValueExpression = columnContext.isSingleValue();
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpression);
    int numDocs = valueBlock.getNumDocs();
    RoaringBitmap nullBitmap = getNullBitmap(blockValSet);
    // Resolved once rather than per row, and only when the block holds a null, so that reading a block without one
    // never records a null group
    int nullKey = nullBitmap != null ? getKeyForNull() : INVALID_ID;

    switch (_storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = isNull(nullBitmap, i) ? nullKey : getKeyForValue(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = isNull(nullBitmap, i) ? nullKey : getKeyForValue(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = isNull(nullBitmap, i) ? nullKey : getKeyForValue(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = isNull(nullBitmap, i) ? nullKey : getKeyForValue(doubleValues[i]);
        }
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = blockValSet.getBigDecimalValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = isNull(nullBitmap, i) ? nullKey : getKeyForValue(bigDecimalValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = isNull(nullBitmap, i) ? nullKey : getKeyForValue(stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < numDocs; i++) {
          groupKeys[i] = isNull(nullBitmap, i) ? nullKey : getKeyForValue(new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _storedType);
    }
  }

  /// Returns the block's null bitmap, or `null` when null handling is disabled or no row in the block is null.
  @Nullable
  private RoaringBitmap getNullBitmap(BlockValSet blockValSet) {
    if (!_nullHandlingEnabled) {
      return null;
    }
    RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
    return nullBitmap != null && !nullBitmap.isEmpty() ? nullBitmap : null;
  }

  /// Helper method to create the group-key map, depending on the data type.
  /// Uses primitive maps when possible.
  ///
  /// @param keyType DataType for the key
  /// @return Map
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
    RoaringBitmap nullBitmap = getNullBitmap(blockValSet);
    // Resolved once rather than per row, and only when the block holds a null, so that reading a block without one
    // never records a null group
    int nullKey = nullBitmap != null ? getKeyForNull() : INVALID_ID;

    if (_isSingleValueExpression) {
      switch (_storedType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < numDocs; i++) {
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
            groupKeys[i] = new int[]{getKeyForValue(intValues[i])};
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < numDocs; i++) {
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
            groupKeys[i] = new int[]{getKeyForValue(longValues[i])};
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < numDocs; i++) {
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
            groupKeys[i] = new int[]{getKeyForValue(floatValues[i])};
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < numDocs; i++) {
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
            groupKeys[i] = new int[]{getKeyForValue(doubleValues[i])};
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < numDocs; i++) {
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
            groupKeys[i] = new int[]{getKeyForValue(stringValues[i])};
          }
          break;
        case BYTES:
          byte[][] byteValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < numDocs; i++) {
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
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
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
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
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
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
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
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
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
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
            if (isNull(nullBitmap, i)) {
              groupKeys[i] = new int[]{nullKey};
              continue;
            }
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

  /// Returns the group id of a null value. The primitive maps cannot hold a null key so it is tracked beside them,
  /// while the object maps hold it as a key of their own.
  private int getKeyForNull() {
    return switch (_storedType) {
      case BIG_DECIMAL -> getKeyForValue((BigDecimal) null);
      case STRING -> getKeyForValue((String) null);
      case BYTES -> getKeyForValue((ByteArray) null);
      default -> getKeyForNullValue();
    };
  }

  private static boolean isNull(@Nullable RoaringBitmap nullBitmap, int row) {
    return nullBitmap != null && nullBitmap.contains(row);
  }

  /// The group id of a null value is handed out beside [#_groupKeyMap] rather than stored in it, because the
  /// primitive-keyed maps cannot hold a null key, so the group count has to come from the id counter itself.
  @Override
  public int getCurrentGroupKeyUpperBound() {
    if (_groupKeyMap == null) {
      return getOffHeapNumGroups();
    }
    return _numGroups;
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    if (_offHeapIntKeyMap != null) {
      return new OffHeapIntKeyIterator();
    }
    if (_offHeapLongKeyMap != null) {
      return new OffHeapLongKeyIterator();
    }
    if (_offHeapBytesKeyMap != null) {
      return new OffHeapBytesKeyIterator();
    }
    return switch (_storedType) {
      case INT -> new IntGroupKeyIterator((Int2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case LONG -> new LongGroupKeyIterator((Long2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case FLOAT -> new FloatGroupKeyIterator((Float2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case DOUBLE -> new DoubleGroupKeyIterator((Double2IntOpenHashMap) _groupKeyMap, _groupIdForNullValue);
      case BIG_DECIMAL, STRING, BYTES -> new ObjectGroupKeyIterator((Object2IntOpenHashMap) _groupKeyMap);
      default -> throw new IllegalStateException();
    };
  }

  private int getKeyForNullValue() {
    if (_groupKeyMap == null) {
      return getOffHeapNullGroupId();
    }
    if (_groupIdForNullValue != null) {
      return _groupIdForNullValue;
    }
    if (_numGroups < _globalGroupIdUpperBound) {
      _groupIdForNullValue = _numGroups++;
      return _groupIdForNullValue;
    }
    return INVALID_ID;
  }

  @Override
  public int getNumKeys() {
    if (_groupKeyMap == null) {
      return getOffHeapNumGroups();
    }
    return _numGroups;
  }

  @Override
  public void close() {
    if (_offHeapIntKeyMap != null) {
      _offHeapIntKeyMap.close();
    }
    if (_offHeapLongKeyMap != null) {
      _offHeapLongKeyMap.close();
    }
    if (_offHeapBytesKeyMap != null) {
      _offHeapBytesKeyMap.close();
    }
  }

  private int getOffHeapMapSize() {
    if (_offHeapIntKeyMap != null) {
      return _offHeapIntKeyMap.size();
    }
    return _offHeapLongKeyMap != null ? _offHeapLongKeyMap.size() : _offHeapBytesKeyMap.size();
  }

  private int getOffHeapNumGroups() {
    return getOffHeapMapSize() + (_groupIdForNullValue != null ? 1 : 0);
  }

  // Upper bound to pass to the off-heap map: reserve one group id for the null group once it is assigned
  private int getOffHeapMapUpperBound() {
    return _groupIdForNullValue != null ? _globalGroupIdUpperBound - 1 : _globalGroupIdUpperBound;
  }

  // Map-internal ids assigned at/after the null group shift up by one to keep global ids dense in assignment order
  private int toGlobalId(int mapId) {
    return mapId != INVALID_ID && _nullGroupIdMapSize >= 0 && mapId >= _nullGroupIdMapSize ? mapId + 1 : mapId;
  }

  private int getOffHeapNullGroupId() {
    if (_groupIdForNullValue != null) {
      return _groupIdForNullValue;
    }
    if (getOffHeapNumGroups() < _globalGroupIdUpperBound) {
      _nullGroupIdMapSize = getOffHeapMapSize();
      // The null group takes the next dense id: all existing map ids stay put, later map ids shift up by one
      _groupIdForNullValue = _nullGroupIdMapSize;
      return _groupIdForNullValue;
    }
    return INVALID_ID;
  }

  private int getOffHeapKeyForBytes(byte[] bytes) {
    return toGlobalId(_offHeapBytesKeyMap.getGroupId(bytes, 0, bytes.length, getOffHeapMapUpperBound()));
  }

  private int getKeyForValue(int value) {
    if (_offHeapIntKeyMap != null) {
      return toGlobalId(_offHeapIntKeyMap.getGroupId(value, getOffHeapMapUpperBound()));
    }
    Int2IntMap map = (Int2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(long value) {
    if (_offHeapLongKeyMap != null) {
      return toGlobalId(_offHeapLongKeyMap.getGroupId(value, getOffHeapMapUpperBound()));
    }
    Long2IntMap map = (Long2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(float value) {
    if (_offHeapIntKeyMap != null) {
      // floatToIntBits (not raw) matches fastutil semantics: all NaNs collapse, +0.0f and -0.0f stay distinct
      return toGlobalId(_offHeapIntKeyMap.getGroupId(Float.floatToIntBits(value), getOffHeapMapUpperBound()));
    }
    Float2IntMap map = (Float2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(double value) {
    if (_offHeapLongKeyMap != null) {
      // doubleToLongBits (not raw) matches fastutil semantics: all NaNs collapse, +0.0 and -0.0 stay distinct
      return toGlobalId(_offHeapLongKeyMap.getGroupId(Double.doubleToLongBits(value), getOffHeapMapUpperBound()));
    }
    Double2IntMap map = (Double2IntMap) _groupKeyMap;
    int groupId = map.get(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(BigDecimal value) {
    if (_offHeapBytesKeyMap != null) {
      if (value == null) {
        return getOffHeapNullGroupId();
      }
      // The serialized form preserves scale and unscaled value, so byte equality == BigDecimal#equals
      return getOffHeapKeyForBytes(BigDecimalUtils.serialize(value));
    }
    Object2IntMap<BigDecimal> map = (Object2IntMap<BigDecimal>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(String value) {
    if (_offHeapBytesKeyMap != null) {
      if (value == null) {
        return getOffHeapNullGroupId();
      }
      int maxLength = value.length() * 3;
      byte[] scratch = OffHeapGroupByUtils.ensureByteCapacity(_stringEncodeScratch, maxLength);
      _stringEncodeScratch = scratch;
      int length = OffHeapGroupByUtils.encodeUtf8(value, scratch);
      return toGlobalId(_offHeapBytesKeyMap.getGroupId(scratch, 0, length, getOffHeapMapUpperBound()));
    }
    Object2IntMap<String> map = (Object2IntMap<String>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  private int getKeyForValue(ByteArray value) {
    if (_offHeapBytesKeyMap != null) {
      if (value == null) {
        return getOffHeapNullGroupId();
      }
      return getOffHeapKeyForBytes(value.getBytes());
    }
    Object2IntMap<ByteArray> map = (Object2IntMap<ByteArray>) _groupKeyMap;
    int groupId = map.getInt(value);
    if (groupId == INVALID_ID && _numGroups < _globalGroupIdUpperBound) {
      groupId = _numGroups++;
      map.put(value, groupId);
    }
    return groupId;
  }

  /// Iterator for the off-heap int-key map (INT/FLOAT stored types). Emits the null group (if assigned) first,
  /// then the map entries with their map-internal ids converted to global ids.
  private class OffHeapIntKeyIterator implements Iterator<GroupKey> {
    private final Iterator<OffHeapIntGroupIdMap.Entry> _iterator = _offHeapIntKeyMap.iterator();
    private final GroupKey _groupKey = new GroupKey();
    private boolean _nullValuePending = _groupIdForNullValue != null;

    @Override
    public boolean hasNext() {
      return _nullValuePending || _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      if (_nullValuePending) {
        _groupKey._groupId = _groupIdForNullValue;
        _groupKey._keys = new Object[]{null};
        _nullValuePending = false;
        return _groupKey;
      }
      OffHeapIntGroupIdMap.Entry entry = _iterator.next();
      _groupKey._groupId = toGlobalId(entry._groupId);
      _groupKey._keys = new Object[]{decodeIntKey(entry._rawKey)};
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private Object decodeIntKey(int rawKey) {
    switch (_storedType) {
      case INT:
        return rawKey;
      case FLOAT:
        return Float.intBitsToFloat(rawKey);
      default:
        throw new IllegalStateException();
    }
  }

  /// Iterator for the off-heap long-key map (LONG/DOUBLE stored types). Emits the null group (if assigned) first,
  /// then the map entries with their map-internal ids converted to global ids.
  private class OffHeapLongKeyIterator implements Iterator<GroupKey> {
    private final Iterator<OffHeapLongGroupIdMap.Entry> _iterator = _offHeapLongKeyMap.iterator();
    private final GroupKey _groupKey = new GroupKey();
    private boolean _nullValuePending = _groupIdForNullValue != null;

    @Override
    public boolean hasNext() {
      return _nullValuePending || _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      if (_nullValuePending) {
        _groupKey._groupId = _groupIdForNullValue;
        _groupKey._keys = new Object[]{null};
        _nullValuePending = false;
        return _groupKey;
      }
      OffHeapLongGroupIdMap.Entry entry = _iterator.next();
      _groupKey._groupId = toGlobalId(entry._groupId);
      _groupKey._keys = new Object[]{decodeLongKey(entry._rawKey)};
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private Object decodeLongKey(long rawKey) {
    switch (_storedType) {
      case LONG:
        return rawKey;
      case DOUBLE:
        return Double.longBitsToDouble(rawKey);
      default:
        throw new IllegalStateException();
    }
  }

  /// Iterator for the off-heap bytes-key map (STRING/BYTES/BIG_DECIMAL stored types). Emits the null group (if
  /// assigned) first, then the dense map ids converted to global ids.
  private class OffHeapBytesKeyIterator implements Iterator<GroupKey> {
    private final GroupKey _groupKey = new GroupKey();
    private boolean _nullValuePending = _groupIdForNullValue != null;
    private int _mapId;

    @Override
    public boolean hasNext() {
      return _nullValuePending || _mapId < _offHeapBytesKeyMap.size();
    }

    @Override
    public GroupKey next() {
      if (_nullValuePending) {
        _groupKey._groupId = _groupIdForNullValue;
        _groupKey._keys = new Object[]{null};
        _nullValuePending = false;
        return _groupKey;
      }
      byte[] keyBytes = _offHeapBytesKeyMap.getKey(_mapId);
      _groupKey._groupId = toGlobalId(_mapId);
      _groupKey._keys = new Object[]{decodeBytesKey(keyBytes)};
      _mapId++;
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private Object decodeBytesKey(byte[] bytes) {
    switch (_storedType) {
      case STRING:
        return new String(bytes, StandardCharsets.UTF_8);
      case BYTES:
        return new ByteArray(bytes);
      case BIG_DECIMAL:
        return BigDecimalUtils.deserialize(bytes);
      default:
        throw new IllegalStateException();
    }
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
