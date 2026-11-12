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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/// Implementation of [GroupKeyGenerator] interface using actual value based group keys, instead of dictionary ids.
/// This implementation is used for group-by key generation when one or more of the group-by columns do not have
/// dictionary.
///
/// With null handling enabled, a null row's column contributes a reserved id to the composed key instead of the id of
/// its stored value, so nulls form groups of their own and are read back as SQL `NULL`. Both the single-value and the
/// multi-value key paths recognize nulls. Nullness is read from the block's null bitmap rather than from the id, so a
/// dictionary-encoded column beside a raw one still composes its own dictionary ids instead of paying for an
/// on-the-fly dictionary.
public class NoDictionaryMultiColumnGroupKeyGenerator implements GroupKeyGenerator {
  private static final int ID_FOR_NULL = INVALID_ID - 1;
  /// Shared by every null row and column: the key composition reads these arrays and never writes to them.
  private static final int[] NULL_KEY_COMPONENT = {ID_FOR_NULL};

  private final ExpressionContext[] _groupByExpressions;
  private final int _numGroupByExpressions;
  private final DataType[] _storedTypes;
  private final Dictionary[] _dictionaries;
  private final ValueToIdMap[] _onTheFlyDictionaries;
  private final Object2IntOpenHashMap<FixedIntArray> _groupKeyMap;
  private final boolean[] _isSingleValueExpressions;
  private final int _numGroupsLimit;
  private final boolean _nullHandlingEnabled;
  private final int _globalGroupIdUpperBound;

  public NoDictionaryMultiColumnGroupKeyGenerator(BaseProjectOperator<?> projectOperator,
      ExpressionContext[] groupByExpressions, int numGroupsLimit, boolean nullHandlingEnabled,
      Map<ExpressionContext, Integer> groupByExpressionSizesFromPredicates) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;
    _storedTypes = new DataType[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _onTheFlyDictionaries = new ValueToIdMap[_numGroupByExpressions];
    _isSingleValueExpressions = new boolean[_numGroupByExpressions];
    _nullHandlingEnabled = nullHandlingEnabled;
    int optimizedGroupByUpperBound = 1;
    boolean canOptimizeGroupByUpperBound = groupByExpressionSizesFromPredicates != null;
    for (int i = 0; i < _numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = groupByExpressions[i];
      ColumnContext columnContext = projectOperator.getResultColumnContext(groupByExpression);
      _storedTypes[i] = columnContext.getDataType().getStoredType();
      // Take the dict-id path only when the forward index is dict-encoded. A column with EncodingType.RAW +
      // dictionaryIndex exposes a Dictionary but BlockValSet#getDictionaryIdsSV throws on its RAW forward
      // index — fall back to an on-the-fly dictionary on raw values for that case.
      Dictionary dictionary = columnContext.isDictionaryEncoded() ? columnContext.getDictionary() : null;
      if (dictionary != null) {
        _dictionaries[i] = dictionary;
      } else {
        _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(_storedTypes[i]);
      }
      if (canOptimizeGroupByUpperBound) {
        Integer size = groupByExpressionSizesFromPredicates.get(groupByExpression);
        if (size == null) {
          canOptimizeGroupByUpperBound = false;
        } else {
          if (optimizedGroupByUpperBound > Integer.MAX_VALUE / size) {
            optimizedGroupByUpperBound = numGroupsLimit;
          } else {
            optimizedGroupByUpperBound *= size;
          }
        }
      }
      _isSingleValueExpressions[i] = columnContext.isSingleValue();
    }

    _groupKeyMap = new Object2IntOpenHashMap<>();
    _groupKeyMap.defaultReturnValue(INVALID_ID);
    _numGroupsLimit = numGroupsLimit;
    _globalGroupIdUpperBound = canOptimizeGroupByUpperBound ? optimizedGroupByUpperBound : numGroupsLimit;
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[] groupKeys) {
    int numDocs = valueBlock.getNumDocs();
    Object[] values = new Object[_numGroupByExpressions];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpressions[i]);
      if (_dictionaries[i] != null) {
        values[i] = blockValSet.getDictionaryIdsSV();
      } else {
        switch (_storedTypes[i]) {
          case INT:
            values[i] = blockValSet.getIntValuesSV();
            break;
          case LONG:
            values[i] = blockValSet.getLongValuesSV();
            break;
          case FLOAT:
            values[i] = blockValSet.getFloatValuesSV();
            break;
          case DOUBLE:
            values[i] = blockValSet.getDoubleValuesSV();
            break;
          case BIG_DECIMAL:
            values[i] = blockValSet.getBigDecimalValuesSV();
            break;
          case STRING:
            values[i] = blockValSet.getStringValuesSV();
            break;
          case BYTES:
            values[i] = blockValSet.getBytesValuesSV();
            break;
          default:
            throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _storedTypes[i]);
        }
      }
    }
    // All null when null handling is disabled, so the per-column check below never matches. The check stays inside
    // the row loop rather than forking the loop per mode: every iteration pays for a hash-map operation, which dwarfs
    // a predicted branch.
    RoaringBitmap[] nullBitmaps = new RoaringBitmap[_numGroupByExpressions];
    if (_nullHandlingEnabled) {
      for (int i = 0; i < _numGroupByExpressions; i++) {
        RoaringBitmap nullBitmap = valueBlock.getBlockValueSet(_groupByExpressions[i]).getNullBitmap();
        // Normalize an empty bitmap to null so the per-row check is a plain null comparison
        nullBitmaps[i] = nullBitmap != null && !nullBitmap.isEmpty() ? nullBitmap : null;
      }
    }
    int[] keyValues = new int[_numGroupByExpressions];
    // note that we are mutating its backing array for memory efficiency
    FixedIntArray flyweightKey = new FixedIntArray(keyValues);
    for (int row = 0; row < numDocs; row++) {
      int numGroups = _groupKeyMap.size();
      boolean hasInvalidKeyValue = false;
      if (numGroups < _numGroupsLimit) {
        for (int col = 0; col < _numGroupByExpressions; col++) {
          if (isNull(nullBitmaps[col], row)) {
            keyValues[col] = ID_FOR_NULL;
          } else {
            Object columnValues = values[col];
            ValueToIdMap onTheFlyDictionary = _onTheFlyDictionaries[col];
            keyValues[col] = switch (columnValues) {
              // A dictionary-encoded column has no on-the-fly dictionary: its values are dictionary ids already, used
              // as key values directly
              case int[] ints -> onTheFlyDictionary != null ? onTheFlyDictionary.put(ints[row]) : ints[row];
              case long[] longs -> onTheFlyDictionary.put(longs[row]);
              case float[] floats -> onTheFlyDictionary.put(floats[row]);
              case double[] doubles -> onTheFlyDictionary.put(doubles[row]);
              case byte[][] bytes -> onTheFlyDictionary.put(new ByteArray(bytes[row]));
              default -> onTheFlyDictionary.put(((Object[]) columnValues)[row]);
            };
          }
        }
      } else {
        for (int col = 0; col < _numGroupByExpressions; col++) {
          if (isNull(nullBitmaps[col], row)) {
            keyValues[col] = ID_FOR_NULL;
          } else {
            Object columnValues = values[col];
            ValueToIdMap onTheFlyDictionary = _onTheFlyDictionaries[col];
            int keyValue = switch (columnValues) {
              // A dictionary-encoded column has no on-the-fly dictionary: its values are dictionary ids already, used
              // as key values directly
              case int[] ints -> onTheFlyDictionary != null ? onTheFlyDictionary.getId(ints[row]) : ints[row];
              case long[] longs -> onTheFlyDictionary.getId(longs[row]);
              case float[] floats -> onTheFlyDictionary.getId(floats[row]);
              case double[] doubles -> onTheFlyDictionary.getId(doubles[row]);
              case byte[][] bytes -> onTheFlyDictionary.getId(new ByteArray(bytes[row]));
              default -> onTheFlyDictionary.getId(((Object[]) columnValues)[row]);
            };
            if (keyValue == INVALID_ID) {
              hasInvalidKeyValue = true;
              break;
            }
            keyValues[col] = keyValue;
          }
        }
      }
      if (hasInvalidKeyValue) {
        groupKeys[row] = INVALID_ID;
      } else {
        int groupId = getGroupIdForKey(flyweightKey);
        if (groupId == numGroups) {
          // When a new group is added, create a new FixedIntArray
          keyValues = new int[_numGroupByExpressions];
          flyweightKey = new FixedIntArray(keyValues);
        }
        groupKeys[row] = groupId;
      }
    }
  }

  private static boolean isNull(@Nullable RoaringBitmap nullBitmap, int row) {
    return nullBitmap != null && nullBitmap.contains(row);
  }

  @Override
  public void generateKeysForBlock(ValueBlock valueBlock, int[][] groupKeys) {
    int numDocs = valueBlock.getNumDocs();
    int[][][] keys = new int[numDocs][_numGroupByExpressions][];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValSet = valueBlock.getBlockValueSet(_groupByExpressions[i]);
      if (_dictionaries[i] != null) {
        if (_isSingleValueExpressions[i]) {
          int[] dictIds = blockValSet.getDictionaryIdsSV();
          for (int j = 0; j < numDocs; j++) {
            keys[j][i] = new int[]{dictIds[j]};
          }
        } else {
          int[][] dictIds = blockValSet.getDictionaryIdsMV();
          for (int j = 0; j < numDocs; j++) {
            keys[j][i] = dictIds[j];
          }
        }
      } else {
        ValueToIdMap onTheFlyDictionary = _onTheFlyDictionaries[i];
        if (_isSingleValueExpressions[i]) {
          switch (_storedTypes[i]) {
            case INT:
              int[] intValues = blockValSet.getIntValuesSV();
              for (int j = 0; j < numDocs; j++) {
                keys[j][i] = new int[]{onTheFlyDictionary.put(intValues[j])};
              }
              break;
            case LONG:
              long[] longValues = blockValSet.getLongValuesSV();
              for (int j = 0; j < numDocs; j++) {
                keys[j][i] = new int[]{onTheFlyDictionary.put(longValues[j])};
              }
              break;
            case FLOAT:
              float[] floatValues = blockValSet.getFloatValuesSV();
              for (int j = 0; j < numDocs; j++) {
                keys[j][i] = new int[]{onTheFlyDictionary.put(floatValues[j])};
              }
              break;
            case DOUBLE:
              double[] doubleValues = blockValSet.getDoubleValuesSV();
              for (int j = 0; j < numDocs; j++) {
                keys[j][i] = new int[]{onTheFlyDictionary.put(doubleValues[j])};
              }
              break;
            case STRING:
              String[] stringValues = blockValSet.getStringValuesSV();
              for (int j = 0; j < numDocs; j++) {
                keys[j][i] = new int[]{onTheFlyDictionary.put(stringValues[j])};
              }
              break;
            case BYTES:
              byte[][] bytesValues = blockValSet.getBytesValuesSV();
              for (int j = 0; j < numDocs; j++) {
                keys[j][i] = new int[]{onTheFlyDictionary.put(new ByteArray(bytesValues[j]))};
              }
              break;
            default:
              throw new IllegalArgumentException(
                  "Illegal data type for no-dictionary key generator: " + _storedTypes[i]);
          }
        } else {
          switch (_storedTypes[i]) {
            case INT:
              int[][] intValues = blockValSet.getIntValuesMV();
              for (int j = 0; j < numDocs; j++) {
                int mvSize = intValues[j].length;
                int[] mvKeys = new int[mvSize];
                for (int k = 0; k < mvSize; k++) {
                  mvKeys[k] = onTheFlyDictionary.put(intValues[j][k]);
                }
                keys[j][i] = mvKeys;
              }
              break;
            case LONG:
              long[][] longValues = blockValSet.getLongValuesMV();
              for (int j = 0; j < numDocs; j++) {
                int mvSize = longValues[j].length;
                int[] mvKeys = new int[mvSize];
                for (int k = 0; k < mvSize; k++) {
                  mvKeys[k] = onTheFlyDictionary.put(longValues[j][k]);
                }
                keys[j][i] = mvKeys;
              }
              break;
            case FLOAT:
              float[][] floatValues = blockValSet.getFloatValuesMV();
              for (int j = 0; j < numDocs; j++) {
                int mvSize = floatValues[j].length;
                int[] mvKeys = new int[mvSize];
                for (int k = 0; k < mvSize; k++) {
                  mvKeys[k] = onTheFlyDictionary.put(floatValues[j][k]);
                }
                keys[j][i] = mvKeys;
              }
              break;
            case DOUBLE:
              double[][] doubleValues = blockValSet.getDoubleValuesMV();
              for (int j = 0; j < numDocs; j++) {
                int mvSize = doubleValues[j].length;
                int[] mvKeys = new int[mvSize];
                for (int k = 0; k < mvSize; k++) {
                  mvKeys[k] = onTheFlyDictionary.put(doubleValues[j][k]);
                }
                keys[j][i] = mvKeys;
              }
              break;
            case STRING:
              String[][] stringValues = blockValSet.getStringValuesMV();
              for (int j = 0; j < numDocs; j++) {
                int mvSize = stringValues[j].length;
                int[] mvKeys = new int[mvSize];
                for (int k = 0; k < mvSize; k++) {
                  mvKeys[k] = onTheFlyDictionary.put(stringValues[j][k]);
                }
                keys[j][i] = mvKeys;
              }
              break;
            default:
              throw new IllegalArgumentException(
                  "Illegal data type for no-dictionary key generator: " + _storedTypes[i]);
          }
        }
      }
      if (_nullHandlingEnabled) {
        // A null row's own value was resolved above and is discarded here. Only the composed key decides a group, so
        // the id it took in the on-the-fly dictionary goes unused rather than becoming a group of its own.
        RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          PeekableIntIterator nullIterator = nullBitmap.getIntIterator();
          while (nullIterator.hasNext()) {
            keys[nullIterator.next()][i] = NULL_KEY_COMPONENT;
          }
        }
      }
    }
    for (int i = 0; i < numDocs; i++) {
      groupKeys[i] = getGroupIdsForKeys(keys[i]);
    }
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _groupKeyMap.size();
  }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return new GroupKeyIterator();
  }

  /// Helper method to get or create group-id for a group key.
  ///
  /// @param keyList Group key, that is a list of objects to be grouped
  /// @return Group id
  private int getGroupIdForKey(FixedIntArray keyList) {
    int numGroups = _groupKeyMap.size();
    if (numGroups < _numGroupsLimit) {
      return _groupKeyMap.computeIfAbsent(keyList, k -> numGroups);
    } else {
      return _groupKeyMap.getInt(keyList);
    }
  }

  /// Helper method to get or create a list of group-id for a list of group key.
  ///
  /// @param keysList Group keys, that is a list of list of objects to be grouped
  /// @return Group ids
  private int[] getGroupIdsForKeys(int[][] keysList) {
    IntArrayList groupIds = new IntArrayList();
    getGroupIdsForKeyHelper(keysList, new int[keysList.length], 0, groupIds);
    return groupIds.toIntArray();
  }

  private void getGroupIdsForKeyHelper(int[][] keysList, int[] groupKeyIds, int level, IntArrayList groupIds) {
    int numGroups = keysList.length;
    if (level == numGroups) {
      groupIds.add(getGroupIdForKey(new FixedIntArray(Arrays.copyOf(groupKeyIds, numGroups))));
      return;
    }
    int numEntriesInGroup = keysList[level].length;
    for (int i = 0; i < numEntriesInGroup; i++) {
      groupKeyIds[level] = keysList[level][i];
      getGroupIdsForKeyHelper(keysList, groupKeyIds, level + 1, groupIds);
    }
  }

  @Override
  public int getNumKeys() {
    return _groupKeyMap.size();
  }

  /// Iterator for [GroupKey].
  private class GroupKeyIterator implements Iterator<GroupKey> {
    private final ObjectIterator<Object2IntMap.Entry<FixedIntArray>> _iterator;
    private final GroupKey _groupKey;

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
      _groupKey._keys = buildKeysFromIds(entry.getKey());
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private Object[] buildKeysFromIds(FixedIntArray keyList) {
    Object[] keys = new Object[_numGroupByExpressions];
    int[] dictIds = keyList.elements();
    for (int i = 0; i < _numGroupByExpressions; i++) {
      if (dictIds[i] == ID_FOR_NULL) {
        keys[i] = null;
      } else {
        if (_dictionaries[i] != null) {
          keys[i] = _dictionaries[i].getInternal(dictIds[i]);
        } else {
          keys[i] = _onTheFlyDictionaries[i].get(dictIds[i]);
        }
      }
    }
    return keys;
  }
}
