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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.DictIdRecord;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMap;
import org.apache.pinot.core.query.aggregation.groupby.utils.ValueToIdMapFactory;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.FixedIntArray;


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
  private final DataType[] _storedTypes;
  private final Dictionary[] _dictionaries;
  private final ValueToIdMap[] _onTheFlyDictionaries;
  private final Object2IntOpenHashMap<FixedIntArray> _groupKeyMap;
  private final boolean[] _isSingleValueExpressions;
  private final int _globalGroupIdUpperBound;

  private int _numGroups = 0;

  public NoDictionaryMultiColumnGroupKeyGenerator(TransformOperator transformOperator,
      ExpressionContext[] groupByExpressions, int numGroupsLimit) {
    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;
    _storedTypes = new DataType[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _onTheFlyDictionaries = new ValueToIdMap[_numGroupByExpressions];
    _isSingleValueExpressions = new boolean[_numGroupByExpressions];

    for (int i = 0; i < _numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = groupByExpressions[i];
      TransformResultMetadata transformResultMetadata = transformOperator.getResultMetadata(groupByExpression);
      _storedTypes[i] = transformResultMetadata.getDataType().getStoredType();
      if (transformResultMetadata.hasDictionary()) {
        _dictionaries[i] = transformOperator.getDictionary(groupByExpression);
      } else {
        _onTheFlyDictionaries[i] = ValueToIdMapFactory.get(_storedTypes[i]);
      }
      _isSingleValueExpressions[i] = transformResultMetadata.isSingleValue();
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
        switch (_storedTypes[i]) {
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
            throw new IllegalArgumentException("Illegal data type for no-dictionary key generator: " + _storedTypes[i]);
        }
      }
    }
    for (int i = 0; i < numDocs; i++) {
      groupKeys[i] = getGroupIdForKey(new FixedIntArray(keys[i]));
    }
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[][] groupKeys) {
    int numDocs = transformBlock.getNumDocs();
    int[][][] keys = new int[numDocs][_numGroupByExpressions][];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValSet = transformBlock.getBlockValueSet(_groupByExpressions[i]);
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
  public Iterator<GroupDictId> getGroupDictId() { return new GroupDictIdIterator(); }

  @Override
  public Iterator<GroupKey> getGroupKeys() {
    return new GroupKeyIterator();
  }

  @Override
  public Iterator<StringGroupKey> getStringGroupKeys() {
    return new StringGroupKeyIterator();
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
   * Helper method to get or create a list of group-id for a list of group key.
   *
   * @param keysList Group keys, that is a list of list of objects to be grouped
   * @return Group ids
   */
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

  @Override
  public int getGroupId(DictIdRecord intermediateRecord) {
    Object[] values = intermediateRecord._record.getValues();
    int[] dictKey = new int[_numGroupByExpressions];
    for (int i = 0; i < _numGroupByExpressions; i++) {
      dictKey[i] = (int)values[i];
    }
    return getGroupIdForKey(new FixedIntArray(dictKey));
  }
  @Override
  public void clearKeyHolder() {
    _groupKeyMap.clear();
    _numGroups = 0;
  }

  @Override
  public Dictionary[] getDictionaries() {
    return _dictionaries;
  }

  private Object[] buildDictIdIterator(FixedIntArray keyList) {
    Object[] keys = new Object[_numGroupByExpressions];
    int[] dictIds = keyList.elements();
    for (int i = 0; i < _numGroupByExpressions; i++) {
      if (_dictionaries[i] != null) {
        keys[i] = dictIds[i];
      } else {
        keys[i] = _onTheFlyDictionaries[i].get(dictIds[i]);
      }
    }
    return keys;
  }

  /**
   * Iterator for {@link GroupKey}.
   */
  private class GroupDictIdIterator implements Iterator<GroupDictId> {
    private final ObjectIterator<Object2IntMap.Entry<FixedIntArray>> _iterator;
    private final GroupDictId _groupDictId;

    public GroupDictIdIterator() {
      _iterator = _groupKeyMap.object2IntEntrySet().fastIterator();
      _groupDictId = new GroupDictId();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupDictId next() {
      Object2IntMap.Entry<FixedIntArray> entry = _iterator.next();
      _groupDictId._groupId = entry.getIntValue();
      _groupDictId._keys = buildDictIdIterator(entry.getKey());
      return _groupDictId;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Iterator for {@link GroupKey}.
   */
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
      if (_dictionaries[i] != null) {
        keys[i] = _dictionaries[i].getInternal(dictIds[i]);
      } else {
        keys[i] = _onTheFlyDictionaries[i].get(dictIds[i]);
      }
    }
    return keys;
  }

  /**
   * Iterator for {@link StringGroupKey}.
   */
  private class StringGroupKeyIterator implements Iterator<StringGroupKey> {
    private final ObjectIterator<Object2IntMap.Entry<FixedIntArray>> _iterator;
    private final StringGroupKey _groupKey;

    public StringGroupKeyIterator() {
      _iterator = _groupKeyMap.object2IntEntrySet().fastIterator();
      _groupKey = new StringGroupKey();
    }

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public StringGroupKey next() {
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
