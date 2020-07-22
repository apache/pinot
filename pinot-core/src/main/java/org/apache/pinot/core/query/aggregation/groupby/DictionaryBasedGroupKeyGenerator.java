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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.segment.index.readers.Dictionary;


/**
 * Class for generating group keys (groupId-stringKey pair) for a given list of dictionary encoded group-by columns.
 * <p>The maximum number of possible group keys is the cardinality product of all the group-by columns.
 * <p>The raw key is generated from the dictionary ids of the group-by columns.
 * <ul>
 *   <li>
 *     If the maximum number of possible group keys is less than a threshold (10K), directly use the raw key as the
 *     group id. (ARRAY_BASED)
 *   </li>
 *   <li>
 *     If the maximum number of possible group keys is larger than the threshold, but still fit into integer, generate
 *     integer raw keys and map them onto contiguous group ids. (INT_MAP_BASED)
 *   </li>
 *   <li>
 *     If the maximum number of possible group keys cannot fit into than integer, but still fit into long, generate long
 *     raw keys and map them onto contiguous group ids. (LONG_MAP_BASED)
 *   </li>
 *   <li>
 *     If the maximum number of possible group keys cannot fit into long, use int arrays as the raw keys to store the
 *     dictionary ids of all the group-by columns and map them onto contiguous group ids. (ARRAY_MAP_BASED)
 *   </li>
 * </ul>
 * <p>All the logic is maintained internally, and to the outside world, the group ids are always int type, and are
 * bounded by the number of groups limit (globalGroupIdUpperBound is always smaller or equal to numGroupsLimit).
 */
public class DictionaryBasedGroupKeyGenerator implements GroupKeyGenerator {
  private final static int INITIAL_MAP_SIZE = 256;
  private final static int MAX_CACHING_MAP_SIZE = 1048576;
  private final ExpressionContext[] _groupByExpressions;
  private final int _numGroupByExpressions;
  private final int[] _cardinalities;
  private final boolean[] _isSingleValueColumn;
  private final Dictionary[] _dictionaries;

  // The first dimension is the index of group-by column
  // Reusable buffer for single-value column dictionary ids
  private final int[][] _singleValueDictIds;
  // Reusable buffer for multi-value column dictionary ids
  private final int[][][] _multiValueDictIds;

  private final int _globalGroupIdUpperBound;
  private final RawKeyHolder _rawKeyHolder;

  public DictionaryBasedGroupKeyGenerator(TransformOperator transformOperator, ExpressionContext[] groupByExpressions,
      int numGroupsLimit, int arrayBasedThreshold, Map mapBasedRawKeyHolders) {
    assert numGroupsLimit >= arrayBasedThreshold;

    _groupByExpressions = groupByExpressions;
    _numGroupByExpressions = groupByExpressions.length;

    _cardinalities = new int[_numGroupByExpressions];
    _isSingleValueColumn = new boolean[_numGroupByExpressions];
    _dictionaries = new Dictionary[_numGroupByExpressions];
    _singleValueDictIds = new int[_numGroupByExpressions][];
    _multiValueDictIds = new int[_numGroupByExpressions][][];

    long cardinalityProduct = 1L;
    boolean longOverflow = false;
    for (int i = 0; i < _numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = groupByExpressions[i];
      _dictionaries[i] = transformOperator.getDictionary(groupByExpression);
      int cardinality = _dictionaries[i].length();
      _cardinalities[i] = cardinality;
      if (!longOverflow) {
        if (cardinalityProduct > Long.MAX_VALUE / cardinality) {
          longOverflow = true;
        } else {
          cardinalityProduct *= cardinality;
        }
      }

      _isSingleValueColumn[i] = transformOperator.getResultMetadata(groupByExpression).isSingleValue();
    }
    if (longOverflow) {
      _globalGroupIdUpperBound = numGroupsLimit;
      Object mapInternal = mapBasedRawKeyHolders.computeIfAbsent(ArrayMapBasedHolder.class.getName(),
          o -> new ArrayMapBasedHolder(INITIAL_MAP_SIZE).getInternal());
      _rawKeyHolder = new ArrayMapBasedHolder(mapInternal);
      if (((Object2IntOpenHashMap) mapInternal).size() > MAX_CACHING_MAP_SIZE) {
        mapBasedRawKeyHolders
            .put(ArrayMapBasedHolder.class.getName(), new ArrayMapBasedHolder(INITIAL_MAP_SIZE).getInternal());
      }
    } else {
      if (cardinalityProduct > Integer.MAX_VALUE) {
        _globalGroupIdUpperBound = numGroupsLimit;
        Object mapInternal = mapBasedRawKeyHolders.computeIfAbsent(LongMapBasedHolder.class.getName(),
            o -> new LongMapBasedHolder(INITIAL_MAP_SIZE).getInternal());
        _rawKeyHolder = new LongMapBasedHolder(mapInternal);
        if (((Long2IntOpenHashMap) mapInternal).size() > MAX_CACHING_MAP_SIZE) {
          mapBasedRawKeyHolders
              .put(ArrayMapBasedHolder.class.getName(), new ArrayMapBasedHolder(INITIAL_MAP_SIZE).getInternal());
        }
      } else {
        _globalGroupIdUpperBound = Math.min((int) cardinalityProduct, numGroupsLimit);
        if (cardinalityProduct > arrayBasedThreshold) {
          Object mapInternal = mapBasedRawKeyHolders.computeIfAbsent(IntMapBasedHolder.class.getName(),
              o -> new IntMapBasedHolder(INITIAL_MAP_SIZE).getInternal());
          _rawKeyHolder = new IntMapBasedHolder(mapInternal);
          if (((Int2IntOpenHashMap) mapInternal).size() > MAX_CACHING_MAP_SIZE) {
            mapBasedRawKeyHolders
                .put(ArrayMapBasedHolder.class.getName(), new ArrayMapBasedHolder(INITIAL_MAP_SIZE).getInternal());
          }
        } else {
          _rawKeyHolder = new ArrayBasedHolder();
        }
      }
    }
  }

  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[] groupKeys) {
    // Fetch dictionary ids in the given block for all group-by columns
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(_groupByExpressions[i]);
      _singleValueDictIds[i] = blockValueSet.getDictionaryIdsSV();
    }

    _rawKeyHolder.processSingleValue(transformBlock.getNumDocs(), groupKeys);
  }

  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[][] groupKeys) {
    // Fetch dictionary ids in the given block for all group-by columns
    for (int i = 0; i < _numGroupByExpressions; i++) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(_groupByExpressions[i]);
      if (_isSingleValueColumn[i]) {
        _singleValueDictIds[i] = blockValueSet.getDictionaryIdsSV();
      } else {
        _multiValueDictIds[i] = blockValueSet.getDictionaryIdsMV();
      }
    }

    _rawKeyHolder.processMultiValue(transformBlock.getNumDocs(), groupKeys);
  }

  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _rawKeyHolder.getGroupIdUpperBound();
  }

  @Override
  public Iterator<GroupKey> getUniqueGroupKeys() {
    return _rawKeyHolder.iterator();
  }

  private interface RawKeyHolder extends Iterable<GroupKey> {

    /**
     * Process a block of documents for all single-valued group-by columns case.
     *
     * @param numDocs Number of documents inside the block
     * @param outGroupIds Buffer for group id results
     */
    void processSingleValue(int numDocs, int[] outGroupIds);

    /**
     * Process a block of documents for case with multi-valued group-by columns.
     *
     * @param numDocs Number of documents inside the block
     * @param outGroupIds Buffer for group id results
     */
    void processMultiValue(int numDocs, int[][] outGroupIds);

    /**
     * Get the upper bound of group id (exclusive) inside the holder.
     *
     * @return Upper bound of group id inside the holder
     */
    int getGroupIdUpperBound();

    Object getInternal();
  }

  private class ArrayBasedHolder implements RawKeyHolder {
    // TODO: using bitmap might better
    private final boolean[] _flags = new boolean[_globalGroupIdUpperBound];

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int groupId = 0;
        for (int j = _numGroupByExpressions - 1; j >= 0; j--) {
          groupId = groupId * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = groupId;
        _flags[groupId] = true;
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int[] groupIds = getIntRawKeys(i);
        for (int groupId : groupIds) {
          _flags[groupId] = true;
        }
        outGroupIds[i] = groupIds;
      }
    }

    @Override
    public int getGroupIdUpperBound() {
      return _globalGroupIdUpperBound;
    }

    @Override
    public Object getInternal() {
      return _flags;
    }

    @Override
    public Iterator<GroupKey> iterator() {
      return new Iterator<GroupKey>() {
        private int _currentGroupId;
        private final GroupKey _groupKey = new GroupKey();

        @Override
        public boolean hasNext() {
          while (_currentGroupId < _globalGroupIdUpperBound && !_flags[_currentGroupId]) {
            _currentGroupId++;
          }
          return _currentGroupId < _globalGroupIdUpperBound;
        }

        @Override
        public GroupKey next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          _groupKey._groupId = _currentGroupId;
          _groupKey._stringKey = getGroupKey(_currentGroupId);
          _currentGroupId++;
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private class IntMapBasedHolder implements RawKeyHolder {
    private final Int2IntOpenHashMap _rawKeyToGroupIdMap;

    private int _numGroups = 0;

    public IntMapBasedHolder(int initialSize) {
      _rawKeyToGroupIdMap = new Int2IntOpenHashMap(initialSize);
      _rawKeyToGroupIdMap.defaultReturnValue(INVALID_ID);
    }

    public IntMapBasedHolder(Object hashMap) {
      _rawKeyToGroupIdMap = (Int2IntOpenHashMap) hashMap;
      _rawKeyToGroupIdMap.clear();
    }

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int rawKey = 0;
        for (int j = _numGroupByExpressions - 1; j >= 0; j--) {
          rawKey = rawKey * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(rawKey);
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int[] groupIds = getIntRawKeys(i);
        int length = groupIds.length;
        for (int j = 0; j < length; j++) {
          groupIds[j] = getGroupId(groupIds[j]);
        }
        outGroupIds[i] = groupIds;
      }
    }

    private int getGroupId(int rawKey) {
      int groupId = _rawKeyToGroupIdMap.get(rawKey);
      if (groupId == INVALID_ID) {
        if (_numGroups < _globalGroupIdUpperBound) {
          groupId = _numGroups;
          _rawKeyToGroupIdMap.put(rawKey, _numGroups++);
        }
      }
      return groupId;
    }

    @Override
    public int getGroupIdUpperBound() {
      return _numGroups;
    }

    @Override
    public Object getInternal() {
      return _rawKeyToGroupIdMap;
    }

    @Override
    public Iterator<GroupKey> iterator() {
      return new Iterator<GroupKey>() {
        private final ObjectIterator<Int2IntMap.Entry> _iterator = _rawKeyToGroupIdMap.int2IntEntrySet().fastIterator();
        private final GroupKey _groupKey = new GroupKey();

        @Override
        public boolean hasNext() {
          return _iterator.hasNext();
        }

        @Override
        public GroupKey next() {
          Int2IntMap.Entry entry = _iterator.next();
          _groupKey._groupId = entry.getIntValue();
          _groupKey._stringKey = getGroupKey(entry.getIntKey());
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /**
   * Helper method to calculate raw keys that can fit into integer for the given index.
   *
   * @param index Index in block
   * @return Array of integer raw keys
   */
  @SuppressWarnings("Duplicates")
  private int[] getIntRawKeys(int index) {
    int[] rawKeys = null;

    // Specialize single multi-value group-by column case
    if (_numGroupByExpressions == 1) {
      rawKeys = _multiValueDictIds[0][index];
    } else {
      // Before having to transform to array, use single value raw key for better performance
      int rawKey = 0;

      for (int i = _numGroupByExpressions - 1; i >= 0; i--) {
        int cardinality = _cardinalities[i];
        if (_isSingleValueColumn[i]) {
          int dictId = _singleValueDictIds[i][index];
          if (rawKeys == null) {
            rawKey = rawKey * cardinality + dictId;
          } else {
            int length = rawKeys.length;
            for (int j = 0; j < length; j++) {
              rawKeys[j] = rawKeys[j] * cardinality + dictId;
            }
          }
        } else {
          int[] multiValueDictIds = _multiValueDictIds[i][index];
          int numValues = multiValueDictIds.length;

          // Specialize multi-value column with only one value inside
          if (numValues == 1) {
            int dictId = multiValueDictIds[0];
            if (rawKeys == null) {
              rawKey = rawKey * cardinality + dictId;
            } else {
              int length = rawKeys.length;
              for (int j = 0; j < length; j++) {
                rawKeys[j] = rawKeys[j] * cardinality + dictId;
              }
            }
          } else {
            if (rawKeys == null) {
              rawKeys = new int[numValues];
              for (int j = 0; j < numValues; j++) {
                int dictId = multiValueDictIds[j];
                rawKeys[j] = rawKey * cardinality + dictId;
              }
            } else {
              int currentLength = rawKeys.length;
              int newLength = currentLength * numValues;
              int[] newRawKeys = new int[newLength];
              for (int j = 0; j < numValues; j++) {
                int startOffset = j * currentLength;
                System.arraycopy(rawKeys, 0, newRawKeys, startOffset, currentLength);
                int dictId = multiValueDictIds[j];
                int endOffset = startOffset + currentLength;
                for (int k = startOffset; k < endOffset; k++) {
                  newRawKeys[k] = newRawKeys[k] * cardinality + dictId;
                }
              }
              rawKeys = newRawKeys;
            }
          }
        }
      }

      if (rawKeys == null) {
        rawKeys = new int[]{rawKey};
      }
    }

    return rawKeys;
  }

  /**
   * Helper method to get group key from raw key.
   *
   * @param rawKey Integer raw key
   * @return String group key
   */
  private String getGroupKey(int rawKey) {
    // Specialize single group-by column case
    if (_numGroupByExpressions == 1) {
      return _dictionaries[0].getStringValue(rawKey);
    } else {
      int cardinality = _cardinalities[0];
      StringBuilder groupKeyBuilder = new StringBuilder(_dictionaries[0].getStringValue(rawKey % cardinality));
      rawKey /= cardinality;
      for (int i = 1; i < _numGroupByExpressions; i++) {
        groupKeyBuilder.append(GroupKeyGenerator.DELIMITER);
        cardinality = _cardinalities[i];
        groupKeyBuilder.append(_dictionaries[i].getStringValue(rawKey % cardinality));
        rawKey /= cardinality;
      }
      return groupKeyBuilder.toString();
    }
  }

  private class LongMapBasedHolder implements RawKeyHolder {
    private final Long2IntOpenHashMap _rawKeyToGroupIdMap;

    private int _numGroups = 0;

    public LongMapBasedHolder(int initialSize) {
      _rawKeyToGroupIdMap = new Long2IntOpenHashMap(initialSize);
      _rawKeyToGroupIdMap.defaultReturnValue(INVALID_ID);
    }

    public LongMapBasedHolder(Object rawKeyToGroupIdMap) {
      _rawKeyToGroupIdMap = (Long2IntOpenHashMap) rawKeyToGroupIdMap;
      _rawKeyToGroupIdMap.clear();
    }

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        long rawKey = 0L;
        for (int j = _numGroupByExpressions - 1; j >= 0; j--) {
          rawKey = rawKey * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(rawKey);
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        long[] rawKeys = getLongRawKeys(i);
        int length = rawKeys.length;
        int[] groupIds = new int[length];
        for (int j = 0; j < length; j++) {
          groupIds[j] = getGroupId(rawKeys[j]);
        }
        outGroupIds[i] = groupIds;
      }
    }

    private int getGroupId(long rawKey) {
      int groupId = _rawKeyToGroupIdMap.get(rawKey);
      if (groupId == INVALID_ID) {
        if (_numGroups < _globalGroupIdUpperBound) {
          groupId = _numGroups;
          _rawKeyToGroupIdMap.put(rawKey, _numGroups++);
        }
      }
      return groupId;
    }

    @Override
    public int getGroupIdUpperBound() {
      return _numGroups;
    }

    @Override
    public Object getInternal() {
      return _rawKeyToGroupIdMap;
    }

    @Override
    public Iterator<GroupKey> iterator() {
      return new Iterator<GroupKey>() {
        private final ObjectIterator<Long2IntMap.Entry> _iterator =
            _rawKeyToGroupIdMap.long2IntEntrySet().fastIterator();
        private final GroupKey _groupKey = new GroupKey();

        @Override
        public boolean hasNext() {
          return _iterator.hasNext();
        }

        @Override
        public GroupKey next() {
          Long2IntMap.Entry entry = _iterator.next();
          _groupKey._groupId = entry.getIntValue();
          _groupKey._stringKey = getGroupKey(entry.getLongKey());
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /**
   * Helper method to calculate raw keys that can fit into integer for the given index.
   *
   * @param index Index in block
   * @return Array of long raw keys
   */
  @SuppressWarnings("Duplicates")
  private long[] getLongRawKeys(int index) {
    long[] rawKeys = null;

    // Before having to transform to array, use single value raw key for better performance
    long rawKey = 0;

    for (int i = _numGroupByExpressions - 1; i >= 0; i--) {
      int cardinality = _cardinalities[i];
      if (_isSingleValueColumn[i]) {
        int dictId = _singleValueDictIds[i][index];
        if (rawKeys == null) {
          rawKey = rawKey * cardinality + dictId;
        } else {
          int length = rawKeys.length;
          for (int j = 0; j < length; j++) {
            rawKeys[j] = rawKeys[j] * cardinality + dictId;
          }
        }
      } else {
        int[] multiValueDictIds = _multiValueDictIds[i][index];
        int numValues = multiValueDictIds.length;

        // Specialize multi-value column with only one value inside
        if (numValues == 1) {
          int dictId = multiValueDictIds[0];
          if (rawKeys == null) {
            rawKey = rawKey * cardinality + dictId;
          } else {
            int length = rawKeys.length;
            for (int j = 0; j < length; j++) {
              rawKeys[j] = rawKeys[j] * cardinality + dictId;
            }
          }
        } else {
          if (rawKeys == null) {
            rawKeys = new long[numValues];
            for (int j = 0; j < numValues; j++) {
              int dictId = multiValueDictIds[j];
              rawKeys[j] = rawKey * cardinality + dictId;
            }
          } else {
            int currentLength = rawKeys.length;
            int newLength = currentLength * numValues;
            long[] newRawKeys = new long[newLength];
            for (int j = 0; j < numValues; j++) {
              int startOffset = j * currentLength;
              System.arraycopy(rawKeys, 0, newRawKeys, startOffset, currentLength);
              int dictId = multiValueDictIds[j];
              int endOffset = startOffset + currentLength;
              for (int k = startOffset; k < endOffset; k++) {
                newRawKeys[k] = newRawKeys[k] * cardinality + dictId;
              }
            }
            rawKeys = newRawKeys;
          }
        }
      }
    }

    if (rawKeys == null) {
      return new long[]{rawKey};
    } else {
      return rawKeys;
    }
  }

  /**
   * Helper method to get group key from raw key.
   *
   * @param rawKey Long raw key
   * @return String group key
   */
  private String getGroupKey(long rawKey) {
    int cardinality = _cardinalities[0];
    StringBuilder groupKeyBuilder = new StringBuilder(_dictionaries[0].getStringValue((int) (rawKey % cardinality)));
    rawKey /= cardinality;
    for (int i = 1; i < _numGroupByExpressions; i++) {
      groupKeyBuilder.append(GroupKeyGenerator.DELIMITER);
      cardinality = _cardinalities[i];
      groupKeyBuilder.append(_dictionaries[i].getStringValue((int) (rawKey % cardinality)));
      rawKey /= cardinality;
    }
    return groupKeyBuilder.toString();
  }

  private class ArrayMapBasedHolder implements RawKeyHolder {
    private final Object2IntOpenHashMap<IntArray> _rawKeyToGroupIdMap;

    private int _numGroups = 0;

    public ArrayMapBasedHolder(int initialSize) {
      _rawKeyToGroupIdMap = new Object2IntOpenHashMap<>(initialSize);
      _rawKeyToGroupIdMap.defaultReturnValue(INVALID_ID);
    }

    public ArrayMapBasedHolder(Object rawKeyToGroupIdMap) {
      _rawKeyToGroupIdMap = (Object2IntOpenHashMap<IntArray>) rawKeyToGroupIdMap;
      _rawKeyToGroupIdMap.clear();
    }

    @Override
    public void processSingleValue(int numDocs, int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int[] dictIds = new int[_numGroupByExpressions];
        for (int j = 0; j < _numGroupByExpressions; j++) {
          dictIds[j] = _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(new IntArray(dictIds));
      }
    }

    @Override
    public void processMultiValue(int numDocs, int[][] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        IntArray[] rawKeys = getIntArrayRawKeys(i);
        int length = rawKeys.length;
        int[] groupIds = new int[length];
        for (int j = 0; j < length; j++) {
          groupIds[j] = getGroupId(rawKeys[j]);
        }
        outGroupIds[i] = groupIds;
      }
    }

    private int getGroupId(IntArray rawKey) {
      int groupId = _rawKeyToGroupIdMap.getInt(rawKey);
      if (groupId == INVALID_ID) {
        if (_numGroups < _globalGroupIdUpperBound) {
          groupId = _numGroups;
          _rawKeyToGroupIdMap.put(rawKey, _numGroups++);
        }
      }
      return groupId;
    }

    @Override
    public int getGroupIdUpperBound() {
      return _numGroups;
    }

    @Override
    public Object getInternal() {
      return _rawKeyToGroupIdMap;
    }

    @Override
    public Iterator<GroupKey> iterator() {
      return new Iterator<GroupKey>() {
        private final ObjectIterator<Object2IntMap.Entry<IntArray>> _iterator =
            _rawKeyToGroupIdMap.object2IntEntrySet().fastIterator();
        private final GroupKey _groupKey = new GroupKey();

        @Override
        public boolean hasNext() {
          return _iterator.hasNext();
        }

        @Override
        public GroupKey next() {
          Object2IntMap.Entry<IntArray> entry = _iterator.next();
          _groupKey._groupId = entry.getIntValue();
          _groupKey._stringKey = getGroupKey(entry.getKey());
          return _groupKey;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /**
   * Helper method to calculate raw keys that can fit into integer for the given index.
   *
   * @param index Index in block
   * @return Array of IntArray raw keys
   */
  @SuppressWarnings("Duplicates")
  private IntArray[] getIntArrayRawKeys(int index) {
    IntArray[] rawKeys = null;

    // Before having to transform to array, use single value raw key for better performance
    int[] dictIds = new int[_numGroupByExpressions];

    for (int i = 0; i < _numGroupByExpressions; i++) {
      if (_isSingleValueColumn[i]) {
        int dictId = _singleValueDictIds[i][index];
        if (rawKeys == null) {
          dictIds[i] = dictId;
        } else {
          for (IntArray rawKey : rawKeys) {
            rawKey._elements[i] = dictId;
          }
        }
      } else {
        int[] multiValueDictIds = _multiValueDictIds[i][index];
        int numValues = multiValueDictIds.length;

        // Specialize multi-value column with only one value inside
        if (numValues == 1) {
          int dictId = multiValueDictIds[0];
          if (rawKeys == null) {
            dictIds[i] = dictId;
          } else {
            for (IntArray rawKey : rawKeys) {
              rawKey._elements[i] = dictId;
            }
          }
        } else {
          if (rawKeys == null) {
            rawKeys = new IntArray[numValues];
            for (int j = 0; j < numValues; j++) {
              int dictId = multiValueDictIds[j];
              rawKeys[j] = new IntArray(dictIds.clone());
              rawKeys[j]._elements[i] = dictId;
            }
          } else {
            int currentLength = rawKeys.length;
            int newLength = currentLength * numValues;
            IntArray[] newRawKeys = new IntArray[newLength];
            System.arraycopy(rawKeys, 0, newRawKeys, 0, currentLength);
            for (int j = 1; j < numValues; j++) {
              int offset = j * currentLength;
              for (int k = 0; k < currentLength; k++) {
                newRawKeys[offset + k] = new IntArray(rawKeys[k]._elements.clone());
              }
            }
            for (int j = 0; j < numValues; j++) {
              int startOffset = j * currentLength;
              int dictId = multiValueDictIds[j];
              int endOffset = startOffset + currentLength;
              for (int k = startOffset; k < endOffset; k++) {
                newRawKeys[k]._elements[i] = dictId;
              }
            }
            rawKeys = newRawKeys;
          }
        }
      }
    }

    if (rawKeys == null) {
      return new IntArray[]{new IntArray(dictIds)};
    } else {
      return rawKeys;
    }
  }

  /**
   * Helper method to get group key from raw key.
   *
   * @param rawKey IntArray raw key
   * @return String group key
   */
  private String getGroupKey(IntArray rawKey) {
    StringBuilder groupKeyBuilder = new StringBuilder(_dictionaries[0].getStringValue(rawKey._elements[0]));
    for (int i = 1; i < _numGroupByExpressions; i++) {
      groupKeyBuilder.append(GroupKeyGenerator.DELIMITER);
      groupKeyBuilder.append(_dictionaries[i].getStringValue(rawKey._elements[i]));
    }
    return groupKeyBuilder.toString();
  }

  /**
   * Drop un-necessary checks for highest performance.
   */
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  private static class IntArray {
    public int[] _elements;

    public IntArray(int[] elements) {
      _elements = elements;
    }

    @Override
    public int hashCode() {
      int result = 1;
      for (int element : _elements) {
        result = 31 * result + element;
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      int[] that = ((IntArray) obj)._elements;
      int length = _elements.length;
      if (length != that.length) {
        return false;
      }
      for (int i = 0; i < length; i++) {
        if (_elements[i] != that[i]) {
          return false;
        }
      }
      return true;
    }
  }
}
