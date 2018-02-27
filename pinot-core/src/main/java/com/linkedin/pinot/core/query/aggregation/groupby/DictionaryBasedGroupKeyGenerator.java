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

import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;


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
 * <p>All the logic is maintained internally, and to the outside world, the group ids are always int type.
 */
// TODO: Revisit to make trimming work. Currently trimming is disabled
public class DictionaryBasedGroupKeyGenerator implements GroupKeyGenerator {
  private final int _numGroupByColumns;
  private final String[] _groupByColumns;
  private final int[] _cardinalities;
  private final boolean[] _isSingleValueColumn;
  private final Dictionary[] _dictionaries;
  private final BlockValSet[] _blockValSets;

  // The first dimension is the index of group-by column
  // Reusable buffer for single-value column dictionary ids per block
  private final int[][] _singleValueDictIds;
  // Reusable buffer for multi-value column dictionary ids per document
  private final int[][] _multiValueDictIds;
  // Reusable buffer for number of values in multi-value column per document
  private final int[] _numValues;

  private final int _globalGroupIdUpperBound;
  private final RawKeyHolder _rawKeyHolder;

//  // The following data structures are used for trimming group keys.
//  // TODO: the key will be contiguous so we should use array here.
//  // Reverse mapping for trimming group keys
//  private Int2LongOpenHashMap _idToGroupKey;
//
//  // Reverse mapping from KeyIds to groupKeys. purgeGroupKeys takes an array of keyIds to remove,
//  // this map tracks keyIds to groupKeys, to serve the purging.
//  private Int2ObjectOpenHashMap<IntArrayList> _idToArrayGroupKey;
//
//  // Enum to reflect if trimming of group keys is ON or OFF. Once ON, we need to start tracking
//  // the keyIds that are removed.
//  private enum TrimMode {
//    OFF,
//    ON
//  }
//  private TrimMode _trimMode;

  /**
   * Constructor for the class. Initializes data members (reusable arrays).
   *
   * @param transformBlock Transform block for which to generate group keys
   * @param groupByColumns Group-by columns
   * @param arrayBasedThreshold Threshold for array based result holder
   */
  public DictionaryBasedGroupKeyGenerator(TransformBlock transformBlock, String[] groupByColumns,
      int arrayBasedThreshold) {
    _numGroupByColumns = groupByColumns.length;
    _groupByColumns = groupByColumns;

    _cardinalities = new int[_numGroupByColumns];
    _isSingleValueColumn = new boolean[_numGroupByColumns];
    _dictionaries = new Dictionary[_numGroupByColumns];
    _blockValSets = new BlockValSet[_numGroupByColumns];
    _singleValueDictIds = new int[_numGroupByColumns][];
    _multiValueDictIds = new int[_numGroupByColumns][];
    _numValues = new int[_numGroupByColumns];

    long cardinalityProduct = 1L;
    boolean longOverflow = false;
    for (int i = 0; i < _numGroupByColumns; i++) {
      BlockMetadata blockMetadata = transformBlock.getBlockMetadata(groupByColumns[i]);
      _dictionaries[i] = blockMetadata.getDictionary();
      int cardinality = _dictionaries[i].length();
      _cardinalities[i] = cardinality;
      if (!longOverflow) {
        if (cardinalityProduct > Long.MAX_VALUE / cardinality) {
          longOverflow = true;
        } else {
          cardinalityProduct *= cardinality;
        }
      }

      if (blockMetadata.isSingleValue()) {
        _isSingleValueColumn[i] = true;
      } else {
        _multiValueDictIds[i] = new int[blockMetadata.getMaxNumberOfMultiValues()];
      }
    }

    if (longOverflow) {
      _globalGroupIdUpperBound = Integer.MAX_VALUE;
      _rawKeyHolder = new ArrayMapBasedHolder();
    } else {
      if (cardinalityProduct > Integer.MAX_VALUE) {
        _globalGroupIdUpperBound = Integer.MAX_VALUE;
        _rawKeyHolder = new LongMapBasedHolder();
      } else {
        _globalGroupIdUpperBound = (int) cardinalityProduct;
        if (cardinalityProduct > arrayBasedThreshold) {
          _rawKeyHolder = new IntMapBasedHolder();
        } else {
          _rawKeyHolder = new ArrayBasedHolder();
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGlobalGroupKeyUpperBound() {
    return _globalGroupIdUpperBound;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[] outGroupIds) {
    // Fetch dictionary ids in the given block for all group-by columns
    for (int i = 0; i < _numGroupByColumns; i++) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(_groupByColumns[i]);
      _singleValueDictIds[i] = blockValueSet.getDictionaryIds();
    }

    _rawKeyHolder.processSingleValue(transformBlock.getNumDocs(), outGroupIds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[][] outGroupIds) {
    int length = transformBlock.getNumDocs();
    int[] docIdSet = transformBlock.getDocIdSetBlock().getDocIdSet();

    for (int i = 0; i < _numGroupByColumns; i++) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(_groupByColumns[i]);
      if (_isSingleValueColumn[i]) {
        // Fetch dictionary ids in the given block for all single value group-by columns
        _singleValueDictIds[i] = blockValueSet.getDictionaryIds();
      } else {
        // Cache block value set for all multi-value group-by columns
        _blockValSets[i] = blockValueSet;
      }
    }

    for (int i = 0; i < length; i++) {
      int docId = docIdSet[i];
      for (int j = 0; j < _numGroupByColumns; j++) {
        if (!_isSingleValueColumn[j]) {
          _numValues[j] = _blockValSets[j].getDictionaryIdsForDocId(docId, _multiValueDictIds[j]);
        }
      }
      outGroupIds[i] = _rawKeyHolder.processMultiValue(i);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getCurrentGroupKeyUpperBound() {
    return _rawKeyHolder.getGroupIdUpperBound();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<GroupKey> getUniqueGroupKeys() {
    return _rawKeyHolder.iterator();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void purgeKeys(int[] keyIdsToPurge) {
    // TODO: Make trimming work
  }

  private interface RawKeyHolder extends Iterable<GroupKey> {

    /**
     * Process a block of documents for all single-value group-by columns case.
     *
     * @param numDocs Number of documents inside the block
     * @param outGroupIds Buffer for group id results
     */
    void processSingleValue(int numDocs, @Nonnull int[] outGroupIds);

    /**
     * Process a single document for case with multi-value group-by columns.
     *
     * @param index Index in block
     * @return Array of group ids for the given document id
     */
    @Nonnull
    int[] processMultiValue(int index);

    /**
     * Get the upper bound of group id (exclusive) inside the holder.
     *
     * @return Upper bound of group id inside the holder
     */
    int getGroupIdUpperBound();
  }

  private class ArrayBasedHolder implements RawKeyHolder {
    // TODO: using bitmap might better
    private final boolean[] _flags = new boolean[_globalGroupIdUpperBound];

    @Override
    public void processSingleValue(int numDocs, @Nonnull int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int groupId = 0;
        for (int j = _numGroupByColumns - 1; j >= 0; j--) {
          groupId = groupId * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = groupId;
        _flags[groupId] = true;
      }
    }

    @Nonnull
    @Override
    public int[] processMultiValue(int index) {
      int[] groupIds = getIntRawKeys(index);
      for (int groupId : groupIds) {
        _flags[groupId] = true;
      }
      return groupIds;
    }

    @Override
    public int getGroupIdUpperBound() {
      return _globalGroupIdUpperBound;
    }

    @Nonnull
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
    private final Int2IntOpenHashMap _rawKeyToGroupIdMap = new Int2IntOpenHashMap();

    public IntMapBasedHolder() {
      _rawKeyToGroupIdMap.defaultReturnValue(INVALID_ID);
    }

    @Override
    public void processSingleValue(int numDocs, @Nonnull int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int rawKey = 0;
        for (int j = _numGroupByColumns - 1; j >= 0; j--) {
          rawKey = rawKey * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(rawKey);
      }
    }

    @Nonnull
    @Override
    public int[] processMultiValue(int index) {
      int[] groupIds = getIntRawKeys(index);

      // Convert raw keys to group ids
      int length = groupIds.length;
      for (int i = 0; i < length; i++) {
        groupIds[i] = getGroupId(groupIds[i]);
      }

      return groupIds;
    }

    private int getGroupId(int rawKey) {
      int groupId = _rawKeyToGroupIdMap.get(rawKey);
      if (groupId == INVALID_ID) {
        groupId = _rawKeyToGroupIdMap.size();
        _rawKeyToGroupIdMap.put(rawKey, groupId);
      }
      return groupId;
    }

    @Override
    public int getGroupIdUpperBound() {
      return _rawKeyToGroupIdMap.size();
    }

    @Nonnull
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
    if (_numGroupByColumns == 1) {
      rawKeys = Arrays.copyOf(_multiValueDictIds[0], _numValues[0]);
    } else {
      // Before having to transform to array, use single value raw key for better performance
      int rawKey = 0;

      for (int i = _numGroupByColumns - 1; i >= 0; i--) {
        int cardinality = _cardinalities[i];
        boolean isSingleValueColumn = _isSingleValueColumn[i];
        int numValues = _numValues[i];
        int[] multiValueDictIds = _multiValueDictIds[i];

        // Specialize multi-value column with only one value inside
        if (isSingleValueColumn || numValues == 1) {
          int dictId;
          if (isSingleValueColumn) {
            dictId = _singleValueDictIds[i][index];
          } else {
            dictId = multiValueDictIds[0];
          }
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
    if (_numGroupByColumns == 1) {
      return _dictionaries[0].get(rawKey).toString();
    } else {
      int cardinality = _cardinalities[0];
      StringBuilder groupKeyBuilder = new StringBuilder(_dictionaries[0].get(rawKey % cardinality).toString());
      rawKey /= cardinality;
      for (int i = 1; i < _numGroupByColumns; i++) {
        groupKeyBuilder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
        cardinality = _cardinalities[i];
        groupKeyBuilder.append(_dictionaries[i].get(rawKey % cardinality));
        rawKey /= cardinality;
      }
      return groupKeyBuilder.toString();
    }
  }

  private class LongMapBasedHolder implements RawKeyHolder {
    private final Long2IntOpenHashMap _rawKeyToGroupIdMap = new Long2IntOpenHashMap();

    public LongMapBasedHolder() {
      _rawKeyToGroupIdMap.defaultReturnValue(INVALID_ID);
    }

    @Override
    public void processSingleValue(int numDocs, @Nonnull int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        long rawKey = 0L;
        for (int j = _numGroupByColumns - 1; j >= 0; j--) {
          rawKey = rawKey * _cardinalities[j] + _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(rawKey);
      }
    }

    @Nonnull
    @Override
    public int[] processMultiValue(int index) {
      long[] rawKeys = getLongRawKeys(index);
      int length = rawKeys.length;
      int[] groupIds = new int[length];
      for (int i = 0; i < length; i++) {
        groupIds[i] = getGroupId(rawKeys[i]);
      }
      return groupIds;
    }

    private int getGroupId(long rawKey) {
      int groupId = _rawKeyToGroupIdMap.get(rawKey);
      if (groupId == INVALID_ID) {
        groupId = _rawKeyToGroupIdMap.size();
        _rawKeyToGroupIdMap.put(rawKey, groupId);
      }
      return groupId;
    }

    @Override
    public int getGroupIdUpperBound() {
      return _rawKeyToGroupIdMap.size();
    }

    @Nonnull
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

    for (int i = _numGroupByColumns - 1; i >= 0; i--) {
      int cardinality = _cardinalities[i];
      boolean isSingleValueColumn = _isSingleValueColumn[i];
      int numValues = _numValues[i];
      int[] multiValueDictIds = _multiValueDictIds[i];

      // Specialize multi-value column with only one value inside
      if (isSingleValueColumn || numValues == 1) {
        int dictId;
        if (isSingleValueColumn) {
          dictId = _singleValueDictIds[i][index];
        } else {
          dictId = multiValueDictIds[0];
        }
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
    StringBuilder groupKeyBuilder = new StringBuilder(_dictionaries[0].get((int) (rawKey % cardinality)).toString());
    rawKey /= cardinality;
    for (int i = 1; i < _numGroupByColumns; i++) {
      groupKeyBuilder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
      cardinality = _cardinalities[i];
      groupKeyBuilder.append(_dictionaries[i].get((int) (rawKey % cardinality)));
      rawKey /= cardinality;
    }
    return groupKeyBuilder.toString();
  }

  private class ArrayMapBasedHolder implements RawKeyHolder {
    private final Object2IntOpenHashMap<IntArray> _rawKeyToGroupIdMap = new Object2IntOpenHashMap<>();

    public ArrayMapBasedHolder() {
      _rawKeyToGroupIdMap.defaultReturnValue(INVALID_ID);
    }

    @Override
    public void processSingleValue(int numDocs, @Nonnull int[] outGroupIds) {
      for (int i = 0; i < numDocs; i++) {
        int[] dictIds = new int[_numGroupByColumns];
        for (int j = 0; j < _numGroupByColumns; j++) {
          dictIds[j] = _singleValueDictIds[j][i];
        }
        outGroupIds[i] = getGroupId(new IntArray(dictIds));
      }
    }

    @Nonnull
    @Override
    public int[] processMultiValue(int index) {
      IntArray[] rawKeys = getIntArrayRawKeys(index);
      int length = rawKeys.length;
      int[] groupIds = new int[length];
      for (int i = 0; i < length; i++) {
        groupIds[i] = getGroupId(rawKeys[i]);
      }
      return groupIds;
    }

    private int getGroupId(IntArray rawKey) {
      int groupId = _rawKeyToGroupIdMap.getInt(rawKey);
      if (groupId == INVALID_ID) {
        groupId = _rawKeyToGroupIdMap.size();
        _rawKeyToGroupIdMap.put(rawKey, groupId);
      }
      return groupId;
    }

    @Override
    public int getGroupIdUpperBound() {
      return _rawKeyToGroupIdMap.size();
    }

    @Nonnull
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
    int[] dictIds = new int[_numGroupByColumns];

    for (int i = 0; i < _numGroupByColumns; i++) {
      boolean isSingleValueColumn = _isSingleValueColumn[i];
      int numValues = _numValues[i];
      int[] multiValueDictIds = _multiValueDictIds[i];

      // Specialize multi-value column with only one value inside
      if (isSingleValueColumn || numValues == 1) {
        int dictId;
        if (isSingleValueColumn) {
          dictId = _singleValueDictIds[i][index];
        } else {
          dictId = multiValueDictIds[0];
        }
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
    StringBuilder groupKeyBuilder = new StringBuilder(_dictionaries[0].get(rawKey._elements[0]).toString());
    for (int i = 1; i < _numGroupByColumns; i++) {
      groupKeyBuilder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
      groupKeyBuilder.append(_dictionaries[i].get(rawKey._elements[i]));
    }
    return groupKeyBuilder.toString();
  }

  /**
   * Drop un-necessary checks for highest performance.
   */
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

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
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
