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
package com.linkedin.pinot.core.operator.aggregation.groupby;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.operator.aggregation.ResultHolderFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;


/**
 * Class for generating group-by keys for a given list of group-by columns.
 * Uses dictionary id's to generate group-by keys.
 *
 * The maximum number of possible group-by keys is calculated based on the cardinality product of all the group-by
 * columns.
 *
 * If the maximum number of possible group-by keys is less than a threshold (10K), return the raw key generated based
 * on group-by column dictionary id as the group key. (ARRAY_BASED storage type)
 *
 * If the maximum number of possible group-by keys is larger than the threshold, but still fit into long, generate long
 * type raw keys based on the group-by column dictionary ids, and map the raw keys onto contiguous indices, use the
 * indices as the group key. (LONG_MAP_BASED storage type)
 *
 * If the maximum number of possible group-by keys cannot fit into long, use int arrays as the raw keys to store all
 * the group-by column dictionary ids, and map the int arrays onto continuous indices, use the indices as the group key.
 * (ARRAY_MAP_BASED storage type)
 *
 * All the logic is maintained internally, and to the outside world, the group keys are always int type.
 */
public class DefaultGroupKeyGenerator implements GroupKeyGenerator {
  private static final int INVALID_ID = -1;
  private static final BlockId BLOCK_ZERO = new BlockId(0);

  public enum StorageType {
    ARRAY_BASED,
    LONG_MAP_BASED,
    ARRAY_MAP_BASED
  }

  private final int _numGroupByColumns;
  private final int[] _cardinalities;
  private long _cardinalityProduct = 1L;
  private final boolean[] _isSingleValueGroupByColumn;
  private boolean _hasMultiValueGroupByColumn = false;
  private final StorageType _storageType;

  private final Dictionary[] _dictionaries;
  // For single value columns.
  private final BlockValSet[] _singleBlockValSets;
  // For multi value columns.
  private final BlockMultiValIterator[] _multiValIterators;

  // Reusable arrays for single value columns.
  private final int[][] _reusableSingleDictIds;
  // Reusable buffer for multi value columns.
  private final int[] _reusableMultiValDictIdBuffer;

  // For ARRAY_BASED storage type.
  private boolean[] _groupKeyFlags;
  // For LONG_MAP_BASED and ARRAY_MAP_BASED storage type to track the number of group keys.
  private int _numGroupKeys = 0;
  // For LONG_MAP_BASED storage type.
  private Long2IntOpenHashMap _groupKeyToId;
  // For ARRAY_MAP_BASED storage type.
  private Object2IntOpenHashMap<IntArrayList> _arrayGroupKeyToId;

  /**
   * Constructor for the class. Initializes data members (reusable arrays).
   *
   * @param dataFetcher data fetcher.
   * @param groupByColumns group-by columns.
   */
  public DefaultGroupKeyGenerator(DataFetcher dataFetcher, String[] groupByColumns) {
    _numGroupByColumns = groupByColumns.length;
    _cardinalities = new int[_numGroupByColumns];
    _isSingleValueGroupByColumn = new boolean[_numGroupByColumns];
    _dictionaries = new Dictionary[_numGroupByColumns];
    _singleBlockValSets = new BlockValSet[_numGroupByColumns];
    _multiValIterators = new BlockMultiValIterator[_numGroupByColumns];
    _reusableSingleDictIds = new int[_numGroupByColumns][];

    // Track the max number of values among all multi value group-by columns.
    int maxNumMultiValues = 0;

    boolean longOverflow = false;
    for (int i = 0; i < _numGroupByColumns; i++) {
      DataSource dataSource = dataFetcher.getDataSourceForColumn(groupByColumns[i]);

      // Store group-by column cardinalities and update cardinality product.
      int cardinality = dataSource.getDataSourceMetadata().cardinality();
      _cardinalities[i] = cardinality;
      if (!longOverflow) {
        if (_cardinalityProduct > Long.MAX_VALUE / cardinality) {
          longOverflow = true;
          _cardinalityProduct = Long.MAX_VALUE;
        } else {
          _cardinalityProduct *= cardinality;
        }
      }

      // Store single/multi value group-by columns, allocate reusable resources based on that.
      boolean isSingleValueGroupByColumn = dataSource.getDataSourceMetadata().isSingleValue();
      _isSingleValueGroupByColumn[i] = isSingleValueGroupByColumn;
      if (!isSingleValueGroupByColumn) {
        _hasMultiValueGroupByColumn = true;
      }
      _dictionaries[i] = dataSource.getDictionary();
      Block block = dataSource.nextBlock(BLOCK_ZERO);
      if (isSingleValueGroupByColumn) {
        _singleBlockValSets[i] = block.getBlockValueSet();
        _reusableSingleDictIds[i] = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      } else {
        maxNumMultiValues = Math.max(maxNumMultiValues, block.getMetadata().getMaxNumberOfMultiValues());
        _multiValIterators[i] = (BlockMultiValIterator) block.getBlockValueSet().iterator();
      }
    }

    // Allocate a big enough buffer for all the multi value group-by columns.
    _reusableMultiValDictIdBuffer = new int[maxNumMultiValues];

    // Decide the storage type based on the over flow flag and cardinality product.
    if (longOverflow) {
      // Array map based storage type.
      _storageType = StorageType.ARRAY_MAP_BASED;
      _arrayGroupKeyToId = new Object2IntOpenHashMap<>();
      _arrayGroupKeyToId.defaultReturnValue(INVALID_ID);
    } else {
      if (_cardinalityProduct > ResultHolderFactory.MAX_INITIAL_RESULT_HOLDER_CAPACITY) {
        // Long map based storage type.
        _storageType = StorageType.LONG_MAP_BASED;
        _groupKeyToId = new Long2IntOpenHashMap();
        _groupKeyToId.defaultReturnValue(INVALID_ID);
      } else {
        // Array based storage type.
        _storageType = StorageType.ARRAY_BASED;
        _groupKeyFlags = new boolean[(int) _cardinalityProduct];
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGlobalGroupKeyUpperBound() {
    if (_cardinalityProduct > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) _cardinalityProduct;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasMultiValueGroupByColumn() {
    return _hasMultiValueGroupByColumn;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[] docIdToGroupKey) {
    // Fetch all dictionary ids according to the document id set for all group-by columns.
    for (int i = 0; i < _numGroupByColumns; i++) {
      _singleBlockValSets[i].readIntValues(docIdSet, startIndex, length, _reusableSingleDictIds[i], 0);
    }

    // Calculate the group key and store it into the result buffer.
    int outIndex = 0;
    int endIndex = startIndex + length;
    switch (_storageType) {
      case ARRAY_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          int groupKey = 0;
          for (int j = _numGroupByColumns - 1; j >= 0; j--) {
            groupKey = groupKey * _cardinalities[j] + _reusableSingleDictIds[j][i];
          }
          docIdToGroupKey[outIndex++] = groupKey;
          _groupKeyFlags[groupKey] = true;
        }
        break;
      case LONG_MAP_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          long rawKey = 0;
          for (int j = _numGroupByColumns - 1; j >= 0; j--) {
            rawKey = rawKey * _cardinalities[j] + _reusableSingleDictIds[j][i];
          }
          docIdToGroupKey[outIndex++] = updateRawKeyToGroupKeyMapping(rawKey);
        }
        break;
      case ARRAY_MAP_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          IntArrayList rawKey = new IntArrayList(_numGroupByColumns);
          rawKey.size(_numGroupByColumns);
          int[] rawKeyArray = rawKey.elements();
          for (int j = 0; j < _numGroupByColumns; j++) {
            rawKeyArray[j] = _reusableSingleDictIds[j][i];
          }
          docIdToGroupKey[outIndex++] = updateRawKeyToGroupKeyMapping(rawKey);
        }
        break;
      default:
        throw new RuntimeException("Unsupported storage type.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void generateKeysForDocIdSet(int[] docIdSet, int startIndex, int length, int[][] docIdToGroupKeys) {
    // Fetch all dictionary ids according to the document id set for all single value group-by columns.
    for (int i = 0; i < _numGroupByColumns; i++) {
      if (_isSingleValueGroupByColumn[i]) {
        _singleBlockValSets[i].readIntValues(docIdSet, startIndex, length, _reusableSingleDictIds[i], 0);
      }
    }

    // Calculate the group keys(int[]) and store it into the result buffer.
    int outIndex = 0;
    int endIndex = startIndex + length;
    switch (_storageType) {
      case ARRAY_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          docIdToGroupKeys[outIndex] = generateKeysForDocIdArrayBased(outIndex, docIdSet[i]);
          outIndex++;
        }
        break;
      case LONG_MAP_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          docIdToGroupKeys[outIndex] = generateKeysForDocIdLongMapBased(outIndex, docIdSet[i]);
          outIndex++;
        }
        break;
      case ARRAY_MAP_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          docIdToGroupKeys[outIndex] = generateKeysForDocIdArrayMapBased(outIndex, docIdSet[i]);
          outIndex++;
        }
        break;
      default:
        throw new RuntimeException("Unsupported storage type.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getCurrentGroupKeyUpperBound() {
    if (_storageType == StorageType.ARRAY_BASED) {
      return (int) _cardinalityProduct;
    } else {
      return _numGroupKeys;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<GroupKey> getUniqueGroupKeys() {
    switch (_storageType) {
      case ARRAY_BASED:
        return new ArrayBasedGroupKeyIterator();
      case LONG_MAP_BASED:
        return new LongMapBasedGroupKeyIterator();
      case ARRAY_MAP_BASED:
        return new ArrayMapBasedGroupKeyIterator();
      default:
        throw new RuntimeException("Unsupported storage type.");
    }
  }

  /**
   * Helper function to get the group key associated to the long type raw key. If there is no one, generate a new group
   * key for this raw key and put them into the map.
   * (LONG_MAP_BASED storage type)
   *
   * @param rawKey raw key.
   * @return group key.
   */
  private int updateRawKeyToGroupKeyMapping(long rawKey) {
    int groupKey = _groupKeyToId.get(rawKey);
    if (groupKey == INVALID_ID) {
      groupKey = _numGroupKeys++;
      _groupKeyToId.put(rawKey, groupKey);
    }
    return groupKey;
  }

  /**
   * Helper function to get the group key associated to the IntArrayList type raw key. If there is no one, generate a
   * new group key for this raw key and put them into the map.
   * (ARRAY_MAP_BASED storage type)
   *
   * @param rawKey raw key.
   * @return group key.
   */
  private int updateRawKeyToGroupKeyMapping(IntArrayList rawKey) {
    int groupKey = _arrayGroupKeyToId.getInt(rawKey);
    if (groupKey == INVALID_ID) {
      groupKey = _numGroupKeys++;
      _arrayGroupKeyToId.put(rawKey, groupKey);
    }
    return groupKey;
  }

  /**
   * Helper function to generate group keys (int[]) according to the document id. This method should only be called when
   * there are multi value group-by columns.
   * (ARRAY_BASED storage type)
   *
   * @param index index of the docIdSet.
   * @param docId document id.
   * @return group keys.
   */
  private int[] generateKeysForDocIdArrayBased(int index, int docId) {
    int[] groupKeys = {0};
    int length = 1;

    for (int i = _numGroupByColumns - 1; i >= 0; i--) {
      int cardinality = _cardinalities[i];
      if (_isSingleValueGroupByColumn[i]) {
        int dictId = _reusableSingleDictIds[i][index];
        for (int j = 0; j < length; j++) {
          groupKeys[j] = groupKeys[j] * cardinality + dictId;
        }
      } else {
        BlockMultiValIterator blockValIterator = _multiValIterators[i];
        blockValIterator.skipTo(docId);
        int numMultiValues = blockValIterator.nextIntVal(_reusableMultiValDictIdBuffer);

        int oldLength = length;
        length *= numMultiValues;
        int[] oldGroupKeys = groupKeys;
        groupKeys = new int[length];
        for (int j = 0; j < numMultiValues; j++) {
          System.arraycopy(oldGroupKeys, 0, groupKeys, j * oldLength, oldLength);
        }
        for (int j = 0; j < numMultiValues; j++) {
          int dictId = _reusableMultiValDictIdBuffer[j];
          int offset = j * oldLength;
          for (int k = 0; k < oldLength; k++) {
            int idx = offset + k;
            groupKeys[idx] = groupKeys[idx] * cardinality + dictId;
          }
        }
      }
    }

    for (int groupKey : groupKeys) {
      _groupKeyFlags[groupKey] = true;
    }
    return groupKeys;
  }

  /**
   * Helper function to generate group keys (int[]) according to the document id. This method should only be called when
   * there are multi value group-by columns.
   * (LONG_MAP_BASED storage type)
   *
   * @param index index of the docIdSet.
   * @param docId document id.
   * @return group keys.
   */
  private int[] generateKeysForDocIdLongMapBased(int index, int docId) {
    long[] rawKeys = {0L};
    int length = 1;

    for (int i = _numGroupByColumns - 1; i >= 0; i--) {
      long cardinality = _cardinalities[i];
      if (_isSingleValueGroupByColumn[i]) {
        long dictId = _reusableSingleDictIds[i][index];
        for (int j = 0; j < length; j++) {
          rawKeys[j] = rawKeys[j] * cardinality + dictId;
        }
      } else {
        BlockMultiValIterator blockValIterator = _multiValIterators[i];
        blockValIterator.skipTo(docId);
        int numMultiValues = blockValIterator.nextIntVal(_reusableMultiValDictIdBuffer);

        int oldLength = length;
        length *= numMultiValues;
        long[] oldRawKeys = rawKeys;
        rawKeys = new long[length];
        for (int j = 0; j < numMultiValues; j++) {
          System.arraycopy(oldRawKeys, 0, rawKeys, j * oldLength, oldLength);
        }
        for (int j = 0; j < numMultiValues; j++) {
          long dictId = _reusableMultiValDictIdBuffer[j];
          int offset = j * oldLength;
          for (int k = 0; k < oldLength; k++) {
            int idx = offset + k;
            rawKeys[idx] = rawKeys[idx] * cardinality + dictId;
          }
        }
      }
    }

    int[] groupKeys = new int[length];
    for (int i = 0; i < length; i++) {
      groupKeys[i] = updateRawKeyToGroupKeyMapping(rawKeys[i]);
    }
    return groupKeys;
  }

  /**
   * Helper function to generate group keys (int[]) according to the document id. This method should only be called when
   * there are multi value group-by columns.
   * (ARRAY_MAP_BASED storage type)
   *
   * @param index index of the docIdSet.
   * @param docId document id.
   * @return group keys.
   */
  private int[] generateKeysForDocIdArrayMapBased(int index, int docId) {
    IntArrayList[] rawKeys = {new IntArrayList(_numGroupByColumns)};
    rawKeys[0].size(_numGroupByColumns);
    int length = 1;

    for (int i = 0; i < _numGroupByColumns; i++) {
      if (_isSingleValueGroupByColumn[i]) {
        int dictId = _reusableSingleDictIds[i][index];
        for (IntArrayList rawKey : rawKeys) {
          rawKey.elements()[i] = dictId;
        }
      } else {
        BlockMultiValIterator blockValIterator = _multiValIterators[i];
        blockValIterator.skipTo(docId);
        int numMultiValues = blockValIterator.nextIntVal(_reusableMultiValDictIdBuffer);

        int oldLength = length;
        length *= numMultiValues;
        IntArrayList[] oldRawKeys = rawKeys;
        rawKeys = new IntArrayList[length];
        System.arraycopy(oldRawKeys, 0, rawKeys, 0, oldLength);
        for (int j = 1; j < numMultiValues; j++) {
          int offset = j * oldLength;
          for (int k = 0; k < oldLength; k++) {
            rawKeys[offset + k] = new IntArrayList(oldRawKeys[k].elements());
          }
        }
        for (int j = 0; j < numMultiValues; j++) {
          int dictId = _reusableMultiValDictIdBuffer[j];
          int offset = j * oldLength;
          for (int k = 0; k < oldLength; k++) {
            int idx = offset + k;
            rawKeys[idx].elements()[i] = dictId;
          }
        }
      }
    }

    int[] groupKeys = new int[length];
    for (int i = 0; i < length; i++) {
      groupKeys[i] = updateRawKeyToGroupKeyMapping(rawKeys[i]);
    }
    return groupKeys;
  }

  /**
   * Inner class to implement group by key iterator for ARRAY_BASED storage.
   */
  private class ArrayBasedGroupKeyIterator implements Iterator<GroupKey> {
    final int _length = _groupKeyFlags.length;
    int _index = 0;
    final GroupKey _groupKey = new GroupKey(INVALID_ID, null);

    @Override
    public boolean hasNext() {
      while (_index < _length) {
        if (_groupKeyFlags[_index]) {
          return true;
        }
        _index++;
      }
      return false;
    }

    @Override
    public GroupKey next() {
      String stringGroupKey = groupKeyToStringGroupKey(_index);
      _groupKey.setFirst(_index++);
      _groupKey.setSecond(stringGroupKey);
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Inner class to implement group by keys iterator for LONG_MAP_BASED storage.
   */
  private class LongMapBasedGroupKeyIterator implements Iterator<GroupKey> {
    final ObjectIterator<Long2IntMap.Entry> _iterator = _groupKeyToId.long2IntEntrySet().fastIterator();
    final GroupKey _groupKey = new GroupKey(INVALID_ID, null);

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Long2IntMap.Entry entry = _iterator.next();

      String stringGroupKey = rawKeyToStringGroupKey(entry.getLongKey());
      _groupKey.setFirst(entry.getIntValue());
      _groupKey.setSecond(stringGroupKey);
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Inner class to implement group by keys iterator for ARRAY_MAP_BASED storage.
   */
  private class ArrayMapBasedGroupKeyIterator implements  Iterator<GroupKey> {
    final ObjectIterator<Object2IntMap.Entry<IntArrayList>> _iterator =
        _arrayGroupKeyToId.object2IntEntrySet().fastIterator();
    final GroupKey _groupKey = new GroupKey(INVALID_ID, null);

    @Override
    public boolean hasNext() {
      return _iterator.hasNext();
    }

    @Override
    public GroupKey next() {
      Object2IntMap.Entry<IntArrayList> entry = _iterator.next();

      String stringGroupKey = rawKeyToStringGroupKey(entry.getKey());
      _groupKey.setFirst(entry.getIntValue());
      _groupKey.setSecond(stringGroupKey);
      return _groupKey;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * With an integer group key, convert group key from dictId based to string based, using actually values corresponding
   * to dictionary id's.
   * (ARRAY_BASED storage type)
   *
   * @param groupKey integer group key.
   * @return string group key.
   */
  private String groupKeyToStringGroupKey(int groupKey) {
    if (_numGroupByColumns == 1) {
      // Special case one group-by column for performance.
      return _dictionaries[0].get(groupKey).toString();
    } else {
      // Decode the group key.
      int cardinality = _cardinalities[0];
      StringBuilder builder = new StringBuilder(_dictionaries[0].get(groupKey % cardinality).toString());
      groupKey /= cardinality;
      for (int i = 1; i < _numGroupByColumns; i++) {
        builder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter);
        cardinality = _cardinalities[i];
        builder.append(_dictionaries[i].get(groupKey % cardinality));
        groupKey /= cardinality;
      }
      return builder.toString();
    }
  }

  /**
   * With a long raw key, convert raw key from dictId based to string based group key, using actually values
   * corresponding to dictionary id's.
   * (LONG_BASED storage type)
   *
   * @param rawKey long raw key.
   * @return string group key.
   */
  private String rawKeyToStringGroupKey(long rawKey) {
    if (_numGroupByColumns == 1) {
      // Special case one group-by column for performance.
      return _dictionaries[0].get((int) rawKey).toString();
    } else {
      // Decode the raw key.
      int cardinality = _cardinalities[0];
      StringBuilder builder = new StringBuilder(_dictionaries[0].get((int) (rawKey % cardinality)).toString());
      rawKey /= cardinality;
      for (int i = 1; i < _numGroupByColumns; i++) {
        builder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter);
        cardinality = _cardinalities[i];
        builder.append(_dictionaries[i].get((int) (rawKey % cardinality)));
        rawKey /= cardinality;
      }
      return builder.toString();
    }
  }

  /**
   * With a IntArrayList raw key, convert raw key from dictId based to string based group key, using actually values
   * corresponding to dictionary id's.
   * (ARRAY_MAP_BASED storage type)
   *
   * @param rawKey IntArrayList raw key.
   * @return string group key.
   */
  private String rawKeyToStringGroupKey(IntArrayList rawKey) {
    int[] rawKeyArray = rawKey.elements();
    StringBuilder builder = new StringBuilder(_dictionaries[0].get(rawKeyArray[0]).toString());
    for (int i = 1; i < _numGroupByColumns; i++) {
      builder.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter);
      builder.append(_dictionaries[i].get(rawKeyArray[i]).toString());
    }
    return builder.toString();
  }
}
