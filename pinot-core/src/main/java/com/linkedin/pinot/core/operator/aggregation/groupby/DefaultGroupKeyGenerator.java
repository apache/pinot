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

import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;


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
  public enum StorageType {
    ARRAY_BASED,
    LONG_MAP_BASED,
    ARRAY_MAP_BASED
  }

  private final String[] _groupByColumns;
  private final int _numGroupByColumns;
  private final int[] _cardinalities;
  private long _cardinalityProduct = 1L;
  private final boolean[] _isSingleValueGroupByColumn;
  private final StorageType _storageType;

  private final Dictionary[] _dictionaries;

  // For transformBlockValSet of columns.
  private final BlockValSet[] _blockValSets;

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

  // The following data structures are used for trimming group keys.

  // TODO: the key will be contiguous so we should use array here.
  // Reverse mapping for trimming group keys
  private Int2LongOpenHashMap _idToGroupKey;

  // Reverse mapping from KeyIds to groupKeys. purgeGroupKeys takes an array of keyIds to remove,
  // this map tracks keyIds to groupKeys, to serve the purging.
  private Int2ObjectOpenHashMap<IntArrayList> _idToArrayGroupKey;

  // Enum to reflect if trimming of group keys is ON or OFF. Once ON, we need to start tracking
  // the keyIds that are removed.
  private enum TrimMode {
    OFF,
    ON
  }
  private TrimMode _trimMode;

  /**
   * Constructor for the class. Initializes data members (reusable arrays).
   *
   * @param transformBlock Transform block for which to generate group keys
   * @param groupByColumns group-by columns.
   */
  public DefaultGroupKeyGenerator(TransformBlock transformBlock, String[] groupByColumns) {
    _numGroupByColumns = groupByColumns.length;
    _groupByColumns = groupByColumns;

    _cardinalities = new int[_numGroupByColumns];
    _isSingleValueGroupByColumn = new boolean[_numGroupByColumns];
    _dictionaries = new Dictionary[_numGroupByColumns];
    _blockValSets = new BlockValSet[_numGroupByColumns];
    _reusableSingleDictIds = new int[_numGroupByColumns][];

    // Track the max number of values among all multi value group-by columns.
    int maxNumMultiValues = 0;

    boolean longOverflow = false;
    for (int i = 0; i < _numGroupByColumns; i++) {
      BlockMetadata blockMetadata = transformBlock.getBlockMetadata(groupByColumns[i]);

      // Store group-by column cardinalities and update cardinality product.
      _dictionaries[i] = blockMetadata.getDictionary();
      int cardinality = _dictionaries[i].length();
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
      boolean isSingleValueGroupByColumn = blockMetadata.isSingleValue();
      _isSingleValueGroupByColumn[i] = isSingleValueGroupByColumn;

      if (!isSingleValueGroupByColumn) {
        maxNumMultiValues = Math.max(maxNumMultiValues, blockMetadata.getMaxNumberOfMultiValues());
      }
    }

    // Allocate a big enough buffer for all the multi value group-by columns.
    _reusableMultiValDictIdBuffer = new int[maxNumMultiValues];

    // We do not trim group keys unless we exceed the _maxCapacity for the holder.
    _trimMode = TrimMode.OFF;

    // Decide the storage type based on the over flow flag and cardinality product.
    if (longOverflow) {
      // Array map based storage type.
      _storageType = StorageType.ARRAY_MAP_BASED;
      _arrayGroupKeyToId = new Object2IntOpenHashMap<>();
      _arrayGroupKeyToId.defaultReturnValue(INVALID_ID);
      _idToArrayGroupKey = null;
    } else {
      if (_cardinalityProduct > DefaultGroupByExecutor.MAX_INITIAL_RESULT_HOLDER_CAPACITY) {
        // Long map based storage type.
        _storageType = StorageType.LONG_MAP_BASED;
        _groupKeyToId = new Long2IntOpenHashMap();
        _groupKeyToId.defaultReturnValue(INVALID_ID);
        _idToGroupKey = null;
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
   *
   */
  @Override
  public void generateKeysForBlock(TransformBlock transformBlock, int[] outGroupKeys) {
    int startIndex = 0;
    int length = transformBlock.getNumDocs();

    // Fetch all dictionary ids according to the document id set for all group-by columns.
    for (int i = 0; i < _numGroupByColumns; i++) {
      BlockValSet blockValueSet = transformBlock.getBlockValueSet(_groupByColumns[i]);
      _reusableSingleDictIds[i] = blockValueSet.getDictionaryIds();
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
          outGroupKeys[outIndex++] = groupKey;
          _groupKeyFlags[groupKey] = true;
        }
        break;
      case LONG_MAP_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          long rawKey = 0;
          for (int j = _numGroupByColumns - 1; j >= 0; j--) {
            rawKey = rawKey * _cardinalities[j] + _reusableSingleDictIds[j][i];
          }
          outGroupKeys[outIndex++] = updateRawKeyToGroupKeyMapping(rawKey);
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
          outGroupKeys[outIndex++] = updateRawKeyToGroupKeyMapping(rawKey);
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
  public void generateKeysForBlock(TransformBlock transformBlock, int[][] outGroupKeys) {
    int startIndex = 0;
    int length = transformBlock.getNumDocs();
    int[] docIdSet = transformBlock.getDocIdSetBlock().getDocIdSet();

    // Fetch all dictionary ids according to the document id set for all single value group-by columns.
    for (int i = 0; i < _numGroupByColumns; i++) {
      _blockValSets[i] = transformBlock.getBlockValueSet(_groupByColumns[i]);
      if (_isSingleValueGroupByColumn[i]) {
        _reusableSingleDictIds[i] = _blockValSets[i].getDictionaryIds();
      }
    }

    // Calculate the group keys(int[]) and store it into the result buffer.
    int outIndex = 0;
    int endIndex = startIndex + length;
    switch (_storageType) {
      case ARRAY_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          outGroupKeys[outIndex] = generateKeysForDocIdArrayBased(outIndex, docIdSet[i]);
          outIndex++;
        }
        break;
      case LONG_MAP_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          outGroupKeys[outIndex] = generateKeysForDocIdLongMapBased(outIndex, docIdSet[i]);
          outIndex++;
        }
        break;
      case ARRAY_MAP_BASED:
        for (int i = startIndex; i < endIndex; i++) {
          outGroupKeys[outIndex] = generateKeysForDocIdArrayMapBased(outIndex, docIdSet[i]);
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
        throw new RuntimeException("Unsupported storage type for key generator " + _storageType);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param keyIdsToPurge Group keys to purge
   */
  @Override
  public void purgeKeys(int[] keyIdsToPurge) {
    switch (_storageType) {
      case ARRAY_BASED:
        if (keyIdsToPurge != null && keyIdsToPurge.length != 0) {
          throw new RuntimeException("Illegal operation: Purging group keys not allowed for array-based key generator");
        } // else nothing to be done.
        break;

      case LONG_MAP_BASED:
        purgeLongMapKeys(keyIdsToPurge);
        break;

      case ARRAY_MAP_BASED:
        purgeArrayMapKeys(keyIdsToPurge);
        break;

      default:
        throw new RuntimeException("Unsupported storage type for key generator " + _storageType);
    }
  }

  /**
   * Helper method to purge group keys that got trimmed from the group by result.
   * Lazily builds a reverse map from id to array group key on the first call.
   *
   * @param groupKeys Group keys to purge
   */
  private void purgeLongMapKeys(int[] groupKeys) {
    if (groupKeys == null || groupKeys.length == 0) {
      return; // Nothing to purge
    }

    // Lazily build the idToGroupKey reverse map only once, and then keep the two maps in-sync after that.
    if (_trimMode == TrimMode.OFF) {
      _idToGroupKey = new Int2LongOpenHashMap(_groupKeyToId.size());
      for (Long2IntMap.Entry entry : _groupKeyToId.long2IntEntrySet()) {
        _idToGroupKey.put(entry.getIntValue(), entry.getLongKey());
      }
      _trimMode = TrimMode.ON;
    }

    // Purge the specified keys
    for (int groupKey : groupKeys) {
      _groupKeyToId.remove(_idToGroupKey.get(groupKey));
      _idToGroupKey.remove(groupKey);
    }
  }

  /**
   * Helper method to purge group keys that got trimmed from the group by result.
   * Lazily builds a reverse map from id to array group key on the first call.
   *
   * @param groupKeys Group keys to purge
   */
  private void purgeArrayMapKeys(int[] groupKeys) {
    if (groupKeys == null || groupKeys.length == 0) {
      return; // Nothing to purge
    }

    // Lazily build the idToGroupKey reverse map only once, and then keep the two maps in-sync after that.
    if (_trimMode == TrimMode.OFF) {
      _idToArrayGroupKey = new Int2ObjectOpenHashMap<>(_arrayGroupKeyToId.size());
      for (Object2IntMap.Entry<IntArrayList> entry : _arrayGroupKeyToId.object2IntEntrySet()) {
        _idToArrayGroupKey.put(entry.getIntValue(), entry.getKey());
      }
      _trimMode = TrimMode.ON;
    }

    // Purge the specified keys
    for (int groupKey : groupKeys) {
      _arrayGroupKeyToId.remove(_idToArrayGroupKey.get(groupKey));
      _idToArrayGroupKey.remove(groupKey);
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
  @SuppressWarnings("Duplicates")
  private int updateRawKeyToGroupKeyMapping(long rawKey) {
    int groupKey = _groupKeyToId.get(rawKey);
    if (groupKey == INVALID_ID) {
      groupKey = _numGroupKeys++;
      _groupKeyToId.put(rawKey, groupKey);

      // We are in trim mode, so we need reverse map from id to group key
      if (_trimMode == TrimMode.ON) {
        _idToGroupKey.put(groupKey, rawKey);
      }
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
  @SuppressWarnings("Duplicates")
  private int updateRawKeyToGroupKeyMapping(IntArrayList rawKey) {
    int id = _arrayGroupKeyToId.getInt(rawKey);
    if (id == INVALID_ID) {
      id = _numGroupKeys++;
      _arrayGroupKeyToId.put(rawKey, id);

      // We are in trim mode, so we need reverse map from id to group key
      if (_trimMode == TrimMode.ON) {
        _idToArrayGroupKey.put(id, rawKey);
      }
    }
    return id;
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
        int numMultiValues = _blockValSets[i].getDictionaryIdsForDocId(docId, _reusableMultiValDictIdBuffer);
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
        int numMultiValues = _blockValSets[i].getDictionaryIdsForDocId(docId, _reusableMultiValDictIdBuffer);
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
        int numMultiValues = _blockValSets[i].getDictionaryIdsForDocId(docId, _reusableMultiValDictIdBuffer);
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
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
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
        builder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
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
        builder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
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
      builder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
      builder.append(_dictionaries[i].get(rawKeyArray[i]).toString());
    }
    return builder.toString();
  }
}
