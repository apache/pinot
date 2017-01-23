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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.Pairs.IntDoublePair;
import com.linkedin.pinot.core.util.IntDoubleIndexedPriorityQueue;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import java.util.Arrays;


/**
 * Result Holder implemented using DoubleArray.
 */
public class DoubleGroupByResultHolder implements GroupByResultHolder {
  private final int _maxCapacity;
  private final int _trimSize;
  private final double _defaultValue;
  private final boolean _minHeap;

  private int _resultHolderCapacity;
  private StorageMode _storageMode;
  private double[] _resultArray;
  private Int2DoubleOpenHashMap _resultMap;
  private IntDoubleIndexedPriorityQueue _priorityQueue;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity Initial capacity for storage
   * @param maxCapacity Max capacity of storage, beyond which trimming kicks in
   * @param trimSize maximum number of groups returned after trimming.
   * @param defaultValue Default value of un-initialized results (in array mode)
   * @param minOrder Min ordering (in case of min aggregation functions)
   */
  public DoubleGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize, double defaultValue,
      boolean minOrder) {
    _resultHolderCapacity = initialCapacity;
    _defaultValue = defaultValue;
    _maxCapacity = maxCapacity;
    _trimSize = trimSize;
    _minHeap = !minOrder; // Max ordering requires min-heap for trimming results, and vice-versa.

    // Used only when group keys need to be trimmed.
    _resultMap = null;
    _priorityQueue = null;

    _storageMode = StorageMode.ARRAY_STORAGE;
    _resultArray = new double[initialCapacity];
    if (defaultValue != 0.0) {
      Arrays.fill(_resultArray, defaultValue);
    }
  }

  /**
   * Constructor for the class, assumes max (max on top) ordering for trim.
   *
   * @param initialCapacity Initial capacity for storage
   * @param maxCapacity Max capacity of storage, beyond which trimming kicks in
   * @param trimSize maximum number of groups returned after trimming.
   * @param defaultValue Default value of un-initialized results (in array mode)
   */
  public DoubleGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize, double defaultValue) {
    this(initialCapacity, maxCapacity, trimSize, defaultValue, false /* minOrdering */);
  }

  /**
   * {@inheritDoc}
   * For array mode, expands the array size as long as it is under {@link #_maxCapacity},
   * else switches to map mode. For map mode, trims the result.
   *
   * @param capacity Capacity required (number of group keys expected to be stored)
   */
  @Override
  public void ensureCapacity(int capacity) {
    Preconditions.checkArgument(capacity <= _maxCapacity);

    // Nothing to be done for map mode.
    if (_storageMode == StorageMode.MAP_STORAGE) {
      return;
    }

    if (capacity > _trimSize) {
      switchToMapMode(capacity);
      return;
    }

    if (capacity > _resultHolderCapacity) {
      int copyLength = _resultHolderCapacity;
      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, capacity);

      // Cap the growth to maximum possible number of group keys
      _resultHolderCapacity = Math.min(_resultHolderCapacity, _maxCapacity);

      double[] current = _resultArray;
      _resultArray = new double[_resultHolderCapacity];
      System.arraycopy(current, 0, _resultArray, 0, copyLength);

      if (_defaultValue != 0.0) {
        Arrays.fill(_resultArray, copyLength, _resultHolderCapacity, _defaultValue);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * Array based result holder assumes group by key fit within integer.
   * This is a valid assumption as ArrayBasedResultHolder gets instantiated
   * iff groupKey are less than 1M.
   *
   * @param groupKey
   * @return
   */
  @Override
  public double getDoubleResult(int groupKey) {
    return (_storageMode == StorageMode.ARRAY_STORAGE) ? _resultArray[groupKey] : _resultMap.get(groupKey);
  }

  @Override
  public <T> T getResult(int groupKey) {
    throw new RuntimeException("Unsupported method getResult (returning Object) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void setValueForKey(int groupKey, double newValue) {
    if (_storageMode == StorageMode.ARRAY_STORAGE) {
      _resultArray[groupKey] = newValue;
    } else {
      _resultMap.put(groupKey, newValue);
      _priorityQueue.put(groupKey, newValue);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey Key for which to set the value
   * @param newValue Value to set
   */
  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    throw new RuntimeException(
        "Unsupported method 'setValueForKey' (with Object param) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * Keys with 'lowest' values (as per the sort order) are trimmed away to reduce the size to _trimSize.
   *
   * @return Array of keys that were trimmed out.
   */
  @Override
  public int[] trimResults() {
    if (_storageMode == StorageMode.ARRAY_STORAGE) {
      return EMPTY_ARRAY; // Still in array mode, trimming has not kicked in yet.
    }

    int numKeysToRemove = _resultMap.size() - _trimSize;
    int[] removedGroupKeys = new int[numKeysToRemove];

    for (int i = 0; i < numKeysToRemove; i++) {
      IntDoublePair pair = _priorityQueue.poll();
      int groupKey = pair.getIntValue();
      _resultMap.remove(groupKey);
      removedGroupKeys[i] = groupKey;
    }
    return removedGroupKeys;
  }

  /**
   * Helper method to switch the storage from array mode to map mode.
   *
   * @param initialPriorityQueueSize Initial size of priority queue
   */
  private void switchToMapMode(int initialPriorityQueueSize) {
    _storageMode = StorageMode.MAP_STORAGE;
    _resultMap = new Int2DoubleOpenHashMap(_resultHolderCapacity);
    _priorityQueue = new IntDoubleIndexedPriorityQueue(initialPriorityQueueSize, _minHeap);

    for (int id = 0; id < _resultHolderCapacity; id++) {
      _resultMap.put(id, _resultArray[id]);
      _priorityQueue.put(id, _resultArray[id]);
    }
  }
}
