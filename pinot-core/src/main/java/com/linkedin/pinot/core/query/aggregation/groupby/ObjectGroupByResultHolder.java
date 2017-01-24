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
import com.linkedin.pinot.common.utils.Pairs.IntObjectPair;
import com.linkedin.pinot.core.util.IntObjectIndexedPriorityQueue;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;


/**
 * Result Holder implemented using ObjectArray.
 */
public class ObjectGroupByResultHolder implements GroupByResultHolder {
  private final int _maxCapacity;
  private final int _trimSize;
  private final boolean _minHeap;

  private int _resultHolderCapacity;
  private StorageMode _storageMode;
  private Object[] _resultArray;
  private Int2ObjectOpenHashMap _resultMap;
  private IntObjectIndexedPriorityQueue _priorityQueue;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity Initial capacity of result holder
   * @param maxCapacity Max capacity of result holder
   * @param trimSize maximum number of groups returned after trimming.
   * @param minOrder Min ordering for trim (in case of min aggregation functions)
   */
  public ObjectGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize, boolean minOrder) {
    _resultArray = new Object[initialCapacity];
    _resultHolderCapacity = initialCapacity;
    _storageMode = StorageMode.ARRAY_STORAGE;
    _maxCapacity = maxCapacity;
    _trimSize = trimSize;
    _minHeap = !minOrder; // Max order requires minHeap for trimming results, and vice-versa

    _resultMap = null;
  }

  /**
   * Constructor for the class, assumes max ordering for trim.
   *
   * @param initialCapacity Initial capacity of result holder
   * @param maxCapacity Max capacity of result holder
   * @param trimSize maximum number of groups returned after trimming.
   */
  public ObjectGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    this(initialCapacity, maxCapacity, trimSize, false /* minOrdering */);
  }

  /**
   * {@inheritDoc}
   *
   * @param capacity
   */
  @Override
  public void ensureCapacity(int capacity) {
    Preconditions.checkArgument(capacity <= _maxCapacity);

    // Nothing to be done for map mode.
    if (_storageMode == StorageMode.MAP_STORAGE) {
      return;
    }

    // If object is not comparable, we cannot use a priority queue and cannot compare.
    if (capacity > _trimSize && (_resultArray[0] instanceof Comparable)) {
      switchToMapMode(capacity);
      return;
    }

    if (capacity > _resultHolderCapacity) {
      int copyLength = _resultHolderCapacity;
      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, capacity);

      // Cap the growth to maximum possible number of group keys
      _resultHolderCapacity = Math.min(_resultHolderCapacity, _maxCapacity);

      Object[] current = _resultArray;
      _resultArray = new Object[_resultHolderCapacity];
      System.arraycopy(current, 0, _resultArray, 0, copyLength);
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
    throw new RuntimeException(
        "Unsupported method getDoubleResult (returning double) for class " + getClass().getName());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getResult(int groupKey) {
    return (T) ((_storageMode == StorageMode.ARRAY_STORAGE) ? _resultArray[groupKey] : _resultMap.get(groupKey));
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void setValueForKey(int groupKey, double newValue) {
    throw new RuntimeException(
        "Unsupported method 'putValueForKey' (with double param) for class " + getClass().getName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    if (_storageMode == StorageMode.ARRAY_STORAGE) {
      _resultArray[groupKey] = newValue;
    } else {
      _resultMap.put(groupKey, newValue);
      _priorityQueue.put(groupKey, (Comparable) newValue);
    }
  }

  /**
   * {@inheritDoc}
   *
   * Keys with 'lowest' values (as per the sort order) are trimmed away to reduce the size to _trimSize.
   *
   * @return Array of keys that were trimmed.
   */
  @Override
  public int[] trimResults() {
    if (_storageMode == StorageMode.ARRAY_STORAGE) {
      return EMPTY_ARRAY; // Still in array mode, trimming has not kicked in yet.
    }

    int numKeysToRemove = _resultMap.size() - _trimSize;
    int[] removedGroupKeys = new int[numKeysToRemove];

    for (int i = 0; i < numKeysToRemove; i++) {
      IntObjectPair pair = _priorityQueue.poll();
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
  @SuppressWarnings("unchecked")
  private void switchToMapMode(int initialPriorityQueueSize) {
    _storageMode = StorageMode.MAP_STORAGE;
    _resultMap = new Int2ObjectOpenHashMap(_resultArray.length);

    _priorityQueue = new IntObjectIndexedPriorityQueue(initialPriorityQueueSize, _minHeap);
    for (int id = 0; id < _resultArray.length; id++) {
      _resultMap.put(id, _resultArray[id]);
      _priorityQueue.put(id, (Comparable) _resultArray[id]);
    }
    _resultArray = null;
  }
}
