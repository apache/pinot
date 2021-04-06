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
package org.apache.pinot.core.util;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.spi.utils.Pairs.IntDoublePair;


/**
 * Heap based Indexed priority queue with primitive 'int' key and 'double' value.
 *
 * Allows for the following:
 * <ul>
 *   <li> O(1) access to values inserted using their corresponding keys. </li>
 *   <li> Dynamic update of values (heap order is maintained after updates). </li>
 *   <li> Min or max ordering, can be specified in the constructor. </li>
 * </ul>
 */
@NotThreadSafe
@SuppressWarnings("Duplicates")
public class IntDoubleIndexedPriorityQueue extends BaseIndexedPriorityQueue {
  DoubleArrayList _values;
  IntDoublePair _reusablePair;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity Initial capacity for the priority queue
   * @param minHeap Min order, ie smallest element on top.
   */
  public IntDoubleIndexedPriorityQueue(int initialCapacity, boolean minHeap) {
    super(initialCapacity, minHeap);
    _values = new DoubleArrayList(initialCapacity);
    _reusablePair = new IntDoublePair(0, 0.0);
  }

  /**
   * Puts the element into the priority queue.
   * <ul>
   *   <li> If key does not exist, it is added to the priority queue. </li>
   *   <li> If key exists, then the value is updated, and the priority queue ordering is maintained. </li>
   *   <li> Runtime complexity of {@code O(log(n)}). </li>
   * </ul>
   * @param key Integer key for the value
   * @param value Double value of the key
   */
  public void put(int key, double value) {
    if (!_keyToIndexMap.containsKey(key)) {
      _values.add(value);

      int last = _values.size() - 1;
      updateKeyIndexMap(key, last);
      siftUp(last);
    } else {
      int index = _keyToIndexMap.get(key);
      _values.set(index, value);

      // Sift the value up or down, as the case may be.
      if (!siftDown(index)) {
        siftUp(index);
      }
    }
  }

  /**
   * Returns the value for the specified key.
   * <ul>
   *   <li> Returns null if the specified key does not exist. </li>
   *   <li> Runtime complexity of O(1). </li>
   * </ul>
   *
   * @param key Key for which to return the value
   * @return Value for the key
   */
  public IntDoublePair get(int key) {
    if (!_keyToIndexMap.containsKey(key)) {
      return null;
    }

    int index = _keyToIndexMap.get(key);
    double value = _values.getDouble(index);
    _reusablePair.setIntValue(index);
    _reusablePair.setDoubleValue(value);

    return _reusablePair;
  }

  /**
   * Returns the key+value pair with the max priority (min for minHeap mode)
   * <ul>
   *   <li> key+value pair is removed from the priority queue. </li>
   *   <li> Returns null if the priority queue is empty. </li>
   *   <li> Runtime complexity of O(1). </li>
   * </ul>
   *
   * @return Key+Value pair
   */
  public IntDoublePair poll() {
    if (isEmpty()) {
      return null;
    }

    IntDoublePair poll = peek();
    int lastIndex = _values.size() - 1;
    swapValues(0, lastIndex);
    _values.removeDouble(lastIndex);

    _keyToIndexMap.remove(_indexToKeyMap.get(lastIndex));
    _indexToKeyMap.remove(lastIndex);

    if (!_values.isEmpty()) {
      siftDown(0);
    }

    return poll;
  }

  /**
   * Returns the key+value pair with the max priority (min for minHeap mode)
   * <ul>
   *   <li> key+value pair is not removed from the priority queue. </li>
   *   <li> Throws runtime exception if the priority queue is empty. </li>
   *   <li> Runtime complexity of O(1). </li>
   * </ul>
   *
   * @return Key+Value pair
   */
  public IntDoublePair peek() {
    if (_values.isEmpty()) {
      throw new RuntimeException("Empty collection");
    }
    _reusablePair.setIntValue(_indexToKeyMap.get(0));
    _reusablePair.setDoubleValue(_values.getDouble(0));
    return _reusablePair;
  }

  /**
   * Returns true if the priority queue is empty, false otherwise.
   *
   * @return True if empty, false otherwise
   */
  public boolean isEmpty() {
    return _values.isEmpty();
  }

  /**
   * Helper method that moves the element at the specified index up
   * until the heap ordering is established.
   *
   * @param index Index of element to sift up.
   */
  private void siftUp(int index) {
    // Return if already at root node.
    if (index == 0) {
      return;
    }

    while (index != 0) {
      int parentIndex = getParentIndex(index);
      double value = _values.getDouble(index);
      double parentValue = _values.getDouble(parentIndex);

      if (compare(parentValue, value) == 1) {
        swapValues(index, parentIndex);
        index = parentIndex;
      } else {
        // No more sifting up required, break
        break;
      }
    }
  }

  /**
   * Helper method that moves the element at the specified index down
   * until the heap ordering is established.
   *
   * @param index Index of element to sift down.
   * @return True if sifted, false otherwise.
   */
  private boolean siftDown(int index) {
    boolean hasChildren = hasChildren(index);
    if (!hasChildren) {
      return false;
    }

    boolean sifted = false;
    while (true) {
      int leftChildIndex = getLeftChildIndex(index);
      int rightChildIndex = getRightChildIndex(index);

      int minIndex;
      int size = _values.size();
      if (leftChildIndex >= size && rightChildIndex >= size) { // This is leaf node, all done.
        break;
      } else if (rightChildIndex >= size) { // Node only has left child which will be the minimum.
        minIndex = leftChildIndex;
      } else { // Node has both left and right children, find the minimum of the two.
        double leftChildValue = _values.getDouble(leftChildIndex);
        double rightChildValue = _values.getDouble(rightChildIndex);

        if (compare(leftChildValue, rightChildValue) <= 0) {
          minIndex = leftChildIndex;
        } else {
          minIndex = rightChildIndex;
        }
      }

      // One of the children is out of order, need to sift down.
      if (compare(_values.getDouble(index), _values.getDouble(minIndex)) == 1) {
        swapValues(index, minIndex);
        index = minIndex;
        sifted = true;
      } else {
        break;
      }
    }
    return sifted;
  }

  /**
   * Compares the two specified values, and returns:
   * <ul>
   *   <li> if v1 < v2, -1 for max, +1 for min mode. </li>
   *   <li> if v1 > v2, -1 for max, -1 for min mode. </li>
   *   <li> if v1 = v2,  0 for max, 0 for min mode. </li>
   * </ul>
   * @param v1 Value to compare
   * @param v2 Value to compare
   * @return Result of comparison (as described above).
   */
  private int compare(double v1, double v2) {
    int ret = Double.compare(v1, v2);
    return (_minHeap) ? ret : -ret;
  }

  /**
   * Helper method that performs all operations required to swap two values.
   * <ul>
   *   <li> Swaps the values in the array that backs the heap. </li>
   *   <li> Updates the indexToKey and keyToIndex maps due to the swap. </li>
   * </ul>
   * @param index1 Index to swap
   * @param index2 Index to swap
   */
  private void swapValues(int index1, int index2) {
    if (index1 == index2) {
      return;
    }

    double tmp = _values.getDouble(index1);
    _values.set(index1, _values.getDouble(index2));
    _values.set(index2, tmp);
    swapKeys(index1, index2);
  }

  /**
   * Returns true if the node at specified index has children, false otherwise.
   * Just checking for existence of left child is sufficient (array backed heap).
   *
   * @param index Index to check
   * @return True if node has children, false otherwise.
   */
  private boolean hasChildren(int index) {
    return (getLeftChildIndex(index) < _values.size());
  }
}
