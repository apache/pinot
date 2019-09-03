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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;


/**
 * Abstract base class for IndexedPriorityQueue's.
 *
 */
public abstract class BaseIndexedPriorityQueue {

  protected boolean _minHeap;
  protected Int2IntOpenHashMap _keyToIndexMap;
  protected Int2IntOpenHashMap _indexToKeyMap;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity Initial capacity for the priority queue
   * @param minHeap Min order, ie smallest element on top.
   */
  public BaseIndexedPriorityQueue(int initialCapacity, boolean minHeap) {
    _minHeap = minHeap;
    _keyToIndexMap = new Int2IntOpenHashMap(initialCapacity);
    _indexToKeyMap = new Int2IntOpenHashMap(initialCapacity);
  }

  /**
   * Helper method to update key/index mappings.
   *
   * @param key Key for position
   * @param position Position for key
   */
  protected void updateKeyIndexMap(int key, int position) {
    _keyToIndexMap.put(key, position);
    _indexToKeyMap.put(position, key);
  }

  /**
   * Helper method to swap keys for the specified indices.
   * @param index1 First index
   * @param index2 Second index
   */
  protected void swapKeys(int index1, int index2) {
    int key1 = _indexToKeyMap.get(index1);
    int key2 = _indexToKeyMap.get(index2);

    updateKeyIndexMap(key1, index2);
    updateKeyIndexMap(key2, index1);
  }

  /**
   * Returns index of left child of the specified index.
   * Does not check for actual existence of the child in the
   * priority queue.
   *
   * @param index Index for which to find the left child.
   * @return Index of the left.
   */
  protected int getLeftChildIndex(int index) {
    return ((2 * (index + 1)) - 1);
  }

  /**
   * Returns index of right child of the specified index.
   * Does not check for actual existence of the child in the
   * priority queue.
   *
   * @param index Index for which to find the right child.
   * @return Index of the right.
   */
  protected int getRightChildIndex(int index) {
    return (2 * (index + 1));
  }

  /**
   * Returns the index of parent for the specified node.
   * Returns -1 for root.
   *
   * @param index Index of node for which to identify the parent.
   * @return Index of parent.
   */
  protected int getParentIndex(int index) {
    return (((index + 1) / 2) - 1);
  }
}
