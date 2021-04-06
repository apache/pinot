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
package org.apache.pinot.broker.routing.segmentpruner.interval;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.Pairs;


/**
 * The {@code IntervalTree} class represents read-only balanced binary interval tree map (from intervals to values)
 */
public class IntervalTree<Value> {

  // List representation of BST with root at index 0. For node with index x, it's left child index is (2x+1), right child index is (2x+2)
  private final List<Node> _nodes;

  public IntervalTree(Map<Value, Interval> valueToIntervalMap) {
    Map<Interval, List<Value>> intervalToValuesMap = new HashMap<>();
    for (Map.Entry<Value, Interval> entry : valueToIntervalMap.entrySet()) {
      intervalToValuesMap.putIfAbsent(entry.getValue(), new ArrayList<>());
      intervalToValuesMap.get(entry.getValue()).add(entry.getKey());
    }

    List<Node<Value>> sortedNodes = new ArrayList<>();
    for (Map.Entry<Interval, List<Value>> entry : intervalToValuesMap.entrySet()) {
      sortedNodes.add(new Node(entry.getKey(), entry.getValue()));
    }
    Collections.sort(sortedNodes);
    _nodes = buildIntervalTree(sortedNodes);
    buildAuxiliaryInfo();
  }

  /**
   * Build interval bst by bfs, the root for each subtree will be the one with median interval.
   * A typical balanced tree:
   *                              [10, 20]
   *                              /       \
   *                       [8, 15]        [12, 20]
   *                          /            /
   *                   [5, 10]       [10, 30]
   * is represented as  { [10, 20], [8, 15], [12, 20], [5, 10], null, [10, 30] }
   */
  private List<Node> buildIntervalTree(List<Node<Value>> sortedNodes) {
    List<Node> resNodes = new ArrayList<>();
    LinkedList<Pairs.IntPair> indexQueue = new LinkedList<>();
    indexQueue.add(new Pairs.IntPair(0, sortedNodes.size()));
    int count = 0;
    while (count < sortedNodes.size()) {
      Pairs.IntPair indexPair = indexQueue.pollFirst();
      int start = indexPair.getLeft();
      int end = indexPair.getRight();

      if (start < end) {
        int mid = start + (end - start) / 2;
        resNodes.add(sortedNodes.get(mid));
        count++;
        indexQueue.add(new Pairs.IntPair(start, mid));
        indexQueue.add(new Pairs.IntPair(mid + 1, end));
      } else {
        resNodes.add(null);
      }
    }
    return resNodes;
  }

  private void buildAuxiliaryInfo() {
    // Build max info for the interval tree by dfs
    buildAuxiliaryInfo(0);
  }

  private void buildAuxiliaryInfo(int nodeIndex) {
    if (!hasNode(nodeIndex)) {
      return;
    }

    int leftChildIndex = getLeftChildIndex(nodeIndex);
    int rightChildIndex = getRightChildIndex(nodeIndex);

    buildAuxiliaryInfo(leftChildIndex);
    buildAuxiliaryInfo(rightChildIndex);

    long max = _nodes.get(nodeIndex)._interval._max;
    max = Math.max(getMax(rightChildIndex), Math.max(max, getMax(leftChildIndex)));
    _nodes.get(nodeIndex)._max = max;
  }

  private int getLeftChildIndex(int nodeIndex) {
    return nodeIndex * 2 + 1;
  }

  private int getRightChildIndex(int nodeIndex) {
    return  nodeIndex * 2 + 2;
  }

  private long getMax(int index) {
    if (!hasNode(index)) {
      return Long.MIN_VALUE;
    }
    return _nodes.get(index)._max;
  }

  /**
   * Find all values whose intervals intersect with the input interval.
   *
   * @param searchInterval search interval
   * @return list of all qualified values.
   */
  public List<Value> searchAll(Interval searchInterval) {
    List<Value> list = new ArrayList<>();
    if (searchInterval == null) {
      return list;
    }
    searchAll(0, searchInterval, list);
    return list;
  }

  private void searchAll(int nodeIndex, Interval searchInterval, List<Value> list) {
    if (!hasNode(nodeIndex)) {
      return;
    }

    int leftChildIndex = getLeftChildIndex(nodeIndex);
    int rightChildIndex = getRightChildIndex(nodeIndex);

    if (hasNode(leftChildIndex) && getMax(leftChildIndex) >= searchInterval._min) {
      searchAll(leftChildIndex, searchInterval, list);
    }

    Node<Value> node = _nodes.get(nodeIndex);
    Interval interval = node._interval;
    if (searchInterval.intersects(interval)) {
      list.addAll(node._values);
    }

    if (interval._min <= searchInterval._max) {
      searchAll(rightChildIndex, searchInterval, list);
    }
  }

  private boolean hasNode(int nodeIndex) {
    return nodeIndex < _nodes.size() && _nodes.get(nodeIndex) != null;
  }

  private class Node<Value> implements Comparable<Node> {
    private final Interval _interval;
    private final List<Value> _values;
    private long _max; // max interval right end of subtree rooted at this node

    Node(Interval interval, List<Value> values) {
      _interval = interval;
      _values = values;
    }

    @Override
    public int compareTo(Node o) {
      Preconditions.checkNotNull(o, "Compare to invalid node: null");
      return _interval.compareTo(o._interval);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Node<?> node = (Node<?>) o;

      return _interval.equals(node._interval);
    }

    @Override
    public int hashCode() {
      return _interval.hashCode();
    }
  }
}
