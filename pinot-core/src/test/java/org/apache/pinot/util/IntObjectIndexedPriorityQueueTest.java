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
package org.apache.pinot.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.spi.utils.Pairs;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
import org.apache.pinot.core.util.IntObjectIndexedPriorityQueue;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link IntObjectIndexedPriorityQueue} class
 */
public class IntObjectIndexedPriorityQueueTest {
  private static final int NUM_RECORDS = 1000;
  private static final int INT_VALUE_BOUND = 10000;

  /**
   * Test for max heap mode.
   */
  @Test
  public void testMax() {
    test(false /* minHeap */);
  }

  /**
   * Test for min heap mode.
   */
  @Test
  public void testMin() {
    test(true /* minHeap */);
  }

  /**
   * Helper method builds the priority queue, randomly updates elements and
   * then asserts the following:
   * <ul>
   *   <li> Elements are popped from the priority queue in the expected order. </li>
   *   <li> Size of the priority queue is as expected (after elements are updated). </li>
   * </ul>
   * @param minHeap Min mode
   */
  public void test(boolean minHeap) {
    Random random = new Random(0);

    IntObjectIndexedPriorityQueue<AvgPair> pq = new IntObjectIndexedPriorityQueue<>(NUM_RECORDS, minHeap);
    Map<Integer, AvgPair> map = new HashMap<>(NUM_RECORDS);

    // Initialize the priority queue.
    for (int i = 0; i < NUM_RECORDS; i++) {
      // Avoid zeros
      double first = 1 + random.nextInt(INT_VALUE_BOUND);
      Long second = (long) 1 + random.nextInt(INT_VALUE_BOUND);

      AvgPair value = new AvgPair(first, second);
      pq.put(i, value);
      map.put(i, value);
    }

    // Update some records randomly
    for (int i = 0; i < NUM_RECORDS; i++) {
      int key = random.nextInt(NUM_RECORDS);

      // Avoid zeros
      double first = 1 + random.nextInt(INT_VALUE_BOUND);
      Long second = (long) 1 + random.nextInt(INT_VALUE_BOUND);

      AvgPair value = new AvgPair(first, second);
      pq.put(key, value);
      map.put(key, value);
    }

    // Transfer the map into list so it can be sorted.
    List<Pairs.IntObjectPair<AvgPair>> list = new ArrayList<>(NUM_RECORDS);
    for (Map.Entry<Integer, AvgPair> entry : map.entrySet()) {
      list.add(new Pairs.IntObjectPair<>(entry.getKey(), entry.getValue()));
    }

    // Comparison for min heap is the same as that for ascending order.
    boolean descendingOrder = !minHeap;
    Collections.sort(list, new Pairs.IntObjectComparator(descendingOrder));

    // Ensure that elements are popped from priority queue in the expected order.
    int i = 0;
    while (!pq.isEmpty()) {
      Pairs.IntObjectPair<AvgPair> actual = pq.poll();
      Pairs.IntObjectPair<AvgPair> expected = list.get(i++);

      Assert.assertEquals(actual.getIntValue(), expected.getIntValue());
      Assert.assertEquals(actual.getObjectValue(), expected.getObjectValue());
    }

    // Assert that priority queue had expected number of elements.
    Assert.assertEquals(i, list.size());
  }
}
