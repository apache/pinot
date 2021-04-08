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

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.pinot.spi.utils.Pairs;
import org.apache.pinot.core.util.IntDoubleIndexedPriorityQueue;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link IntDoubleIndexedPriorityQueue} class
 */
public class IntDoubleIndexedPriorityQueueTest {
  private static final int NUM_RECORDS = 1000;

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
   * @param minHeap Min or max mode
   */
  public void test(boolean minHeap) {
    Random random = new Random(0);

    IntDoubleIndexedPriorityQueue pq = new IntDoubleIndexedPriorityQueue(NUM_RECORDS, minHeap);
    Int2DoubleOpenHashMap map = new Int2DoubleOpenHashMap(NUM_RECORDS);

    // Initialize the priority queue.
    for (int i = 0; i < NUM_RECORDS; i++) {
      double value = random.nextDouble();
      pq.put(i, value);
      map.put(i, value);
    }

    // Update some records randomly
    for (int i = 0; i < NUM_RECORDS; i++) {
      int key = random.nextInt(NUM_RECORDS);
      double value = random.nextDouble();
      pq.put(key, value);
      map.put(key, value);
    }

    // Transfer the map into list so it can be sorted.
    List<Pairs.IntDoublePair> list = new ArrayList<>(NUM_RECORDS);
    for (Int2DoubleMap.Entry entry : map.int2DoubleEntrySet()) {
      list.add(new Pairs.IntDoublePair(entry.getIntKey(), entry.getDoubleValue()));
    }

    // Comparison for min heap is the same as that for ascending order.
    boolean descendingOrder = !minHeap;
    Collections.sort(list, new Pairs.IntDoubleComparator(descendingOrder));

    // Ensure that elements are popped from priority queue in the expected order.
    int i = 0;
    while (!pq.isEmpty()) {
      Pairs.IntDoublePair actual = pq.poll();
      Pairs.IntDoublePair expected = list.get(i++);

      Assert.assertEquals(actual.getIntValue(), expected.getIntValue());
      Assert.assertEquals(actual.getDoubleValue(), expected.getDoubleValue());
    }

    // Assert that priority queue had expected number of elements.
    Assert.assertEquals(i, list.size());
  }
}
