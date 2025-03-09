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
package org.apache.pinot.core.query.aggregation.function.distinct;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class OffHeapDistinctSetTest {
  private static final int NUM_VALUES = 10000;
  private static final int INITIAL_EXPECTED_VALUES = 10;
  private static final Random RANDOM = new Random();

  @Test
  public void testOffHeap32BitDistinctSet() {
    IntSet onHeapSet = new IntOpenHashSet();
    try (OffHeap32BitDistinctSet offHeapSet = new OffHeap32BitDistinctSet(INITIAL_EXPECTED_VALUES)) {
      if (RANDOM.nextBoolean()) {
        offHeapSet.add(0);
        onHeapSet.add(0);
      }
      for (int i = 0; i < NUM_VALUES; i++) {
        int value = randomInt();
        offHeapSet.add(value);
        onHeapSet.add(HashCommon.mix(value));
      }
      verify(offHeapSet, onHeapSet);

      try (OffHeap32BitDistinctSet offHeapSet2 = OffHeap32BitDistinctSet.deserialize(
          ByteBuffer.wrap(offHeapSet.serialize()))) {
        verify(offHeapSet2, onHeapSet);
      }

      try (OffHeap32BitDistinctSet offHeapSet2 = new OffHeap32BitDistinctSet(INITIAL_EXPECTED_VALUES)) {
        if (RANDOM.nextBoolean()) {
          offHeapSet2.add(0);
          onHeapSet.add(0);
        }
        for (int i = 0; i < NUM_VALUES; i++) {
          int value = randomInt();
          offHeapSet2.add(value);
          onHeapSet.add(HashCommon.mix(value));
        }
        offHeapSet.merge(offHeapSet2);
      }
      verify(offHeapSet, onHeapSet);
    }
  }

  private void verify(OffHeap32BitDistinctSet offHeapSet, IntSet onHeapSet) {
    assertEquals(offHeapSet.size(), onHeapSet.size());
    int numValues = 0;
    IntIterator iterator = offHeapSet.iterator();
    while (iterator.hasNext()) {
      numValues++;
      assertTrue(onHeapSet.contains(iterator.nextInt()));
    }
    assertEquals(numValues, onHeapSet.size());
  }

  private static int randomInt() {
    return RANDOM.nextInt(4 * NUM_VALUES);
  }

  @Test
  public void testOffHeap64BitDistinctSet() {
    LongSet onHeapSet = new LongOpenHashSet();
    try (OffHeap64BitDistinctSet offHeapSet = new OffHeap64BitDistinctSet(INITIAL_EXPECTED_VALUES)) {
      if (RANDOM.nextBoolean()) {
        offHeapSet.add(0);
        onHeapSet.add(0);
      }
      for (int i = 0; i < NUM_VALUES; i++) {
        long value = randomLong();
        offHeapSet.add(value);
        onHeapSet.add(HashCommon.mix(value));
      }
      verify(offHeapSet, onHeapSet);

      try (OffHeap64BitDistinctSet offHeapSet2 = OffHeap64BitDistinctSet.deserialize(
          ByteBuffer.wrap(offHeapSet.serialize()))) {
        verify(offHeapSet2, onHeapSet);
      }

      try (OffHeap64BitDistinctSet offHeapSet2 = new OffHeap64BitDistinctSet(INITIAL_EXPECTED_VALUES)) {
        if (RANDOM.nextBoolean()) {
          offHeapSet2.add(0);
          onHeapSet.add(0);
        }
        for (int i = 0; i < NUM_VALUES; i++) {
          long value = randomLong();
          offHeapSet2.add(value);
          onHeapSet.add(HashCommon.mix(value));
        }
        offHeapSet.merge(offHeapSet2);
      }
      verify(offHeapSet, onHeapSet);
    }
  }

  private void verify(OffHeap64BitDistinctSet offHeapSet, LongSet onHeapSet) {
    assertEquals(offHeapSet.size(), onHeapSet.size());
    int numValues = 0;
    LongIterator iterator = offHeapSet.iterator();
    while (iterator.hasNext()) {
      numValues++;
      assertTrue(onHeapSet.contains(iterator.nextLong()));
    }
    assertEquals(numValues, onHeapSet.size());
  }

  private static long randomLong() {
    long randomInt = randomInt();
    return RANDOM.nextBoolean() ? Integer.MAX_VALUE + randomInt : Integer.MAX_VALUE - randomInt;
  }

  @Test
  public void testOffHeap128BitDistinctSet() {
    Set<OffHeap128BitDistinctSet.Value> onHeapSet = new HashSet<>();
    try (OffHeap128BitDistinctSet offHeapSet = new OffHeap128BitDistinctSet(INITIAL_EXPECTED_VALUES)) {
      if (RANDOM.nextBoolean()) {
        offHeapSet.add(0, 0);
        onHeapSet.add(new OffHeap128BitDistinctSet.Value(0, 0));
      }
      for (int i = 0; i < NUM_VALUES; i++) {
        long high = randomLong();
        long low = randomLong();
        offHeapSet.add(high, low);
        onHeapSet.add(new OffHeap128BitDistinctSet.Value(HashCommon.mix(high), HashCommon.mix(low)));
      }
      verify(offHeapSet, onHeapSet);

      try (OffHeap128BitDistinctSet offHeapSet2 = OffHeap128BitDistinctSet.deserialize(
          ByteBuffer.wrap(offHeapSet.serialize()))) {
        verify(offHeapSet2, onHeapSet);
      }

      try (OffHeap128BitDistinctSet offHeapSet2 = new OffHeap128BitDistinctSet(INITIAL_EXPECTED_VALUES)) {
        if (RANDOM.nextBoolean()) {
          offHeapSet.add(0, 0);
          onHeapSet.add(new OffHeap128BitDistinctSet.Value(0, 0));
        }
        for (int i = 0; i < NUM_VALUES; i++) {
          long high = randomLong();
          long low = randomLong();
          offHeapSet2.add(high, low);
          onHeapSet.add(new OffHeap128BitDistinctSet.Value(HashCommon.mix(high), HashCommon.mix(low)));
        }
        offHeapSet.merge(offHeapSet2);
      }
      verify(offHeapSet, onHeapSet);
    }
  }

  private void verify(OffHeap128BitDistinctSet offHeapSet, Set<OffHeap128BitDistinctSet.Value> onHeapSet) {
    assertEquals(offHeapSet.size(), onHeapSet.size());
    int numValues = 0;
    OffHeap128BitDistinctSet.Value buffer = new OffHeap128BitDistinctSet.Value();
    Iterator<Void> iterator = offHeapSet.iterator(buffer);
    while (iterator.hasNext()) {
      numValues++;
      iterator.next();
      assertTrue(onHeapSet.contains(buffer));
    }
    assertEquals(numValues, onHeapSet.size());
  }
}
