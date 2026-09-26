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
package org.apache.pinot.segment.local.aggregator;

import java.util.Random;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Covers the serialized-bitmap fold of [DistinctCountBitmapValueAggregator], which unions inputs lazily and repairs
/// the accumulator at serialization. Input cardinalities cross the lazy container-promotion threshold so both
/// promoted and non-promoted accumulator states are verified against an eagerly unioned reference.
public class DistinctCountBitmapValueAggregatorTest {
  private static final Random RANDOM = new Random(42);

  private static byte[][] serializedBitmaps(int numBitmaps, int valuesPerBitmap, int maxValue) {
    byte[][] serialized = new byte[numBitmaps][];
    for (int i = 0; i < numBitmaps; i++) {
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int j = 0; j < valuesPerBitmap; j++) {
        bitmap.add(RANDOM.nextInt(maxValue));
      }
      serialized[i] = RoaringBitmapUtils.serialize(bitmap);
    }
    return serialized;
  }

  private static RoaringBitmap eagerUnion(byte[][] serialized) {
    RoaringBitmap expected = new RoaringBitmap();
    for (byte[] bytes : serialized) {
      expected.or(RoaringBitmapUtils.deserialize(bytes));
    }
    return expected;
  }

  @Test
  public void testApplyRawSerializedBitmaps() {
    byte[][] serialized = serializedBitmaps(200, 100, 200_000);
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();

    RoaringBitmap value = aggregator.getInitialAggregatedValue(serialized[0]);
    for (int i = 1; i < serialized.length; i++) {
      value = aggregator.applyRawValue(value, serialized[i]);
    }
    byte[] result = aggregator.serializeAggregatedValue(value);

    RoaringBitmap expected = eagerUnion(serialized);
    assertEquals(RoaringBitmapUtils.deserialize(result), expected);
    assertEquals(value.getCardinality(), expected.getCardinality());
    // The tracked max byte size must upper-bound every serialized value
    assertTrue(aggregator.getMaxAggregatedValueByteSize() >= result.length,
        "maxByteSize " + aggregator.getMaxAggregatedValueByteSize() + " < serialized length " + result.length);
  }

  @Test
  public void testApplyAggregatedValueWithLazyInput() {
    // Mirrors the on-heap star-tree builder: aggregated records are built from other records' possibly-lazy
    // accumulators via cloneAggregatedValue + applyAggregatedValue, and only serialized at the end
    byte[][] serializedA = serializedBitmaps(100, 100, 150_000);
    byte[][] serializedB = serializedBitmaps(100, 100, 150_000);
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();

    RoaringBitmap recordA = aggregator.getInitialAggregatedValue(serializedA[0]);
    for (int i = 1; i < serializedA.length; i++) {
      recordA = aggregator.applyRawValue(recordA, serializedA[i]);
    }
    RoaringBitmap recordB = aggregator.getInitialAggregatedValue(serializedB[0]);
    for (int i = 1; i < serializedB.length; i++) {
      recordB = aggregator.applyRawValue(recordB, serializedB[i]);
    }

    // recordA and recordB are both potentially lazy here
    RoaringBitmap aggregated = aggregator.cloneAggregatedValue(recordA);
    aggregated = aggregator.applyAggregatedValue(aggregated, recordB);

    RoaringBitmap expected = eagerUnion(serializedA);
    expected.or(eagerUnion(serializedB));
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(aggregated)), expected);

    // The source records must remain intact and serializable
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(recordA)),
        eagerUnion(serializedA));
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(recordB)),
        eagerUnion(serializedB));
  }

  @Test
  public void testApplyRawValuesOnly() {
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();
    RoaringBitmap value = aggregator.getInitialAggregatedValue(1);
    for (int i = 2; i <= 1000; i++) {
      value = aggregator.applyRawValue(value, i);
    }
    byte[] result = aggregator.serializeAggregatedValue(value);
    assertEquals(RoaringBitmapUtils.deserialize(result).getCardinality(), 1000);
    assertTrue(aggregator.getMaxAggregatedValueByteSize() >= result.length);
  }
}
