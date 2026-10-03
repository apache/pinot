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
import org.apache.pinot.common.utils.RoaringBitmapUnion;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;


/// Covers the serialized-bitmap fold of [DistinctCountBitmapValueAggregator], which unions inputs lazily through a
/// [RoaringBitmapUnion] and finalizes the bitmap when it is read or serialized. Input cardinalities cross the lazy
/// container-promotion threshold so both promoted and non-promoted accumulator states are verified against an eagerly
/// unioned reference.
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

  private static RoaringBitmapUnion fold(DistinctCountBitmapValueAggregator aggregator, byte[][] serialized) {
    RoaringBitmapUnion value = aggregator.getInitialAggregatedValue(serialized[0]);
    for (int i = 1; i < serialized.length; i++) {
      value = aggregator.applyRawValue(value, serialized[i]);
    }
    return value;
  }

  @Test
  public void testApplyRawSerializedBitmaps() {
    byte[][] serialized = serializedBitmaps(200, 100, 200_000);
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();
    assertFalse(aggregator.isAggregatedValueFixedSize());
    // Nothing has been serialized yet, so no size is known: the star-tree builders serialize every record before
    // they consume the size
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), 0);

    RoaringBitmapUnion value = fold(aggregator, serialized);
    byte[] result = aggregator.serializeAggregatedValue(value);

    RoaringBitmap expected = eagerUnion(serialized);
    assertEquals(RoaringBitmapUtils.deserialize(result), expected);
    assertEquals(value.get().getCardinality(), expected.getCardinality());
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), result.length);

    // Serializing is non-consuming and repeatable
    assertEquals(aggregator.serializeAggregatedValue(value), result);
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), result.length);
  }

  @Test
  public void testMaxByteSizeTracksLargestSerializedValue() {
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();
    byte[] small = aggregator.serializeAggregatedValue(fold(aggregator, serializedBitmaps(2, 10, 1_000)));
    byte[] large = aggregator.serializeAggregatedValue(fold(aggregator, serializedBitmaps(50, 100, 200_000)));
    byte[] medium = aggregator.serializeAggregatedValue(fold(aggregator, serializedBitmaps(5, 100, 200_000)));
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), Math.max(small.length, Math.max(large.length,
        medium.length)));
  }

  @Test
  public void testApplyAggregatedValueWithPendingInput() {
    // Mirrors the on-heap star-tree builder: aggregated records are built from other records' accumulators via
    // cloneAggregatedValue + applyAggregatedValue, and every record is only serialized at the end
    byte[][] serializedA = serializedBitmaps(100, 100, 150_000);
    byte[][] serializedB = serializedBitmaps(100, 100, 150_000);
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();

    RoaringBitmapUnion recordA = fold(aggregator, serializedA);
    RoaringBitmapUnion recordB = fold(aggregator, serializedB);

    // recordA and recordB both have pending lazy unions here
    RoaringBitmapUnion aggregated = aggregator.cloneAggregatedValue(recordA);
    assertNotSame(aggregated, recordA);
    aggregated = aggregator.applyAggregatedValue(aggregated, recordB);

    RoaringBitmap expected = eagerUnion(serializedA);
    expected.or(eagerUnion(serializedB));
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(aggregated)), expected);

    // Serializing is non-consuming: the same accumulator remains usable for further aggregation, and the value
    // published by the earlier serialization is not affected
    RoaringBitmap published = aggregated.get();
    RoaringBitmap publishedCopy = published.clone();
    byte[] extra = serializedBitmaps(1, 100, 150_000)[0];
    aggregated = aggregator.applyRawValue(aggregated, extra);
    assertEquals(published, publishedCopy);
    expected.or(RoaringBitmapUtils.deserialize(extra));
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(aggregated)), expected);

    // The source records must remain intact and serializable
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(recordA)),
        eagerUnion(serializedA));
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(recordB)),
        eagerUnion(serializedB));
    // The clone does not share state with its source
    recordA = aggregator.applyRawValue(recordA, serializedB[0]);
    assertEquals(RoaringBitmapUtils.deserialize(aggregator.serializeAggregatedValue(aggregated)), expected);
  }

  @Test
  public void testApplyRawValuesOnly() {
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();
    RoaringBitmapUnion value = aggregator.getInitialAggregatedValue(1);
    for (int i = 2; i <= 1000; i++) {
      value = aggregator.applyRawValue(value, i);
    }
    value = aggregator.applyRawValue(value, new Object[]{1001, 1002, 1002});
    byte[] result = aggregator.serializeAggregatedValue(value);
    assertEquals(RoaringBitmapUtils.deserialize(result).getCardinality(), 1002);
    assertEquals(aggregator.getMaxAggregatedValueByteSize(), result.length);
  }

  @Test
  public void testDeserializeRoundTrip() {
    DistinctCountBitmapValueAggregator aggregator = new DistinctCountBitmapValueAggregator();
    byte[][] serialized = serializedBitmaps(20, 100, 50_000);
    byte[] bytes = aggregator.serializeAggregatedValue(fold(aggregator, serialized));
    RoaringBitmapUnion restored = aggregator.deserializeAggregatedValue(bytes);
    assertEquals(restored.get(), eagerUnion(serialized));
    assertEquals(aggregator.serializeAggregatedValue(restored), bytes);
  }
}
