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
package org.apache.pinot.common.utils;

import java.nio.ByteBuffer;
import java.util.Random;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Covers the interim [MutableRoaringBitmapUnion] with the inputs Pinot feeds it: read-only bitmaps over heap and
/// direct buffers, as the index readers hand out, next to plain mutable bitmaps.
public class MutableRoaringBitmapUnionTest {
  private static final long SEED = 20260929L;

  private static MutableRoaringBitmap[] inputs(Random random, int numInputs) {
    MutableRoaringBitmap[] inputs = new MutableRoaringBitmap[numInputs];
    for (int i = 0; i < numInputs; i++) {
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      int base = random.nextInt(8) << 16;
      switch (i % 3) {
        case 0:
          for (int j = 0; j < 300; j++) {
            bitmap.add(base | random.nextInt(1 << 16));
          }
          break;
        case 1:
          bitmap.add((long) base + random.nextInt(1000), (long) base + 10000);
          break;
        default:
          bitmap.add((long) base + random.nextInt(1000), (long) base + 6000);
          bitmap.runOptimize();
          break;
      }
      inputs[i] = bitmap;
    }
    return inputs;
  }

  /// The input as the readers present it: every third one over a read-only heap buffer, every third one over a
  /// read-only direct buffer, the rest as is.
  private static ImmutableRoaringBitmap present(MutableRoaringBitmap bitmap, int index) {
    if (index % 3 == 0) {
      return bitmap;
    }
    int size = bitmap.serializedSizeInBytes();
    ByteBuffer buffer = index % 3 == 1 ? ByteBuffer.allocate(size) : ByteBuffer.allocateDirect(size);
    bitmap.serialize(buffer);
    buffer.flip();
    return new ImmutableRoaringBitmap(buffer.asReadOnlyBuffer());
  }

  private static MutableRoaringBitmap eagerUnion(MutableRoaringBitmap[] inputs, int numInputs) {
    MutableRoaringBitmap expected = new MutableRoaringBitmap();
    for (int i = 0; i < numInputs; i++) {
      expected.or(inputs[i]);
    }
    return expected;
  }

  private static void assertValid(MutableRoaringBitmap actual, MutableRoaringBitmap expected) {
    // A bitmap left in lazy state reports wrong cardinalities and fails validation
    assertTrue(actual.validate());
    assertTrue(actual.equals(expected));
    assertEquals(actual.getLongCardinality(), expected.getLongCardinality());
    ByteBuffer buffer = ByteBuffer.allocate(actual.serializedSizeInBytes());
    actual.serialize(buffer);
    buffer.flip();
    assertTrue(new ImmutableRoaringBitmap(buffer).equals(expected));
  }

  @Test
  public void testFoldOverMappedInputsMatchesEagerUnion() {
    Random random = new Random(SEED);
    MutableRoaringBitmap[] inputs = inputs(random, 60);
    MutableRoaringBitmapUnion union = new MutableRoaringBitmapUnion();
    for (int i = 0; i < inputs.length; i++) {
      MutableRoaringBitmap copy = inputs[i].clone();
      ImmutableRoaringBitmap presented = present(inputs[i], i);
      union.add(presented);
      if (i % 7 == 0) {
        union.add(presented);
        union.add(new MutableRoaringBitmap());
        union.add(union.get());
      }
      assertTrue(presented.equals(copy));
      if (i % 10 == 9) {
        assertValid(union.get(), eagerUnion(inputs, i + 1));
      }
    }
    assertValid(union.take(), eagerUnion(inputs, inputs.length));
  }

  @Test
  public void testGetIsStableAndTakeResets() {
    Random random = new Random(SEED + 1);
    MutableRoaringBitmap[] inputs = inputs(random, 20);
    MutableRoaringBitmapUnion union = new MutableRoaringBitmapUnion();
    for (int i = 0; i < 10; i++) {
      union.add(present(inputs[i], i));
    }
    MutableRoaringBitmap published = union.get();
    assertSame(union.get(), published);
    MutableRoaringBitmap snapshot = published.clone();
    for (int i = 10; i < 20; i++) {
      union.add(present(inputs[i], i));
    }
    union.add(42);
    assertValid(published, snapshot);

    MutableRoaringBitmap expected = eagerUnion(inputs, 20);
    expected.add(42);
    MutableRoaringBitmap taken = union.take();
    assertNotSame(taken, published);
    assertValid(taken, expected);
    assertTrue(union.take().isEmpty());
  }

  @Test
  public void testMappedInputsWithManyNewKeys() {
    Random random = new Random(SEED + 3);
    MutableRoaringBitmap[] dense = inputs(random, 30);
    MutableRoaringBitmapUnion union = new MutableRoaringBitmapUnion();
    MutableRoaringBitmap expected = new MutableRoaringBitmap();
    for (int i = 0; i < dense.length; i++) {
      // Hashed values: every input brings many container keys the accumulator does not have yet
      MutableRoaringBitmap hashed = new MutableRoaringBitmap();
      for (int j = 0; j < 500; j++) {
        hashed.add(random.nextInt());
      }
      union.add(present(dense[i], i));
      union.add(present(hashed, i + 1));
      expected.or(dense[i]);
      expected.or(hashed);
      if (i % 8 == 7) {
        assertValid(union.get(), expected);
      }
    }
    assertValid(union.take(), expected);
  }

  @Test
  public void testSparseMappedInputsInterleavedWithDenseInputs() {
    Random random = new Random(SEED + 4);
    MutableRoaringBitmap[] dense = inputs(random, 80);
    MutableRoaringBitmapUnion union = new MutableRoaringBitmapUnion();
    MutableRoaringBitmap expected = new MutableRoaringBitmap();
    for (int i = 0; i < dense.length; i++) {
      union.add(present(dense[i], i));
      expected.or(dense[i]);
      if (i % 8 == 7) {
        // A few values spread over new keys, added to an accumulator that mostly holds dense containers
        MutableRoaringBitmap sparse = new MutableRoaringBitmap();
        for (int j = 0; j < 24; j++) {
          sparse.add(random.nextInt());
        }
        union.add(present(sparse, i));
        expected.or(sparse);
      }
      if (i % 16 == 15) {
        assertValid(union.get(), expected);
      }
    }
    assertValid(union.take(), expected);
  }

  @Test
  public void testTakeOwnershipAndNullArguments() {
    Random random = new Random(SEED + 2);
    MutableRoaringBitmap[] inputs = inputs(random, 3);
    MutableRoaringBitmapUnion union = new MutableRoaringBitmapUnion();
    union.add(inputs[0]);
    MutableRoaringBitmap result = union.take();
    // A bitmap handed out by a union is adopted as is. Any other bitmap is relinquished by the caller, so only the
    // values of the result are checked
    assertSame(MutableRoaringBitmapUnion.takeOwnership(result).take(), result);
    MutableRoaringBitmapUnion adopting = MutableRoaringBitmapUnion.takeOwnership(inputs[1].clone());
    adopting.add(inputs[2]);
    MutableRoaringBitmap expected = inputs[1].clone();
    expected.or(inputs[2]);
    assertValid(adopting.take(), expected);

    assertThrows(NullPointerException.class, () -> new MutableRoaringBitmapUnion().add(null));
    assertThrows(NullPointerException.class, () -> MutableRoaringBitmapUnion.takeOwnership(null));
  }
}
