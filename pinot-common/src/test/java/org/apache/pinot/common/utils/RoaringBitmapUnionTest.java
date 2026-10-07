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

import java.util.Random;
import org.roaringbitmap.Container;
import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Covers the interim [RoaringBitmapUnion]: folds are compared with eager unions, and the ownership rules of
/// `get()`, `take()` and `takeOwnership()` are pinned so that the swap to the library class cannot change behavior
/// unnoticed.
public class RoaringBitmapUnionTest {
  private static final long SEED = 20260929L;

  /// Inputs mixing sparse values, dense ranges and run-optimized ranges, so that array, bitmap and run containers
  /// all take part, including keys in the upper (unsigned) half of the range.
  private static RoaringBitmap[] inputs(Random random, int numInputs) {
    RoaringBitmap[] inputs = new RoaringBitmap[numInputs];
    for (int i = 0; i < numInputs; i++) {
      RoaringBitmap bitmap = new RoaringBitmap();
      int base = random.nextInt(8) << 16;
      if (random.nextBoolean()) {
        base |= 0x80000000;
      }
      switch (i % 3) {
        case 0:
          for (int j = 0; j < 300; j++) {
            bitmap.add(base | random.nextInt(1 << 16));
          }
          break;
        case 1:
          long start = (base & 0xFFFFFFFFL) + random.nextInt(1000);
          bitmap.add(start, start + 9000);
          break;
        default:
          long runStart = (base & 0xFFFFFFFFL) + random.nextInt(1000);
          bitmap.add(runStart, runStart + 5000);
          bitmap.runOptimize();
          break;
      }
      inputs[i] = bitmap;
    }
    return inputs;
  }

  private static RoaringBitmap eagerUnion(RoaringBitmap[] inputs, int numInputs) {
    RoaringBitmap expected = new RoaringBitmap();
    for (int i = 0; i < numInputs; i++) {
      expected.or(inputs[i]);
    }
    return expected;
  }

  private static void assertValid(RoaringBitmap actual, RoaringBitmap expected) {
    // A bitmap left in lazy state reports wrong cardinalities and fails validation
    assertTrue(actual.validate());
    assertTrue(actual.equals(expected));
    assertEquals(actual.getLongCardinality(), expected.getLongCardinality());
    assertTrue(RoaringBitmapUtils.deserialize(RoaringBitmapUtils.serialize(actual)).equals(expected));
  }

  /// Bitmaps of hashed values: a few hundred values spread over the whole unsigned range, so that every input brings
  /// many container keys the accumulator does not have yet.
  private static RoaringBitmap[] hashedInputs(Random random, int numInputs, int valuesPerInput) {
    RoaringBitmap[] inputs = new RoaringBitmap[numInputs];
    for (int i = 0; i < numInputs; i++) {
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int j = 0; j < valuesPerInput; j++) {
        bitmap.add(random.nextInt());
      }
      inputs[i] = bitmap;
    }
    return inputs;
  }

  @Test
  public void testFoldMatchesEagerUnionAndLeavesInputsUntouched() {
    Random random = new Random(SEED);
    RoaringBitmap[] inputs = inputs(random, 60);
    RoaringBitmap[] copies = new RoaringBitmap[inputs.length];
    RoaringBitmapUnion union = new RoaringBitmapUnion();
    for (int i = 0; i < inputs.length; i++) {
      copies[i] = inputs[i].clone();
      union.add(inputs[i]);
      // The same input twice, an empty input and the union's own result never change the outcome
      if (i % 7 == 0) {
        union.add(inputs[i]);
        union.add(new RoaringBitmap());
        union.add(union.get());
      }
      if (i % 10 == 9) {
        assertValid(union.get(), eagerUnion(inputs, i + 1));
      }
    }
    assertValid(union.take(), eagerUnion(inputs, inputs.length));
    for (int i = 0; i < inputs.length; i++) {
      assertTrue(inputs[i].equals(copies[i]));
    }
  }

  @Test
  public void testGetIsStableAndNeverInvalidatedByLaterAdds() {
    Random random = new Random(SEED + 1);
    RoaringBitmap[] inputs = inputs(random, 20);
    RoaringBitmapUnion union = new RoaringBitmapUnion();
    for (int i = 0; i < 10; i++) {
      union.add(inputs[i]);
    }
    RoaringBitmap published = union.get();
    assertSame(union.get(), published);
    RoaringBitmap snapshot = published.clone();

    for (int i = 10; i < 20; i++) {
      union.add(inputs[i]);
    }
    union.add(12345);
    // The published bitmap was copied before the union changed again
    assertValid(published, snapshot);
    RoaringBitmap expected = eagerUnion(inputs, 20);
    expected.add(12345);
    RoaringBitmap latest = union.get();
    assertNotSame(latest, published);
    assertValid(latest, expected);
  }

  @Test
  public void testTakeTransfersOwnershipAndResets() {
    Random random = new Random(SEED + 2);
    RoaringBitmap[] inputs = inputs(random, 10);
    RoaringBitmapUnion union = new RoaringBitmapUnion();
    for (RoaringBitmap input : inputs) {
      union.add(input);
    }
    RoaringBitmap published = union.get();
    RoaringBitmap taken = union.take();
    assertSame(taken, published);
    RoaringBitmap snapshot = taken.clone();

    assertTrue(union.get().isEmpty());
    union.add(inputs[0]);
    union.add(7);
    assertValid(taken, snapshot);
    RoaringBitmap expected = inputs[0].clone();
    expected.add(7);
    assertValid(union.take(), expected);
    assertTrue(union.take().isEmpty());
  }

  @Test
  public void testDeserializeToUnionAdoptsWithoutCopyAndKeepsAccumulating() {
    Random random = new Random(SEED + 3);
    RoaringBitmap[] inputs = inputs(random, 12);
    RoaringBitmapUnion union = RoaringBitmapUtils.deserializeToUnion(RoaringBitmapUtils.serialize(inputs[0]));
    assertValid(union.get(), inputs[0]);
    for (int i = 1; i < inputs.length; i++) {
      union.add(inputs[i]);
    }
    RoaringBitmap result = union.take();
    assertValid(result, eagerUnion(inputs, inputs.length));

    // A bitmap handed out by a union is adopted as is. Any other bitmap is relinquished by the caller, so only the
    // values of the result are checked
    assertSame(RoaringBitmapUnion.takeOwnership(result).take(), result);
    RoaringBitmapUnion adopting = RoaringBitmapUnion.takeOwnership(inputs[1].clone());
    adopting.add(inputs[2]);
    RoaringBitmap expected = inputs[1].clone();
    expected.or(inputs[2]);
    assertValid(adopting.take(), expected);
  }

  @Test
  public void testInputsWithManyNewKeysInterleavedWithDenseInputs() {
    Random random = new Random(SEED + 5);
    RoaringBitmap[] hashed = hashedInputs(random, 40, 500);
    RoaringBitmap[] dense = inputs(random, 40);
    RoaringBitmapUnion union = new RoaringBitmapUnion();
    RoaringBitmap expected = new RoaringBitmap();
    for (int i = 0; i < hashed.length; i++) {
      RoaringBitmap hashedCopy = hashed[i].clone();
      // Inputs that bring many new keys and inputs that bring few alternate, so pending lazy state has to be
      // repaired before the accumulator is unioned eagerly
      union.add(dense[i]);
      union.add(hashed[i]);
      expected.or(dense[i]);
      expected.or(hashed[i]);
      assertTrue(hashed[i].equals(hashedCopy));
      if (i % 8 == 7) {
        assertValid(union.get(), expected);
      }
    }
    // Once the accumulator has most keys, the same inputs bring few new ones
    for (RoaringBitmap input : hashed) {
      union.add(input);
    }
    assertValid(union.take(), expected);
  }

  @Test
  public void testSparseInputsInterleavedWithDenseInputs() {
    Random random = new Random(SEED + 6);
    RoaringBitmap[] dense = inputs(random, 80);
    RoaringBitmap[] sparse = hashedInputs(random, 10, 24);
    RoaringBitmapUnion union = new RoaringBitmapUnion();
    RoaringBitmap expected = new RoaringBitmap();
    for (int i = 0; i < dense.length; i++) {
      union.add(dense[i]);
      expected.or(dense[i]);
      if (i % 8 == 7) {
        // A few values spread over new keys, added to an accumulator that mostly holds dense containers
        union.add(sparse[i / 8]);
        expected.or(sparse[i / 8]);
      }
      if (i % 16 == 15) {
        assertValid(union.get(), expected);
      }
    }
    // Enough hashed values to leave the accumulator with mostly sparse containers, then dense inputs again
    for (RoaringBitmap input : hashedInputs(random, 40, 500)) {
      union.add(input);
      expected.or(input);
    }
    assertValid(union.get(), expected);
    for (RoaringBitmap input : inputs(random, 20)) {
      union.add(input);
      expected.or(input);
    }
    assertValid(union.take(), expected);
  }

  @Test
  public void testSingleValuesMixedWithBitmaps() {
    Random random = new Random(SEED + 4);
    RoaringBitmap[] inputs = inputs(random, 30);
    RoaringBitmapUnion union = new RoaringBitmapUnion();
    RoaringBitmap expected = new RoaringBitmap();
    for (int i = 0; i < inputs.length; i++) {
      union.add(inputs[i]);
      expected.or(inputs[i]);
      // Single values landing in containers the previous lazy union just touched, and in new ones
      for (int j = 0; j < 5; j++) {
        int value = random.nextBoolean() ? inputs[i].first() + j : random.nextInt();
        union.add(value);
        expected.add(value);
      }
    }
    union.add(-1);
    expected.add(-1);
    assertValid(union.take(), expected);
  }

  @Test
  public void testRawValuesOnlyCrossTheArrayToBitmapThreshold() {
    RoaringBitmapUnion union = new RoaringBitmapUnion();
    RoaringBitmap expected = new RoaringBitmap();
    for (int i = 0; i < 5000; i++) {
      union.add(3 * i);
      expected.add(3 * i);
    }
    assertValid(union.get(), expected);
  }

  @Test
  public void testRepeatedSparseInputsStayInPlace() {
    RoaringBitmap input = new RoaringBitmap();
    for (int key = 0; key < 24; key++) {
      input.add(key << 16);
    }
    RoaringBitmapUnion seed = new RoaringBitmapUnion();
    seed.add(input);
    RoaringBitmap owned = seed.take();
    Container[] containers = new Container[owned.getContainerCount()];
    ContainerPointer pointer = owned.getContainerPointer();
    for (int i = 0; i < containers.length; i++) {
      containers[i] = pointer.getContainer();
      pointer.advance();
    }
    RoaringBitmapUnion union = RoaringBitmapUnion.takeOwnership(owned);
    // Duplicate counts exceed the lazy threshold, but the result remains one value per container.
    for (int i = 0; i < 2048; i++) {
      union.add(input);
    }
    RoaringBitmap result = union.take();
    assertValid(result, input);
    pointer = result.getContainerPointer();
    for (Container container : containers) {
      // Eager unions reuse these sparse arrays; lazy unions replace them even when no values change.
      assertSame(pointer.getContainer(), container);
      pointer.advance();
    }
  }

  @Test
  public void testSparseContainersStayInPlaceWithSkewedDensity() {
    for (int numSparseKeys : new int[]{1, 50}) {
      RoaringBitmap initial = new RoaringBitmap();
      initial.add(0L, 1L << 16);
      for (int key = 1; key <= numSparseKeys; key++) {
        initial.add(key << 16);
      }
      RoaringBitmapUnion seed = new RoaringBitmapUnion();
      seed.add(initial);
      RoaringBitmap owned = seed.take();
      ContainerPointer pointer = owned.getContainerPointer();
      pointer.advance();
      Container[] sparseContainers = new Container[numSparseKeys];
      for (int i = 0; i < numSparseKeys; i++) {
        sparseContainers[i] = pointer.getContainer();
        pointer.advance();
      }
      RoaringBitmapUnion union = RoaringBitmapUnion.takeOwnership(owned);
      RoaringBitmap expected = initial.clone();
      // The full first container masks the sparse arrays in the average density, even with 50 sparse keys.
      for (int i = 1; i <= 500; i++) {
        RoaringBitmap input = new RoaringBitmap();
        for (int key = 1; key <= numSparseKeys; key++) {
          input.add((key << 16) | (i * 2));
        }
        union.add(input);
        expected.or(input);
      }
      RoaringBitmap result = union.take();
      assertValid(result, expected);
      pointer = result.getContainerPointer();
      pointer.advance();
      for (Container container : sparseContainers) {
        // Eager unions grow these arrays in place; lazy unions allocate a replacement on every input.
        assertSame(pointer.getContainer(), container);
        pointer.advance();
      }
    }
  }

  @Test
  public void testDenseInputsStillUseLazyUnion() {
    RoaringBitmap input = new RoaringBitmap();
    for (int value = 0; value < 1024; value++) {
      input.add(value);
    }
    RoaringBitmapUnion seed = new RoaringBitmapUnion();
    seed.add(input);
    RoaringBitmap owned = seed.take();
    Container container = owned.getContainerPointer().getContainer();
    RoaringBitmapUnion union = RoaringBitmapUnion.takeOwnership(owned);
    union.add(RoaringBitmap.bitmapOf(0));
    RoaringBitmap result = union.take();
    assertValid(result, input);
    // Lazy union promotes the dense array and repair replaces it; eager union would keep the array.
    assertNotSame(result.getContainerPointer().getContainer(), container);
  }

  @Test
  public void testNullArguments() {
    assertThrows(NullPointerException.class, () -> new RoaringBitmapUnion().add((RoaringBitmap) null));
    assertThrows(NullPointerException.class, () -> RoaringBitmapUnion.takeOwnership(null));
  }

  /// Fails once the RoaringBitmap library on the classpath ships the classes these interim ones stand in for, with
  /// the steps of the swap in the failure message.
  @Test
  public void testLibraryDoesNotShipTheUnionClassesYet() {
    for (String libraryClass : new String[]{
        "org.roaringbitmap.RoaringBitmapUnion", "org.roaringbitmap.buffer.MutableRoaringBitmapUnion"
    }) {
      try {
        Class.forName(libraryClass);
      } catch (ClassNotFoundException e) {
        continue;
      }
      fail("RoaringBitmap now ships " + libraryClass + ". Replace Pinot's interim copies: import the union classes "
          + "from org.roaringbitmap and org.roaringbitmap.buffer at the call sites, make "
          + "RoaringBitmapUtils.deserializeToUnion call RoaringBitmapUnion.takeOwnership(deserialize(bytes)), and "
          + "delete org.apache.pinot.common.utils.RoaringBitmapUnion, MutableRoaringBitmapUnion and their two tests");
    }
  }
}
