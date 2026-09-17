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
package org.roaringbitmap;

/// Unions a small intermediate bitmap into a larger accumulator without scanning untouched container keys.
/// The accumulator remains fully valid after every call; the input is not modified or retained. Like
/// [RoaringBitmap#or(RoaringBitmap)], callers must exclusively own the accumulator during mutation.
/// This class lives in the Roaring package to access its container array without exposing it through Pinot APIs.
public final class RoaringBitmapMerge {
  private RoaringBitmapMerge() {
  }

  public static void or(RoaringBitmap accumulator, RoaringBitmap input) {
    if (accumulator == input) {
      return;
    }
    RoaringArray left = accumulator.highLowContainer;
    RoaringArray right = input.highLowContainer;
    // Similar-sized key sets are best handled by the library's linear merge.
    if (left.size < 4 * right.size) {
      accumulator.or(input);
      return;
    }
    orSparse(left, right);
  }

  private static void orSparse(RoaringArray left, RoaringArray right) {
    int leftIndex = 0;
    for (int rightIndex = 0; rightIndex < right.size; rightIndex++) {
      char key = right.keys[rightIndex];
      leftIndex = left.advanceUntil(key, leftIndex - 1);
      if (leftIndex == left.size) {
        left.appendCopy(right, rightIndex, right.size);
        return;
      }
      if (left.keys[leftIndex] != key) {
        // Batch missing keys as the library does. Repeated insertion could shift the growing array quadratically.
        left.mergeBulk(right, leftIndex, leftIndex, rightIndex, RoaringArray.MERGE_OR);
        return;
      }
      Container incoming = right.values[rightIndex];
      Container current = left.values[leftIndex];
      // Insert a singleton directly instead of shifting and merging the entire existing array container.
      left.values[leftIndex] = incoming instanceof ArrayContainer && incoming.getCardinality() == 1
          ? current.add(((ArrayContainer) incoming).content[0]) : current.ior(incoming);
      leftIndex++;
    }
  }
}
