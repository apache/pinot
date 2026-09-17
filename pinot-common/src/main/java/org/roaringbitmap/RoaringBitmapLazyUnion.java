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

/// Exposes RoaringBitmap's protected lazy-union primitives (the ones [FastAggregation] uses
/// internally) to streaming aggregation code that folds many bitmaps into one accumulator. This
/// class lives in the `org.roaringbitmap` package purely for access to those primitives; keep it
/// free of Pinot types.
///
/// A lazy union skips per-container cardinality maintenance, and once a container grows past the
/// lazy promotion threshold it is held in bitmap form, so each further union of that container is a
/// plain word-wise OR. Repeated [RoaringBitmap#or(RoaringBitmap)] calls instead recompute
/// cardinality and re-normalize containers on every union, which dominates CPU when thousands of
/// bitmaps are folded into a dense accumulator.
///
/// Contract: after any [#lazyOr] call the accumulator is in an internal lazy state — its
/// cardinality, equality, containment checks and serialized form are all invalid until [#repair]
/// is called. Between [#lazyOr] and [#repair], the only operation allowed on the accumulator is
/// another [#lazyOr].
public final class RoaringBitmapLazyUnion {
  private RoaringBitmapLazyUnion() {
  }

  /// Unions `input` into `accumulator` without maintaining cardinality. The `input` bitmap is not
  /// modified and must itself not be in a lazy state. The accumulator must be repaired via
  /// [#repair] before it is read or serialized.
  public static void lazyOr(RoaringBitmap accumulator, RoaringBitmap input) {
    accumulator.lazyor(input);
  }

  /// Restores an accumulator mutated by [#lazyOr] to a fully valid bitmap, recomputing container
  /// cardinalities and re-normalizing container types. Safe (and cheap) to call on bitmaps that
  /// were never lazily modified.
  public static void repair(RoaringBitmap bitmap) {
    bitmap.repairAfterLazy();
  }
}
