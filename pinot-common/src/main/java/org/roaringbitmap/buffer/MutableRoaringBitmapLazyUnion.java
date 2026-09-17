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
package org.roaringbitmap.buffer;

/// Buffer-bitmap counterpart of [org.roaringbitmap.RoaringBitmapLazyUnion]: exposes the protected
/// lazy-union primitives of [MutableRoaringBitmap] (the ones [BufferFastAggregation] uses
/// internally) to streaming code that folds many bitmaps into one accumulator. This class lives in
/// the `org.roaringbitmap.buffer` package purely for access to those primitives; keep it free of
/// Pinot types.
///
/// Contract: after any [#lazyOr] call the accumulator is in an internal lazy state — its
/// cardinality, equality, containment checks and serialized form are all invalid until [#repair]
/// is called. Between [#lazyOr] and [#repair], the only operation allowed on the accumulator is
/// another [#lazyOr].
public final class MutableRoaringBitmapLazyUnion {
  private MutableRoaringBitmapLazyUnion() {
  }

  /// Unions `input` into `accumulator` without maintaining cardinality. The `input` bitmap is not
  /// modified and must itself not be in a lazy state. The accumulator must be repaired via
  /// [#repair] before it is read or serialized.
  public static void lazyOr(MutableRoaringBitmap accumulator, ImmutableRoaringBitmap input) {
    accumulator.lazyor(input);
  }

  /// Restores an accumulator mutated by [#lazyOr] to a fully valid bitmap, recomputing container
  /// cardinalities and re-normalizing container types. Safe (and cheap) to call on bitmaps that
  /// were never lazily modified.
  public static void repair(MutableRoaringBitmap bitmap) {
    bitmap.repairAfterLazy();
  }
}
