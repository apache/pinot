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

import java.util.Objects;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MappeableContainerPointer;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Interim stand-in for `org.roaringbitmap.buffer.MutableRoaringBitmapUnion`, the buffer counterpart of
/// [RoaringBitmapUnion]. It exposes only the proposed methods used by Pinot, so that the swap is an import change
/// plus deleting this class. It is internal to Pinot and goes away with the swap. See [RoaringBitmapUnion] for the
/// contract and the swap steps.
///
/// Inputs may be read-only bitmaps over memory-mapped buffers; they are only read.
///
/// Differences from the library class:
///
/// - The accumulated state is a private subclass of [MutableRoaringBitmap] that reaches the protected lazy primitives
///   through inheritance; [#get()] and [#take()] hand out instances of it.
/// - An input is unioned eagerly while the accumulator holds few values per container, when overlapping arrays would
///   stay small, or when it would insert many container keys that are new to the accumulator, because the released
///   lazy union only pays off for dense containers and inserts new keys one at a time.
///
/// Instances are not thread-safe.
public final class MutableRoaringBitmapUnion {
  // See RoaringBitmapUnion
  private static final int MIN_VALUES_PER_CONTAINER_FOR_LAZY_UNION = 1024;
  private static final int MAX_NEW_KEYS_FOR_LAZY_UNION = 4;

  // The accumulated bitmap. Never null.
  private LazyBitmap _bitmap;
  // Whether the bitmap holds lazy state that must be repaired before it is read
  private boolean _dirty;
  // Whether an alias returned by get() is outstanding and must not be mutated
  private boolean _published;
  // Number of values added so far, counting duplicates: an upper bound of the cardinality that is known without
  // repairing, used to estimate how dense the containers are
  private long _numValuesAdded;
  // A cardinality observed on repaired state. Unions never remove values, so this remains a lower bound used to
  // check average density without repairing every lazy union. Overlapping arrays are checked separately.
  private long _lastKnownCardinality;

  /// Creates an empty union.
  public MutableRoaringBitmapUnion() {
    _bitmap = new LazyBitmap();
  }

  /// Unions the input into the accumulated state. The input is neither modified nor retained. Adding this union's own
  /// [#get()] result, or an empty bitmap, is a no-op.
  public void add(ImmutableRoaringBitmap input) {
    Objects.requireNonNull(input, "input");
    if (input == _bitmap || input.isEmpty()) {
      return;
    }
    beforeMutation();
    boolean dense = isDenseForLazyUnion();
    _numValuesAdded += input.getLongCardinality();
    if (dense && !shouldUnionEagerly(input)) {
      _dirty = true;
      _bitmap.lazyOr(input);
    } else {
      repairIfDirty();
      _bitmap.or(input);
    }
  }

  /// Returns the accumulated bitmap with all pending lazy state repaired. The returned instance is the union's
  /// current state, not a copy; the union copies it before its next mutation, so the returned bitmap stays valid.
  /// Repeated calls without an intervening mutation return the same instance.
  public MutableRoaringBitmap get() {
    repairIfDirty();
    _published = true;
    return _bitmap;
  }

  /// Returns the accumulated bitmap, repaired as by [#get()], and transfers its ownership to the caller. The union is
  /// empty afterwards and can be reused.
  public MutableRoaringBitmap take() {
    repairIfDirty();
    MutableRoaringBitmap result = _bitmap;
    _bitmap = new LazyBitmap();
    _dirty = false;
    _published = false;
    _numValuesAdded = 0;
    _lastKnownCardinality = 0;
    return result;
  }

  /// Returns whether the lazy union would copy small overlapping arrays or insert more than a few new keys between
  /// existing ones. Keys past the accumulator's last key do not count: they are appended in one step.
  private boolean shouldUnionEagerly(ImmutableRoaringBitmap input) {
    // Called after the average density check: an empty or sole dense container has no small-array overlaps.
    // A small input cannot insert too many keys. Avoid allocating scan pointers for this common case.
    if (_bitmap.getContainerCount() <= 1 && input.getContainerCount() <= MAX_NEW_KEYS_FOR_LAZY_UNION) {
      return false;
    }
    MappeableContainerPointer own = _bitmap.getContainerPointer();
    MappeableContainerPointer other = input.getContainerPointer();
    int numNewKeys = 0;
    while (other.hasContainer()) {
      char key = other.key();
      while (own.hasContainer() && own.key() < key) {
        own.advance();
      }
      if (!own.hasContainer()) {
        return false;
      }
      if (own.key() == key) {
        if (!own.isBitmapContainer() && !own.isRunContainer() && !other.isBitmapContainer() && !other.isRunContainer()
            && own.getCardinality() + other.getCardinality() <= MIN_VALUES_PER_CONTAINER_FOR_LAZY_UNION) {
          return true;
        }
      } else if (++numNewKeys > MAX_NEW_KEYS_FOR_LAZY_UNION) {
        return true;
      }
      other.advance();
    }
    return false;
  }

  private boolean isDenseForLazyUnion() {
    long minCardinality = (long) MIN_VALUES_PER_CONTAINER_FOR_LAZY_UNION * _bitmap.getContainerCount();
    if (_lastKnownCardinality >= minCardinality) {
      return true;
    }
    if (_numValuesAdded < minCardinality) {
      return false;
    }
    // The estimate counts duplicates. Confirm actual density before switching a sparse accumulator to lazy unions.
    repairIfDirty();
    _lastKnownCardinality = _bitmap.getLongCardinality();
    _numValuesAdded = _lastKnownCardinality;
    return _lastKnownCardinality >= minCardinality;
  }

  private void repairIfDirty() {
    if (_dirty) {
      _bitmap.repair();
      _dirty = false;
    }
  }

  private void beforeMutation() {
    if (_published) {
      _bitmap = (LazyBitmap) _bitmap.clone();
      _published = false;
    }
  }

  /// Reaches the protected lazy primitives of [MutableRoaringBitmap] through inheritance.
  private static final class LazyBitmap extends MutableRoaringBitmap {
    // Public so that an instance that escaped through get() or take() stays deserializable like a plain bitmap
    public LazyBitmap() {
    }

    void lazyOr(ImmutableRoaringBitmap other) {
      lazyor(other);
    }

    void repair() {
      repairAfterLazy();
    }
  }
}
