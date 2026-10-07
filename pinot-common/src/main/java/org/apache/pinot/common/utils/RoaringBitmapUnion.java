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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.roaringbitmap.ContainerPointer;
import org.roaringbitmap.RoaringBitmap;


/// Interim stand-in for `org.roaringbitmap.RoaringBitmapUnion`, which is proposed to RoaringBitmap for
/// apache/pinot#19587 and not released yet. It exposes only the proposed methods used by Pinot, so that once a
/// RoaringBitmap release ships the class the swap is mechanical:
///
/// - change the import at the call sites to `org.roaringbitmap.RoaringBitmapUnion`,
/// - make [RoaringBitmapUtils#deserializeToUnion] call `RoaringBitmapUnion.takeOwnership(deserialize(bytes))`,
/// - delete this class, [MutableRoaringBitmapUnion] and their two tests (the library tests its own classes).
///
/// `RoaringBitmapUnionTest` fails as soon as the library class is on the classpath, as a reminder. This class is
/// internal to Pinot and goes away with the swap; code outside this repository should not depend on it.
///
/// An incremental union accumulator: it folds bitmaps that arrive over time into one bitmap with RoaringBitmap's lazy
/// union, which skips cardinality maintenance and keeps overlapping containers in a form that makes further unions
/// cheap, and it repairs the result only when it is read. Callers only ever observe valid bitmaps:
///
/// - [#add(RoaringBitmap)] never modifies or retains its argument.
/// - [#get()] returns the accumulated bitmap without copying. The union never modifies that instance again: its next
///   mutating call first copies it, so the returned bitmap stays valid. Callers must treat it as read-only while they
///   still intend to use the union.
/// - [#take()] transfers ownership of the accumulated bitmap and leaves the union empty.
///
/// Differences from the library class:
///
/// - The lazy primitives of [RoaringBitmap] are `protected`, so the accumulated state is a private subclass that
///   reaches them through inheritance. The bitmaps handed out by [#get()] and [#take()] are instances of that
///   subclass; it adds no state and overrides nothing.
/// - Bitmaps are deserialized straight into a union with [RoaringBitmapUtils#deserializeToUnion], and copies are
///   made by adding to an empty union.
/// - [#add(int)] repairs pending lazy state before inserting, because inserting into a lazy container needs the
///   library's internals, and it does not re-encode a run container that received the value. A given accumulator
///   receives either bitmaps or single values in Pinot, never both.
/// - The released lazy union only pays off once containers are dense: while they are small arrays it allocates a new
///   container per union where the eager union merges in place, and it inserts container keys that are new to the
///   accumulator one at a time, which is quadratic when an input brings many of them. So an input is unioned eagerly
///   while the accumulator holds few values per container (hashed values spread over many containers stay there for
///   good), when overlapping arrays would stay small, or when the input would insert many new keys, and lazily
///   otherwise. The library class does not need this: its lazy union merges small arrays in place and new keys in
///   one pass.
///
/// Instances are not thread-safe.
public final class RoaringBitmapUnion {
  // The lazy union starts to pay off when containers hold more values than the library's lazy array bound: past it
  // they are kept as bitmaps whose unions skip cardinality maintenance, below it they are arrays that the lazy union
  // copies on every union
  private static final int MIN_VALUES_PER_CONTAINER_FOR_LAZY_UNION = 1024;
  // An input that would insert more new keys than this between existing ones is unioned eagerly. Each insert shifts
  // the accumulator's key and container arrays, and the eager union's single merge pass costs about as much as a
  // few of those shifts.
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
  public RoaringBitmapUnion() {
    _bitmap = new LazyBitmap();
  }

  private RoaringBitmapUnion(LazyBitmap adopted) {
    _bitmap = adopted;
    // Adopted state is normalized on the first read, like the library class does
    _dirty = true;
    _numValuesAdded = adopted.getLongCardinality();
    _lastKnownCardinality = _numValuesAdded;
  }

  /// Deserializes a bitmap straight into a union that owns it. Not part of the library class: callers go through
  /// [RoaringBitmapUtils#deserializeToUnion].
  static RoaringBitmapUnion deserialize(ByteBuffer byteBuffer) {
    LazyBitmap bitmap = new LazyBitmap();
    try {
      bitmap.deserialize(byteBuffer);
    } catch (IOException e) {
      throw new RuntimeException("Caught exception while deserializing RoaringBitmap", e);
    }
    return new RoaringBitmapUnion(bitmap);
  }

  /// Unions the input into the accumulated state. The input is neither modified nor retained, so it may be reused or
  /// mutated afterwards. Adding this union's own [#get()] result, or an empty bitmap, is a no-op.
  public void add(RoaringBitmap input) {
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

  /// Unions a single value, treated as unsigned, into the accumulated state.
  public void add(int value) {
    beforeMutation();
    repairIfDirty();
    _bitmap.add(value);
    _numValuesAdded++;
  }

  /// Returns the accumulated bitmap with all pending lazy state repaired. The returned instance is the union's
  /// current state, not a copy; the union copies it before its next mutation, so the returned bitmap stays valid.
  /// Repeated calls without an intervening mutation return the same instance.
  public RoaringBitmap get() {
    repairIfDirty();
    _published = true;
    return _bitmap;
  }

  /// Returns the accumulated bitmap, repaired as by [#get()], and transfers its ownership to the caller. The union is
  /// empty afterwards and can be reused.
  public RoaringBitmap take() {
    repairIfDirty();
    RoaringBitmap result = _bitmap;
    _bitmap = new LazyBitmap();
    _dirty = false;
    _published = false;
    _numValuesAdded = 0;
    _lastKnownCardinality = 0;
    return result;
  }

  /// Returns whether the lazy union would copy small overlapping arrays or insert more than a few new keys between
  /// existing ones. Keys past the accumulator's last key do not count: they are appended in one step.
  private boolean shouldUnionEagerly(RoaringBitmap input) {
    // Called after the average density check: an empty or sole dense container has no small-array overlaps.
    // A small input cannot insert too many keys. Avoid allocating scan pointers for this common case.
    if (_bitmap.getContainerCount() <= 1 && input.getContainerCount() <= MAX_NEW_KEYS_FOR_LAZY_UNION) {
      return false;
    }
    ContainerPointer own = _bitmap.getContainerPointer();
    ContainerPointer other = input.getContainerPointer();
    int numNewKeys = 0;
    while (other.getContainer() != null) {
      char key = other.key();
      while (own.getContainer() != null && own.key() < key) {
        own.advance();
      }
      if (own.getContainer() == null) {
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

  /// Reaches the protected lazy primitives of [RoaringBitmap] through inheritance.
  private static final class LazyBitmap extends RoaringBitmap {
    // Public so that an instance that escaped through get() or take() stays deserializable like a plain bitmap
    public LazyBitmap() {
    }

    void lazyOr(RoaringBitmap other) {
      lazyor(other);
    }

    void repair() {
      repairAfterLazy();
    }
  }
}
