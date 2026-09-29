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
/// [RoaringBitmapUnion]: same name and methods as the class proposed to RoaringBitmap, so that the swap is an import
/// change plus deleting this class. It is internal to Pinot and goes away with the swap. See [RoaringBitmapUnion] for
/// the contract and the swap steps.
///
/// Inputs may be read-only bitmaps over memory-mapped buffers; they are only read.
///
/// Differences from the library class:
///
/// - The accumulated state is a private subclass of [MutableRoaringBitmap] that reaches the protected lazy primitives
///   through inheritance; [#get()] and [#take()] hand out instances of it.
/// - [#takeOwnership(MutableRoaringBitmap)] adopts without copying only a bitmap that a union handed out; any other
///   bitmap is copied. Callers should still treat the argument as relinquished, and should not pass subclasses.
/// - [#add(int)] repairs pending lazy state before inserting and does not re-encode a run container that received
///   the value.
/// - An input is unioned eagerly while the accumulator holds few values per container, or when it would insert many
///   container keys that are new to the accumulator, because the released lazy union only pays off for dense
///   containers and inserts new keys one at a time.
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

  /// Creates an empty union.
  public MutableRoaringBitmapUnion() {
    _bitmap = new LazyBitmap();
  }

  private MutableRoaringBitmapUnion(LazyBitmap adopted) {
    _bitmap = adopted;
    // Adopted state is normalized on the first read, like the library class does
    _dirty = true;
    _numValuesAdded = adopted.getLongCardinality();
  }

  /// Creates a union whose initial state is the given bitmap. The caller relinquishes the instance: it must not be
  /// used again except through the union.
  public static MutableRoaringBitmapUnion takeOwnership(MutableRoaringBitmap bitmap) {
    Objects.requireNonNull(bitmap, "bitmap");
    if (bitmap instanceof LazyBitmap) {
      return new MutableRoaringBitmapUnion((LazyBitmap) bitmap);
    }
    LazyBitmap copy = new LazyBitmap();
    copy.lazyOr(bitmap);
    return new MutableRoaringBitmapUnion(copy);
  }

  /// Unions the input into the accumulated state. The input is neither modified nor retained. Adding this union's own
  /// [#get()] result, or an empty bitmap, is a no-op.
  public void add(ImmutableRoaringBitmap input) {
    Objects.requireNonNull(input, "input");
    if (input == _bitmap || input.isEmpty()) {
      return;
    }
    beforeMutation();
    boolean dense = _numValuesAdded >= (long) MIN_VALUES_PER_CONTAINER_FOR_LAZY_UNION * _bitmap.getContainerCount();
    _numValuesAdded += input.getLongCardinality();
    if (dense && !insertsManyNewKeys(input)) {
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
    return result;
  }

  /// Returns whether the lazy union would insert more than a few of the input's keys between keys the accumulator
  /// already has. Keys past the accumulator's last key do not count: they are appended in one step.
  private boolean insertsManyNewKeys(ImmutableRoaringBitmap input) {
    if (input.getContainerCount() <= MAX_NEW_KEYS_FOR_LAZY_UNION) {
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
      if (own.key() != key && ++numNewKeys > MAX_NEW_KEYS_FOR_LAZY_UNION) {
        return true;
      }
      other.advance();
    }
    return false;
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
