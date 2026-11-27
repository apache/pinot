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
package org.apache.pinot.core.query.aggregation.groupby.offheap;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Off-heap hash table mapping a 64-bit raw group key to a dense int group id, modeled on ClickHouse key64 hash
/// tables and `DictionaryBasedGroupKeyGenerator.IntGroupIdMap`. It is an off-heap replacement for
/// `Long2IntOpenHashMap` style maps in group-by key generation.
///
/// Group ids are assigned densely in insertion order: 0, 1, 2, ... [int)][#getGroupId(long,] returns the
/// existing id if the key is present (regardless of the upper bound); otherwise it assigns id `size()` if
/// `size() < groupIdUpperBound`, or returns [GroupKeyGenerator#INVALID_ID] without inserting.
///
/// Implementation: open addressing with linear probing (step +1) over a power-of-two capacity with load factor
/// 0.5. Each slot is 16 bytes: [long key][int groupId][4 bytes unused]. An empty slot is identified by key == 0, so
/// the real key 0 is held out-of-band in a field (the ClickHouse zero-value trick) and does not occupy a slot.
/// Resizing doubles the capacity and rehashes all occupied slots; assigned group ids are never changed by a resize.
///
/// Memory is direct (off-heap) via [PinotDataBuffer]. [#close()] releases it and is idempotent;
/// behavior of all other methods after close is undefined.
///
/// This class is not thread-safe.
@NotThreadSafe
public class OffHeapLongGroupIdMap implements AutoCloseable {
  public static final int INVALID_ID = GroupKeyGenerator.INVALID_ID;

  // Slot layout: [long key][int groupId][4 bytes unused] = 16 bytes
  private static final int SLOT_SHIFT = 4;
  private static final int GROUP_ID_OFFSET_IN_SLOT = Long.BYTES;
  private static final int MIN_CAPACITY = 512;
  // Largest power-of-two capacity representable as a positive int; expand() past this would overflow
  private static final int MAX_CAPACITY = 1 << 30;
  // Reusable zero block for bulk zero-filling freshly allocated buffers through the direct view
  private static final byte[] ZERO_CHUNK = new byte[8192];

  private PinotDataBuffer _buffer;
  // Absolute-indexed direct view of _buffer for the per-row probe loop (monomorphic, intrinsified ByteBuffer
  // access instead of the PinotDataBuffer wrapper); null when the table exceeds the 2GB view limit
  private ByteBuffer _view;
  private int _capacity;
  private int _mask;
  // Resize when the number of occupied slots exceeds this (i.e. load factor 0.5). The out-of-band zero key does
  // not occupy a slot and is not counted here.
  private int _maxOccupiedSlots;
  private int _occupiedSlots;
  private int _zeroKeyGroupId = INVALID_ID;
  private boolean _closed;

  public OffHeapLongGroupIdMap(int expectedNumEntries) {
    Preconditions.checkArgument(expectedNumEntries >= 0, "Invalid expectedNumEntries: %s", expectedNumEntries);
    long desiredCapacity = Math.max(MIN_CAPACITY, (long) expectedNumEntries << 1);
    _capacity = (int) Math.min(MAX_CAPACITY, Long.highestOneBit((desiredCapacity << 1) - 1));
    _mask = _capacity - 1;
    _maxOccupiedSlots = _capacity >>> 1;
    long sizeBytes = (long) _capacity << SLOT_SHIFT;
    _buffer = allocate(sizeBytes);
    _view = OffHeapGroupByUtils.createView(_buffer, sizeBytes);
    zeroFill(_buffer, _view, sizeBytes);
  }

  /// Returns the number of groups assigned so far, including the group for the raw key 0 if assigned.
  public int size() {
    return _zeroKeyGroupId != INVALID_ID ? _occupiedSlots + 1 : _occupiedSlots;
  }

  /// Returns the amount of off-heap memory held by this map in bytes.
  public long getOffHeapMemoryBytes() {
    return (long) _capacity << SLOT_SHIFT;
  }

  /// Returns the group id for the given raw key. If the key is present, always returns its id (even when
  /// `size() >= groupIdUpperBound`). If absent and `size() < groupIdUpperBound`, assigns the next dense
  /// id (`size()`) and returns it; otherwise returns [#INVALID_ID] without inserting.
  public int getGroupId(long rawKey, int groupIdUpperBound) {
    if (rawKey == 0) {
      int zeroKeyGroupId = _zeroKeyGroupId;
      if (zeroKeyGroupId != INVALID_ID) {
        return zeroKeyGroupId;
      }
      // Zero key not assigned yet, so size() == _occupiedSlots here
      int size = size();
      if (size < groupIdUpperBound) {
        _zeroKeyGroupId = size;
        return size;
      }
      return INVALID_ID;
    }
    ByteBuffer view = _view;
    if (view == null) {
      return getGroupIdSlow(rawKey, groupIdUpperBound);
    }
    // While the view exists, slot offsets fit in an int (view size <= Integer.MAX_VALUE)
    int slot = (int) (mix(rawKey) & _mask);
    while (true) {
      int slotOffset = slot << SLOT_SHIFT;
      long key = view.getLong(slotOffset);
      if (key == rawKey) {
        return view.getInt(slotOffset + GROUP_ID_OFFSET_IN_SLOT);
      }
      if (key == 0) {
        int size = size();
        if (size >= groupIdUpperBound) {
          return INVALID_ID;
        }
        view.putLong(slotOffset, rawKey);
        view.putInt(slotOffset + GROUP_ID_OFFSET_IN_SLOT, size);
        if (++_occupiedSlots > _maxOccupiedSlots) {
          expand();
        }
        return size;
      }
      slot = (slot + 1) & _mask;
    }
  }

  private int getGroupIdSlow(long rawKey, int groupIdUpperBound) {
    PinotDataBuffer buffer = _buffer;
    int slot = (int) (mix(rawKey) & _mask);
    while (true) {
      long slotOffset = (long) slot << SLOT_SHIFT;
      long key = buffer.getLong(slotOffset);
      if (key == rawKey) {
        return buffer.getInt(slotOffset + GROUP_ID_OFFSET_IN_SLOT);
      }
      if (key == 0) {
        int size = size();
        if (size >= groupIdUpperBound) {
          return INVALID_ID;
        }
        buffer.putLong(slotOffset, rawKey);
        buffer.putInt(slotOffset + GROUP_ID_OFFSET_IN_SLOT, size);
        if (++_occupiedSlots > _maxOccupiedSlots) {
          expand();
        }
        return size;
      }
      slot = (slot + 1) & _mask;
    }
  }

  /// Returns an iterator over all (rawKey, groupId) entries in arbitrary slot order, with the zero-key entry (if
  /// assigned) yielded last. Yields exactly [#size()] entries.
  ///
  /// NOTE: The returned [Entry] instance is a flyweight reused across `next()` calls; copy the values
  /// out if they need to outlive the next call.
  public Iterator<Entry> iterator() {
    return new Iterator<>() {
      private final Entry _entry = new Entry();
      private int _slot;
      private int _remainingOccupiedSlots = _occupiedSlots;
      private boolean _returnZeroKey = _zeroKeyGroupId != INVALID_ID;

      @Override
      public boolean hasNext() {
        return _remainingOccupiedSlots > 0 || _returnZeroKey;
      }

      @Override
      public Entry next() {
        if (_remainingOccupiedSlots > 0) {
          long key;
          long slotOffset;
          do {
            slotOffset = (long) _slot << SLOT_SHIFT;
            key = _buffer.getLong(slotOffset);
            _slot++;
          } while (key == 0);
          _entry._rawKey = key;
          _entry._groupId = _buffer.getInt(slotOffset + GROUP_ID_OFFSET_IN_SLOT);
          _remainingOccupiedSlots--;
          return _entry;
        }
        if (_returnZeroKey) {
          _returnZeroKey = false;
          _entry._rawKey = 0;
          _entry._groupId = _zeroKeyGroupId;
          return _entry;
        }
        throw new NoSuchElementException();
      }
    };
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    closeBuffer(_buffer);
    // Null the buffer and view so any use-after-close (or a second release of a pooled buffer) fails loudly with
    // an NPE instead of silently aliasing memory that the pool may have handed to another query
    _buffer = null;
    _view = null;
  }

  private void expand() {
    Preconditions.checkState(_capacity < MAX_CAPACITY, "Cannot expand beyond max capacity: %s", MAX_CAPACITY);
    int newCapacity = _capacity << 1;
    int newMask = newCapacity - 1;
    long newSizeBytes = (long) newCapacity << SLOT_SHIFT;
    PinotDataBuffer newBuffer = allocate(newSizeBytes);
    ByteBuffer newView = OffHeapGroupByUtils.createView(newBuffer, newSizeBytes);
    zeroFill(newBuffer, newView, newSizeBytes);
    ByteBuffer oldView = _view;
    if (oldView != null && newView != null) {
      // Hot path: rehash through the direct views (slot offsets fit in an int while a view exists)
      for (int slot = 0; slot < _capacity; slot++) {
        int slotOffset = slot << SLOT_SHIFT;
        long key = oldView.getLong(slotOffset);
        if (key != 0) {
          int newSlot = (int) (mix(key) & newMask);
          int newSlotOffset = newSlot << SLOT_SHIFT;
          while (newView.getLong(newSlotOffset) != 0) {
            newSlot = (newSlot + 1) & newMask;
            newSlotOffset = newSlot << SLOT_SHIFT;
          }
          newView.putLong(newSlotOffset, key);
          newView.putInt(newSlotOffset + GROUP_ID_OFFSET_IN_SLOT, oldView.getInt(slotOffset + GROUP_ID_OFFSET_IN_SLOT));
        }
      }
    } else {
      for (int slot = 0; slot < _capacity; slot++) {
        long slotOffset = (long) slot << SLOT_SHIFT;
        long key = _buffer.getLong(slotOffset);
        if (key != 0) {
          int newSlot = (int) (mix(key) & newMask);
          long newSlotOffset = (long) newSlot << SLOT_SHIFT;
          while (newBuffer.getLong(newSlotOffset) != 0) {
            newSlot = (newSlot + 1) & newMask;
            newSlotOffset = (long) newSlot << SLOT_SHIFT;
          }
          newBuffer.putLong(newSlotOffset, key);
          newBuffer.putInt(newSlotOffset + GROUP_ID_OFFSET_IN_SLOT,
              _buffer.getInt(slotOffset + GROUP_ID_OFFSET_IN_SLOT));
        }
      }
    }
    closeBuffer(_buffer);
    _buffer = newBuffer;
    _capacity = newCapacity;
    _mask = newMask;
    _maxOccupiedSlots = newCapacity >>> 1;
    _view = newView;
  }

  private static PinotDataBuffer allocate(long sizeBytes) {
    return OffHeapGroupByBufferPool.acquire(sizeBytes, "OffHeapLongGroupIdMap hash table");
  }

  /// Zero-fills a freshly allocated buffer (contents of [PinotDataBuffer#allocateDirect] are undefined, and
  /// this map relies on key == 0 marking an empty slot). Uses bulk puts through the direct view when available.
  private static void zeroFill(PinotDataBuffer buffer, ByteBuffer view, long sizeBytes) {
    if (view != null) {
      int size = (int) sizeBytes;
      for (int offset = 0; offset < size; offset += ZERO_CHUNK.length) {
        view.put(offset, ZERO_CHUNK, 0, Math.min(ZERO_CHUNK.length, size - offset));
      }
    } else {
      for (long offset = 0; offset < sizeBytes; offset += Long.BYTES) {
        buffer.putLong(offset, 0L);
      }
    }
  }

  private static void closeBuffer(PinotDataBuffer buffer) {
    OffHeapGroupByBufferPool.release(buffer);
  }

  /// Murmur3 fmix64 finalizer. Bijective over longs, so a non-zero key always hashes deterministically, and the
  /// rehash on resize simply recomputes it.
  private static long mix(long key) {
    long h = key;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }

  /// Flyweight entry for [#iterator()]. The same instance is reused across `next()` calls.
  public static class Entry {
    public long _rawKey;
    public int _groupId;
  }
}
