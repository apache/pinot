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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Off-heap implementation of [GroupByResultHolder] for `DISTINCT_COUNT_ULL` group-by state. Each group's
/// [UltraLogLog] register array (`2^p` bytes) lives in direct memory instead of a per-group heap object, so a
/// group-by with many groups carries no per-group heap state and no GC pressure from the sketches.
///
/// ### Storage layout
/// Slots are assigned append-only on first touch through a `groupKey -> slotId` indirection, so direct memory
/// grows with the number of groups actually seen (like the on-heap holder's lazy per-group allocation), never
/// with the group-count upper bound. Slots live in fixed-size chunks acquired from [OffHeapGroupByBufferPool];
/// chunks are never resized or moved, so slot addresses are stable. An all-zero slot is exactly the state of an
/// empty [UltraLogLog], so slots are zero-filled on assignment.
///
/// ### Register update math
/// [#add(int, long)] applies hash4j's `UltraLogLog.add(long)` register update (including `pack`/`unpack`)
/// vendored verbatim from hash4j 0.30.0 (Apache License 2.0), since [UltraLogLog] is final and only operates on
/// heap `byte[]` state. `OffHeapUltraLogLogGroupByResultHolderTest` pins the vendored math byte-identical to the
/// library across precisions.
///
/// ### Modes
/// The owning aggregation function stores different state types depending on the input column: raw values hash
/// into ULL registers (off-heap slots, via [#touch(int)] / [#add(int, long)]), while dictionary-encoded columns
/// and pre-serialized ULL BYTES columns keep per-group heap objects through the generic
/// [#getResult(int)] / [#setValueForKey(int, Object)] API — those are routed to a lazily-created on-heap
/// [ObjectGroupByResultHolder] delegate. A segment uses exactly one mode per holder (one column encoding per
/// segment); this is asserted, not branched per row. [#getResult(int)] in slot mode materializes a fresh heap
/// [UltraLogLog] copy of the slot (extraction-time only), and returns `null` for untouched groups to match the
/// on-heap holder.
///
/// [#close()] releases the direct memory and is idempotent; the behavior of all other methods after close is
/// undefined. This class is single-threaded and not thread-safe.
@NotThreadSafe
public class OffHeapUltraLogLogGroupByResultHolder implements GroupByResultHolder, AutoCloseable {
  // Matches OffHeapBytesGroupIdMap's chunk size; slots of this size and larger (p >= 18) get one slot per chunk
  private static final int TARGET_CHUNK_BYTES = 256 * 1024;
  private static final String BUFFER_DESCRIPTION = "OffHeapUltraLogLogGroupByResultHolder";

  private final int _p;
  private final int _slotBytes;
  private final int _slotsPerChunkShift;
  private final int _slotIndexMask;
  private final long _chunkBytes;
  // q = 64 - p, hoisted for the vendored register update (see hash4j UltraLogLog.add(long))
  private final int _q;
  private final int _maxCapacity;

  private int _resultHolderCapacity;
  // groupKey -> slotId; -1 = group never touched (getResult returns null, matching the on-heap holder)
  private int[] _slotIds;
  private int _numSlots;
  private List<PinotDataBuffer> _chunkBuffers = new ArrayList<>();
  // Absolute-indexed direct views of the chunks for the per-row hot path; a null element means the view limit
  // was exceeded (test hook) and accesses fall back to the PinotDataBuffer wrapper
  private List<ByteBuffer> _chunkViews = new ArrayList<>();
  private ObjectGroupByResultHolder _delegate;
  private boolean _closed;

  /// Constructor for the class.
  ///
  /// @param p UltraLogLog precision parameter (slot size is `2^p` bytes)
  /// @param initialCapacity Initial capacity of the result holder
  /// @param maxCapacity Maximum capacity of the result holder
  public OffHeapUltraLogLogGroupByResultHolder(int p, int initialCapacity, int maxCapacity) {
    // The aggregation function validates p at plan time; re-check here because p sizes the direct-memory slots
    // (1 << p) without going through UltraLogLog.create's own bound check, and an out-of-range p would either
    // allocate absurd chunks (p up to 30) or let register indexes walk outside the slot (p > 30, int-shift wrap)
    Preconditions.checkArgument(p >= 3 && p <= 26, "Invalid UltraLogLog p: %s, must be in [3, 26]", p);
    _p = p;
    _slotBytes = 1 << p;
    int slotsPerChunk = Math.max(1, TARGET_CHUNK_BYTES / _slotBytes);
    _slotsPerChunkShift = Integer.numberOfTrailingZeros(slotsPerChunk);
    _slotIndexMask = slotsPerChunk - 1;
    _chunkBytes = (long) slotsPerChunk * _slotBytes;
    _q = 64 - p;
    _maxCapacity = maxCapacity;

    _resultHolderCapacity = initialCapacity;
    _slotIds = new int[initialCapacity];
    Arrays.fill(_slotIds, GroupKeyGenerator.INVALID_ID);
  }

  @Override
  public void ensureCapacity(int capacity) {
    Preconditions.checkArgument(capacity <= _maxCapacity);

    if (capacity > _resultHolderCapacity) {
      int copyLength = _resultHolderCapacity;
      int newCapacity = Math.min(Math.max(_resultHolderCapacity * 2, capacity), _maxCapacity);
      // _slotIds is null in delegate mode (the delegate tracks its own capacity)
      if (_slotIds != null) {
        _slotIds = Arrays.copyOf(_slotIds, newCapacity);
        Arrays.fill(_slotIds, copyLength, newCapacity, GroupKeyGenerator.INVALID_ID);
      }
      _resultHolderCapacity = newCapacity;
    }
    if (_delegate != null) {
      _delegate.ensureCapacity(capacity);
    }
  }

  /// Ensures the group has an (all-zero) slot, mirroring the on-heap path's eager `UltraLogLog.create(p)` on
  /// first access so an untouched-vs-empty distinction never diverges between the two modes.
  public void touch(int groupKey) {
    if (groupKey != GroupKeyGenerator.INVALID_ID) {
      slotIdFor(groupKey);
    }
  }

  /// Adds a 64-bit hash value into the group's off-heap ULL registers. Vendored verbatim from hash4j 0.30.0
  /// `UltraLogLog.add(long)` with the state array replaced by the group's slot.
  public void add(int groupKey, long hashValue) {
    if (groupKey == GroupKeyGenerator.INVALID_ID) {
      return;
    }
    int slotId = slotIdFor(groupKey);
    int chunkIndex = slotId >>> _slotsPerChunkShift;
    int slotOffset = (slotId & _slotIndexMask) * _slotBytes;
    int idx = (int) (hashValue >>> _q);
    int nlz = Long.numberOfLeadingZeros(~(~hashValue << -_q)); // nlz in {0, 1, ..., 64-p}
    int registerOffset = slotOffset + idx;
    ByteBuffer view = _chunkViews.get(chunkIndex);
    if (view != null) {
      byte oldRegister = view.get(registerOffset);
      long hashPrefix = unpack(oldRegister) | (1L << (nlz + ~_q)); // (nlz + ~q) = (nlz + p - 1) mod 64
      view.put(registerOffset, pack(hashPrefix));
    } else {
      PinotDataBuffer chunk = _chunkBuffers.get(chunkIndex);
      byte oldRegister = chunk.getByte(registerOffset);
      long hashPrefix = unpack(oldRegister) | (1L << (nlz + ~_q));
      chunk.putByte(registerOffset, pack(hashPrefix));
    }
  }

  @Override
  public double getDoubleResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  /// In delegate mode, returns the delegate's live per-group object. In slot mode, returns a **snapshot**: a
  /// fresh heap [UltraLogLog] copy of the slot, so mutating the returned object does NOT update the slot. Slot
  /// mode is therefore only correct for extraction-style reads; per-row read-modify-write callers (the
  /// pre-serialized-BYTES merge path) always run in delegate mode because their first write goes through
  /// [#setValueForKey(int, Object)].
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getResult(int groupKey) {
    if (_delegate != null) {
      return _delegate.getResult(groupKey);
    }
    if (groupKey == GroupKeyGenerator.INVALID_ID) {
      return null;
    }
    // See OffHeapLongGroupByResultHolder: unchecked buffer access means an out-of-range key would read arbitrary
    // memory instead of throwing, so guard the sizing contract with an assert
    assert groupKey >= 0 && groupKey < _resultHolderCapacity : "groupKey " + groupKey + " out of bounds";
    int slotId = _slotIds[groupKey];
    if (slotId < 0) {
      return null;
    }
    int chunkIndex = slotId >>> _slotsPerChunkShift;
    int slotOffset = (slotId & _slotIndexMask) * _slotBytes;
    byte[] state = new byte[_slotBytes];
    ByteBuffer view = _chunkViews.get(chunkIndex);
    if (view != null) {
      view.get(slotOffset, state);
    } else {
      _chunkBuffers.get(chunkIndex).copyTo(slotOffset, state, 0, _slotBytes);
    }
    return (T) UltraLogLog.wrap(state);
  }

  @Override
  public void setValueForKey(int groupKey, double newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, int newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, long newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    if (groupKey == GroupKeyGenerator.INVALID_ID) {
      return;
    }
    // Heap-object mode (dictionary id wrappers, pre-serialized ULL merges): route to the on-heap delegate. A
    // segment uses exactly one mode per holder; a violation would silently drop state (getResult prefers the
    // delegate), so enforce it hard — this runs once per group, not per row.
    Preconditions.checkState(_numSlots == 0,
        "Off-heap ULL slots and heap-object delegate used on the same holder");
    if (_delegate == null) {
      _delegate = new ObjectGroupByResultHolder(_resultHolderCapacity, _maxCapacity);
      // Delegate mode never uses the slot indirection; nulling it frees 4 bytes/group and turns any stray slot
      // access into a loud NPE instead of a silent divergence
      _slotIds = null;
    }
    _delegate.setValueForKey(groupKey, newValue);
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    try {
      RuntimeException firstFailure = null;
      for (PinotDataBuffer chunk : _chunkBuffers) {
        // Release every chunk even if one release fails, matching ResourceTrackingGroupKeyGenerator's policy
        try {
          OffHeapGroupByBufferPool.release(chunk);
        } catch (RuntimeException e) {
          if (firstFailure == null) {
            firstFailure = e;
          }
        }
      }
      if (firstFailure != null) {
        throw firstFailure;
      }
    } finally {
      // Null the buffers and views so any use-after-close (or a second release of a pooled buffer) fails loudly
      // with an NPE instead of silently aliasing memory that the pool may have handed to another query
      _chunkBuffers = null;
      _chunkViews = null;
      _slotIds = null;
      _delegate = null;
    }
  }

  private int slotIdFor(int groupKey) {
    // See getResult: unchecked buffer access means an out-of-range key would corrupt memory, not throw
    assert groupKey >= 0 && groupKey < _resultHolderCapacity : "groupKey " + groupKey + " out of bounds";
    assert _delegate == null : "off-heap ULL slots and heap-object delegate used on the same holder";
    int slotId = _slotIds[groupKey];
    if (slotId >= 0) {
      return slotId;
    }
    slotId = _numSlots++;
    _slotIds[groupKey] = slotId;
    int chunkIndex = slotId >>> _slotsPerChunkShift;
    if (chunkIndex == _chunkBuffers.size()) {
      PinotDataBuffer chunk = OffHeapGroupByBufferPool.acquire(_chunkBytes, BUFFER_DESCRIPTION);
      _chunkBuffers.add(chunk);
      _chunkViews.add(OffHeapGroupByUtils.createView(chunk, _chunkBytes));
    }
    // Pooled buffers come back dirty; an all-zero slot is exactly an empty UltraLogLog state
    int slotOffset = (slotId & _slotIndexMask) * _slotBytes;
    ByteBuffer view = _chunkViews.get(chunkIndex);
    if (view != null) {
      for (int i = 0; i < _slotBytes; i += Long.BYTES) {
        view.putLong(slotOffset + i, 0L);
      }
    } else {
      PinotDataBuffer chunk = _chunkBuffers.get(chunkIndex);
      for (int i = 0; i < _slotBytes; i += Long.BYTES) {
        chunk.putLong(slotOffset + i, 0L);
      }
    }
    return slotId;
  }

  // pack/unpack vendored verbatim from hash4j 0.30.0 UltraLogLog (Apache License 2.0)
  private static long unpack(byte register) {
    return (4L | (register & 3)) << ((register >>> 2) - 2);
  }

  private static byte pack(long hashPrefix) {
    int nlz = Long.numberOfLeadingZeros(hashPrefix) + 1;
    return (byte) ((-nlz << 2) | ((hashPrefix << nlz) >>> 62));
  }
}
