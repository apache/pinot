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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Off-heap hash map from arbitrary `byte[]` keys to dense int group ids, used as a replacement for
/// `Object2IntOpenHashMap` in group-by key generation.
///
/// The design is modeled on DuckDB's two-part aggregate hash table:
/// <ul>
///   <li><b>Directory</b>: an open-addressing table of 8-byte entries (power-of-two slot count, load factor 0.5,
///   linear probing). An entry of `0` means empty; otherwise the top 16 bits hold a salt (the top 16 bits of
///   the key's 64-bit hash) and the low 48 bits hold `payloadGlobalOffset + 1`. The `+1` keeps payload
///   offset 0 distinguishable from an empty slot. The salt is compared before touching the payload, so most probe
///   misses stay within the directory.</li>
///   <li><b>Payload</b>: append-only chunks (256KB each) storing one record per key:
///   `[long hash][int groupId][int keyLength][key bytes]` (16-byte header). Records never span chunks: a
///   record that does not fit in the remaining space of the current chunk starts a new chunk, and a record larger
///   than the normal chunk size gets a dedicated chunk of exactly the record size (placed at offset 0, with the
///   next record starting a fresh normal chunk). Because the 64-bit hash is stored in the payload, directory resize
///   never rehashes or compares key bytes.</li>
///   <li><b>Id index</b>: a growable off-heap long array mapping group id to payload global offset, supporting
///   dense-id reverse lookup ([#getKey], [#readKey], [#getKeyLength]) without an entry
///   iterator: ids are dense `0..size()-1` assigned in insertion order.</li>
/// </ul>
///
/// All off-heap memory is allocated through [PinotDataBuffer#allocateDirect] and released by
/// [#close()], which is idempotent. Behavior of all other methods after `close()` is undefined.
///
/// This class is <b>not thread-safe</b>: it is a per-query scratch structure intended to be used from a single
/// thread.
@NotThreadSafe
public class OffHeapBytesGroupIdMap implements AutoCloseable {
  // Normal payload chunk size. Records never span chunks, and a record larger than this gets a dedicated chunk.
  @VisibleForTesting
  static final int CHUNK_SIZE = 1 << 18;

  private static final int MIN_NUM_SLOTS = 1024;
  private static final int MAX_NUM_SLOTS = 1 << 30;
  // Directory entry: 0 == empty; else (salt << 48) | (payloadGlobalOffset + 1).
  private static final long SALT_MASK = 0xFFFF000000000000L;
  private static final long OFFSET_MASK = 0x0000FFFFFFFFFFFFL;
  // Payload record layout: [long hash][int groupId][int keyLength][key bytes].
  private static final int RECORD_HEADER_SIZE = 16;
  private static final int GROUP_ID_OFFSET = 8;
  private static final int KEY_LENGTH_OFFSET = 12;
  private static final long MURMUR3_SEED = 0x9747b28cL;
  private static final String DIRECTORY_DESCRIPTION = "OffHeapBytesGroupIdMap: directory";
  private static final String ID_INDEX_DESCRIPTION = "OffHeapBytesGroupIdMap: id index";
  private static final String CHUNK_DESCRIPTION = "OffHeapBytesGroupIdMap: payload chunk";
  // Reusable zero block for bulk zero-filling freshly allocated directories through the direct view
  private static final byte[] ZERO_CHUNK = new byte[8192];

  private final List<PinotDataBuffer> _chunks = new ArrayList<>();
  // Absolute-indexed direct views for the per-row hot path (monomorphic, intrinsified ByteBuffer access instead
  // of the PinotDataBuffer wrapper); a view is null when its buffer exceeds the 2GB view limit
  private final List<ByteBuffer> _chunkViews = new ArrayList<>();

  private ByteBuffer _directoryView;
  private PinotDataBuffer _directory;
  private ByteBuffer _idIndexView;
  private PinotDataBuffer _idIndex;
  private int _numSlots;
  private int _size;
  // Chunk currently accepting appends; null until the first normal-size record, and reset to null after an
  // oversized record so that no record is ever appended after one.
  private PinotDataBuffer _currentChunk;
  private int _currentChunkIndex;
  private int _currentChunkOffset;
  // Reusable scratch for bulk key comparison in matchRecord (grown on demand, contents transient)
  private byte[] _compareScratch = new byte[64];
  private boolean _closed;

  public OffHeapBytesGroupIdMap(int expectedNumEntries) {
    Preconditions.checkArgument(expectedNumEntries >= 0, "Invalid expectedNumEntries: %s", expectedNumEntries);
    _numSlots = computeNumSlots(expectedNumEntries);
    _directory = allocateZeroFilledDirectory(_numSlots);
    _directoryView = OffHeapGroupByUtils.createView(_directory, (long) _numSlots << 3);
    try {
      // Sized to the directory's max fill (load factor 0.5) so both grow together.
      _idIndex = OffHeapGroupByBufferPool.acquire((long) (_numSlots >> 1) << 3, ID_INDEX_DESCRIPTION);
      _idIndexView = OffHeapGroupByUtils.createView(_idIndex, (long) (_numSlots >> 1) << 3);
    } catch (Throwable t) {
      closeBuffer(_directory);
      if (_idIndex != null) {
        closeBuffer(_idIndex);
      }
      throw t;
    }
  }

  /// Returns the dense group id for the given key:
  /// <ul>
  ///   <li>If the key is already present, always returns its id (even when `size() == groupIdUpperBound`).</li>
  ///   <li>If the key is absent and `size() < groupIdUpperBound`, assigns the next dense id
  ///   (`size()`), inserts the key and returns the id.</li>
  ///   <li>If the key is absent and `size() >= groupIdUpperBound`, returns
  ///   [GroupKeyGenerator#INVALID_ID] without inserting.</li>
  /// </ul>
  public int getGroupId(byte[] key, int offset, int length, int groupIdUpperBound) {
    long hash = murmurHash3X64Bit64(key, offset, length);
    int mask = _numSlots - 1;
    int slot = (int) (hash & mask);
    long saltBits = hash & SALT_MASK;
    ByteBuffer directoryView = _directoryView;
    while (true) {
      // While the view exists, slot offsets fit in an int (view size <= Integer.MAX_VALUE)
      long entry = directoryView != null ? directoryView.getLong(slot << 3) : _directory.getLong((long) slot << 3);
      if (entry == 0L) {
        if (_size >= groupIdUpperBound) {
          return GroupKeyGenerator.INVALID_ID;
        }
        return insert(hash, key, offset, length, slot);
      }
      if ((entry & SALT_MASK) == saltBits) {
        int groupId = matchRecord((entry & OFFSET_MASK) - 1, hash, key, offset, length);
        if (groupId != GroupKeyGenerator.INVALID_ID) {
          return groupId;
        }
      }
      slot = (slot + 1) & mask;
    }
  }

  /// Convenience variant of [int, int, int)][#getGroupId(byte[],] covering the full key array.
  public int getGroupId(byte[] key, int groupIdUpperBound) {
    return getGroupId(key, 0, key.length, groupIdUpperBound);
  }

  /// Returns the number of keys in the map. Group ids are dense: `0..size()-1`.
  public int size() {
    return _size;
  }

  /// Returns the length in bytes of the key with the given group id. The id must be within `[0, size())`.
  public int getKeyLength(int groupId) {
    // The id index is unchecked PinotDataBuffer memory: guard the dense-id contract with an assert
    assert groupId >= 0 && groupId < _size : "groupId " + groupId + " out of bounds";
    long globalOffset = _idIndex.getLong((long) groupId << 3);
    return _chunks.get((int) (globalOffset / CHUNK_SIZE)).getInt((globalOffset % CHUNK_SIZE) + KEY_LENGTH_OFFSET);
  }

  /// Copies the key with the given group id into `dest` at `destOffset`. The id must be within
  /// `[0, size())`, and `dest` must have at least [#getKeyLength(int)] bytes of room.
  public void readKey(int groupId, byte[] dest, int destOffset) {
    // See getKeyLength: unchecked buffer access, so guard the dense-id contract with an assert
    assert groupId >= 0 && groupId < _size : "groupId " + groupId + " out of bounds";
    long globalOffset = _idIndex.getLong((long) groupId << 3);
    PinotDataBuffer chunk = _chunks.get((int) (globalOffset / CHUNK_SIZE));
    long offsetInChunk = globalOffset % CHUNK_SIZE;
    int keyLength = chunk.getInt(offsetInChunk + KEY_LENGTH_OFFSET);
    chunk.copyTo(offsetInChunk + RECORD_HEADER_SIZE, dest, destOffset, keyLength);
  }

  /// Returns a copy of the key with the given group id. The id must be within `[0, size())`.
  public byte[] getKey(int groupId) {
    byte[] key = new byte[getKeyLength(groupId)];
    readKey(groupId, key, 0);
    return key;
  }

  /// Returns the total off-heap memory held by the map (directory + id index + payload chunks).
  public long getOffHeapMemoryBytes() {
    long bytes = _directory.size() + _idIndex.size();
    for (PinotDataBuffer chunk : _chunks) {
      bytes += chunk.size();
    }
    return bytes;
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    RuntimeException firstException = closeBufferQuietly(_directory, null);
    firstException = closeBufferQuietly(_idIndex, firstException);
    for (PinotDataBuffer chunk : _chunks) {
      firstException = closeBufferQuietly(chunk, firstException);
    }
    // Null every buffer and view so any use-after-close (or a second release of a pooled buffer) fails loudly
    // with an NPE instead of silently aliasing memory that the pool may have handed to another query
    _directory = null;
    _directoryView = null;
    _idIndex = null;
    _idIndexView = null;
    _chunks.clear();
    _chunkViews.clear();
    _currentChunk = null;
    if (firstException != null) {
      throw firstException;
    }
  }

  @VisibleForTesting
  long getPayloadGlobalOffset(int groupId) {
    ByteBuffer idIndexView = _idIndexView;
    return idIndexView != null ? idIndexView.getLong(groupId << 3) : _idIndex.getLong((long) groupId << 3);
  }

  /// Compares the key against the payload record at the given global offset, and returns the record's group id on a
  /// full match (stored hash, key length, key bytes), or [GroupKeyGenerator#INVALID_ID] on a mismatch.
  private int matchRecord(long globalOffset, long hash, byte[] key, int offset, int length) {
    int chunkIndex = (int) (globalOffset / CHUNK_SIZE);
    ByteBuffer chunkView = _chunkViews.get(chunkIndex);
    if (chunkView == null) {
      return matchRecordSlow(chunkIndex, globalOffset % CHUNK_SIZE, hash, key, offset, length);
    }
    // Record start offsets within a chunk are always < CHUNK_SIZE, so they fit in an int
    int offsetInChunk = (int) (globalOffset % CHUNK_SIZE);
    if (chunkView.getLong(offsetInChunk) != hash || chunkView.getInt(offsetInChunk + KEY_LENGTH_OFFSET) != length) {
      return GroupKeyGenerator.INVALID_ID;
    }
    if (length > 0) {
      // Bulk copy + range equals instead of per-byte buffer reads: this runs on every successful lookup (the
      // dominant case — every row after the first per group), and both the bulk get and Arrays.equals are
      // intrinsified. The absolute bulk get does not touch the view's position.
      byte[] scratch = _compareScratch;
      if (scratch.length < length) {
        scratch = new byte[Math.max(length, scratch.length << 1)];
        _compareScratch = scratch;
      }
      chunkView.get(offsetInChunk + RECORD_HEADER_SIZE, scratch, 0, length);
      if (!Arrays.equals(scratch, 0, length, key, offset, offset + length)) {
        return GroupKeyGenerator.INVALID_ID;
      }
    }
    return chunkView.getInt(offsetInChunk + GROUP_ID_OFFSET);
  }

  /// Wrapper-based fallback of [#matchRecord] for the rare chunk without a direct view (larger than 2GB).
  private int matchRecordSlow(int chunkIndex, long offsetInChunk, long hash, byte[] key, int offset, int length) {
    PinotDataBuffer chunk = _chunks.get(chunkIndex);
    if (chunk.getLong(offsetInChunk) != hash || chunk.getInt(offsetInChunk + KEY_LENGTH_OFFSET) != length) {
      return GroupKeyGenerator.INVALID_ID;
    }
    if (length > 0) {
      byte[] scratch = _compareScratch;
      if (scratch.length < length) {
        scratch = new byte[Math.max(length, scratch.length << 1)];
        _compareScratch = scratch;
      }
      chunk.copyTo(offsetInChunk + RECORD_HEADER_SIZE, scratch, 0, length);
      if (!Arrays.equals(scratch, 0, length, key, offset, offset + length)) {
        return GroupKeyGenerator.INVALID_ID;
      }
    }
    return chunk.getInt(offsetInChunk + GROUP_ID_OFFSET);
  }

  /// Inserts a new key: appends the payload record, records it in the id index, writes the directory entry into the
  /// given empty slot, and resizes the directory when it reaches the 0.5 load factor.
  private int insert(long hash, byte[] key, int offset, int length, int slot) {
    int groupId = _size;
    long globalOffset = appendRecord(hash, groupId, key, offset, length);
    if (((long) groupId << 3) == _idIndex.size()) {
      growIdIndex();
    }
    ByteBuffer idIndexView = _idIndexView;
    if (idIndexView != null) {
      idIndexView.putLong(groupId << 3, globalOffset);
    } else {
      _idIndex.putLong((long) groupId << 3, globalOffset);
    }
    _directory.putLong((long) slot << 3, (hash & SALT_MASK) | (globalOffset + 1));
    _size++;
    if (_size >= (_numSlots >> 1)) {
      resizeDirectory();
    }
    return groupId;
  }

  /// Appends a payload record and returns its global offset (`chunkIndex * CHUNK_SIZE + offsetInChunk`).
  private long appendRecord(long hash, int groupId, byte[] key, int offset, int length) {
    long recordSize = RECORD_HEADER_SIZE + (long) length;
    PinotDataBuffer chunk;
    int chunkIndex;
    int recordStart;
    if (recordSize > CHUNK_SIZE) {
      // Oversized record: dedicated chunk of exactly the record size, record at offset 0. The next record starts
      // a fresh normal chunk, so global offset decoding (division by CHUNK_SIZE) stays valid.
      chunk = allocateChunk(recordSize);
      chunkIndex = _chunks.size() - 1;
      recordStart = 0;
      _currentChunk = null;
    } else {
      if (_currentChunk == null || CHUNK_SIZE - _currentChunkOffset < recordSize) {
        _currentChunk = allocateChunk(CHUNK_SIZE);
        _currentChunkIndex = _chunks.size() - 1;
        _currentChunkOffset = 0;
      }
      chunk = _currentChunk;
      chunkIndex = _currentChunkIndex;
      recordStart = _currentChunkOffset;
      _currentChunkOffset += (int) recordSize;
    }
    // Record start offsets within a chunk are always < CHUNK_SIZE, which the global offset encoding relies on.
    assert recordStart < CHUNK_SIZE;
    chunk.putLong(recordStart, hash);
    chunk.putInt((long) recordStart + GROUP_ID_OFFSET, groupId);
    chunk.putInt((long) recordStart + KEY_LENGTH_OFFSET, length);
    if (length > 0) {
      chunk.readFrom((long) recordStart + RECORD_HEADER_SIZE, key, offset, length);
    }
    return (long) chunkIndex * CHUNK_SIZE + recordStart;
  }

  private PinotDataBuffer allocateChunk(long sizeBytes) {
    PinotDataBuffer chunk = OffHeapGroupByBufferPool.acquire(sizeBytes, CHUNK_DESCRIPTION);
    _chunks.add(chunk);
    _chunkViews.add(OffHeapGroupByUtils.createView(chunk, sizeBytes));
    return chunk;
  }

  /// Doubles the directory. Only the hash stored in each payload record is re-read (via the id index) to recompute
  /// the slot and salt; key bytes are never touched.
  private void resizeDirectory() {
    int newNumSlots = _numSlots << 1;
    Preconditions.checkState(newNumSlots > 0 && newNumSlots <= MAX_NUM_SLOTS, "Cannot grow directory beyond %s slots",
        MAX_NUM_SLOTS);
    PinotDataBuffer newDirectory = allocateZeroFilledDirectory(newNumSlots);
    ByteBuffer newDirectoryView = OffHeapGroupByUtils.createView(newDirectory, (long) newNumSlots << 3);
    int newMask = newNumSlots - 1;
    for (int groupId = 0; groupId < _size; groupId++) {
      long globalOffset = getPayloadGlobalOffset(groupId);
      int chunkIndex = (int) (globalOffset / CHUNK_SIZE);
      ByteBuffer chunkView = _chunkViews.get(chunkIndex);
      long hash = chunkView != null ? chunkView.getLong((int) (globalOffset % CHUNK_SIZE))
          : _chunks.get(chunkIndex).getLong(globalOffset % CHUNK_SIZE);
      int slot = (int) (hash & newMask);
      if (newDirectoryView != null) {
        while (newDirectoryView.getLong(slot << 3) != 0L) {
          slot = (slot + 1) & newMask;
        }
        newDirectoryView.putLong(slot << 3, (hash & SALT_MASK) | (globalOffset + 1));
      } else {
        while (newDirectory.getLong((long) slot << 3) != 0L) {
          slot = (slot + 1) & newMask;
        }
        newDirectory.putLong((long) slot << 3, (hash & SALT_MASK) | (globalOffset + 1));
      }
    }
    closeBuffer(_directory);
    _directory = newDirectory;
    _directoryView = newDirectoryView;
    _numSlots = newNumSlots;
  }

  private void growIdIndex() {
    long oldSizeBytes = _idIndex.size();
    PinotDataBuffer newIdIndex = OffHeapGroupByBufferPool.acquire(oldSizeBytes << 1, ID_INDEX_DESCRIPTION);
    _idIndex.copyTo(0, newIdIndex, 0, oldSizeBytes);
    closeBuffer(_idIndex);
    _idIndex = newIdIndex;
    _idIndexView = OffHeapGroupByUtils.createView(newIdIndex, oldSizeBytes << 1);
  }

  private static PinotDataBuffer allocateZeroFilledDirectory(int numSlots) {
    long sizeBytes = (long) numSlots << 3;
    PinotDataBuffer directory = OffHeapGroupByBufferPool.acquire(sizeBytes, DIRECTORY_DESCRIPTION);
    // Contents of allocateDirect are undefined, and 0 means an empty slot, so zero-fill explicitly (bulk puts
    // through the direct view when available)
    ByteBuffer view = OffHeapGroupByUtils.createView(directory, sizeBytes);
    if (view != null) {
      int size = (int) sizeBytes;
      for (int offset = 0; offset < size; offset += ZERO_CHUNK.length) {
        view.put(offset, ZERO_CHUNK, 0, Math.min(ZERO_CHUNK.length, size - offset));
      }
    } else {
      for (long offset = 0; offset < sizeBytes; offset += 8) {
        directory.putLong(offset, 0L);
      }
    }
    return directory;
  }

  private static int computeNumSlots(int expectedNumEntries) {
    long target = Math.max(MIN_NUM_SLOTS, 2L * expectedNumEntries);
    long numSlots = Long.highestOneBit(target);
    if (numSlots < target) {
      numSlots <<= 1;
    }
    return (int) Math.min(numSlots, MAX_NUM_SLOTS);
  }

  private static void closeBuffer(PinotDataBuffer buffer) {
    OffHeapGroupByBufferPool.release(buffer);
  }

  private static RuntimeException closeBufferQuietly(PinotDataBuffer buffer, RuntimeException firstException) {
    try {
      closeBuffer(buffer);
      return firstException;
    } catch (RuntimeException e) {
      return firstException != null ? firstException : e;
    }
  }

  /// Standard MurmurHash3 x64 128 (Austin Appleby), returning the low 64 bits (`h1`). Implemented privately
  /// because `MurmurHashFunctions` has no 64-bit variant accepting `(byte[], offset, length)`.
  /// Deterministic within a process, which is all this per-query scratch structure needs.
  private static long murmurHash3X64Bit64(byte[] data, int offset, int length) {
    final long c1 = 0x87c37b91114253d5L;
    final long c2 = 0x4cf5ad432745937fL;
    long h1 = MURMUR3_SEED;
    long h2 = MURMUR3_SEED;
    int end = offset + (length & ~15);
    for (int i = offset; i < end; i += 16) {
      long k1 = getLongLittleEndian(data, i);
      long k2 = getLongLittleEndian(data, i + 8);
      k1 *= c1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= c2;
      h1 ^= k1;
      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729L;
      k2 *= c2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5L;
    }
    long k1 = 0;
    long k2 = 0;
    // CHECKSTYLE:OFF: checkstyle:coding
    switch (length & 15) {
      case 15:
        k2 ^= (data[end + 14] & 0xffL) << 48;
      case 14:
        k2 ^= (data[end + 13] & 0xffL) << 40;
      case 13:
        k2 ^= (data[end + 12] & 0xffL) << 32;
      case 12:
        k2 ^= (data[end + 11] & 0xffL) << 24;
      case 11:
        k2 ^= (data[end + 10] & 0xffL) << 16;
      case 10:
        k2 ^= (data[end + 9] & 0xffL) << 8;
      case 9:
        k2 ^= data[end + 8] & 0xffL;
        k2 *= c2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= c1;
        h2 ^= k2;
      case 8:
        k1 ^= (data[end + 7] & 0xffL) << 56;
      case 7:
        k1 ^= (data[end + 6] & 0xffL) << 48;
      case 6:
        k1 ^= (data[end + 5] & 0xffL) << 40;
      case 5:
        k1 ^= (data[end + 4] & 0xffL) << 32;
      case 4:
        k1 ^= (data[end + 3] & 0xffL) << 24;
      case 3:
        k1 ^= (data[end + 2] & 0xffL) << 16;
      case 2:
        k1 ^= (data[end + 1] & 0xffL) << 8;
      case 1:
        k1 ^= data[end] & 0xffL;
        k1 *= c1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }
    // CHECKSTYLE:ON: checkstyle:coding
    h1 ^= length;
    h2 ^= length;
    h1 += h2;
    h2 += h1;
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 += h2;
    return h1;
  }

  private static long getLongLittleEndian(byte[] data, int offset) {
    return (data[offset] & 0xffL) | ((data[offset + 1] & 0xffL) << 8) | ((data[offset + 2] & 0xffL) << 16) | (
        (data[offset + 3] & 0xffL) << 24) | ((data[offset + 4] & 0xffL) << 32) | ((data[offset + 5] & 0xffL) << 40) | (
        (data[offset + 6] & 0xffL) << 48) | ((data[offset + 7] & 0xffL) << 56);
  }

  private static long fmix64(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }
}
