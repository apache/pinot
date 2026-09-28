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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;


/// A single key's mutable column for an OPEN_STRUCT column: forward index (dictionary-encoded)
/// + presence bitmap tracking which docIds had this key set.
///
/// Single-writer during ingestion. The presence bitmap is a {@link ThreadSafeMutableRoaringBitmap},
/// so queries may read it concurrently with ingestion; the forward index, dictionary and inverted
/// index carry the same single-writer/multiple-readers contract.
public class MutableKeyColumn implements Closeable {
  private static final int DEFAULT_AVG_STRING_LENGTH = 32;
  /// Floor on the forward-index chunk size, used when a segment's capacity is small enough that
  /// the derived size would be smaller.
  private static final int MIN_ROWS_PER_CHUNK = 1000;
  /// Chunk count this sizing aims for over a segment's life, used to derive the chunk size from
  /// segment capacity. Chunks are allocated lazily but contiguously from docId 0, so total bytes
  /// for a key are governed by its highest docId, not by this value — raising it only splits the
  /// same footprint into more allocations. Each allocation costs a log line, an off-heap
  /// allocation, and a copy of the copy-on-write reader list (so the copying is quadratic in the
  /// chunk count). Deriving from capacity rather than using it outright (as schema columns do)
  /// keeps the floor for a rarely-seen key small: the OPEN_STRUCT key space is user-controlled, and
  /// mutable mode holds every observed key, since maxDenseKeys is applied at seal rather than
  /// during consumption. Above a capacity of MAX_ROWS_PER_CHUNK * MAX_CHUNKS_PER_KEY the ceiling
  /// takes over and the realised chunk count grows past this target.
  private static final int MAX_CHUNKS_PER_KEY = 256;
  /// Ceiling on the forward-index chunk size, and therefore on the off-heap a key reserves merely
  /// by being observed: [FixedByteSVMutableForwardIndex] allocates its first chunk eagerly in its
  /// constructor, so every key costs `numRowsPerChunk * Integer.BYTES` the moment it appears, even
  /// if it is only ever written at docId 0. That floor multiplies across a key space that is
  /// user-controlled and never pruned during consumption, so it must not scale with segment
  /// capacity. At 4000 rows it is 16 KB per observed key; the uncapped `capacity / 256` derivation
  /// reserved 78 KB per key at a 5M-row capacity, which a segment that met a few thousand rare keys
  /// would turn into hundreds of MB it never writes into.
  ///
  /// 4000 keeps most of what the capacity-derived size buys for keys that do reach high docIds: at
  /// a 5M-row capacity such a key allocates 1250 chunks, against 5000 under the flat 1000-row size
  /// this sizing replaced (a 16x drop in reader-list copying) and 256 under the uncapped
  /// derivation. Capacities at or below 1_024_000 are unaffected — their derived size is already
  /// under the ceiling.
  private static final int MAX_ROWS_PER_CHUNK = 4000;

  private final String _key;
  private final DataType _storedType;
  private final PinotDataType _destType;
  private final boolean _needsInferenceCheck;
  private final MutableForwardIndex _forwardIndex;
  private final ThreadSafeMutableRoaringBitmap _presenceBitmap;
  private final MutableDictionary _dictionary;
  private final RealtimeInvertedIndex _invertedIndex;

  /// Highest docId written for this key. Volatile store after the forward-index write publishes
  /// the row to query threads; docIds beyond the watermark may not have allocated chunks yet.
  private volatile int _lastIndexedDocId = -1;

  /// Whether any doc has explicitly written the reserved default value (dictId 0). Distinguishes
  /// a phantom dictionary entry (default reserved but never observed) from a real one.
  private volatile boolean _defaultObserved;

  private final ForwardIndexReader<ForwardIndexReaderContext> _guardedForwardIndex = new TailGuardedReader();

  public MutableKeyColumn(String key, DataType storedType, Object defaultNullValue,
      PinotDataBufferMemoryManager memoryManager, int capacity) {
    this(key, storedType, defaultNullValue, memoryManager, capacity, key, false);
  }

  public MutableKeyColumn(String key, DataType storedType, Object defaultNullValue,
      PinotDataBufferMemoryManager memoryManager, int capacity, String allocationContext,
      boolean needsInferenceCheck) {
    _key = key;
    _storedType = storedType;
    _needsInferenceCheck = needsInferenceCheck;
    _destType = ColumnDataType.fromDataTypeSV(storedType).toPinotDataType();
    _presenceBitmap = new ThreadSafeMutableRoaringBitmap();
    _invertedIndex = new RealtimeInvertedIndex();

    int estimatedCardinality = Math.max(capacity / 100, 16);
    int avgLength = storedType.isFixedWidth() ? storedType.size() : DEFAULT_AVG_STRING_LENGTH;
    _dictionary = MutableDictionaryFactory.getMutableDictionary(
        storedType, false, memoryManager, avgLength, estimatedCardinality,
        allocationContext + ".dict");
    // Reserve dictId 0 for the default null value. Forward-index chunks are zero-initialized, so
    // any doc where this key is absent reads dictId 0 and resolves to the default — the same
    // value a sealed segment folds in at build time for absent docs. Not marked present: the
    // presence bitmap only reflects explicit writes. Side effect: getDistinctValues() includes
    // the default even if never observed (seal-time cardinality estimate is +1; estimation only).
    _dictionary.index(defaultNullValue);
    // Mirror the reservation in the inverted index: slot 0 stays empty unless a doc explicitly
    // writes the default value, keeping dictIds and bitmap slots contiguous.
    _invertedIndex.reserveNextDictId();

    int numRowsPerChunk =
        Math.min(Math.max(MIN_ROWS_PER_CHUNK, capacity / MAX_CHUNKS_PER_KEY), MAX_ROWS_PER_CHUNK);
    _forwardIndex = new FixedByteSVMutableForwardIndex(true, DataType.INT,
        numRowsPerChunk, memoryManager, allocationContext + ".fwd");
  }

  public String getKey() {
    return _key;
  }

  public DataType getStoredType() {
    return _storedType;
  }

  public PinotDataType getDestType() {
    return _destType;
  }

  /// Whether a value on this key can ever produce a type-inference failure. True only for a key
  /// with no declared child spec whose stored type fell back to STRING; fixed at allocation, since
  /// neither the child spec nor the stored type changes afterwards. Lets the per-row path skip the
  /// inference call entirely for every other key.
  public boolean needsInferenceCheck() {
    return _needsInferenceCheck;
  }

  public MutableForwardIndex getForwardIndex() {
    return _forwardIndex;
  }

  /// Bitmap of docIds where this key was present (non-null).
  public ThreadSafeMutableRoaringBitmap getPresenceBitmap() {
    return _presenceBitmap;
  }

  /// Number of documents where this key had a non-null value.
  public int getNumNonNullDocs() {
    return _presenceBitmap.getCardinality();
  }

  /// Distinct values in this key's dictionary, for cardinality estimation at seal time.
  public Set<String> getDistinctValues() {
    int len = _dictionary.length();
    Set<String> result = new java.util.HashSet<>(len);
    for (int i = 0; i < len; i++) {
      Object val = _dictionary.get(i);
      result.add(val == null ? null : val.toString());
    }
    return result;
  }

  public MutableDictionary getDictionary() {
    return _dictionary;
  }

  public RealtimeInvertedIndex getInvertedIndex() {
    return _invertedIndex;
  }

  /// Highest docId written for this key, or -1 if never written.
  public int getLastIndexedDocId() {
    return _lastIndexedDocId;
  }

  /// Indexes `value` at `docId`. The value must already be coerced to the stored type.
  public void setValue(int docId, Object value) {
    _presenceBitmap.add(docId);
    int dictId = _dictionary.index(value);
    if (dictId == 0) {
      _defaultObserved = true;
    }
    _forwardIndex.setDictId(docId, dictId);
    _invertedIndex.add(dictId, docId);
    _lastIndexedDocId = docId;
  }

  /// Whether any doc has explicitly written the reserved default value (dictId 0), as opposed to
  /// the default being a phantom entry no doc actually carries.
  public boolean isDefaultObserved() {
    return _defaultObserved;
  }

  public Object getValue(int docId) {
    // The forward index returns whatever bit pattern is at this offset, even for docs that were
    // never written for this key. The presence bitmap is the source of truth — without this check,
    // an absent doc would deserialize as if it held the first dictionary entry (dictId 0).
    if (!_presenceBitmap.contains(docId)) {
      return null;
    }
    int dictId = _forwardIndex.getDictId(docId, null);
    if (dictId < 0 || dictId >= _dictionary.length()) {
      return null;
    }
    return _dictionary.get(dictId);
  }

  @Override
  public void close()
      throws IOException {
    _forwardIndex.close();
    _dictionary.close();
    _invertedIndex.close();
  }

  /// Read-side view of the forward index for query threads. The raw index only has chunks
  /// allocated up to the highest docId written for this key, but scans read every docId in
  /// `[0, numDocs)`; docIds past the watermark read as dictId 0 — the reserved default null
  /// value — matching what a sealed segment folds in at build time for absent docs. In-range
  /// holes need no guard: chunks are zero-initialized, so they already read dictId 0.
  public ForwardIndexReader<ForwardIndexReaderContext> getGuardedForwardIndex() {
    return _guardedForwardIndex;
  }

  private final class TailGuardedReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    @Override
    public boolean isDictionaryEncoded() {
      return true;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public DataType getStoredType() {
      return _forwardIndex.getStoredType();
    }

    @Override
    public int getDictId(int docId, ForwardIndexReaderContext context) {
      return docId <= _lastIndexedDocId ? _forwardIndex.getDictId(docId) : 0;
    }

    @Override
    public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
      int watermark = _lastIndexedDocId;
      if (length > 0 && docIds[length - 1] <= watermark) {
        // Callers (block-based scan/filter) pass docIds in ascending order within a block, so the
        // last element bounds the whole batch. This assumption is load-bearing: if it didn't hold
        // (e.g. an unsorted batch with its max docId in the middle), a docId past the watermark
        // could sit earlier in the array and be delegated to the raw index here without a guard.
        _forwardIndex.readDictIds(docIds, length, dictIdBuffer, null);
        return;
      }
      for (int i = 0; i < length; i++) {
        int docId = docIds[i];
        dictIdBuffer[i] = docId <= watermark ? _forwardIndex.getDictId(docId) : 0;
      }
    }

    @Override
    public void close() {
      // The raw forward index is owned and closed by MutableKeyColumn.
    }
  }
}
