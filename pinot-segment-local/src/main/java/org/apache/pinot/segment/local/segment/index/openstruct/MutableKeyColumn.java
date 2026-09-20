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
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
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
  private static final int DEFAULT_ROWS_PER_CHUNK = 1000;
  /// Chunk-sizing hint for a multi-value key, and a hard cap: FixedByteMVMutableForwardIndex rejects a row with
  /// more values than this. A key discovered during consumption has no declared bound, so this is the one the
  /// column is built with; a longer value is dropped and metered like any other value the column cannot hold.
  public static final int MAX_NUM_MULTI_VALUES = 1000;
  private static final int AVG_NUM_MULTI_VALUES = 4;

  private final String _key;
  private final DataType _storedType;
  private final PinotDataType _destType;
  private final boolean _needsInferenceCheck;
  private final MutableForwardIndex _forwardIndex;
  private final boolean _singleValue;
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
    this(key, storedType, defaultNullValue, memoryManager, capacity, key, false, true);
  }

  public MutableKeyColumn(String key, DataType storedType, Object defaultNullValue,
      PinotDataBufferMemoryManager memoryManager, int capacity, String allocationContext,
      boolean needsInferenceCheck) {
    this(key, storedType, defaultNullValue, memoryManager, capacity, allocationContext, needsInferenceCheck, true);
  }

  public MutableKeyColumn(String key, DataType storedType, Object defaultNullValue,
      PinotDataBufferMemoryManager memoryManager, int capacity, String allocationContext,
      boolean needsInferenceCheck, boolean singleValue) {
    _key = key;
    _storedType = storedType;
    _needsInferenceCheck = needsInferenceCheck;
    _singleValue = singleValue;
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

    _forwardIndex = singleValue
        ? new FixedByteSVMutableForwardIndex(true, DataType.INT, DEFAULT_ROWS_PER_CHUNK, memoryManager,
            allocationContext + ".fwd")
        : new FixedByteMVMutableForwardIndex(MAX_NUM_MULTI_VALUES, AVG_NUM_MULTI_VALUES, DEFAULT_ROWS_PER_CHUNK,
            Integer.BYTES, memoryManager, allocationContext + ".fwd", true, DataType.INT);
  }

  /// Whether this key holds one value per document or a list. Fixed at allocation from the first value the key
  /// presented, matching [OpenStructColumnSplitter]'s rule on the sealed side.
  public boolean isSingleValue() {
    return _singleValue;
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

  /// Indexes a list of values at `docId`. Elements must already be coerced to the stored type. Throws
  /// [IllegalArgumentException] when the list is longer than [#MAX_NUM_MULTI_VALUES]; the caller drops and meters
  /// it, the same as a value that cannot be coerced.
  public void setValues(int docId, Object[] values) {
    int[] dictIds = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      dictIds[i] = _dictionary.index(values[i]);
      if (dictIds[i] == 0) {
        _defaultObserved = true;
      }
    }
    // Before the presence bitmap, so a rejected row is not published as present.
    _forwardIndex.setDictIdMV(docId, dictIds);
    _presenceBitmap.add(docId);
    for (int dictId : dictIds) {
      _invertedIndex.add(dictId, docId);
    }
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
      return _singleValue;
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
    public int getDictIdMV(int docId, int[] dictIdBuffer, ForwardIndexReaderContext context) {
      if (docId > _lastIndexedDocId) {
        // Past the watermark the key has no row yet, which reads as the reserved default -- the same one value a
        // sealed segment folds in for an absent multi-value doc.
        dictIdBuffer[0] = 0;
        return 1;
      }
      return _forwardIndex.getDictIdMV(docId, dictIdBuffer);
    }

    @Override
    public int[] getDictIdMV(int docId, ForwardIndexReaderContext context) {
      if (docId > _lastIndexedDocId) {
        return new int[]{0};
      }
      return _forwardIndex.getDictIdMV(docId);
    }

    @Override
    public void close() {
      // The raw forward index is owned and closed by MutableKeyColumn.
    }
  }
}
