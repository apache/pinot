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
import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.PinotDataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// A single key's mutable column for an OPEN_STRUCT column: forward index (dictionary-encoded)
/// + presence bitmap tracking which docIds had this key set.
///
/// Single-writer during ingestion; presence bitmap and forward index are not thread-safe for
/// concurrent writes.
public class MutableKeyColumn implements Closeable {
  private static final int DEFAULT_AVG_STRING_LENGTH = 32;
  private static final int DEFAULT_ROWS_PER_CHUNK = 1000;

  private final String _key;
  private final DataType _storedType;
  private final PinotDataType _destType;
  private final MutableForwardIndex _forwardIndex;
  private final MutableRoaringBitmap _presenceBitmap;
  private final MutableDictionary _dictionary;
  private final RealtimeInvertedIndex _invertedIndex;

  public MutableKeyColumn(String key, DataType storedType, PinotDataBufferMemoryManager memoryManager, int capacity) {
    this(key, storedType, memoryManager, capacity, key);
  }

  public MutableKeyColumn(String key, DataType storedType, PinotDataBufferMemoryManager memoryManager, int capacity,
      String allocationContext) {
    _key = key;
    _storedType = storedType;
    _destType = ColumnDataType.fromDataTypeSV(storedType).toPinotDataType();
    _presenceBitmap = new MutableRoaringBitmap();
    _invertedIndex = new RealtimeInvertedIndex();

    int estimatedCardinality = Math.max(capacity / 100, 16);
    int avgLength = storedType.isFixedWidth() ? storedType.size() : DEFAULT_AVG_STRING_LENGTH;
    _dictionary = MutableDictionaryFactory.getMutableDictionary(
        storedType, false, memoryManager, avgLength, estimatedCardinality,
        allocationContext + ".dict");

    _forwardIndex = new FixedByteSVMutableForwardIndex(true, DataType.INT,
        DEFAULT_ROWS_PER_CHUNK, memoryManager, allocationContext + ".fwd");
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

  public MutableForwardIndex getForwardIndex() {
    return _forwardIndex;
  }

  /// Bitmap of docIds where this key was present (non-null).
  public ImmutableRoaringBitmap getPresenceBitmap() {
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

  /// Indexes `value` at `docId`. The value must already be coerced to the stored type.
  public void setValue(int docId, Object value) {
    _presenceBitmap.add(docId);
    int dictId = _dictionary.index(value);
    _forwardIndex.setDictId(docId, dictId);
    _invertedIndex.add(dictId, docId);
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
}
