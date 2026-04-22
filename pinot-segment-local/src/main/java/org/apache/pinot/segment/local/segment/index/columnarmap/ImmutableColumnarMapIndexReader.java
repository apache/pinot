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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Memory-mapped immutable reader for the ColumnarMap index.
 * Provides O(1) typed key lookup via presence bitmap rank operations.
 *
 * <p>Binary format (CMAP, written by {@link OnHeapColumnarMapIndexCreator}):
 * <ul>
 *   <li>Header (72 bytes): magic(int), version(int), numKeys(int), numDocs(int),
 *       numDenseKeys(int), numSparseKeys(int), keyDictOffset(long),
 *       keyMetaOffset(long), perKeyDataOffset(long), valueDictOffset(long),
 *       sparseDataOffset(long), sparseDataLength(long)</li>
 *   <li>Key dictionary: numKeys(int), per key: keyLen(int) + keyBytes</li>
 *   <li>Key metadata (70 bytes/key, big-endian): tierFlag(byte), storedTypeOrdinal(byte),
 *       numDocs(int), nullBitmapOffset(long), nullBitmapLen(long), fwdOffset(long),
 *       fwdLen(long), invOffset(long), invLen(long), dictIdFwdOffset(long),
 *       dictIdFwdLen(long)</li>
 *   <li>Per-key data: null bitmap (RLE-optimized Roaring) + forward index + optional
 *       inverted index + optional dictId forward index. Dense keys only; sparse keys
 *       store just a presence bitmap here (reusing the nullBitmap slot).</li>
 *   <li>Value dictionary: per-key sorted distinct values for dictionary-encoded keys</li>
 *   <li>Sparse data (5th section): per-doc JSON blobs for sparse-tier keys.
 *       Layout: numDocs(int) + offsets[(numDocs+1) x int] + UTF-8 JSON blobs.
 *       Section length is recorded in the header (sparseDataLength); 0 if no sparse keys.</li>
 * </ul>
 * <p>Null-means-absent policy: keys with null values are not recorded during ingestion.
 * A key absent from the presence bitmap is indistinguishable from a key explicitly set to null.
 */
public class ImmutableColumnarMapIndexReader implements ColumnarMapIndexReader {

  private static final int MAGIC = 0x434D4150;   // "CMAP" (Columnar MAP)
  private static final int CURRENT_VERSION = 1;
  private static final int HEADER_SIZE = 72;
  private static final int KEY_METADATA_ENTRY_SIZE = 70;

  public static final byte TIER_DENSE = OnHeapColumnarMapIndexCreator.TIER_DENSE;
  public static final byte TIER_SPARSE = OnHeapColumnarMapIndexCreator.TIER_SPARSE;

  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableColumnarMapIndexReader.class);

  private final PinotDataBuffer _dataBuffer;
  private final int _numDocs;
  private final int _numKeys;

  // per-key data (indexed by keyId)
  private final String[] _keys;
  private final Map<String, Integer> _keyToId;
  private final DataType[] _keyStoredTypes;
  private final int[] _numDocsPerKey;
  private final byte[] _tierFlags;
  private final ImmutableRoaringBitmap[] _nullBitmaps;   // null bitmap for dense keys (absent docs)
  private final ImmutableRoaringBitmap[] _presenceBitmaps;

  // offsets/lengths within _dataBuffer for forward and inverted indexes
  private final long _perKeyDataSectionOffset;
  private final long[] _fwdOffsets;
  private final long[] _fwdLengths;
  private final long[] _invOffsets;
  private final long[] _invLengths;
  private final long[] _dictIdFwdOffsets;
  private final long[] _dictIdFwdLengths;

  // value dictionary section
  private final long _valueDictionarySectionOffset;
  private final Map<String, ColumnarMapKeyDictionary> _keyDictionaries;

  // Pre-built dictId readers for keys with dictionary encoding (indexed by keyId)
  private final FixedBitIntReaderWriter[] _dictIdReaders;

  // Cached inverted index entry offsets per keyId (avoids O(numUnique) scan per query)
  private final long[][] _invEntryOffsets;

  // Sparse-tier data: lives at the end of the CMAP byte stream (5th section after value
  // dictionary). Sliced from _dataBuffer in the constructor when sparseDataLength > 0.
  // Per-doc JSON blobs are read from the buffer on demand via getSparseJsonBlob(docId).
  private final PinotDataBuffer _sparseDataBuffer;     // mmapped view, null if no sparse keys
  private final int[] _sparseOffsets;                  // numDocs+1 offsets into the JSON-data sub-region
  private final int _sparseDataRegionStart;            // byte offset within _sparseDataBuffer where
                                                        // the JSON blob bytes begin (after numDocs(4) +
                                                        // (numDocs+1) * 4 bytes of offsets)

  // Default value type for keys not found in CMAP metadata (from schema's ComplexFieldSpec)
  @Nullable
  private final DataType _defaultValueType;
  private int _numSparseKeys;

  public ImmutableColumnarMapIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IOException {
    _dataBuffer = dataBuffer;

    // Extract defaultValueType from schema's ComplexFieldSpec if available
    if (metadata != null) {
      FieldSpec fieldSpec = metadata.getFieldSpec();
      if (fieldSpec instanceof ComplexFieldSpec) {
        _defaultValueType = ((ComplexFieldSpec) fieldSpec).getDefaultValueType();
      } else {
        _defaultValueType = null;
      }
    } else {
      _defaultValueType = null;
    }

    // ---- Parse Header (BIG_ENDIAN throughout) ----
    byte[] headerBytes = new byte[HEADER_SIZE];
    dataBuffer.copyTo(0, headerBytes, 0, HEADER_SIZE);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
    int magic = headerBuf.getInt();
    if (magic != MAGIC) {
      throw new IOException(
          String.format("Invalid ColumnarMap index: expected magic 0x%08X but got 0x%08X", MAGIC, magic));
    }
    int version = headerBuf.getInt();
    if (version != CURRENT_VERSION) {
      throw new IOException(
          String.format("CMAP version %d not supported (expected %d). Re-ingest segment to upgrade.",
              version, CURRENT_VERSION));
    }
    _numKeys = headerBuf.getInt();
    _numDocs = headerBuf.getInt();
    int numDenseKeys = headerBuf.getInt();
    int numSparseKeys = headerBuf.getInt();
    long keyDictOffset = headerBuf.getLong();
    long keyMetaOffset = headerBuf.getLong();
    _perKeyDataSectionOffset = headerBuf.getLong();
    _valueDictionarySectionOffset = headerBuf.getLong();
    long sparseDataOffset = headerBuf.getLong();
    long sparseDataLength = headerBuf.getLong();
    _numSparseKeys = numSparseKeys;

    // ---- Parse Key Dictionary (big-endian) ----
    _keys = new String[_numKeys];
    _keyToId = new HashMap<>(_numKeys * 2);
    long dictPos = keyDictOffset;
    int dictNumKeys = _dataBuffer.getInt(dictPos);
    if (dictNumKeys != _numKeys) {
      throw new IOException("Key dictionary count mismatch: header=" + _numKeys + ", dict=" + dictNumKeys);
    }
    dictPos += 4;
    for (int i = 0; i < _numKeys; i++) {
      int keyLen = _dataBuffer.getInt(dictPos);
      dictPos += 4;
      byte[] keyBytes = new byte[keyLen];
      dataBuffer.copyTo(dictPos, keyBytes, 0, keyLen);
      _keys[i] = new String(keyBytes, StandardCharsets.UTF_8);
      _keyToId.put(_keys[i], i);
      dictPos += keyLen;
    }

    // ---- Parse Key Metadata (BIG_ENDIAN, 70 bytes per key) ----
    _keyStoredTypes = new DataType[_numKeys];
    _numDocsPerKey = new int[_numKeys];
    _tierFlags = new byte[_numKeys];
    _nullBitmaps = new ImmutableRoaringBitmap[_numKeys];
    _presenceBitmaps = new ImmutableRoaringBitmap[_numKeys];
    _fwdOffsets = new long[_numKeys];
    _fwdLengths = new long[_numKeys];
    _invOffsets = new long[_numKeys];
    _invLengths = new long[_numKeys];
    _dictIdFwdOffsets = new long[_numKeys];
    _dictIdFwdLengths = new long[_numKeys];

    byte[] metaBlock = new byte[_numKeys * KEY_METADATA_ENTRY_SIZE];
    dataBuffer.copyTo(keyMetaOffset, metaBlock, 0, metaBlock.length);
    ByteBuffer metaBuf = ByteBuffer.wrap(metaBlock).order(ByteOrder.BIG_ENDIAN);
    DataType[] allTypes = DataType.values();

    for (int i = 0; i < _numKeys; i++) {
      _tierFlags[i] = metaBuf.get();
      int storedTypeOrdinal = metaBuf.get() & 0xFF;
      if (storedTypeOrdinal >= allTypes.length) {
        throw new IOException(
            "Invalid ColumnarMap index: unknown DataType ordinal " + storedTypeOrdinal
                + " for key index " + i + " (max=" + (allTypes.length - 1) + ")");
      }
      _keyStoredTypes[i] = allTypes[storedTypeOrdinal];
      _numDocsPerKey[i] = metaBuf.getInt();
      long nullBitmapOffset = metaBuf.getLong();
      long nullBitmapLen = metaBuf.getLong();
      _fwdOffsets[i] = metaBuf.getLong();
      _fwdLengths[i] = metaBuf.getLong();
      _invOffsets[i] = metaBuf.getLong();
      _invLengths[i] = metaBuf.getLong();
      _dictIdFwdOffsets[i] = metaBuf.getLong();
      _dictIdFwdLengths[i] = metaBuf.getLong();

      if (_tierFlags[i] == TIER_DENSE && nullBitmapLen > 0) {
        // Dense key: load null bitmap (docs where key is absent)
        byte[] bitmapBytes = new byte[(int) nullBitmapLen];
        dataBuffer.copyTo(_perKeyDataSectionOffset + nullBitmapOffset, bitmapBytes, 0, bitmapBytes.length);
        _nullBitmaps[i] = new ImmutableRoaringBitmap(ByteBuffer.wrap(bitmapBytes));
        _presenceBitmaps[i] = ImmutableRoaringBitmap.flip(_nullBitmaps[i], 0L, (long) _numDocs);
      } else if (_tierFlags[i] == TIER_SPARSE) {
        // Sparse key: no per-key data in CMAP
        _presenceBitmaps[i] = ImmutableRoaringBitmap.bitmapOf();
        _nullBitmaps[i] = null;
      }
    }

    // ---- Parse Value Dictionary Section ----
    _keyDictionaries = new HashMap<>();
    if (_valueDictionarySectionOffset > 0) {
      long pos = _valueDictionarySectionOffset;
      for (int i = 0; i < _numKeys; i++) {
        if (_dictIdFwdLengths[i] == 0 && _invLengths[i] == 0) {
          continue;
        }
        int numDistinctValues = dataBuffer.getInt(pos);
        pos += 4;

        // skip numBitsPerValue (recomputed from dictionary size when needed)
        pos += 4;

        String[] values = new String[numDistinctValues];
        for (int v = 0; v < numDistinctValues; v++) {
          int vLen = dataBuffer.getInt(pos);
          pos += 4;
          byte[] vBytes = new byte[vLen];
          dataBuffer.copyTo(pos, vBytes, 0, vLen);
          values[v] = new String(vBytes, StandardCharsets.UTF_8);
          pos += vLen;
        }
        _keyDictionaries.put(_keys[i], new ColumnarMapKeyDictionary(_keyStoredTypes[i], values));
      }
    }

    // Pre-build dictId readers for all keys with dictId forward index.
    // Dense keys: full forward index with _numDocs entries (direct docId access).
    // Sparse keys: no forward index in CMAP (handled by JSON column).
    _dictIdReaders = new FixedBitIntReaderWriter[_numKeys];
    for (int i = 0; i < _numKeys; i++) {
      if (_dictIdFwdLengths[i] > 0) {
        ColumnarMapKeyDictionary dict = _keyDictionaries.get(_keys[i]);
        if (dict != null) {
          int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(dict.length() - 1, 0));
          long offset = _perKeyDataSectionOffset + _dictIdFwdOffsets[i];
          PinotDataBuffer slice = _dataBuffer.view(offset, offset + _dictIdFwdLengths[i]);
          // Dense keys have _numDocs entries (full forward index)
          int numEntries = (_tierFlags[i] == TIER_DENSE) ? _numDocs : _numDocsPerKey[i];
          _dictIdReaders[i] = new FixedBitIntReaderWriter(slice, numEntries, numBitsPerValue);
        }
      }
    }

    // Pre-build inverted index entry offset tables for keys with inverted indexes.
    // This avoids rebuilding the O(numUnique) offset table on every getDocsWithKeyValue() call.
    _invEntryOffsets = new long[_numKeys][];
    for (int i = 0; i < _numKeys; i++) {
      if (_invLengths[i] > 0) {
        long invBase = _perKeyDataSectionOffset + _invOffsets[i];
        int numUnique = _dataBuffer.getInt(invBase);
        long[] offsets = new long[numUnique];
        long pos = invBase + 4;
        for (int j = 0; j < numUnique; j++) {
          offsets[j] = pos;
          int vLen = _dataBuffer.getInt(pos);
          pos += 4 + vLen;
          int bLen = _dataBuffer.getInt(pos);
          pos += 4 + bLen;
        }
        _invEntryOffsets[i] = offsets;
      }
    }

    // ---- Initialize sparse-tier view from CMAP 5th section ----
    if (sparseDataLength > 0) {
      _sparseDataBuffer = _dataBuffer.view(sparseDataOffset, sparseDataOffset + sparseDataLength);
      int sparseNumDocs = _sparseDataBuffer.getInt(0);
      if (sparseNumDocs != _numDocs) {
        throw new IOException(String.format(
            "Sparse section numDocs mismatch: header numDocs=%d, sparse section numDocs=%d",
            _numDocs, sparseNumDocs));
      }
      _sparseOffsets = new int[_numDocs + 1];
      long offsetTableStart = 4L;  // after numDocs(4)
      for (int i = 0; i <= _numDocs; i++) {
        _sparseOffsets[i] = _sparseDataBuffer.getInt(offsetTableStart + 4L * i);
      }
      _sparseDataRegionStart = (int) (offsetTableStart + 4L * (_numDocs + 1));
    } else {
      _sparseDataBuffer = null;
      _sparseOffsets = null;
      _sparseDataRegionStart = 0;
    }
  }

  /**
   * Returns the raw JSON blob string for sparse keys at the given docId,
   * or null if no sparse keys exist for this doc.
   */
  @Nullable
  public String getSparseJsonBlob(int docId) {
    if (_sparseDataBuffer == null || _sparseOffsets == null) {
      return null;
    }
    int start = _sparseOffsets[docId];
    int end = _sparseOffsets[docId + 1];
    if (start == end) {
      return null;
    }
    int len = end - start;
    byte[] blobBytes = new byte[len];
    _sparseDataBuffer.copyTo(_sparseDataRegionStart + start, blobBytes, 0, len);
    return new String(blobBytes, StandardCharsets.UTF_8);
  }

  /**
   * Parses the sparse JSON blob at the given docId and returns a map of key-value pairs.
   * Returns empty map if no sparse keys exist for this doc.
   */
  public Map<String, Object> getSparseMap(int docId) {
    String json = getSparseJsonBlob(docId);
    if (json == null) {
      return Collections.emptyMap();
    }
    try {
      @SuppressWarnings("unchecked")
      Map<String, Object> result = JsonUtils.stringToObject(json, Map.class);
      return result;
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse sparse JSON blob for docId " + docId, e);
    }
  }

  @Override
  public Set<String> getKeys() {
    Set<String> keys = new HashSet<>(_numKeys * 2);
    for (String key : _keys) {
      keys.add(key);
    }
    return keys;
  }

  /**
   * Returns the tier flag for the given key: TIER_DENSE or TIER_SPARSE.
   * Returns 0 if the key is not found.
   */
  public byte getTierFlag(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _tierFlags[keyId] : 0;
  }

  /**
   * Returns the null bitmap for a dense key (docs where the key is absent).
   * Returns null if the key is not found or is sparse.
   */
  @Nullable
  public ImmutableRoaringBitmap getNullBitmap(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _nullBitmaps[keyId] : null;
  }

  @Nullable
  @Override
  public DataType getKeyValueType(String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId != null) {
      return _keyStoredTypes[keyId];
    }
    // Key not found in this segment's CMAP — fall back to schema's defaultValueType.
    // This avoids NPE in buildKeyDataSource() when querying keys that exist in other segments
    // but not this one (MAP keys are discovered dynamically during ingestion).
    return _defaultValueType;
  }

  @Override
  public int getNumDocsWithKey(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _numDocsPerKey[keyId] : 0;
  }

  @Override
  public ImmutableRoaringBitmap getPresenceBitmap(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _presenceBitmaps[keyId] : ImmutableRoaringBitmap.bitmapOf();
  }

  @Override
  public int getInt(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _tierFlags[keyId] == TIER_SPARSE) {
      return 0;
    }
    // Dense key: direct docId access (no rank())
    if (_nullBitmaps[keyId] != null && _nullBitmaps[keyId].contains(docId)) {
      return 0;
    }
    if (_dictIdReaders[keyId] != null) {
      int dictId = _dictIdReaders[keyId].readInt(docId);
      return _keyDictionaries.get(_keys[keyId]).getIntValue(dictId);
    }
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Integer.BYTES;
    return _dataBuffer.getInt(bufOffset);
  }

  @Override
  public long getLong(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _tierFlags[keyId] == TIER_SPARSE) {
      return 0L;
    }
    if (_nullBitmaps[keyId] != null && _nullBitmaps[keyId].contains(docId)) {
      return 0L;
    }
    if (_dictIdReaders[keyId] != null) {
      int dictId = _dictIdReaders[keyId].readInt(docId);
      return _keyDictionaries.get(_keys[keyId]).getLongValue(dictId);
    }
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Long.BYTES;
    return _dataBuffer.getLong(bufOffset);
  }

  @Override
  public float getFloat(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _tierFlags[keyId] == TIER_SPARSE) {
      return 0.0f;
    }
    if (_nullBitmaps[keyId] != null && _nullBitmaps[keyId].contains(docId)) {
      return 0.0f;
    }
    if (_dictIdReaders[keyId] != null) {
      int dictId = _dictIdReaders[keyId].readInt(docId);
      return _keyDictionaries.get(_keys[keyId]).getFloatValue(dictId);
    }
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Float.BYTES;
    return _dataBuffer.getFloat(bufOffset);
  }

  @Override
  public double getDouble(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _tierFlags[keyId] == TIER_SPARSE) {
      return 0.0;
    }
    if (_nullBitmaps[keyId] != null && _nullBitmaps[keyId].contains(docId)) {
      return 0.0;
    }
    if (_dictIdReaders[keyId] != null) {
      int dictId = _dictIdReaders[keyId].readInt(docId);
      return _keyDictionaries.get(_keys[keyId]).getDoubleValue(dictId);
    }
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Double.BYTES;
    return _dataBuffer.getDouble(bufOffset);
  }

  @Override
  public String getString(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _tierFlags[keyId] == TIER_SPARSE) {
      return "";
    }
    if (_nullBitmaps[keyId] != null && _nullBitmaps[keyId].contains(docId)) {
      return "";
    }
    if (_dictIdReaders[keyId] != null) {
      int dictId = _dictIdReaders[keyId].readInt(docId);
      return _keyDictionaries.get(_keys[keyId]).getStringValue(dictId);
    }
    return readStringAtOrdinal(keyId, docId);
  }

  @Override
  public byte[] getBytes(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _tierFlags[keyId] == TIER_SPARSE) {
      return new byte[0];
    }
    if (_nullBitmaps[keyId] != null && _nullBitmaps[keyId].contains(docId)) {
      return new byte[0];
    }
    if (_dictIdReaders[keyId] != null) {
      int dictId = _dictIdReaders[keyId].readInt(docId);
      return _keyDictionaries.get(_keys[keyId]).getBytesValue(dictId);
    }
    return readBytesAtOrdinal(keyId, docId);
  }

  private String readStringAtOrdinal(int keyId, int ordinal) {
    byte[] raw = readBytesAtOrdinal(keyId, ordinal);
    return new String(raw, StandardCharsets.UTF_8);
  }

  /**
   * Reads variable-length bytes from the STRING/BYTES forward index at the given ordinal.
   * Format: numValues(int), offsets(int[numValues+1]), data(bytes)
   */
  private byte[] readBytesAtOrdinal(int keyId, int ordinal) {
    // Header: numValues(4 bytes), then offsets array ((numValues+1)*4 bytes), then data
    long fwdBase = _perKeyDataSectionOffset + _fwdOffsets[keyId];
    byte[] numValBytes = new byte[4];
    _dataBuffer.copyTo(fwdBase, numValBytes, 0, 4);
    int numValues = ByteBuffer.wrap(numValBytes).order(ByteOrder.BIG_ENDIAN).getInt();
    // Dense keys have numDocs entries, sparse would have numDocsPerKey
    int expectedCount = (_tierFlags[keyId] == TIER_DENSE) ? _numDocs : _numDocsPerKey[keyId];
    if (numValues != expectedCount) {
      throw new IllegalStateException(
          "ColumnarMap forward index corrupt for key '" + _keys[keyId]
              + "': numValues=" + numValues + " but expected=" + expectedCount);
    }

    // Read the two offsets for this ordinal: offsets[ordinal] and offsets[ordinal+1]
    long offsetBase = fwdBase + 4 + (long) ordinal * Integer.BYTES;
    byte[] offsetBytes = new byte[8];
    _dataBuffer.copyTo(offsetBase, offsetBytes, 0, 8);
    ByteBuffer ob = ByteBuffer.wrap(offsetBytes).order(ByteOrder.BIG_ENDIAN);
    int startOffset = ob.getInt();
    int endOffset = ob.getInt();

    int dataLen = endOffset - startOffset;
    if (dataLen <= 0) {
      return new byte[0];
    }

    // Data starts after the offsets array: fwdBase + 4 (numValues) + (numValues+1)*4 (offsets)
    long dataBase = fwdBase + 4 + (long) (numValues + 1) * Integer.BYTES;
    byte[] result = new byte[dataLen];
    _dataBuffer.copyTo(dataBase + startOffset, result, 0, dataLen);
    return result;
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getDocsWithKeyValue(String key, Object value) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _invLengths[keyId] == 0) {
      return null;
    }
    String valueStr = _keyStoredTypes[keyId].toString(value);

    // Fast O(1) negative check: if the in-memory value dictionary is loaded and does not
    // contain the value, we can return null immediately without any I/O.
    ColumnarMapKeyDictionary dict = _keyDictionaries.get(key);
    if (dict != null && dict.indexOf(valueStr) < 0) {
      return null;
    }

    // Scan the inverted index entries using binary search on the sorted values.
    // Format: numUnique(int), then per entry: valueLen(int), valueBytes, bitmapLen(int), bitmapBytes.
    // Values are sorted (lexicographic for STRING, numeric order for numeric types via TreeMap),
    // so we first load all entry offsets, then binary search by value.
    byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);
    long[] entryOffsets = _invEntryOffsets[keyId];
    if (entryOffsets == null) {
      return null;
    }
    int numUnique = entryOffsets.length;

    // Binary search over the sorted inverted index entries.
    // The inverted index uses a TreeMap which sorts lexicographically for all types.
    int lo = 0;
    int hi = numUnique - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      long entryPos = entryOffsets[mid];
      int vLen = _dataBuffer.getInt(entryPos);
      entryPos += 4;
      byte[] vBytes = new byte[vLen];
      _dataBuffer.copyTo(entryPos, vBytes, 0, vLen);
      entryPos += vLen;

      int cmp = compareBytes(vBytes, valueBytes);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid - 1;
      } else {
        // Found the matching value; read the bitmap
        int bLen = _dataBuffer.getInt(entryPos);
        entryPos += 4;
        byte[] bitmapBytes = new byte[bLen];
        _dataBuffer.copyTo(entryPos, bitmapBytes, 0, bLen);
        return new ImmutableRoaringBitmap(ByteBuffer.wrap(bitmapBytes));
      }
    }
    return null;
  }

  /// Lexicographic comparison of two byte arrays, equivalent to comparing the UTF-8 strings.
  private static int compareBytes(byte[] a, byte[] b) {
    int len = Math.min(a.length, b.length);
    for (int i = 0; i < len; i++) {
      int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
      if (diff != 0) {
        return diff;
      }
    }
    return a.length - b.length;
  }

  @Override
  public boolean hasInvertedIndex(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null && _invLengths[keyId] > 0;
  }

  @Override
  @Nullable
  public String[] getDistinctValuesForKey(String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _invLengths[keyId] == 0) {
      return null;
    }

    long invBase = _perKeyDataSectionOffset + _invOffsets[keyId];
    int numUnique = _dataBuffer.getInt(invBase);

    String[] distinctValues = new String[numUnique];
    long pos = invBase + 4;
    for (int i = 0; i < numUnique; i++) {
      int vLen = _dataBuffer.getInt(pos);
      pos += 4;

      byte[] vBytes = new byte[vLen];
      _dataBuffer.copyTo(pos, vBytes, 0, vLen);
      distinctValues[i] = new String(vBytes, StandardCharsets.UTF_8);
      pos += vLen;

      // Skip bitmap: read bLen and advance past bitmap bytes
      int bLen = _dataBuffer.getInt(pos);
      pos += 4 + bLen;
    }
    return distinctValues;
  }

  @Override
  public DataSource getKeyDataSource(String key) {
    // Implemented in ColumnarMapDataSource (Task 15)
    return null;
  }

  /// Returns a FixedBitIntReaderWriter for reading bit-packed dictIds for the given key,
  /// or null if no dictId forward index is available.
  @Nullable
  public FixedBitIntReaderWriter getDictIdReader(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _dictIdReaders[keyId] : null;
  }

  /// Returns the cached ColumnarMapKeyDictionary for the given key, or null if not available.
  @Nullable
  public ColumnarMapKeyDictionary getKeyDictionary(String key) {
    return _keyDictionaries.get(key);
  }

  /**
   * Returns a PinotDataBuffer view over the dictId forward index for the given dense key,
   * suitable for creating a FixedBitSVForwardIndexReaderV2. Returns null if the key is not
   * found or has no dictId forward index.
   */
  @Nullable
  public PinotDataBuffer getDictIdFwdBuffer(String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _dictIdFwdLengths[keyId] == 0) {
      return null;
    }
    long offset = _perKeyDataSectionOffset + _dictIdFwdOffsets[keyId];
    return _dataBuffer.view(offset, offset + _dictIdFwdLengths[keyId]);
  }

  /**
   * Returns the number of bits per dictId value for the given key's dictionary,
   * or 0 if the key has no dictionary.
   */
  public int getNumBitsPerDictId(String key) {
    ColumnarMapKeyDictionary dict = _keyDictionaries.get(key);
    if (dict == null) {
      return 0;
    }
    return PinotDataBitSet.getNumBitsPerValue(Math.max(dict.length() - 1, 0));
  }

  /// Returns the total number of documents in this index.
  public int getNumDocs() {
    return _numDocs;
  }

  /**
   * Returns the value for the given (docId, keyId) pair without any HashMap lookup.
   * The keyId must be valid and the caller must have already confirmed the doc is present
   * via the presence bitmap. The ordinal (rank - 1) is computed here.
   */
  private Object getValueByKeyId(int docId, int keyId) {
    // For dense keys: docId IS the index into the full forward index (no rank() needed)
    // For sparse keys: this method should not be called (handled by JSON column)
    if (_dictIdReaders[keyId] != null) {
      int dictId = _dictIdReaders[keyId].readInt(docId);
      ColumnarMapKeyDictionary dict = _keyDictionaries.get(_keys[keyId]);
      switch (_keyStoredTypes[keyId]) {
        case INT:
          return dict.getIntValue(dictId);
        case LONG:
          return dict.getLongValue(dictId);
        case FLOAT:
          return dict.getFloatValue(dictId);
        case DOUBLE:
          return dict.getDoubleValue(dictId);
        case BYTES:
          return dict.getBytesValue(dictId);
        default:
          return dict.getStringValue(dictId);
      }
    }
    // Raw forward index path — direct docId access
    switch (_keyStoredTypes[keyId]) {
      case INT: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Integer.BYTES;
        return _dataBuffer.getInt(bufOffset);
      }
      case LONG: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Long.BYTES;
        return _dataBuffer.getLong(bufOffset);
      }
      case FLOAT: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Float.BYTES;
        return _dataBuffer.getFloat(bufOffset);
      }
      case DOUBLE: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) docId * Double.BYTES;
        return _dataBuffer.getDouble(bufOffset);
      }
      case BYTES:
        return readBytesAtOrdinal(keyId, docId);
      default:
        return readStringAtOrdinal(keyId, docId);
    }
  }

  @Override
  public Map<String, Object> getMap(int docId) {
    Map<String, Object> result = new HashMap<>();
    // Dense keys from CMAP
    for (int i = 0; i < _numKeys; i++) {
      if (_tierFlags[i] == TIER_SPARSE) {
        continue;
      }
      if (_nullBitmaps[i] != null && _nullBitmaps[i].contains(docId)) {
        continue;
      }
      result.put(_keys[i], getValueByKeyId(docId, i));
    }
    // Sparse keys from sidecar
    Map<String, Object> sparseMap = getSparseMap(docId);
    result.putAll(sparseMap);
    return result;
  }

  @Override
  public void close()
      throws IOException {
    for (FixedBitIntReaderWriter reader : _dictIdReaders) {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
