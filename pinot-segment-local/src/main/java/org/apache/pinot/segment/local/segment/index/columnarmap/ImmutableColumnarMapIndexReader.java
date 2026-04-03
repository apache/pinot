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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Memory-mapped immutable reader for the ColumnarMap index.
 * Provides O(1) typed key lookup via presence bitmap rank operations.
 *
 * <p>Binary format (written by {@link OnHeapColumnarMapIndexCreator}):
 * <ul>
 *   <li>Header (64 bytes): version(int), numKeys(int), numDocs(int), keyDictOffset(long),
 *       keyMetaOffset(long), perKeyDataOffset(long), padding</li>
 *   <li>Key dictionary: numKeys(int), per key: keyLen(int) + keyBytes</li>
 *   <li>Key metadata (53 bytes/key, big-endian): storedTypeOrdinal(byte), numDocs(int),
 *       presenceOffset(long), presenceLen(long), fwdOffset(long), fwdLen(long),
 *       invOffset(long), invLen(long)</li>
 *   <li>Per-key data: presence bitmap, typed forward index, optional inverted index</li>
 * </ul>
 * <p>Null-means-absent policy: keys with null values are not recorded during ingestion.
 * A key absent from the presence bitmap is indistinguishable from a key explicitly set to null.
 */
public class ImmutableColumnarMapIndexReader implements ColumnarMapIndexReader {

  private static final int MAGIC = 0x53504D58;   // "SPMX"
  private static final int CURRENT_VERSION = 2;
  private static final int HEADER_SIZE = 64;
  private static final int KEY_METADATA_ENTRY_SIZE = 69;

  private final PinotDataBuffer _dataBuffer;
  private final int _numDocs;
  private final int _numKeys;

  // per-key data (indexed by keyId)
  private final String[] _keys;
  private final Map<String, Integer> _keyToId;
  private final DataType[] _keyStoredTypes;
  private final int[] _numDocsPerKey;
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

  public ImmutableColumnarMapIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IOException {
    _dataBuffer = dataBuffer;

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
    if (version > CURRENT_VERSION) {
      throw new IOException(
          "Unsupported ColumnarMap index version: " + version + " (max supported: " + CURRENT_VERSION + ")");
    }
    _numKeys = headerBuf.getInt();
    _numDocs = headerBuf.getInt();
    long keyDictOffset = headerBuf.getLong();
    long keyMetaOffset = headerBuf.getLong();
    _perKeyDataSectionOffset = headerBuf.getLong();
    _valueDictionarySectionOffset = headerBuf.getLong();

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

    // ---- Parse Key Metadata (BIG_ENDIAN, 69 bytes per key) ----
    _keyStoredTypes = new DataType[_numKeys];
    _numDocsPerKey = new int[_numKeys];
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
      int storedTypeOrdinal = metaBuf.get() & 0xFF;
      if (storedTypeOrdinal >= allTypes.length) {
        throw new IOException(
            "Invalid ColumnarMap index: unknown DataType ordinal " + storedTypeOrdinal
                + " for key index " + i + " (max=" + (allTypes.length - 1) + ")");
      }
      _keyStoredTypes[i] = allTypes[storedTypeOrdinal];
      _numDocsPerKey[i] = metaBuf.getInt();
      long presenceOffset = metaBuf.getLong();
      long presenceLen = metaBuf.getLong();
      _fwdOffsets[i] = metaBuf.getLong();
      _fwdLengths[i] = metaBuf.getLong();
      _invOffsets[i] = metaBuf.getLong();
      _invLengths[i] = metaBuf.getLong();
      _dictIdFwdOffsets[i] = metaBuf.getLong();
      _dictIdFwdLengths[i] = metaBuf.getLong();

      // Load presence bitmap
      byte[] bitmapBytes = new byte[(int) presenceLen];
      dataBuffer.copyTo(_perKeyDataSectionOffset + presenceOffset, bitmapBytes, 0, bitmapBytes.length);
      _presenceBitmaps[i] = new ImmutableRoaringBitmap(ByteBuffer.wrap(bitmapBytes));
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

    // Pre-build dictId readers for all keys with dictId forward index
    // The dictId forward index is sparse: it only contains entries for docs that have the key.
    // Use _numDocsPerKey[i] (not _numDocs) as the number of entries.
    _dictIdReaders = new FixedBitIntReaderWriter[_numKeys];
    for (int i = 0; i < _numKeys; i++) {
      if (_dictIdFwdLengths[i] > 0) {
        ColumnarMapKeyDictionary dict = _keyDictionaries.get(_keys[i]);
        if (dict != null) {
          int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(dict.length() - 1, 0));
          long offset = _perKeyDataSectionOffset + _dictIdFwdOffsets[i];
          PinotDataBuffer slice = _dataBuffer.view(offset, offset + _dictIdFwdLengths[i]);
          _dictIdReaders[i] = new FixedBitIntReaderWriter(slice, _numDocsPerKey[i], numBitsPerValue);
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
  }

  @Override
  public Set<String> getKeys() {
    Set<String> keys = new HashSet<>(_numKeys * 2);
    for (String key : _keys) {
      keys.add(key);
    }
    return keys;
  }

  @Nullable
  @Override
  public DataType getKeyValueType(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _keyStoredTypes[keyId] : null;
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
    if (keyId == null) {
      return 0;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0;
    }
    // Dictionary-encoded path: use bitmap rank to find ordinal in sparse dictId array
    if (_dictIdReaders[keyId] != null) {
      int ordinal = bitmap.rank(docId) - 1;
      int dictId = _dictIdReaders[keyId].readInt(ordinal);
      return _keyDictionaries.get(_keys[keyId]).getIntValue(dictId);
    }
    // Raw forward index path
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Integer.BYTES;
    return _dataBuffer.getInt(bufOffset);
  }

  @Override
  public long getLong(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return 0L;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0L;
    }
    if (_dictIdReaders[keyId] != null) {
      int ordinal = bitmap.rank(docId) - 1;
      int dictId = _dictIdReaders[keyId].readInt(ordinal);
      return _keyDictionaries.get(_keys[keyId]).getLongValue(dictId);
    }
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Long.BYTES;
    return _dataBuffer.getLong(bufOffset);
  }

  @Override
  public float getFloat(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return 0.0f;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0.0f;
    }
    if (_dictIdReaders[keyId] != null) {
      int ordinal = bitmap.rank(docId) - 1;
      int dictId = _dictIdReaders[keyId].readInt(ordinal);
      return _keyDictionaries.get(_keys[keyId]).getFloatValue(dictId);
    }
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Float.BYTES;
    return _dataBuffer.getFloat(bufOffset);
  }

  @Override
  public double getDouble(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return 0.0;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0.0;
    }
    if (_dictIdReaders[keyId] != null) {
      int ordinal = bitmap.rank(docId) - 1;
      int dictId = _dictIdReaders[keyId].readInt(ordinal);
      return _keyDictionaries.get(_keys[keyId]).getDoubleValue(dictId);
    }
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Double.BYTES;
    return _dataBuffer.getDouble(bufOffset);
  }

  @Override
  public String getString(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return "";
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return "";
    }
    if (_dictIdReaders[keyId] != null) {
      int ordinal = bitmap.rank(docId) - 1;
      int dictId = _dictIdReaders[keyId].readInt(ordinal);
      return _keyDictionaries.get(_keys[keyId]).getStringValue(dictId);
    }
    int ordinal = bitmap.rank(docId) - 1;
    return readStringAtOrdinal(keyId, ordinal);
  }

  @Override
  public byte[] getBytes(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return new byte[0];
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return new byte[0];
    }
    if (_dictIdReaders[keyId] != null) {
      int ordinal = bitmap.rank(docId) - 1;
      int dictId = _dictIdReaders[keyId].readInt(ordinal);
      return _keyDictionaries.get(_keys[keyId]).getBytesValue(dictId);
    }
    int ordinal = bitmap.rank(docId) - 1;
    return readBytesAtOrdinal(keyId, ordinal);
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
    if (numValues != _numDocsPerKey[keyId]) {
      throw new IllegalStateException(
          "ColumnarMap forward index corrupt for key '" + _keys[keyId]
              + "': numValues=" + numValues + " but presence cardinality=" + _numDocsPerKey[keyId]);
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
    // Dictionary-encoded path: use bitmap rank for sparse dictId lookup
    if (_dictIdReaders[keyId] != null) {
      int ordinal = _presenceBitmaps[keyId].rank(docId) - 1;
      int dictId = _dictIdReaders[keyId].readInt(ordinal);
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
    // Raw forward index path
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    int ordinal = bitmap.rank(docId) - 1;
    switch (_keyStoredTypes[keyId]) {
      case INT: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Integer.BYTES;
        return _dataBuffer.getInt(bufOffset);
      }
      case LONG: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Long.BYTES;
        return _dataBuffer.getLong(bufOffset);
      }
      case FLOAT: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Float.BYTES;
        return _dataBuffer.getFloat(bufOffset);
      }
      case DOUBLE: {
        long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Double.BYTES;
        return _dataBuffer.getDouble(bufOffset);
      }
      case BYTES:
        return readBytesAtOrdinal(keyId, ordinal);
      default:
        return readStringAtOrdinal(keyId, ordinal);
    }
  }

  @Override
  public Map<String, Object> getMap(int docId) {
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < _numKeys; i++) {
      if (!_presenceBitmaps[i].contains(docId)) {
        continue;
      }
      result.put(_keys[i], getValueByKeyId(docId, i));
    }
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
