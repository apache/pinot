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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.creator.ColumnarMapIndexCreator;
import org.apache.pinot.spi.config.table.ColumnarMapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * On-heap implementation of {@link ColumnarMapIndexCreator} for immutable (offline) segments.
 * Accumulates per-document sparse maps in memory and writes a binary index file on seal.
 * Uses more heap memory but avoids disk I/O during indexing.
 */
public class OnHeapColumnarMapIndexCreator implements ColumnarMapIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapColumnarMapIndexCreator.class);

  private static final int MAGIC = 0x53504D58;   // "SPMX"
  private static final int VERSION = 2;
  private static final int HEADER_SIZE = 64;
  private static final int KEY_METADATA_ENTRY_SIZE = 69;

  private final File _indexDir;
  private final String _columnName;
  private final Map<String, DataType> _keyTypes;
  private final DataType _defaultValueType;
  private final ColumnarMapIndexConfig _config;
  private final Set<String> _indexedKeys;
  private final int _maxKeys;

  private static final double NO_DICTIONARY_SIZE_RATIO_THRESHOLD = 0.85;

  private final Map<String, RoaringBitmap> _presenceBitmaps = new HashMap<>();
  private final Map<String, List<Object>> _values = new HashMap<>();
  private final Map<String, Set<String>> _distinctValuesPerKey = new HashMap<>();
  private final Map<String, Long> _totalRawBytesPerKey = new HashMap<>();
  private int _numDocs;
  private int _distinctKeyCount;
  private final Set<String> _droppedKeys = new HashSet<>();

  public OnHeapColumnarMapIndexCreator(IndexCreationContext context, ColumnarMapIndexConfig config)
      throws IOException {
    this(context.getIndexDir(), context.getFieldSpec().getName(),
        context.getFieldSpec(), config);
  }

  public OnHeapColumnarMapIndexCreator(File indexDir, String columnName, FieldSpec fieldSpec,
      ColumnarMapIndexConfig config)
      throws IOException {
    this(indexDir, columnName, fieldSpec, config, null, null);
  }

  public OnHeapColumnarMapIndexCreator(File indexDir, String columnName, FieldSpec fieldSpec,
      ColumnarMapIndexConfig config,
      @Nullable Map<String, FieldSpec.DataType> explicitKeyTypes,
      @Nullable FieldSpec.DataType explicitDefaultValueType)
      throws IOException {
    _indexDir = indexDir;
    _columnName = columnName;
    _config = config;
    _indexedKeys = config.getIndexedKeys();
    _maxKeys = config.getMaxKeys();

    Map<String, FieldSpec.DataType> keyTypes = explicitKeyTypes;
    FieldSpec.DataType defaultType = explicitDefaultValueType;
    if (keyTypes == null && fieldSpec instanceof ComplexFieldSpec) {
      ComplexFieldSpec.MapFieldSpec mapSpec = ComplexFieldSpec.toMapFieldSpec((ComplexFieldSpec) fieldSpec);
      keyTypes = mapSpec.getKeyTypes();
      defaultType = mapSpec.getDefaultValueType();
    }
    _keyTypes = keyTypes != null ? new HashMap<>(keyTypes) : new HashMap<>();
    _defaultValueType = defaultType != null ? defaultType : FieldSpec.DataType.STRING;
  }

  @Override
  public void add(Map<String, Object> columnarMap)
      throws IOException {
    if (columnarMap != null && !columnarMap.isEmpty()) {
      for (Map.Entry<String, Object> entry : columnarMap.entrySet()) {
        String key = entry.getKey();
        if (_indexedKeys != null && !_indexedKeys.contains(key)) {
          continue;
        }
        Object rawValue = entry.getValue();
        if (rawValue == null) {
          // Null values are treated as absent: the key is not recorded for this document.
          // There is no distinction between "key absent" and "key present with null value".
          continue;
        }
        if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
          if (_droppedKeys.add(key)) {
            LOGGER.warn(
                "ColumnarMap index for column '{}' reached maxKeys limit ({}). Dropping key '{}'. "
                    + "Total distinct dropped keys so far: {}.",
                _columnName, _maxKeys, key, _droppedKeys.size());
          }
          continue;
        }
        DataType valueType = _keyTypes.getOrDefault(key, _defaultValueType);
        if (!_presenceBitmaps.containsKey(key)) {
          _presenceBitmaps.put(key, new RoaringBitmap());
          _values.put(key, new ArrayList<>());
          _distinctKeyCount++;
        }
        _presenceBitmaps.get(key).add(_numDocs);
        Object coerced;
        try {
          coerced = coerceValue(rawValue, valueType);
        } catch (ClassCastException | NumberFormatException e) {
          LOGGER.warn(
              "OnHeapColumnarMapIndexCreator for column '{}': failed to coerce value '{}' (type {}) to {} for key '{}'."
                  + " Skipping key for this document.",
              _columnName, rawValue, rawValue.getClass().getSimpleName(), valueType, key, e);
          _presenceBitmaps.get(key).remove(_numDocs);
          continue;
        }
        _values.get(key).add(coerced);

        // Track per-key stats for dictionary optimization decision
        DataType storedType = valueType.getStoredType();
        String stringRep = storedType.toString(coerced);
        _distinctValuesPerKey.computeIfAbsent(key, k -> new HashSet<>()).add(stringRep);
        if (storedType == DataType.STRING || storedType == DataType.BYTES) {
          byte[] rawBytes;
          if (storedType == DataType.BYTES) {
            rawBytes = (byte[]) coerced;
          } else {
            rawBytes = ((String) coerced).getBytes(StandardCharsets.UTF_8);
          }
          _totalRawBytesPerKey.merge(key, (long) rawBytes.length, Long::sum);
        }
      }
    }
    _numDocs++;
  }

  @Override
  public void add(Object value, int dictId)
      throws IOException {
    if (!(value instanceof Map)) {
      return;
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> columnarMap = (Map<String, Object>) value;
    add(columnarMap);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("MAP with sparse map index is single-value only");
  }

  private Object coerceValue(Object value, DataType dataType) {
    if (value == null) {
      return coerceValueForType(dataType, null);
    }
    DataType storedType = dataType.getStoredType();
    switch (storedType) {
      case INT:
        if (value instanceof Boolean) {
          return (Boolean) value ? 1 : 0;
        }
        if (value instanceof Number) {
          return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString().trim());
      case LONG:
        if (value instanceof Number) {
          return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString().trim());
      case FLOAT:
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString().trim());
      case DOUBLE:
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString().trim());
      case STRING:
        return value.toString();
      case BYTES:
        if (value instanceof byte[]) {
          return value;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
      default:
        return value.toString();
    }
  }

  private Object coerceValueForType(DataType dataType, Object nullValue) {
    DataType storedType = dataType.getStoredType();
    switch (storedType) {
      case INT:
        return nullValue != null ? ((Number) nullValue).intValue() : 0;
      case LONG:
        return nullValue != null ? ((Number) nullValue).longValue() : 0L;
      case FLOAT:
        return nullValue != null ? ((Number) nullValue).floatValue() : 0.0f;
      case DOUBLE:
        return nullValue != null ? ((Number) nullValue).doubleValue() : 0.0;
      case STRING:
        return nullValue != null ? nullValue.toString() : "";
      case BYTES:
        return nullValue instanceof byte[] ? nullValue : new byte[0];
      default:
        return "";
    }
  }

  /**
   * Decides whether to use dictionary encoding for a sparse map key.
   * Mirrors the logic in DictionaryIndexType.canSafelyCreateDictionaryWithinThreshold.
   * Returns true if dictionary+dictIdFwd is more compact than raw forward index.
   */
  private boolean shouldUseDictionary(String key, DataType storedType, int numDocsForKey) {
    // Keys with inverted index always get dictionary (inverted index requires it)
    if (_config.shouldEnableInvertedIndexForKey(key)) {
      return true;
    }

    // Explicit override: noDictionaryKeys forces raw encoding
    if (!_config.shouldUseDictionaryForKey(key)) {
      return false;
    }

    Set<String> distinctValues = _distinctValuesPerKey.get(key);
    if (distinctValues == null || distinctValues.isEmpty()) {
      return false;
    }

    // Cardinality is just the distinct values (no default value in sparse dictId forward index)
    int cardinality = distinctValues.size();
    if (cardinality == 0) {
      return false;
    }

    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(cardinality - 1, 0));
    // Sparse dictId forward index: only stores entries for docs that have the key,
    // same as the raw forward index. Both cover numDocsForKey entries.
    long dictIdFwdSize = ((long) numDocsForKey * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;

    long rawSize;
    long dictSize;

    switch (storedType) {
      case INT:
        rawSize = (long) numDocsForKey * Integer.BYTES;
        dictSize = (long) cardinality * Integer.BYTES;
        break;
      case LONG:
        rawSize = (long) numDocsForKey * Long.BYTES;
        dictSize = (long) cardinality * Long.BYTES;
        break;
      case FLOAT:
        rawSize = (long) numDocsForKey * Float.BYTES;
        dictSize = (long) cardinality * Float.BYTES;
        break;
      case DOUBLE:
        rawSize = (long) numDocsForKey * Double.BYTES;
        dictSize = (long) cardinality * Double.BYTES;
        break;
      case STRING:
      case BYTES: {
        // Raw size: numValues(int) + offsets[numDocsForKey+1](int[]) + totalRawBytes
        long totalRawBytes = _totalRawBytesPerKey.getOrDefault(key, 0L);
        rawSize = Integer.BYTES + (long) (numDocsForKey + 1) * Integer.BYTES + totalRawBytes;
        // Dict size: sum of (int + bytes) per distinct value in the value dictionary section
        dictSize = 0;
        for (String v : distinctValues) {
          dictSize += Integer.BYTES + v.getBytes(StandardCharsets.UTF_8).length;
        }
        break;
      }
      default:
        return true;
    }

    // If raw is cheaper than dict+dictIdFwd (within threshold), use raw
    double ratio = (double) rawSize / (dictSize + dictIdFwdSize);
    return ratio > NO_DICTIONARY_SIZE_RATIO_THRESHOLD;
  }

  @Override
  public void seal()
      throws IOException {
    List<String> sortedKeys = new ArrayList<>(_presenceBitmaps.keySet());
    Collections.sort(sortedKeys);
    int numKeys = sortedKeys.size();

    byte[] keyDictionarySection = buildKeyDictionarySection(sortedKeys);
    byte[] keyMetadataSection = new byte[numKeys * KEY_METADATA_ENTRY_SIZE];
    Map<String, TreeMap<String, RoaringBitmap>> cachedValueToDocIds = new HashMap<>();
    byte[] perKeyDataSection = buildPerKeyDataSection(sortedKeys, keyMetadataSection, cachedValueToDocIds);
    byte[] valueDictionarySection = buildValueDictionarySection(sortedKeys, cachedValueToDocIds);

    long keyDictionaryOffset = HEADER_SIZE;
    long keyMetadataOffset = keyDictionaryOffset + keyDictionarySection.length;
    long perKeyDataOffset = keyMetadataOffset + keyMetadataSection.length;
    long valueDictionaryOffset = perKeyDataOffset + perKeyDataSection.length;

    File indexFile = new File(_indexDir, _columnName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    try (FileOutputStream fos = new FileOutputStream(indexFile);
        DataOutputStream dos = new DataOutputStream(fos)) {
      writeHeader(dos, numKeys, keyDictionaryOffset, keyMetadataOffset, perKeyDataOffset, valueDictionaryOffset);
      dos.write(keyDictionarySection);
      dos.write(keyMetadataSection);
      dos.write(perKeyDataSection);
      dos.write(valueDictionarySection);
    }
  }

  private byte[] buildKeyDictionarySection(List<String> sortedKeys)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeInt(sortedKeys.size());
      for (String key : sortedKeys) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(keyBytes.length);
        dos.write(keyBytes);
      }
    }
    return baos.toByteArray();
  }

  private byte[] buildPerKeyDataSection(List<String> sortedKeys, byte[] keyMetadataSection,
      Map<String, TreeMap<String, RoaringBitmap>> cachedValueToDocIds)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      long currentOffset = 0;

      for (int i = 0; i < sortedKeys.size(); i++) {
        String key = sortedKeys.get(i);
        RoaringBitmap presence = _presenceBitmaps.get(key);
        List<Object> values = _values.get(key);
        DataType dataType = _keyTypes.getOrDefault(key, _defaultValueType);
        DataType storedType = dataType.getStoredType();

        // Always write presence bitmap
        byte[] presenceBytes = RoaringBitmapUtils.serialize(presence);
        long presenceOffset = currentOffset;
        dos.write(presenceBytes);
        currentOffset += presenceBytes.length;

        boolean useDictionary = shouldUseDictionary(key, storedType, values.size());
        boolean enableInverted = _config.shouldEnableInvertedIndexForKey(key);

        // Write forward index: either raw OR dictId, never both
        long forwardOffset = 0;
        long forwardLength = 0;
        long dictIdFwdOffset = 0;
        long dictIdFwdLength = 0;

        TreeMap<String, RoaringBitmap> valueToDocIds = null;

        if (useDictionary) {
          // Dictionary-encoded path: build valueToDocIds and dictId forward index
          valueToDocIds = buildValueToDocIds(presence, values, storedType);
          cachedValueToDocIds.put(key, valueToDocIds);

          byte[] dictIdFwdBytes = buildDictIdForwardIndex(valueToDocIds, storedType, presence);
          dictIdFwdOffset = currentOffset;
          dictIdFwdLength = dictIdFwdBytes.length;
          dos.write(dictIdFwdBytes);
          currentOffset += dictIdFwdBytes.length;
        } else {
          // Raw forward index path
          byte[] forwardBytes = buildForwardIndex(values, storedType);
          forwardOffset = currentOffset;
          forwardLength = forwardBytes.length;
          dos.write(forwardBytes);
          currentOffset += forwardBytes.length;
        }

        // Inverted index (independent of dictionary decision)
        byte[] invertedBytes;
        long invertedOffset = 0;
        long invertedLength = 0;
        if (enableInverted) {
          if (valueToDocIds == null) {
            valueToDocIds = buildValueToDocIds(presence, values, storedType);
            cachedValueToDocIds.put(key, valueToDocIds);
          }
          invertedBytes = serializeInvertedIndex(valueToDocIds);
          invertedOffset = currentOffset;
          invertedLength = invertedBytes.length;
          dos.write(invertedBytes);
          currentOffset += invertedBytes.length;
        }

        int entryOffset = i * KEY_METADATA_ENTRY_SIZE;
        keyMetadataSection[entryOffset] = (byte) storedType.ordinal();
        writeInt(keyMetadataSection, entryOffset + 1, values.size());
        writeLong(keyMetadataSection, entryOffset + 5, presenceOffset);
        writeLong(keyMetadataSection, entryOffset + 13, presenceBytes.length);
        writeLong(keyMetadataSection, entryOffset + 21, forwardOffset);
        writeLong(keyMetadataSection, entryOffset + 29, forwardLength);
        writeLong(keyMetadataSection, entryOffset + 37, invertedOffset);
        writeLong(keyMetadataSection, entryOffset + 45, invertedLength);
        writeLong(keyMetadataSection, entryOffset + 53, dictIdFwdOffset);
        writeLong(keyMetadataSection, entryOffset + 61, dictIdFwdLength);
      }
    }
    return baos.toByteArray();
  }

  private static void writeInt(byte[] buf, int offset, int value) {
    buf[offset] = (byte) (value >> 24);
    buf[offset + 1] = (byte) (value >> 16);
    buf[offset + 2] = (byte) (value >> 8);
    buf[offset + 3] = (byte) (value);
  }

  private static void writeLong(byte[] buf, int offset, long value) {
    for (int i = 0; i < 8; i++) {
      buf[offset + i] = (byte) (value >> ((7 - i) * 8));
    }
  }

  private byte[] buildForwardIndex(List<Object> values, DataType storedType)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    writeForwardIndex(dos, values, storedType);
    return baos.toByteArray();
  }

  private void writeForwardIndex(DataOutputStream dos, List<Object> values, DataType storedType)
      throws IOException {
    switch (storedType) {
      case INT:
        for (Object v : values) {
          dos.writeInt((Integer) v);
        }
        break;
      case LONG:
        for (Object v : values) {
          dos.writeLong((Long) v);
        }
        break;
      case FLOAT:
        for (Object v : values) {
          dos.writeFloat((Float) v);
        }
        break;
      case DOUBLE:
        for (Object v : values) {
          dos.writeDouble((Double) v);
        }
        break;
      case STRING: {
        int numValues = values.size();
        dos.writeInt(numValues);
        ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
        int[] offsets = new int[numValues + 1];
        int offset = 0;
        for (int i = 0; i < numValues; i++) {
          offsets[i] = offset;
          byte[] b = ((String) values.get(i)).getBytes(StandardCharsets.UTF_8);
          dataBaos.write(b);
          offset += b.length;
        }
        offsets[numValues] = offset;
        for (int o : offsets) {
          dos.writeInt(o);
        }
        dos.write(dataBaos.toByteArray());
        break;
      }
      case BYTES: {
        int numValues = values.size();
        dos.writeInt(numValues);
        ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
        int[] offsets = new int[numValues + 1];
        int offset = 0;
        for (int i = 0; i < numValues; i++) {
          offsets[i] = offset;
          byte[] b = (byte[]) values.get(i);
          dataBaos.write(b);
          offset += b.length;
        }
        offsets[numValues] = offset;
        for (int o : offsets) {
          dos.writeInt(o);
        }
        dos.write(dataBaos.toByteArray());
        break;
      }
      default:
        throw new IllegalStateException("Unsupported stored type for forward index: " + storedType);
    }
  }

  private TreeMap<String, RoaringBitmap> buildValueToDocIds(RoaringBitmap presence, List<Object> values,
      DataType storedType) {
    TreeMap<String, RoaringBitmap> valueToDocIds = new TreeMap<>();
    int ordinal = 0;
    for (int docId : presence) {
      Object value = values.get(ordinal);
      String keyRep = storedType.toString(value);
      valueToDocIds.computeIfAbsent(keyRep, k -> new RoaringBitmap()).add(docId);
      ordinal++;
    }
    return valueToDocIds;
  }

  private byte[] serializeInvertedIndex(TreeMap<String, RoaringBitmap> valueToDocIds)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(valueToDocIds.size());
    for (Map.Entry<String, RoaringBitmap> entry : valueToDocIds.entrySet()) {
      byte[] valueBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
      dos.writeInt(valueBytes.length);
      dos.write(valueBytes);
      byte[] bitmapBytes = RoaringBitmapUtils.serialize(entry.getValue());
      dos.writeInt(bitmapBytes.length);
      dos.write(bitmapBytes);
    }
    return baos.toByteArray();
  }

  /// Builds a sparse bit-packed dictId forward index for only the docs that have the key.
  /// Uses presence bitmap rank to map docId → ordinal, same as the raw forward index.
  /// This preserves the space benefit of columnar_map by not wasting space on absent docs.
  private byte[] buildDictIdForwardIndex(TreeMap<String, RoaringBitmap> valueToDocIds,
      DataType storedType, RoaringBitmap presence)
      throws IOException {
    // Build sorted distinct values array (no default value needed — absent docs use presence bitmap)
    String[] distinctValues = valueToDocIds.keySet().toArray(new String[0]);
    sortValues(distinctValues, storedType);

    // Build value → dictId mapping
    Map<String, Integer> valueToDictId = new HashMap<>();
    for (int i = 0; i < distinctValues.length; i++) {
      valueToDictId.put(distinctValues[i], i);
    }

    int numDocsForKey = (int) presence.getCardinality();
    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(distinctValues.length - 1, 0));

    // Build sparse dictId array: one entry per doc that has the key, in bitmap iteration order
    int[] dictIdArray = new int[numDocsForKey];
    int ordinal = 0;
    for (int docId : presence) {
      // Find which value this doc has by checking valueToDocIds bitmaps
      boolean found = false;
      for (Map.Entry<String, RoaringBitmap> entry : valueToDocIds.entrySet()) {
        if (entry.getValue().contains(docId)) {
          dictIdArray[ordinal] = valueToDictId.get(entry.getKey());
          found = true;
          break;
        }
      }
      if (!found) {
        dictIdArray[ordinal] = 0;
      }
      ordinal++;
    }

    // Write bit-packed
    long bufferSize = ((long) numDocsForKey * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    byte[] buffer = new byte[(int) bufferSize];
    writeBitPacked(buffer, dictIdArray, numDocsForKey, numBitsPerValue);
    return buffer;
  }

  /// Writes bit-packed integers into a byte array (big-endian bit order).
  private static void writeBitPacked(byte[] buffer, int[] values, int numValues, int numBitsPerValue) {
    long bitOffset = 0;
    for (int i = 0; i < numValues; i++) {
      int value = values[i];
      for (int bit = numBitsPerValue - 1; bit >= 0; bit--) {
        if (((value >> bit) & 1) == 1) {
          int byteIndex = (int) (bitOffset / 8);
          int bitIndex = (int) (7 - (bitOffset % 8));
          buffer[byteIndex] |= (1 << bitIndex);
        }
        bitOffset++;
      }
    }
  }

  /**
   * Sorts string-encoded values using numeric comparison for numeric types,
   * or lexicographic comparison for STRING/BYTES.
   */
  private static void sortValues(String[] values, DataType storedType) {
    Comparator<String> cmp;
    switch (storedType) {
      case INT:
        cmp = Comparator.comparingInt(Integer::parseInt);
        break;
      case LONG:
        cmp = Comparator.comparingLong(Long::parseLong);
        break;
      case FLOAT:
        cmp = (a, b) -> Float.compare(Float.parseFloat(a), Float.parseFloat(b));
        break;
      case DOUBLE:
        cmp = Comparator.comparingDouble(Double::parseDouble);
        break;
      default:
        cmp = null;
        break;
    }
    if (cmp != null) {
      java.util.Arrays.sort(values, cmp);
    } else {
      java.util.Arrays.sort(values);
    }
  }

  /// Builds the value dictionary section written after all per-key data.
  /// For each key with dictionary encoding: numDistinctValues(int), numBitsPerValue(int),
  /// then [valueLen(int) + valueBytes] × numDistinctValues.
  private byte[] buildValueDictionarySection(List<String> sortedKeys,
      Map<String, TreeMap<String, RoaringBitmap>> cachedValueToDocIds)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    for (String key : sortedKeys) {
      // Write value dictionary for all keys that have cached valueToDocIds
      // (includes both dictionary-encoded keys and inverted-index keys)
      if (!cachedValueToDocIds.containsKey(key)) {
        continue;
      }

      DataType dataType = _keyTypes.getOrDefault(key, _defaultValueType);
      DataType storedType = dataType.getStoredType();

      TreeMap<String, RoaringBitmap> valueToDocIds = cachedValueToDocIds.get(key);

      // Use only the actual distinct values (no default value needed for sparse dictId forward index)
      String[] allValues = valueToDocIds != null ? valueToDocIds.keySet().toArray(new String[0]) : new String[0];

      // Sort numerically for numeric types, lexicographically for strings
      sortValues(allValues, storedType);

      int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(allValues.length - 1);
      dos.writeInt(allValues.length);
      dos.writeInt(numBitsPerValue);
      for (String v : allValues) {
        byte[] vBytes = v.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(vBytes.length);
        dos.write(vBytes);
      }
    }
    return baos.toByteArray();
  }

  private void writeHeader(DataOutputStream dos, int numKeys, long keyDictOffset, long keyMetaOffset,
      long perKeyOffset, long valueDictOffset)
      throws IOException {
    dos.writeInt(MAGIC);
    dos.writeInt(VERSION);
    dos.writeInt(numKeys);
    dos.writeInt(_numDocs);
    dos.writeLong(keyDictOffset);
    dos.writeLong(keyMetaOffset);
    dos.writeLong(perKeyOffset);
    dos.writeLong(valueDictOffset);
    // 64 - (4 ints * 4 bytes) - (4 longs * 8 bytes) = 64 - 16 - 32 = 16 bytes padding
    for (int i = 0; i < 16; i++) {
      dos.write(0);
    }
  }

  @Override
  public void close()
      throws IOException {
  }
}
