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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.ColumnarMapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * In-memory mutable ColumnarMap index for real-time segments.
 * Implements both {@link MutableIndex} (for segment indexing) and
 * {@link ColumnarMapIndexReader} (for query access during real-time serving).
 *
 * <p>Each MAP key gets a dense per-key forward index ({@link FixedByteSVMutableForwardIndex})
 * indexed by docId, providing O(1) lock-free reads. For dictionary-encoded keys, a
 * {@link MutableDictionary} maps values to dictIds stored in the forward index. For raw
 * (noDictionary) keys, typed values are stored directly.
 *
 * <p>Thread-safe: writes use a {@link ReentrantReadWriteLock}. Forward index reads are lock-free
 * via the {@link java.util.concurrent.CopyOnWriteArrayList} pattern inside
 * {@link FixedByteSVMutableForwardIndex}.
 */
public class MutableColumnarMapIndexImpl implements MutableIndex, ColumnarMapIndexReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(MutableColumnarMapIndexImpl.class);
  private static final int NUM_ROWS_PER_CHUNK = 1024;

  private final Map<String, DataType> _keyTypes;
  private final DataType _defaultValueType;
  private final ColumnarMapIndexConfig _config;
  private final int _maxKeys;
  private final String _columnName;
  private final PinotDataBufferMemoryManager _memoryManager;

  // per-key presence bitmaps (mutable for writes)
  private final Map<String, MutableRoaringBitmap> _presenceBitmaps = new HashMap<>();
  // per-key dense forward indexes indexed by docId (lock-free reads)
  private final ConcurrentHashMap<String, MutableForwardIndex> _perKeyForwardIndexes = new ConcurrentHashMap<>();
  // per-key mutable dictionaries for dictionary-encoded keys
  private final ConcurrentHashMap<String, MutableDictionary> _perKeyDictionaries = new ConcurrentHashMap<>();
  // per-key dictId-based inverted indexes for dictionary-encoded keys (used by InvertedIndexFilterOperator)
  private final ConcurrentHashMap<String, ColumnarMapRealtimeInvertedIndex> _perKeyInvertedIndexes =
      new ConcurrentHashMap<>();
  // tracks which keys are dictionary-encoded
  private final ConcurrentHashMap<String, Boolean> _isDictEncoded = new ConcurrentHashMap<>();
  // optional inverted index: key -> value-string -> docId bitmap
  private final Map<String, TreeMap<String, MutableRoaringBitmap>> _invertedIndexes = new HashMap<>();

  private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
  private int _distinctKeyCount;
  private int _droppedKeyCount;

  public MutableColumnarMapIndexImpl(MutableIndexContext context, ColumnarMapIndexConfig config) {
    this(context, config, null, null);
  }

  public MutableColumnarMapIndexImpl(MutableIndexContext context, ColumnarMapIndexConfig config,
      @Nullable Map<String, DataType> explicitKeyTypes,
      @Nullable DataType explicitDefaultValueType) {
    FieldSpec fieldSpec = context.getFieldSpec();
    Map<String, DataType> keyTypes = explicitKeyTypes;
    DataType defaultType = explicitDefaultValueType;
    if (keyTypes == null && fieldSpec instanceof ComplexFieldSpec) {
      ComplexFieldSpec.MapFieldSpec mapFieldSpec = ComplexFieldSpec.toMapFieldSpec((ComplexFieldSpec) fieldSpec);
      keyTypes = mapFieldSpec.getKeyTypes();
      defaultType = mapFieldSpec.getDefaultValueType();
    }
    _keyTypes = keyTypes != null ? new HashMap<>(keyTypes) : new HashMap<>();
    _defaultValueType = defaultType != null ? defaultType : DataType.STRING;
    _config = config;
    _maxKeys = config.getMaxKeys();
    _columnName = context.getFieldSpec().getName();
    _memoryManager = context.getMemoryManager();
  }

  // ---- MutableIndex methods ----

  @Override
  public void add(Object value, int dictId, int docId) {
    if (!(value instanceof Map)) {
      return;
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> columnarMap = (Map<String, Object>) value;
    _lock.writeLock().lock();
    try {
      for (Map.Entry<String, Object> entry : columnarMap.entrySet()) {
        String key = entry.getKey();
        Object rawValue = entry.getValue();
        if (rawValue == null) {
          continue;
        }
        if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
          _droppedKeyCount++;
          if (_droppedKeyCount == 1 || _droppedKeyCount % 1000 == 0) {
            LOGGER.warn(
                "MutableColumnarMapIndex for column '{}' reached maxKeys limit ({}). Key '{}' dropped. "
                    + "Total drops: {}.",
                _columnName, _maxKeys, key, _droppedKeyCount);
          }
          continue;
        }
        DataType valueType = _keyTypes.getOrDefault(key, _defaultValueType);
        DataType storedType = valueType.getStoredType();
        if (!_presenceBitmaps.containsKey(key)) {
          _presenceBitmaps.put(key, new MutableRoaringBitmap());
          initPerKeyStorage(key, storedType);
          if (_config.shouldEnableInvertedIndexForKey(key)) {
            _invertedIndexes.put(key, new TreeMap<>());
          }
          _distinctKeyCount++;
        }
        Object coerced;
        try {
          coerced = coerceValue(rawValue, valueType);
        } catch (ClassCastException | NumberFormatException e) {
          LOGGER.warn(
              "MutableColumnarMapIndex for column '{}': failed to coerce value '{}' (type {}) to {} for key '{}'"
                  + " in docId {}. Skipping key for this document.",
              _columnName, rawValue, rawValue.getClass().getSimpleName(), valueType, key, docId, e);
          continue;
        }
        _presenceBitmaps.get(key).add(docId);
        writeValueToForwardIndex(key, docId, coerced, storedType);
        if (_config.shouldEnableInvertedIndexForKey(key)) {
          String valueStr = valueType.toString(coerced);
          _invertedIndexes.get(key).computeIfAbsent(valueStr, k -> new MutableRoaringBitmap()).add(docId);
        }
      }
    } finally {
      _lock.writeLock().unlock();
    }
  }

  private void initPerKeyStorage(String key, DataType storedType) {
    boolean useDictionary = _config.shouldUseDictionaryForKey(key);
    _isDictEncoded.put(key, useDictionary);

    String allocationContext = _columnName + "." + key;
    if (useDictionary) {
      // Dictionary-encoded: forward index stores INT dictIds
      MutableDictionary dict = MutableDictionaryFactory.getMutableDictionary(
          storedType, false, _memoryManager, 0, 0, allocationContext + ".dict");
      // Pre-index default value at dictId 0 so zeroed forward index slots map to the correct default
      dict.index(getDefaultValue(storedType));
      _perKeyDictionaries.put(key, dict);
      _perKeyForwardIndexes.put(key,
          new FixedByteSVMutableForwardIndex(true, DataType.INT, NUM_ROWS_PER_CHUNK, _memoryManager,
              allocationContext + ".fwd"));
      // DictId-based inverted index for InvertedIndexFilterOperator.
      // Use ColumnarMapRealtimeInvertedIndex which handles non-sequential dictIds (gap-safe),
      // since the default value is pre-indexed at dictId 0 in the dictionary but may never
      // appear in actual documents.
      ColumnarMapRealtimeInvertedIndex invertedIndex = new ColumnarMapRealtimeInvertedIndex();
      _perKeyInvertedIndexes.put(key, invertedIndex);
    } else {
      // Raw: forward index stores typed values directly
      _perKeyForwardIndexes.put(key,
          new FixedByteSVMutableForwardIndex(false, storedType, NUM_ROWS_PER_CHUNK, _memoryManager,
              allocationContext + ".fwd"));
    }
  }

  private void writeValueToForwardIndex(String key, int docId, Object coerced, DataType storedType) {
    MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
    if (_isDictEncoded.get(key)) {
      MutableDictionary dict = _perKeyDictionaries.get(key);
      int valueDictId = dict.index(coerced);
      fwdIndex.setDictId(docId, valueDictId);
      ColumnarMapRealtimeInvertedIndex invertedIndex = _perKeyInvertedIndexes.get(key);
      if (invertedIndex != null) {
        invertedIndex.add(valueDictId, docId);
      }
    } else {
      switch (storedType) {
        case INT:
          fwdIndex.setInt(docId, (Integer) coerced);
          break;
        case LONG:
          fwdIndex.setLong(docId, (Long) coerced);
          break;
        case FLOAT:
          fwdIndex.setFloat(docId, (Float) coerced);
          break;
        case DOUBLE:
          fwdIndex.setDouble(docId, (Double) coerced);
          break;
        case STRING:
          fwdIndex.setString(docId, (String) coerced);
          break;
        case BYTES:
          fwdIndex.setBytes(docId, (byte[]) coerced);
          break;
        default:
          fwdIndex.setString(docId, coerced.toString());
          break;
      }
    }
  }

  private static Object getDefaultValue(DataType storedType) {
    switch (storedType) {
      case INT:
        return 0;
      case LONG:
        return 0L;
      case FLOAT:
        return 0.0f;
      case DOUBLE:
        return 0.0;
      case STRING:
        return "";
      case BYTES:
        return new byte[0];
      default:
        return "";
    }
  }

  /// Coerces a raw value to the target stored type.
  private Object coerceValue(Object value, DataType dataType) {
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
        return value instanceof byte[] ? value : value.toString().getBytes(StandardCharsets.UTF_8);
      default:
        return value.toString();
    }
  }

  // ---- ColumnarMapIndexReader methods ----

  @Override
  public Set<String> getKeys() {
    _lock.readLock().lock();
    try {
      return new HashSet<>(_presenceBitmaps.keySet());
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public DataType getKeyValueType(String key) {
    DataType type = _keyTypes.get(key);
    return type != null ? type : _defaultValueType;
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds, int docId) {
    if (values != null && values.length > 0) {
      add(values[0], dictIds != null ? dictIds[0] : -1, docId);
    }
  }

  @Override
  public int getNumDocsWithKey(String key) {
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      return bitmap != null ? (int) bitmap.getLongCardinality() : 0;
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public ImmutableRoaringBitmap getPresenceBitmap(String key) {
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      return bitmap != null ? bitmap.clone().toImmutableRoaringBitmap() : ImmutableRoaringBitmap.bitmapOf();
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public int getInt(int docId, String key) {
    MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
    if (fwdIndex == null) {
      return 0;
    }
    if (Boolean.TRUE.equals(_isDictEncoded.get(key))) {
      int dictId = fwdIndex.getDictId(docId);
      MutableDictionary dict = _perKeyDictionaries.get(key);
      return dict.getIntValue(dictId);
    }
    return fwdIndex.getInt(docId);
  }

  @Override
  public long getLong(int docId, String key) {
    MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
    if (fwdIndex == null) {
      return 0L;
    }
    if (Boolean.TRUE.equals(_isDictEncoded.get(key))) {
      int dictId = fwdIndex.getDictId(docId);
      MutableDictionary dict = _perKeyDictionaries.get(key);
      return dict.getLongValue(dictId);
    }
    return fwdIndex.getLong(docId);
  }

  @Override
  public float getFloat(int docId, String key) {
    MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
    if (fwdIndex == null) {
      return 0.0f;
    }
    if (Boolean.TRUE.equals(_isDictEncoded.get(key))) {
      int dictId = fwdIndex.getDictId(docId);
      MutableDictionary dict = _perKeyDictionaries.get(key);
      return dict.getFloatValue(dictId);
    }
    return fwdIndex.getFloat(docId);
  }

  @Override
  public double getDouble(int docId, String key) {
    MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
    if (fwdIndex == null) {
      return 0.0;
    }
    if (Boolean.TRUE.equals(_isDictEncoded.get(key))) {
      int dictId = fwdIndex.getDictId(docId);
      MutableDictionary dict = _perKeyDictionaries.get(key);
      return dict.getDoubleValue(dictId);
    }
    return fwdIndex.getDouble(docId);
  }

  @Override
  public String getString(int docId, String key) {
    MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
    if (fwdIndex == null) {
      return "";
    }
    if (Boolean.TRUE.equals(_isDictEncoded.get(key))) {
      int dictId = fwdIndex.getDictId(docId);
      MutableDictionary dict = _perKeyDictionaries.get(key);
      return dict.getStringValue(dictId);
    }
    return fwdIndex.getString(docId);
  }

  @Override
  public byte[] getBytes(int docId, String key) {
    MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
    if (fwdIndex == null) {
      return new byte[0];
    }
    if (Boolean.TRUE.equals(_isDictEncoded.get(key))) {
      int dictId = fwdIndex.getDictId(docId);
      MutableDictionary dict = _perKeyDictionaries.get(key);
      return dict.getBytesValue(dictId);
    }
    return fwdIndex.getBytes(docId);
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getDocsWithKeyValue(String key, Object value) {
    if (!_config.shouldEnableInvertedIndexForKey(key)) {
      return null;
    }
    _lock.readLock().lock();
    try {
      TreeMap<String, MutableRoaringBitmap> inv = _invertedIndexes.get(key);
      if (inv == null) {
        return null;
      }
      DataType type = _keyTypes.getOrDefault(key, _defaultValueType);
      String valueStr = type.toString(value);
      MutableRoaringBitmap bitmap = inv.get(valueStr);
      return bitmap != null ? bitmap.clone().toImmutableRoaringBitmap() : ImmutableRoaringBitmap.bitmapOf();
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public boolean hasInvertedIndex(String key) {
    _lock.readLock().lock();
    try {
      TreeMap<String, MutableRoaringBitmap> inv = _invertedIndexes.get(key);
      return inv != null && !inv.isEmpty();
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Nullable
  @Override
  public String[] getDistinctValuesForKey(String key) {
    _lock.readLock().lock();
    try {
      TreeMap<String, MutableRoaringBitmap> inv = _invertedIndexes.get(key);
      return inv != null ? inv.keySet().toArray(new String[0]) : null;
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public DataSource getKeyDataSource(String key) {
    // Per-key DataSource construction lives in ColumnarMapDataSource. Calling this method
    // directly bypasses the data source layer and would skip the unknown-key fall-back to
    // NullDataSource that callers rely on.
    throw new UnsupportedOperationException("Use ColumnarMapDataSource.getKeyDataSource() instead");
  }

  @Override
  public Map<String, Object> getMap(int docId) {
    _lock.readLock().lock();
    try {
      Map<String, Object> result = new HashMap<>();
      for (Map.Entry<String, MutableRoaringBitmap> entry : _presenceBitmaps.entrySet()) {
        String key = entry.getKey();
        MutableRoaringBitmap bitmap = entry.getValue();
        if (!bitmap.contains(docId)) {
          continue;
        }
        DataType valueType = _keyTypes.getOrDefault(key, _defaultValueType);
        DataType storedType = valueType.getStoredType();
        MutableForwardIndex fwdIndex = _perKeyForwardIndexes.get(key);
        if (fwdIndex == null) {
          continue;
        }
        if (Boolean.TRUE.equals(_isDictEncoded.get(key))) {
          int dictId = fwdIndex.getDictId(docId);
          MutableDictionary dict = _perKeyDictionaries.get(key);
          result.put(key, getTypedValueFromDict(dict, dictId, storedType));
        } else {
          result.put(key, getTypedValueFromFwdIndex(fwdIndex, docId, storedType));
        }
      }
      return result;
    } finally {
      _lock.readLock().unlock();
    }
  }

  private static Object getTypedValueFromDict(MutableDictionary dict, int dictId, DataType storedType) {
    switch (storedType) {
      case INT:
        return dict.getIntValue(dictId);
      case LONG:
        return dict.getLongValue(dictId);
      case FLOAT:
        return dict.getFloatValue(dictId);
      case DOUBLE:
        return dict.getDoubleValue(dictId);
      case STRING:
        return dict.getStringValue(dictId);
      case BYTES:
        return dict.getBytesValue(dictId);
      default:
        return dict.getStringValue(dictId);
    }
  }

  private static Object getTypedValueFromFwdIndex(MutableForwardIndex fwdIndex, int docId, DataType storedType) {
    switch (storedType) {
      case INT:
        return fwdIndex.getInt(docId);
      case LONG:
        return fwdIndex.getLong(docId);
      case FLOAT:
        return fwdIndex.getFloat(docId);
      case DOUBLE:
        return fwdIndex.getDouble(docId);
      case STRING:
        return fwdIndex.getString(docId);
      case BYTES:
        return fwdIndex.getBytes(docId);
      default:
        return fwdIndex.getString(docId);
    }
  }

  // ---- Accessors for ColumnarMapDataSource ----

  /// Ensures per-key storage exists for the given key, even if no documents have been ingested
  /// with this key yet. Called by {@link ColumnarMapDataSource} when a query references a key
  /// that has a known type but was never ingested. The forward index will return default values
  /// for all docIds.
  public void ensureKeyStorage(String key) {
    if (_perKeyForwardIndexes.containsKey(key)) {
      return;
    }
    DataType valueType = _keyTypes.getOrDefault(key, _defaultValueType);
    DataType storedType = valueType.getStoredType();
    _lock.writeLock().lock();
    try {
      if (!_perKeyForwardIndexes.containsKey(key)) {
        initPerKeyStorage(key, storedType);
      }
    } finally {
      _lock.writeLock().unlock();
    }
  }

  @Nullable
  public MutableForwardIndex getPerKeyForwardIndex(String key) {
    return _perKeyForwardIndexes.get(key);
  }

  @Nullable
  public MutableDictionary getPerKeyDictionary(String key) {
    return _perKeyDictionaries.get(key);
  }

  public boolean isDictionaryEncodedKey(String key) {
    return Boolean.TRUE.equals(_isDictEncoded.get(key));
  }

  @Nullable
  public ColumnarMapRealtimeInvertedIndex getPerKeyInvertedIndex(String key) {
    return _perKeyInvertedIndexes.get(key);
  }

  @Override
  public void close()
      throws IOException {
    for (MutableForwardIndex fwdIndex : _perKeyForwardIndexes.values()) {
      fwdIndex.close();
    }
    for (MutableDictionary dict : _perKeyDictionaries.values()) {
      dict.close();
    }
  }
}
