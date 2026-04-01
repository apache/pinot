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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
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
 * <p>Thread-safe: uses a ReentrantReadWriteLock for concurrent reads and writes.
 */
public class MutableColumnarMapIndexImpl implements MutableIndex, ColumnarMapIndexReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(MutableColumnarMapIndexImpl.class);

  private final Map<String, DataType> _keyTypes;
  private final DataType _defaultValueType;
  private final ColumnarMapIndexConfig _config;
  private final int _maxKeys;
  private final String _columnName;

  // per-key presence bitmaps (mutable for writes)
  private final Map<String, MutableRoaringBitmap> _presenceBitmaps = new HashMap<>();
  // per-key typed value lists (ordinal-indexed)
  private final Map<String, List<Object>> _values = new HashMap<>();
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
          // Null values are treated as absent: the key is not recorded for this document.
          // There is no distinction between "key absent" and "key present with null value".
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
        if (!_presenceBitmaps.containsKey(key)) {
          _presenceBitmaps.put(key, new MutableRoaringBitmap());
          _values.put(key, new ArrayList<>());
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
        _values.get(key).add(coerced);
        if (_config.shouldEnableInvertedIndexForKey(key)) {
          String valueStr = valueType.toString(coerced);
          _invertedIndexes.get(key).computeIfAbsent(valueStr, k -> new MutableRoaringBitmap()).add(docId);
        }
      }
    } finally {
      _lock.writeLock().unlock();
    }
  }

  /// Coerces a raw value to the target stored type.
  ///
  /// For numeric types, if the value is already a {@link Number} the conversion is direct.
  /// Otherwise the value is parsed from its {@code toString()} representation, which handles
  /// the common case of JSON-decoded string numerics (e.g. {@code "42"} to INT).
  ///
  /// @throws NumberFormatException if a non-numeric string is provided for a numeric target type
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

  @Nullable
  @Override
  public DataType getKeyValueType(String key) {
    DataType type = _keyTypes.get(key);
    if (type != null) {
      return type;
    }
    _lock.readLock().lock();
    try {
      return _presenceBitmaps.containsKey(key) ? _defaultValueType : null;
    } finally {
      _lock.readLock().unlock();
    }
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
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      if (bitmap == null || !bitmap.contains(docId)) {
        return 0;
      }
      // rank(docId) returns count of elements <= docId (1-indexed), subtract 1 for 0-based list index
      int ordinal = bitmap.rank(docId) - 1;
      List<Object> vals = _values.get(key);
      return vals != null && ordinal >= 0 && ordinal < vals.size() ? (Integer) vals.get(ordinal) : 0;
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public long getLong(int docId, String key) {
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      if (bitmap == null || !bitmap.contains(docId)) {
        return 0L;
      }
      int ordinal = bitmap.rank(docId) - 1;
      List<Object> vals = _values.get(key);
      return vals != null && ordinal >= 0 && ordinal < vals.size() ? (Long) vals.get(ordinal) : 0L;
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public float getFloat(int docId, String key) {
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      if (bitmap == null || !bitmap.contains(docId)) {
        return 0.0f;
      }
      int ordinal = bitmap.rank(docId) - 1;
      List<Object> vals = _values.get(key);
      return vals != null && ordinal >= 0 && ordinal < vals.size() ? (Float) vals.get(ordinal) : 0.0f;
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public double getDouble(int docId, String key) {
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      if (bitmap == null || !bitmap.contains(docId)) {
        return 0.0;
      }
      int ordinal = bitmap.rank(docId) - 1;
      List<Object> vals = _values.get(key);
      return vals != null && ordinal >= 0 && ordinal < vals.size() ? (Double) vals.get(ordinal) : 0.0;
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public String getString(int docId, String key) {
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      if (bitmap == null || !bitmap.contains(docId)) {
        return "";
      }
      int ordinal = bitmap.rank(docId) - 1;
      List<Object> vals = _values.get(key);
      return vals != null && ordinal >= 0 && ordinal < vals.size() ? String.valueOf(vals.get(ordinal)) : "";
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public byte[] getBytes(int docId, String key) {
    _lock.readLock().lock();
    try {
      MutableRoaringBitmap bitmap = _presenceBitmaps.get(key);
      if (bitmap == null || !bitmap.contains(docId)) {
        return new byte[0];
      }
      int ordinal = bitmap.rank(docId) - 1;
      List<Object> vals = _values.get(key);
      return vals != null && ordinal >= 0 && ordinal < vals.size() ? (byte[]) vals.get(ordinal) : new byte[0];
    } finally {
      _lock.readLock().unlock();
    }
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
    // Implemented in ColumnarMapDataSource (Task 15)
    return null;
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
        int ordinal = bitmap.rank(docId) - 1;
        List<Object> vals = _values.get(key);
        if (vals != null && ordinal >= 0 && ordinal < vals.size()) {
          result.put(key, vals.get(ordinal));
        }
      }
      return result;
    } finally {
      _lock.readLock().unlock();
    }
  }

  @Override
  public void close()
      throws IOException {
    // Nothing to close for in-memory state
  }
}
