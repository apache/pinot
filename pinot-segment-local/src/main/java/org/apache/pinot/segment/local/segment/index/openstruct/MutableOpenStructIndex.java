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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.segment.index.map.SimpleColumnMetadata;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.OpenStructIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructTypeInference;
import org.apache.pinot.spi.utils.PinotDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Manages per-key mutable columns for an OPEN_STRUCT column during real-time consumption.
/// Each discovered key gets its own {@link MutableKeyColumn} (dictionary-encoded forward index +
/// presence bitmap). Dense/sparse classification is deferred to seal time.
///
/// Single-writer for [#index]: the consuming thread calls this method. Readers may
/// concurrently read [#getKeys()] and [#getKeyColumns()] via the volatile map swap.
@SuppressWarnings("rawtypes")
public class MutableOpenStructIndex implements OpenStructIndexReader<ForwardIndexReaderContext>, MutableIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableOpenStructIndex.class);

  private final String _openStructColumn;
  private final OpenStructIndexConfig _config;
  private final Map<String, FieldSpec> _childFieldSpecs;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final int _capacity;

  // Volatile for lock-free reader access; writer always holds the consuming-thread lock.
  private volatile Map<String, MutableKeyColumn> _keyColumns = new HashMap<>();

  public MutableOpenStructIndex(String openStructColumn, ComplexFieldSpec fieldSpec,
      OpenStructIndexConfig config, PinotDataBufferMemoryManager memoryManager, int capacity) {
    _openStructColumn = openStructColumn;
    _config = config;
    _memoryManager = memoryManager;
    _capacity = capacity;

    Map<String, FieldSpec> childFieldSpecs = fieldSpec.getChildFieldSpecs();
    _childFieldSpecs = childFieldSpecs != null ? new HashMap<>(childFieldSpecs) : new HashMap<>();
  }

  @Override
  public void add(Object value, int dictId, int docId) {
    index(docId, value);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException("OPEN_STRUCT does not support multi-value indexing");
  }

  /// Indexes the OPEN_STRUCT value for the given document. `value` must be a
  /// `Map<String, Object>` or `null`; null and non-Map values are silently skipped.
  @SuppressWarnings("unchecked")
  public void index(int docId, @Nullable Object value) {
    if (!(value instanceof Map)) {
      return;
    }
    Map<String, Object> map = (Map<String, Object>) value;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      Object rawValue = entry.getValue();
      if (rawValue == null) {
        continue;
      }

      MutableKeyColumn keyCol = _keyColumns.get(key);
      if (keyCol == null) {
        // Mutable mode holds every observed key (see MutableOpenStructDataSource#isFullyMaterialized);
        // dense/sparse classification (maxDenseKeys / denseKeys) is applied at seal time by the segment
        // build, so no key is dropped during consumption.
        // Resolve stored type and coerce BEFORE allocating a column so a first-row coercion failure
        // does not allocate a column that was never usable.
        DataType resolvedType = resolveStoredType(key, rawValue);
        if (resolvedType == null) {
          continue;
        }
        Object coerced = tryCoerce(key, rawValue, resolvedType);
        if (coerced == null) {
          continue;
        }
        keyCol = allocateKeyColumn(key, resolvedType);
        keyCol.setValue(docId, coerced);
        continue;
      }

      DataType storedType = keyCol.getStoredType();
      Object coerced = tryCoerce(key, rawValue, storedType);
      if (coerced == null) {
        continue;
      }
      keyCol.setValue(docId, coerced);
    }
  }

  /// Resolves the stored type for a key without allocating any state. Returns null when the type
  /// cannot be inferred (caller should skip the entry).
  @Nullable
  private DataType resolveStoredType(String key, Object rawValue) {
    FieldSpec spec = _childFieldSpecs.get(key);
    DataType valueType;
    if (spec != null) {
      valueType = spec.getDataType();
    } else {
      valueType = OpenStructTypeInference.inferDataType(rawValue);
      if (valueType == null) {
        LOGGER.warn("OPEN_STRUCT '{}': could not infer DataType for key '{}' from value of class '{}'."
                + " Dropping the entry.",
            _openStructColumn, key, rawValue.getClass().getName());
        return null;
      }
    }
    return valueType.getStoredType();
  }

  /// Coerces rawValue to storedType. Returns null on failure (logged at WARN); the caller drops
  /// the entry. Note: a successful coerce of a "null"-shaped raw value would also return null —
  /// but callers gate on rawValue != null before reaching here.
  @Nullable
  private Object tryCoerce(String key, Object rawValue, DataType storedType) {
    try {
      PinotDataType sourceType = PinotDataType.getSingleValueType(rawValue);
      PinotDataType destType = ColumnDataType.fromDataTypeSV(storedType).toPinotDataType();
      return destType.convert(rawValue, sourceType);
    } catch (Exception e) {
      LOGGER.warn("OPEN_STRUCT '{}': coercion failed for key '{}' to {}. Skipping.",
          _openStructColumn, key, storedType, e);
      return null;
    }
  }

  /// Allocates a new MutableKeyColumn for {@code key} with the resolved {@code storedType} and
  /// publishes it via volatile copy-on-write.
  private MutableKeyColumn allocateKeyColumn(String key, DataType storedType) {
    String allocationContext = _openStructColumn + "$" + key;
    MutableKeyColumn newCol =
        new MutableKeyColumn(key, storedType, _memoryManager, _capacity, allocationContext);
    Map<String, MutableKeyColumn> updated = new HashMap<>(_keyColumns);
    updated.put(key, newCol);
    _keyColumns = updated;
    return newCol;
  }

  /// Returns the set of keys discovered so far.
  public Set<String> getKeys() {
    return _keyColumns.keySet();
  }

  /// Returns a snapshot of the per-key column map.
  public Map<String, MutableKeyColumn> getKeyColumns() {
    return _keyColumns;
  }

  /// Returns the {@link MutableKeyColumn} for `key`, or `null` if not seen yet.
  @Nullable
  public MutableKeyColumn getKeyColumn(String key) {
    return _keyColumns.get(key);
  }

  /// Reconstructs the OPEN_STRUCT value for {@code docId} as a {@code Map<String, Object>} from the
  /// per-key columns, including only keys present at that doc (presence-aware). Returns {@code null}
  /// when no key is present. Used by the realtime seal path to re-feed the OPEN_STRUCT column into
  /// the immutable segment build, where dense/sparse classification is (re)applied.
  @Nullable
  public Map<String, Object> getMapValue(int docId) {
    Map<String, MutableKeyColumn> keyColumns = _keyColumns;
    Map<String, Object> result = null;
    for (Map.Entry<String, MutableKeyColumn> entry : keyColumns.entrySet()) {
      Object value = entry.getValue().getValue(docId);
      if (value != null) {
        if (result == null) {
          result = new HashMap<>();
        }
        result.put(entry.getKey(), value);
      }
    }
    return result;
  }

  @Override
  public Map<IndexType, IndexReader> getIndexes(String key) {
    MutableKeyColumn col = _keyColumns.get(key);
    if (col == null) {
      return Map.of();
    }
    return Map.of(
        StandardIndexes.forward(), col.getForwardIndex(),
        StandardIndexes.dictionary(), col.getDictionary(),
        StandardIndexes.inverted(), col.getInvertedIndex());
  }

  @Nullable
  @Override
  public ColumnMetadata getColumnMetadata(String key) {
    MutableKeyColumn col = _keyColumns.get(key);
    if (col == null) {
      return null;
    }
    FieldSpec spec = _childFieldSpecs.get(key);
    if (spec == null) {
      spec = new DimensionFieldSpec(key, col.getStoredType(), true);
    }
    return new SimpleColumnMetadata(spec, _capacity);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return DataType.OPEN_STRUCT;
  }

  @Override
  public void close()
      throws IOException {
    for (MutableKeyColumn keyCol : _keyColumns.values()) {
      keyCol.close();
    }
  }
}
