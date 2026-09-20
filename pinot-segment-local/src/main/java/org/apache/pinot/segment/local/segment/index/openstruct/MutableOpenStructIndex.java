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
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
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
import org.apache.pinot.spi.data.OpenStructKeyFlattener;
import org.apache.pinot.spi.data.OpenStructTypeInference;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.utils.PinotDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Manages per-key mutable columns for an OPEN_STRUCT column during real-time consumption.
/// Each discovered key gets its own [MutableKeyColumn] (dictionary-encoded forward index +
/// presence bitmap). Dense/sparse classification is deferred to seal time.
///
/// Single-writer for [#index]: the consuming thread calls this method. Readers may
/// concurrently read [#getKeys()] and [#getKeyColumns()] via the volatile map swap.
@SuppressWarnings("rawtypes")
public class MutableOpenStructIndex implements OpenStructIndexReader<ForwardIndexReaderContext>, MutableIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableOpenStructIndex.class);

  private final String _openStructColumn;
  private final String _tableNameWithType;
  private final OpenStructIndexConfig _config;
  private final int _maxNestedKeyDepth;
  private final Map<String, FieldSpec> _childFieldSpecs;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final int _capacity;

  // Volatile copy-on-write: the writer (consuming thread) creates a fresh HashMap copy and publishes
  // atomically via volatile write (see allocateKeyColumn). Readers see a consistent snapshot of the
  // entire map. ConcurrentHashMap is NOT appropriate here — it would allow readers to observe
  // partially-updated state during a put. Single-writer is guaranteed by the Pinot consuming thread
  // model (one thread per partition).
  private volatile Map<String, MutableKeyColumn> _keyColumns = new HashMap<>();
  // Single-writer (see #index), but close() may run on a different thread, so volatile for
  // visibility; flushed to ServerMetrics on close() to avoid a metered-value call on every
  // ignored key of every consumed row.
  private volatile long _ignoredKeyDropCount;
  // Single-writer (see #index): the consuming thread caches and reuses the PinotMeter, which skips
  // the per-value metric-name rebuild and registry lookup addMeteredTableValue would otherwise do,
  // while still marking the meter live instead of batching the count to close().
  @Nullable
  private PinotMeter _typeCoercionFailureMeter;
  @Nullable
  private PinotMeter _typeInferenceFailureMeter;

  public MutableOpenStructIndex(String openStructColumn, String tableNameWithType, ComplexFieldSpec fieldSpec,
      OpenStructIndexConfig config, PinotDataBufferMemoryManager memoryManager, int capacity) {
    _openStructColumn = openStructColumn;
    _tableNameWithType = tableNameWithType;
    _config = config;
    _maxNestedKeyDepth = config.getMaxNestedKeyDepth();
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
    // Flattened the same way as the sealed build path ([OpenStructColumnSplitter#addMap]) so a nested value resolves
    // under the same key before and after seal. Consuming mode materializes every key, so the container flag the
    // splitter uses for dense selection is not needed here.
    OpenStructKeyFlattener.flatten(map, _maxNestedKeyDepth, (key, rawValue, container) ->
        indexEntry(docId, key, rawValue));
  }

  private void indexEntry(int docId, String key, @Nullable Object rawValue) {
    if (rawValue == null) {
      return;
    }
    if (_config.isIgnoredKey(key)) {
      _ignoredKeyDropCount++;
      return;
    }

    MutableKeyColumn keyCol = _keyColumns.get(key);
    Object[] elements = OpenStructTypeInference.asMultiValue(rawValue);
    if (elements != null && elements.length == 0) {
      // No elements, so no value: the key is not in this document, the same as a null value above. Matches
      // OpenStructColumnSplitter, where a materialized multi-value column has no empty state to store.
      return;
    }
    if (keyCol != null && elements != null && keyCol.isSingleValue()) {
      // Shape is fixed by the first value, as on the sealed side: a collection arriving on a scalar key is
      // handled as any other value the column cannot represent, not by reshaping the column underneath it.
      elements = null;
    }
    if (keyCol == null) {
      // Mutable mode holds every observed key (see MutableOpenStructDataSource#isFullyMaterialized);
      // dense/sparse classification (maxDenseKeys / denseKeys) is applied at seal time by the segment
      // build, so no key is dropped during consumption.
      // Resolve stored type and coerce BEFORE allocating a column so a first-row coercion failure
      // does not allocate a column that was never usable.
      // A declaration decides the shape in both directions; only an undeclared key takes it from the data.
      FieldSpec declaredSpec = _childFieldSpecs.get(key);
      boolean multiValue = declaredSpec != null ? !declaredSpec.isSingleValueField() : elements != null;
      if (!multiValue) {
        elements = null;
      }
      DataType resolvedType = multiValue
          ? resolveElementStoredType(key, elements)
          : resolveStoredType(key, rawValue, null);
      PinotDataType destType = ColumnDataType.fromDataTypeSV(resolvedType).toPinotDataType();
      Object coerced = elements != null ? tryCoerceAll(key, elements, destType) : tryCoerce(key, rawValue, destType);
      if (coerced == null) {
        return;
      }
      keyCol = allocateKeyColumn(key, resolvedType, !multiValue);
      setOn(keyCol, docId, coerced);
      return;
    }

    if (keyCol.needsInferenceCheck()) {
      meterIfUninferable(rawValue);
    }
    Object coerced = elements != null
        ? tryCoerceAll(key, elements, keyCol.getDestType())
        : tryCoerce(key, rawValue, keyCol.getDestType());
    if (coerced == null) {
      return;
    }
    setOn(keyCol, docId, coerced);
  }

  /// Writes a coerced value, wrapping a scalar into a one-element list on a multi-value key -- the shape the
  /// column was built with wins, the same as on the sealed side. A list too long for the column is dropped and
  /// metered like a coercion failure rather than failing the whole segment.
  private void setOn(MutableKeyColumn keyCol, int docId, Object coerced) {
    if (keyCol.isSingleValue()) {
      keyCol.setValue(docId, coerced);
      return;
    }
    Object[] values = coerced instanceof Object[] ? (Object[]) coerced : new Object[]{coerced};
    try {
      keyCol.setValues(docId, values);
    } catch (IllegalArgumentException e) {
      _typeCoercionFailureMeter = meterFailure(ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES,
          _typeCoercionFailureMeter);
    }
  }

  /// Stored type for a multi-value key: the declared child spec when there is one, otherwise the element type.
  private DataType resolveElementStoredType(String key, @Nullable Object[] elements) {
    FieldSpec spec = _childFieldSpecs.get(key);
    if (spec != null) {
      return spec.getDataType().getStoredType();
    }
    DataType inferred = elements == null ? null : OpenStructTypeInference.inferElementDataType(elements);
    if (inferred == null) {
      // Empty, or all nulls: STRING holds whatever the key turns out to carry.
      return DataType.STRING;
    }
    return inferred;
  }

  /// Coerces every element, or returns null when any of them fails -- an array's length is part of its value, so
  /// dropping one element would shift every index after it.
  @Nullable
  private Object[] tryCoerceAll(String key, Object[] elements, PinotDataType destType) {
    Object[] coerced = new Object[elements.length];
    for (int i = 0; i < elements.length; i++) {
      if (elements[i] == null) {
        _typeCoercionFailureMeter = meterFailure(ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES,
            _typeCoercionFailureMeter);
        return null;
      }
      coerced[i] = tryCoerce(key, elements[i], destType);
      if (coerced[i] == null) {
        return null;
      }
    }
    return coerced;
  }

  /// Resolves the stored type for a key without allocating any state, and meters a value that took
  /// the STRING fallback. `establishedType` is the key's already-resolved stored type, or `null` on
  /// first sighting.
  ///
  /// The fallback rule (unmappable value → STRING) must match the sealed build path
  /// ([OpenStructColumnSplitter#addMap]) so a value reads the same before and after seal.
  private DataType resolveStoredType(String key, Object rawValue, @Nullable DataType establishedType) {
    FieldSpec spec = _childFieldSpecs.get(key);
    if (spec != null) {
      return spec.getDataType().getStoredType();
    }
    if (establishedType != null && establishedType != DataType.STRING) {
      return establishedType;
    }
    DataType inferred = OpenStructTypeInference.inferDataType(rawValue);
    if (inferred == null) {
      if (establishedType == null) {
        LOGGER.warn("OPEN_STRUCT '{}': could not infer DataType for key '{}' from value of class '{}'."
                + " Falling back to STRING.",
            _openStructColumn, key, rawValue.getClass().getName());
      }
      _typeInferenceFailureMeter = meterFailure(ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES,
          _typeInferenceFailureMeter);
      return DataType.STRING;
    }
    return establishedType != null ? establishedType : inferred;
  }

  /// Counts an inference failure for a value on a STRING-fallback key. This is the metering-only
  /// half of [#resolveStoredType]: on the established path that method always returns the key's
  /// own stored type, so the return value is unused and only the side effect matters.
  private void meterIfUninferable(Object rawValue) {
    if (OpenStructTypeInference.inferDataType(rawValue) == null) {
      _typeInferenceFailureMeter = meterFailure(ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES,
          _typeInferenceFailureMeter);
    }
  }

  /// Coerces rawValue to storedType. Returns null on failure; the caller drops the entry. Failures
  /// are reported through [ServerMeter#OPEN_STRUCT_TYPE_COERCION_FAILURES] rather than a log line,
  /// because this runs per value on the consuming path. Note: a successful coerce of a
  /// "null"-shaped raw value would also return null — but callers gate on rawValue != null before
  /// reaching here.
  @Nullable
  private Object tryCoerce(String key, Object rawValue, PinotDataType destType) {
    try {
      PinotDataType sourceType = PinotDataType.getSingleValueType(rawValue);
      return destType.convert(rawValue, sourceType);
    } catch (Exception e) {
      _typeCoercionFailureMeter = meterFailure(ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES,
          _typeCoercionFailureMeter);
      return null;
    }
  }

  /// Marks one occurrence on `meter`, reusing `reusedMeter` when present to skip the metric-name
  /// rebuild and registry lookup a fresh [ServerMetrics#addMeteredTableValue] call would do. Returns
  /// the meter to reuse on the next call (unchanged when no [ServerMetrics] is registered).
  private PinotMeter meterFailure(ServerMeter meter, @Nullable PinotMeter reusedMeter) {
    ServerMetrics serverMetrics = ServerMetrics.get();
    if (serverMetrics == null) {
      return reusedMeter;
    }
    return serverMetrics.addMeteredTableValue(_tableNameWithType, _openStructColumn, meter, 1, reusedMeter);
  }

  /// Allocates a new MutableKeyColumn for `key` with the resolved `storedType` and
  /// publishes it via volatile copy-on-write.
  private MutableKeyColumn allocateKeyColumn(String key, DataType storedType, boolean singleValue) {
    String allocationContext = _openStructColumn + "$" + key;
    // A declared key takes its declared default, computed from the declared data type rather than the type it is
    // stored as -- the same rule OpenStructColumnSplitter#materializedFieldSpec applies on the sealed side, so a doc
    // without the key resolves identically before and after seal.
    FieldSpec declared = _childFieldSpecs.get(key);
    Object defaultNullValue = declared != null
        ? declared.getDefaultNullValue()
        : FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, storedType, null);
    boolean needsInferenceCheck = !_childFieldSpecs.containsKey(key) && storedType == DataType.STRING;
    MutableKeyColumn newCol = new MutableKeyColumn(key, storedType, defaultNullValue, _memoryManager, _capacity,
        allocationContext, needsInferenceCheck, singleValue);
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

  /// Returns the [MutableKeyColumn] for `key`, or `null` if not seen yet.
  @Nullable
  public MutableKeyColumn getKeyColumn(String key) {
    return _keyColumns.get(key);
  }

  /// Reconstructs the OPEN_STRUCT value for `docId` as a `Map<String, Object>` from the
  /// per-key columns, including only keys present at that doc (presence-aware). Returns `null`
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
        StandardIndexes.forward(), col.getGuardedForwardIndex(),
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
      // Shape comes from the column, not a fixed single-value assumption: a key holding lists has a multi-value
      // forward index, and metadata that disagreed with it would tell the query planner the wrong thing.
      spec = new DimensionFieldSpec(key, col.getStoredType(), col.isSingleValue());
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
    try {
      flushMeters();
    } finally {
      for (MutableKeyColumn keyCol : _keyColumns.values()) {
        keyCol.close();
      }
    }
  }

  /// Emits the batched ignored-key-drop counter. It accumulates per row on the consuming path and
  /// is flushed once here, mirroring what [OpenStructColumnSplitter] does at seal time. Zeroed after
  /// emitting so a second close() (e.g. destroy() after commit()) does not double-count.
  private void flushMeters() {
    if (_ignoredKeyDropCount > 0) {
      ServerMetrics serverMetrics = ServerMetrics.get();
      if (serverMetrics != null) {
        serverMetrics.addMeteredTableValue(_tableNameWithType, _openStructColumn,
            ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS, _ignoredKeyDropCount);
      }
      _ignoredKeyDropCount = 0;
    }
  }
}
