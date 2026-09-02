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
import org.apache.pinot.segment.spi.index.creator.OpenStructColumnarSource;
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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
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
  // Batched for the same reason as _ignoredKeyDropCount: keep a metered-value call, which rebuilds
  // the metric name and hits the registry, off the per-row consuming path. Flushed in close().
  private volatile long _typeCoercionFailureCount;
  private volatile long _typeInferenceFailureCount;

  public MutableOpenStructIndex(String openStructColumn, String tableNameWithType, ComplexFieldSpec fieldSpec,
      OpenStructIndexConfig config, PinotDataBufferMemoryManager memoryManager, int capacity) {
    _openStructColumn = openStructColumn;
    _tableNameWithType = tableNameWithType;
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
      if (_config.isIgnoredKey(key)) {
        _ignoredKeyDropCount++;
        continue;
      }

      MutableKeyColumn keyCol = _keyColumns.get(key);
      if (keyCol == null) {
        // Mutable mode holds every observed key (see MutableOpenStructDataSource#isFullyMaterialized);
        // dense/sparse classification (maxDenseKeys / denseKeys) is applied at seal time by the segment
        // build, so no key is dropped during consumption.
        // Resolve stored type and coerce BEFORE allocating a column so a first-row coercion failure
        // does not allocate a column that was never usable.
        DataType resolvedType = resolveStoredType(key, rawValue, null);
        Object coerced = tryCoerce(key, rawValue,
            ColumnDataType.fromDataTypeSV(resolvedType).toPinotDataType());
        if (coerced == null) {
          continue;
        }
        keyCol = allocateKeyColumn(key, resolvedType);
        keyCol.setValue(docId, coerced);
        continue;
      }

      if (keyCol.needsInferenceCheck()) {
        meterIfUninferable(rawValue);
      }
      Object coerced = tryCoerce(key, rawValue, keyCol.getDestType());
      if (coerced == null) {
        continue;
      }
      keyCol.setValue(docId, coerced);
    }
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
      _typeInferenceFailureCount++;
      return DataType.STRING;
    }
    return establishedType != null ? establishedType : inferred;
  }

  /// Counts an inference failure for a value on a STRING-fallback key. This is the metering-only
  /// half of [#resolveStoredType]: on the established path that method always returns the key's
  /// own stored type, so the return value is unused and only the side effect matters.
  private void meterIfUninferable(Object rawValue) {
    if (OpenStructTypeInference.inferDataType(rawValue) == null) {
      _typeInferenceFailureCount++;
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
      _typeCoercionFailureCount++;
      return null;
    }
  }

  /// Allocates a new MutableKeyColumn for `key` with the resolved `storedType` and
  /// publishes it via volatile copy-on-write.
  private MutableKeyColumn allocateKeyColumn(String key, DataType storedType) {
    String allocationContext = _openStructColumn + "$" + key;
    // Use the standard dimension default for the resolved stored type, regardless of any child
    // spec's own default: OpenStructColumnSplitter#writeDenseKeyColumn (the sealed build path)
    // always derives the absent-doc default from a throwaway DimensionFieldSpec(key, storedType,
    // true) rather than the real child spec, so mirroring that exactly is what keeps a doc's
    // resolved value identical before and after seal.
    Object defaultNullValue = FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, storedType, null);
    boolean needsInferenceCheck = !_childFieldSpecs.containsKey(key) && storedType == DataType.STRING;
    MutableKeyColumn newCol = new MutableKeyColumn(key, storedType, defaultNullValue, _memoryManager, _capacity,
        allocationContext, needsInferenceCheck);
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

  /// Returns a columnar snapshot of this index over `[0, numDocs)`, for the seal path to hand
  /// straight to the segment's OPEN_STRUCT index creator. The key-column map is read once so the
  /// snapshot is stable even if the consuming thread allocates a new key afterwards.
  public OpenStructColumnarSource asColumnarSource(int numDocs) {
    Map<String, MutableKeyColumn> keyColumns = _keyColumns;
    return new OpenStructColumnarSource() {
      @Override
      public int getNumDocs() {
        return numDocs;
      }

      @Override
      public Set<String> getKeys() {
        return keyColumns.keySet();
      }

      @Override
      public DataType getStoredType(String key) {
        return keyColumns.get(key).getStoredType();
      }

      @Override
      public void forEachPresentValue(String key, PresentValueConsumer consumer) {
        MutableKeyColumn column = keyColumns.get(key);
        // ThreadSafeMutableRoaringBitmap#getMutableRoaringBitmap() already clones under its own
        // monitor, so the copy returned here is safe to iterate even though the consuming thread
        // may still be adding docIds; this snapshot is bounded at numDocs regardless.
        MutableRoaringBitmap presence = column.getPresenceBitmap().getMutableRoaringBitmap();
        IntIterator iterator = presence.getIntIterator();
        while (iterator.hasNext()) {
          int docId = iterator.next();
          if (docId >= numDocs) {
            return;
          }
          Object value = column.getValue(docId);
          if (value != null) {
            consumer.accept(docId, value);
          }
        }
      }
    };
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
    flushMeters();
    for (MutableKeyColumn keyCol : _keyColumns.values()) {
      keyCol.close();
    }
  }

  /// Emits the batched ingestion counters. Counters accumulate per row on the consuming path and
  /// are flushed once here, mirroring what [OpenStructColumnSplitter] does at seal time.
  private void flushMeters() {
    ServerMetrics serverMetrics = ServerMetrics.get();
    if (serverMetrics == null) {
      return;
    }
    if (_ignoredKeyDropCount > 0) {
      serverMetrics.addMeteredTableValue(_tableNameWithType, _openStructColumn,
          ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS, _ignoredKeyDropCount);
    }
    if (_typeCoercionFailureCount > 0) {
      serverMetrics.addMeteredTableValue(_tableNameWithType, _openStructColumn,
          ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES, _typeCoercionFailureCount);
    }
    if (_typeInferenceFailureCount > 0) {
      serverMetrics.addMeteredTableValue(_tableNameWithType, _openStructColumn,
          ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES, _typeInferenceFailureCount);
    }
  }
}
