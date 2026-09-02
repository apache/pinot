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
package org.apache.pinot.segment.local.segment.creator.impl.openstruct;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Utf8;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.segment.creator.impl.BaseSegmentCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StatsCollectorUtil;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.openstruct.OpenStructSupportedIndexes;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ColumnarOpenStructIndexCreator;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.creator.OpenStructColumnarSource;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.OpenStructTypeInference;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.PinotDataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Splits an OPEN_STRUCT column into per-key materialized columns using standard Pinot index
/// creators. Dense keys become independent virtual columns; remaining keys go into a single
/// synthetic JSON column for sparse storage.
///
/// Lifecycle: instantiated by `BaseSegmentCreator` for OPEN_STRUCT columns. Receives
/// per-doc `Map<String, Object>` values via [#add(Map, int)], accumulates in memory,
/// then on [#seal()] writes per-key column files using standard creators.
public class OpenStructColumnSplitter implements ColumnarOpenStructIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenStructColumnSplitter.class);

  /// Documents scattered per pass in [#writeSparseJsonColumn]. Each pass holds one LinkedHashMap
  /// per document carrying at least one sparse key, so this caps that transient heap: at roughly
  /// 150-200 bytes for a one-or-two-entry map, 64k documents peak near 10-13 MB whatever the
  /// segment size, where scattering a 5M-document column in a single pass peaks in the hundreds of
  /// MB on a live server's commit path. The window's own reference array (64k refs, 512 KB) is
  /// allocated once and reused. Cost of windowing is one sweep over the sparse key list per window
  /// — 77 sweeps for a 5M-document column — because the presence iterators are carried across
  /// windows and need no repositioning; so a larger window buys nothing measurable, and a smaller
  /// one only multiplies that sweep.
  @VisibleForTesting
  static final int SPARSE_SCATTER_WINDOW_SIZE = 64 * 1024;

  private final File _indexDir;
  private final String _columnName;
  private final String _tableNameWithType;
  private final Map<String, FieldSpec> _childFieldSpecs;
  private final OpenStructIndexConfig _config;
  private final int _maxDenseKeys;

  // Per-key accumulation
  private final Map<String, RoaringBitmap> _presenceBitmaps = new HashMap<>();
  private final Map<String, List<Object>> _values = new HashMap<>();
  private final Map<String, DataType> _inferredTypes = new HashMap<>();
  private final Map<String, Long> _coercionFailuresPerKey = new HashMap<>();
  private final Map<String, Long> _inferenceFailuresPerKey = new HashMap<>();
  private int _numDocs;
  private int _ignoredKeyDropCount;

  // Resolved at seal time
  @Nullable
  private Set<String> _resolvedDenseKeys;
  private final Map<String, PropertiesConfiguration> _materializedColumnMetadata = new LinkedHashMap<>();

  public OpenStructColumnSplitter(File indexDir, String columnName, String tableNameWithType, FieldSpec fieldSpec,
      OpenStructIndexConfig config) {
    _indexDir = indexDir;
    _columnName = columnName;
    _tableNameWithType = tableNameWithType;
    _config = config;
    _maxDenseKeys = config.getMaxDenseKeys();

    Map<String, FieldSpec> childFieldSpecs = null;
    if (fieldSpec instanceof ComplexFieldSpec) {
      ComplexFieldSpec complexSpec = (ComplexFieldSpec) fieldSpec;
      childFieldSpecs = complexSpec.getChildFieldSpecs();
    }
    _childFieldSpecs = childFieldSpecs != null ? new HashMap<>(childFieldSpecs) : new HashMap<>();
  }

  @Override
  public void add(Object value, int docId)
      throws IOException {
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      addMap(map);
    } else {
      addMap(null);
    }
  }

  @Override
  public void add(Map<String, Object> openStructValue, int docId)
      throws IOException {
    addMap(openStructValue);
  }

  @Override
  public boolean supportsColumnarAdd() {
    return true;
  }

  /// Ingests a columnar source directly, skipping the per-doc map rebuild and the type
  /// re-resolution and re-coercion in [#addMap]. The source's values are already coerced to the
  /// key's stored type, and its stored type is resolved by the same rules addMap applies, so the
  /// accumulated state is identical to feeding the same data one map at a time.
  ///
  /// Must be the only ingestion method called on a given splitter, and only once: mixing it with
  /// [#add(Map, int)] or calling it twice would clobber [#_numDocs] while appending to the same
  /// per-key value lists.
  @Override
  public void addColumnar(OpenStructColumnarSource source)
      throws IOException {
    Preconditions.checkState(_numDocs == 0,
        "addColumnar must be the only ingestion method used on a splitter, and can only be called "
            + "once; this splitter has already accumulated %s doc(s)", _numDocs);
    for (String key : source.getKeys()) {
      // The mutable index drops ignored keys during consumption and meters them itself, so a key
      // reaching here should never be ignored. Skip defensively without counting: counting again
      // would double-report against the mutable index's own OPEN_STRUCT_IGNORED_KEY_DROPS.
      if (_config.isIgnoredKey(key)) {
        continue;
      }
      DataType storedType = source.getStoredType(key);
      // Only consulted for keys with no declared child spec, matching addMap; for those the
      // mutable column's stored type is exactly what addMap would have inferred.
      boolean hasDeclaredType = _childFieldSpecs.containsKey(key);
      // A fresh consumer per key: it registers this key's state lazily on the first present value
      // (matching addMap, which never registers a key in _presenceBitmaps/_values until it
      // actually sees one -- registering eagerly for a key with zero present docs would make
      // classify() treat it as a configured dense key with an empty bitmap, which addMap would
      // never do), then caches the bitmap and value list as fields so every later value for the
      // same key costs one null check instead of three map lookups.
      source.forEachPresentValue(key,
          new LazyKeyStateConsumer(key, storedType, hasDeclaredType, _presenceBitmaps, _values, _inferredTypes));
    }
    _numDocs = source.getNumDocs();
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("OPEN_STRUCT index is single-value only");
  }

  /// Present-value consumer used by [#addColumnar] to accumulate one key's state with a single
  /// null check per value instead of three map lookups. On the first `accept` call for its key it
  /// registers (or reuses) that key's presence bitmap and value list in the splitter's shared
  /// per-key maps, and records the inferred type when the key has no declared child spec -- the
  /// same lazy registration [#addMap] performs on a key's first present value. Every subsequent
  /// call reuses the cached bitmap and list fields directly, without touching the shared maps
  /// again. Not thread-safe and not reusable across keys: a fresh instance is created per key
  /// per [#addColumnar] call, and its fields are only ever touched by the single thread driving
  /// that call.
  private static final class LazyKeyStateConsumer implements OpenStructColumnarSource.PresentValueConsumer {
    private final String _key;
    private final DataType _storedType;
    private final boolean _hasDeclaredType;
    private final Map<String, RoaringBitmap> _presenceBitmaps;
    private final Map<String, List<Object>> _values;
    private final Map<String, DataType> _inferredTypes;

    private RoaringBitmap _presence;
    private List<Object> _keyValues;
    private boolean _initialized;

    private LazyKeyStateConsumer(String key, DataType storedType, boolean hasDeclaredType,
        Map<String, RoaringBitmap> presenceBitmaps, Map<String, List<Object>> values,
        Map<String, DataType> inferredTypes) {
      _key = key;
      _storedType = storedType;
      _hasDeclaredType = hasDeclaredType;
      _presenceBitmaps = presenceBitmaps;
      _values = values;
      _inferredTypes = inferredTypes;
    }

    @Override
    public void accept(int docId, Object value) {
      if (!_initialized) {
        _presence = _presenceBitmaps.computeIfAbsent(_key, k -> new RoaringBitmap());
        _keyValues = _values.computeIfAbsent(_key, k -> new ArrayList<>());
        if (!_hasDeclaredType) {
          _inferredTypes.putIfAbsent(_key, _storedType);
        }
        _initialized = true;
      }
      _presence.add(docId);
      _keyValues.add(value);
    }
  }

  /// Returns the resolved dense-key set after [#seal()] or [#classify()].
  /// Returns an empty set before resolution.
  public Set<String> getResolvedDenseKeys() {
    return _resolvedDenseKeys != null ? Collections.unmodifiableSet(_resolvedDenseKeys) : Set.of();
  }

  /// Resolves dense vs sparse keys without writing any files. Exposed for testing and for callers
  /// that need the classification independent of file output. [#seal()] calls this internally.
  public Set<String> classify() {
    if (_resolvedDenseKeys != null) {
      return _resolvedDenseKeys;
    }
    if (_numDocs == 0 || _presenceBitmaps.isEmpty()) {
      _resolvedDenseKeys = new LinkedHashSet<>();
      return _resolvedDenseKeys;
    }
    List<String> allKeys = new ArrayList<>(_presenceBitmaps.keySet());
    allKeys.sort((a, b) -> {
      double fillA = (double) _presenceBitmaps.get(a).getCardinality() / _numDocs;
      double fillB = (double) _presenceBitmaps.get(b).getCardinality() / _numDocs;
      int cmp = Double.compare(fillB, fillA);
      return cmp != 0 ? cmp : a.compareTo(b);
    });

    double minFillRate = _config.getDenseKeyMinFillRate();
    _resolvedDenseKeys = new LinkedHashSet<>();

    Set<String> configuredDenseKeys = _config.getDenseKeys();
    for (String key : configuredDenseKeys) {
      if (_presenceBitmaps.containsKey(key) && (_maxDenseKeys < 0 || _resolvedDenseKeys.size() < _maxDenseKeys)) {
        _resolvedDenseKeys.add(key);
      }
    }

    for (String key : allKeys) {
      if (_resolvedDenseKeys.contains(key)) {
        continue;
      }
      double fillRate = (double) _presenceBitmaps.get(key).getCardinality() / _numDocs;
      if ((_maxDenseKeys < 0 || _resolvedDenseKeys.size() < _maxDenseKeys) && fillRate >= minFillRate) {
        _resolvedDenseKeys.add(key);
      }
    }
    return _resolvedDenseKeys;
  }

  private void addMap(@Nullable Map<String, Object> map) {
    if (map != null && !map.isEmpty()) {
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
        FieldSpec keySpec = _childFieldSpecs.get(key);
        DataType valueType;
        if (keySpec != null) {
          valueType = keySpec.getDataType();
        } else {
          DataType established = _inferredTypes.get(key);
          if (established != null && established != DataType.STRING) {
            // Sticky: a key already resolved to a non-STRING type can't flip later, so skip
            // inference entirely -- matches MutableOpenStructIndex's fast path. An unmappable
            // value here is a coercion failure below, not a fresh inference decision; overriding
            // valueType to STRING per-row would desync it from _inferredTypes and corrupt _values
            // with a mix of types for one key.
            valueType = established;
          } else {
            // Resolve per value rather than only on first sighting: the key's inferred type is
            // cached, so folding the counter into a computeIfAbsent would record one failure per
            // key no matter how many values actually took the STRING fallback.
            DataType inferred = OpenStructTypeInference.inferDataType(rawValue);
            if (inferred == null) {
              valueType = DataType.STRING;
              _inferenceFailuresPerKey.merge(key, 1L, Long::sum);
            } else {
              // established is STRING here (or null): once a key falls back to STRING it stays
              // STRING even if a later value would infer cleanly on its own.
              valueType = established != null ? established : inferred;
            }
            _inferredTypes.putIfAbsent(key, valueType);
          }
        }
        RoaringBitmap bitmap = _presenceBitmaps.computeIfAbsent(key, k -> new RoaringBitmap());
        List<Object> values = _values.computeIfAbsent(key, k -> new ArrayList<>());
        bitmap.add(_numDocs);
        Object coerced;
        try {
          PinotDataType sourceType = PinotDataType.getSingleValueType(rawValue);
          PinotDataType destType = ColumnDataType.fromDataTypeSV(valueType.getStoredType()).toPinotDataType();
          coerced = destType.convert(rawValue, sourceType);
        } catch (Exception e) {
          _coercionFailuresPerKey.merge(key, 1L, Long::sum);
          bitmap.remove(_numDocs);
          continue;
        }
        values.add(coerced);
      }
    }
    _numDocs++;
  }

  @Override
  public void seal()
      throws IOException {
    classify();
    if (_resolvedDenseKeys == null || (_numDocs == 0 && _presenceBitmaps.isEmpty())) {
      return;
    }

    for (String key : _resolvedDenseKeys) {
      writeDenseKeyColumn(key);
    }

    List<String> sparseKeys = new ArrayList<>();
    for (String key : _presenceBitmaps.keySet()) {
      if (!_resolvedDenseKeys.contains(key)) {
        sparseKeys.add(key);
      }
    }
    if (!sparseKeys.isEmpty()) {
      writeSparseJsonColumn(sparseKeys);
    }

    long totalCoercionFailures = sumValues(_coercionFailuresPerKey);
    if (totalCoercionFailures > 0) {
      LOGGER.info("OPEN_STRUCT '{}': dropped {} values due to type coercion failures across {} keys",
          _columnName, totalCoercionFailures, _coercionFailuresPerKey.size());
      // The key space is user-controlled, so the per-key breakdown is DEBUG-only.
      LOGGER.debug("OPEN_STRUCT '{}': full coercion failure counts: {}", _columnName, _coercionFailuresPerKey);
    }
    long totalInferenceFailures = sumValues(_inferenceFailuresPerKey);
    if (totalInferenceFailures > 0) {
      LOGGER.info("OPEN_STRUCT '{}': {} values across {} keys fell back to STRING after type inference failed",
          _columnName, totalInferenceFailures, _inferenceFailuresPerKey.size());
      LOGGER.debug("OPEN_STRUCT '{}': full inference failure counts: {}", _columnName, _inferenceFailuresPerKey);
    }
    emitMetrics(sparseKeys.size(), totalCoercionFailures, totalInferenceFailures);

    if (_ignoredKeyDropCount > 0) {
      LOGGER.info("OPEN_STRUCT '{}': dropped {} entries for ignored keys", _columnName, _ignoredKeyDropCount);
      ServerMetrics serverMetrics = ServerMetrics.get();
      if (serverMetrics != null) {
        serverMetrics.addMeteredTableValue(_tableNameWithType, _columnName,
            ServerMeter.OPEN_STRUCT_IGNORED_KEY_DROPS, _ignoredKeyDropCount);
      }
    }

    emitParentColumnMetadata(sparseKeys);
  }

  private static long sumValues(Map<String, Long> counts) {
    return counts.values().stream().mapToLong(Long::longValue).sum();
  }

  private void emitMetrics(int sparseKeyCount, long totalCoercionFailures, long totalInferenceFailures) {
    ServerMetrics serverMetrics = ServerMetrics.get();
    if (serverMetrics == null || _numDocs == 0) {
      return;
    }

    if (totalCoercionFailures > 0) {
      serverMetrics.addMeteredTableValue(_tableNameWithType, _columnName,
          ServerMeter.OPEN_STRUCT_TYPE_COERCION_FAILURES, totalCoercionFailures);
    }
    if (totalInferenceFailures > 0) {
      serverMetrics.addMeteredTableValue(_tableNameWithType, _columnName,
          ServerMeter.OPEN_STRUCT_TYPE_INFERENCE_FAILURES, totalInferenceFailures);
    }

    serverMetrics.setOrUpdateTableGauge(_tableNameWithType, _columnName,
        ServerGauge.OPEN_STRUCT_LAST_SEGMENT_DENSE_KEY_COUNT, _resolvedDenseKeys.size());
    serverMetrics.setOrUpdateTableGauge(_tableNameWithType, _columnName,
        ServerGauge.OPEN_STRUCT_LAST_SEGMENT_SPARSE_KEY_COUNT, sparseKeyCount);
    serverMetrics.setOrUpdateTableGauge(_tableNameWithType, _columnName,
        ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_COUNT, _presenceBitmaps.size());
    // Denominator for the per-key fill rate. Emitted as a raw count rather than folding the ratio into a
    // single percentage gauge: integer division truncates a key present in a handful of docs to 0, which
    // is indistinguishable from no data and is exactly the case worth alerting on.
    serverMetrics.setOrUpdateTableGauge(_tableNameWithType, _columnName,
        ServerGauge.OPEN_STRUCT_LAST_SEGMENT_DOC_COUNT, _numDocs);

    if (_config.isPerKeyMetricsEnabled()) {
      // Emit for every key in the segment. Registry entries follow the ingested key space;
      // table deletion can only sweep keys recoverable from denseKeys.
      _presenceBitmaps.forEach((key, presence) -> serverMetrics.setOrUpdateTableGauge(_tableNameWithType,
          OpenStructNaming.metricKey(_columnName, key),
          ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, presence.getCardinality()));
    } else {
      // Emit only for configured dense keys — bounded by the table config.
      for (String key : _config.getDenseKeys()) {
        RoaringBitmap presence = _presenceBitmaps.get(key);
        if (presence != null) {
          serverMetrics.setOrUpdateTableGauge(_tableNameWithType,
              OpenStructNaming.metricKey(_columnName, key),
              ServerGauge.OPEN_STRUCT_LAST_SEGMENT_KEY_DOC_COUNT, presence.getCardinality());
        }
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    // Nothing to close — sub-creators are created and closed within seal()
  }

  @Override
  public Map<String, PropertiesConfiguration> getMaterializedColumnMetadata() {
    return _materializedColumnMetadata;
  }

  private void writeDenseKeyColumn(String key)
      throws IOException {
    String materializedCol = OpenStructNaming.materializedColumnName(_columnName, key);
    FieldSpec keySpec = _childFieldSpecs.get(key);
    DataType valueType = keySpec != null
        ? keySpec.getDataType()
        : _inferredTypes.getOrDefault(key, DataType.STRING);
    DataType storedType = valueType.getStoredType();
    RoaringBitmap presence = _presenceBitmaps.get(key);
    List<Object> values = _values.get(key);

    // Synthetic field spec for the materialized child. Its natural Pinot dimension null value is the value
    // stored for absent docs, so column metadata stays consistent with on-disk content.
    DimensionFieldSpec childFieldSpec = new DimensionFieldSpec(materializedCol, storedType, true);
    Object defaultValue = childFieldSpec.getDefaultNullValue();

    // Collect statistics the standard way: present docs contribute their value, absent docs the default
    // (absent docs are also marked in the null vector below).
    AbstractColumnStatisticsCollector statsCollector =
        StatsCollectorUtil.createStatsCollector(childFieldSpec, null);
    // Step the presence bitmap with a cursor rather than probing it per doc. values is already in
    // presence-ordinal order, so the ordinal the loop already tracks is the same cursor. Documents
    // are still visited in ascending order, which the collector's sortedness tracking relies on.
    PeekableIntIterator statsIterator = presence.getIntIterator();
    int nextPresentDocId = statsIterator.hasNext() ? statsIterator.next() : -1;
    int statsOrdinal = 0;
    for (int docId = 0; docId < _numDocs; docId++) {
      if (docId == nextPresentDocId) {
        statsCollector.collect(values.get(statsOrdinal++));
        nextPresentDocId = statsIterator.hasNext() ? statsIterator.next() : -1;
      } else {
        statsCollector.collect(defaultValue);
      }
    }
    statsCollector.seal();

    // Build per-key index configuration from the key's FieldConfig (falling back to the default), then apply
    // the OPEN_STRUCT inverted-on default. No TableConfig/Schema required.
    FieldConfig keyFieldConfig = _config.getValueFieldConfig(key);
    if (keyFieldConfig == null) {
      keyFieldConfig = _config.getDefaultValueFieldConfig();
    }
    boolean enableInverted = _config.shouldEnableInvertedIndexForKey(key);
    FieldIndexConfigs configsForDecision = new FieldIndexConfigs.Builder(
        FieldIndexConfigsUtil.fromFieldConfig(keyFieldConfig, childFieldSpec))
        .add(StandardIndexes.inverted(), enableInverted ? IndexConfig.ENABLED : IndexConfig.DISABLED)
        .build();

    boolean useDictionary = resolveUseDictionary(childFieldSpec, configsForDecision, statsCollector);

    // Reconcile dictionary + forward encoding with the final decision (mirrors BaseSegmentCreator.adaptConfig);
    // ForwardIndexCreatorFactory selects dict-vs-raw from the forward config's EncodingType. A compression codec
    // applies only to the raw forward format (LZ4 preserves the dense child's current on-disk layout); attaching
    // one to a dictionary-encoded forward is rejected by ForwardIndexType.validate.
    ForwardIndexConfig.Builder forwardBuilder = new ForwardIndexConfig.Builder(
        useDictionary ? FieldConfig.EncodingType.DICTIONARY : FieldConfig.EncodingType.RAW);
    if (!useDictionary) {
      forwardBuilder.withCompressionCodec(FieldConfig.CompressionCodec.LZ4);
    }
    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder(configsForDecision)
        .add(StandardIndexes.dictionary(),
            useDictionary ? DictionaryIndexConfig.DEFAULT : DictionaryIndexConfig.DISABLED)
        .add(StandardIndexes.forward(), forwardBuilder.build())
        .build();

    // The null vector marks exactly the docs the index walk sees as absent, so it is filled inside
    // that walk rather than in a third pass of its own over every doc.
    int dictElementSize;
    NullValueVectorCreator nullCreator = new NullValueVectorCreator(_indexDir, materializedCol);
    try {
      dictElementSize = writeColumnIndexes(materializedCol, storedType, presence, values,
          defaultValue, statsCollector, useDictionary, fieldIndexConfigs, childFieldSpec, nullCreator);
      nullCreator.seal();
    } finally {
      nullCreator.close();
    }

    PropertiesConfiguration props = new PropertiesConfiguration();
    FieldConfig.EncodingType encoding =
        useDictionary ? FieldConfig.EncodingType.DICTIONARY : FieldConfig.EncodingType.RAW;
    BaseSegmentCreator.addColumnMetadataInfo(props, materializedCol, statsCollector, _numDocs, childFieldSpec,
        useDictionary, dictElementSize, encoding, false);
    // OPEN_STRUCT-specific keys not written by addColumnMetadataInfo.
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol, V1Constants.MetadataKeys.Column.PARENT_COLUMN),
        _columnName);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(materializedCol, "hasNullValue"), true);
    if (enableInverted && useDictionary) {
      props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(materializedCol, "hasInvertedIndex"), true);
    }
    _materializedColumnMetadata.put(materializedCol, props);
  }

  /// Decides dictionary vs raw encoding for a materialized child column, mirroring the three steps of
  /// `BaseSegmentCreator.createDictionaryForColumn` with standard default flags (optimizeDictionary
  /// off => dictionary unless explicitly disabled and not required by an enabled index).
  private boolean resolveUseDictionary(FieldSpec childFieldSpec, FieldIndexConfigs fieldIndexConfigs,
      AbstractColumnStatisticsCollector statsCollector) {
    if (DictionaryIndexConfig.requiresDictionary(childFieldSpec, fieldIndexConfigs)) {
      return true;
    }
    if (fieldIndexConfigs.getConfig(StandardIndexes.dictionary()).isDisabled()) {
      return false;
    }
    return DictionaryIndexType.ignoreDictionaryOverride(false, false,
        IndexingConfig.DEFAULT_NO_DICTIONARY_SIZE_RATIO_THRESHOLD, null, childFieldSpec, fieldIndexConfigs,
        statsCollector.getCardinality(), statsCollector.getTotalNumberOfEntries());
  }

  /// Writes the dictionary (when used) plus all vetted, enabled indexes for a materialized child column through
  /// the standard index-creator family, driven from a single per-doc loop that also fills the caller's null
  /// vector for absent docs. Returns the dictionary element size in
  /// bytes (0 when raw-encoded), for column metadata. The dictionary is built separately because its build
  /// lifecycle is CUSTOM and it supplies the dictIds the per-row creators consume.
  private int writeColumnIndexes(String materializedCol, DataType storedType, RoaringBitmap presence,
      List<Object> values, Object defaultValue, AbstractColumnStatisticsCollector statsCollector,
      boolean useDictionary, FieldIndexConfigs fieldIndexConfigs, FieldSpec childFieldSpec,
      NullValueVectorCreator nullCreator)
      throws IOException {
    int dictElementSize = 0;
    SegmentDictionaryCreator dictCreator = null;
    try {
      if (useDictionary) {
        dictCreator = new SegmentDictionaryCreator(materializedCol, storedType,
            new File(_indexDir, materializedCol + V1Constants.Dict.FILE_EXTENSION), true);
        dictCreator.build(statsCollector.getUniqueValuesSet());
      }

      // Index-creation context built from the sealed collector (a ColumnShape) — no TableConfig required.
      IndexCreationContext context =
          new IndexCreationContext.Builder(_indexDir, null, statsCollector, useDictionary, false)
              .withOnHeap(false).build();

      List<IndexCreator> creators = new ArrayList<>();
      try {
        for (IndexType<?, ?, ?> indexType : IndexService.getInstance().getAllIndexes()) {
          if (indexType.getIndexBuildLifecycle() != IndexType.BuildLifecycle.DURING_SEGMENT_CREATION) {
            continue;   // excludes dictionary (lifecycle CUSTOM), built separately above
          }
          if (!OpenStructSupportedIndexes.ALLOWED_PRETTY_NAMES.contains(indexType.getPrettyName())) {
            continue;   // non-vetted indexes already rejected at table-config validation; defensive backstop
          }
          IndexCreator creator = createColumnIndexCreator(indexType, context, fieldIndexConfigs, materializedCol,
              childFieldSpec);
          if (creator != null) {
            creators.add(creator);
          }
        }

        PeekableIntIterator presenceIterator = presence.getIntIterator();
        int nextPresentDocId = presenceIterator.hasNext() ? presenceIterator.next() : -1;
        int ordinal = 0;
        for (int docId = 0; docId < _numDocs; docId++) {
          Object value;
          if (docId == nextPresentDocId) {
            value = values.get(ordinal++);
            nextPresentDocId = presenceIterator.hasNext() ? presenceIterator.next() : -1;
          } else {
            value = defaultValue;
            nullCreator.setNull(docId);
          }
          int dictId = useDictionary ? dictCreator.indexOfSV(value) : -1;
          for (IndexCreator creator : creators) {
            creator.add(value, dictId);
          }
        }
        for (IndexCreator creator : creators) {
          creator.seal();
        }
      } finally {
        for (IndexCreator creator : creators) {
          creator.close();
        }
        if (dictCreator != null) {
          dictElementSize = dictCreator.getNumBytesPerEntry();
          dictCreator.seal();
        }
      }
    } finally {
      if (dictCreator != null) {
        dictCreator.close();
      }
    }
    return dictElementSize;
  }

  @Nullable
  private static <C extends IndexConfig> IndexCreator createColumnIndexCreator(IndexType<C, ?, ?> indexType,
      IndexCreationContext context, FieldIndexConfigs fieldIndexConfigs, String materializedCol,
      FieldSpec childFieldSpec)
      throws IOException {
    // Materialized child columns exist in no schema/TableConfig, so the standard table-config-time validation
    // never sees them. Run the index type's own guards here against the resolved child FieldSpec (e.g. range
    // rejects a non-numeric column without a dictionary) so misconfigurations fail with the canonical message
    // instead of crashing opaquely inside the creator. validate() internally no-ops when the index is disabled.
    indexType.validate(fieldIndexConfigs, childFieldSpec, null);
    C config = fieldIndexConfigs.getConfig(indexType);
    if (!config.isEnabled() || !indexType.shouldCreateIndex(context, config)) {
      return null;
    }
    try {
      return indexType.createIndexCreator(context, config);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Failed to create " + indexType.getPrettyName() + " creator for: " + materializedCol, e);
    }
  }

  private void writeSparseJsonColumn(List<String> sparseKeys)
      throws IOException {
    String sparseCol = OpenStructNaming.sparseColumnName(_columnName);
    int maxLen = 1;
    String[] jsonPerDoc = new String[_numDocs];

    // Scatter each key's values into per-doc buckets by walking its presence bitmap once, rather
    // than asking every key whether it is present at every doc. The per-doc form cost
    // numDocs * numSparseKeys probes plus a rank() on each hit, and rank() is the expensive half.
    //
    // Scattering the whole column at once would hold one LinkedHashMap per document live, so the
    // walk is windowed instead: each window is scattered, serialized, and released before the next
    // one starts, capping the live maps at O(window) rather than O(numDocs). The per-key presence
    // iterators and value ordinals are hoisted out of the window loop and carried across windows —
    // an iterator bounded by peekNext() is left standing on the first docId of the next window, and
    // each key's ordinal must keep counting the values it has consumed so far, since values is
    // indexed by presence ordinal over the whole column, not per window.
    //
    // Within a window, keys are visited in sparseKeys order and appended into LinkedHashMaps, so
    // each doc's JSON key order is identical to the per-doc build this replaces — the JSON string
    // is the forward index content, so that ordering is load-bearing, not cosmetic.
    int numSparseKeys = sparseKeys.size();
    PeekableIntIterator[] presenceIterators = new PeekableIntIterator[numSparseKeys];
    @SuppressWarnings("unchecked")
    List<Object>[] valuesPerKey = new List[numSparseKeys];
    int[] ordinalPerKey = new int[numSparseKeys];
    for (int keyIndex = 0; keyIndex < numSparseKeys; keyIndex++) {
      RoaringBitmap presence = _presenceBitmaps.get(sparseKeys.get(keyIndex));
      if (presence == null) {
        continue;
      }
      presenceIterators[keyIndex] = presence.getIntIterator();
      valuesPerKey[keyIndex] = _values.get(sparseKeys.get(keyIndex));
    }

    // The max keeps the stride positive for a zero-document column (reachable when every key was
    // registered before any document was counted), which the loop below would otherwise never
    // advance past if its guard ever stopped short-circuiting.
    int windowSize = Math.min(SPARSE_SCATTER_WINDOW_SIZE, Math.max(_numDocs, 1));
    @SuppressWarnings("unchecked")
    Map<String, Object>[] entriesInWindow = new Map[windowSize];
    for (int windowStart = 0; windowStart < _numDocs; windowStart += windowSize) {
      int windowEnd = Math.min(windowStart + windowSize, _numDocs);
      for (int keyIndex = 0; keyIndex < numSparseKeys; keyIndex++) {
        PeekableIntIterator it = presenceIterators[keyIndex];
        if (it == null) {
          continue;
        }
        String key = sparseKeys.get(keyIndex);
        List<Object> keyValues = valuesPerKey[keyIndex];
        // peekNext() rather than next() at the window edge: consuming the first docId of the next
        // window here would drop it from that window's output entirely.
        while (it.hasNext() && it.peekNext() < windowEnd) {
          int docId = it.next();
          int slot = docId - windowStart;
          Map<String, Object> sparseEntries = entriesInWindow[slot];
          if (sparseEntries == null) {
            sparseEntries = new LinkedHashMap<>();
            entriesInWindow[slot] = sparseEntries;
          }
          sparseEntries.put(key, keyValues.get(ordinalPerKey[keyIndex]++));
        }
      }

      for (int docId = windowStart; docId < windowEnd; docId++) {
        int slot = docId - windowStart;
        Map<String, Object> sparseEntries = entriesInWindow[slot];
        if (sparseEntries == null) {
          continue;
        }
        // Release as we go, and leave the slot clean for the window that reuses this array.
        entriesInWindow[slot] = null;
        try {
          String json = JsonUtils.objectToString(sparseEntries);
          jsonPerDoc[docId] = json;
          maxLen = Math.max(maxLen, Utf8.encodedLength(json));
        } catch (IOException e) {
          throw new RuntimeException("Failed to serialize sparse entries for docId " + docId, e);
        }
      }
    }

    // Absent docs store "" in the raw forward index (see loop below) and are flagged in the null vector, so feed
    // the same placeholder through the stats collector and record it as the default null value. Collected inside
    // the write loop rather than in a pass of its own: it needs the exact same per-doc branch.
    DimensionFieldSpec sparseFieldSpec = new DimensionFieldSpec(sparseCol, DataType.STRING, true);
    String defaultValue = "";
    AbstractColumnStatisticsCollector statsCollector = StatsCollectorUtil.createStatsCollector(sparseFieldSpec, null);

    SingleValueVarByteRawIndexCreator fwdCreator = new SingleValueVarByteRawIndexCreator(
        _indexDir, ChunkCompressionType.LZ4, sparseCol, _numDocs, DataType.STRING, maxLen);
    NullValueVectorCreator nullCreator = new NullValueVectorCreator(_indexDir, sparseCol);
    JsonIndexCreator jsonCreator = _config.isSparseJsonIndex()
        ? new OffHeapJsonIndexCreator(_indexDir, sparseCol, null, false, JsonIndexConfig.DEFAULT)
        : null;
    try {
      for (int docId = 0; docId < _numDocs; docId++) {
        if (jsonPerDoc[docId] != null) {
          statsCollector.collect(jsonPerDoc[docId]);
          fwdCreator.putString(jsonPerDoc[docId]);
          if (jsonCreator != null) {
            jsonCreator.add(jsonPerDoc[docId]);
          }
        } else {
          statsCollector.collect(defaultValue);
          fwdCreator.putString("");
          nullCreator.setNull(docId);
          if (jsonCreator != null) {
            jsonCreator.add("{}");
          }
        }
      }
      fwdCreator.seal();
      nullCreator.seal();
      if (jsonCreator != null) {
        jsonCreator.seal();
      }
    } finally {
      fwdCreator.close();
      nullCreator.close();
      if (jsonCreator != null) {
        jsonCreator.close();
      }
    }

    statsCollector.seal();

    PropertiesConfiguration props = new PropertiesConfiguration();
    // Route through the same metadata writer as dense child columns (BaseSegmentCreator.addColumnMetadataInfo) so
    // this raw, no-dictionary column carries every property ColumnMetadataImpl.fromPropertiesConfiguration()
    // expects, instead of a hand-rolled subset that can silently drift from what the reader requires.
    BaseSegmentCreator.addColumnMetadataInfo(props, sparseCol, statsCollector, _numDocs, sparseFieldSpec,
        false /* hasDictionary */, 0 /* dictionaryElementSize */, FieldConfig.EncodingType.RAW,
        false /* autoGenerated */);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, "hasNullValue"), true);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(sparseCol, V1Constants.MetadataKeys.Column.PARENT_COLUMN),
        _columnName);
    _materializedColumnMetadata.put(sparseCol, props);
  }

  private void emitParentColumnMetadata(List<String> sparseKeys) {
    boolean hasSparseColumn = !sparseKeys.isEmpty();
    PropertiesConfiguration props = new PropertiesConfiguration();
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.COLUMN_NAME),
        _columnName);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.DATA_TYPE),
        FieldSpec.DataType.OPEN_STRUCT.name());
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
        FieldSpec.FieldType.COMPLEX.name());
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED),
        true);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
        _numDocs);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.HAS_SPARSE_COLUMN),
        hasSparseColumn);
    if (!sparseKeys.isEmpty()) {
      try {
        props.setProperty(
            V1Constants.MetadataKeys.Column.getKeyFor(_columnName, V1Constants.MetadataKeys.Column.SPARSE_KEYS),
            JsonUtils.objectToString(sparseKeys));
      } catch (IOException e) {
        throw new RuntimeException("Failed to serialize sparse-key manifest", e);
      }
    }
    _materializedColumnMetadata.put(_columnName, props);
  }
}
