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
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.TimestampIndexUtils;


/// Index loading config with shared table-level state and segment-local mutable overrides.
public class IndexLoadingConfig {
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;
  public static final String READ_MODE_KEY = "readMode";

  private final ImmutableState _immutableState;

  // Mutable config and segment-specific overrides.
  @Nullable
  private ReadMode _readModeOverride;
  @Nullable
  private SegmentVersion _segmentVersionOverride;
  private String _segmentTier;
  private Set<String> _knownColumns;
  private String _tableDataDir;
  private boolean _errorOnColumnBuildFailure;
  private boolean _forwardIndexOnly;
  private ResolvedIndexState _resolvedIndexState;

  /// Immutable table-level state shared by derived segment configs.
  private static final class ImmutableState {
    @Nullable
    private final InstanceDataManagerConfig _instanceDataManagerConfig;
    @Nullable
    private final TableConfig _tableConfig;
    @Nullable
    private final Schema _schema;
    private final ReadMode _readMode;
    @Nullable
    private final SegmentVersion _segmentVersion;
    @Nullable
    private final String _instanceId;
    private final boolean _isRealtimeOffHeapAllocation;
    private final boolean _isDirectRealtimeOffHeapAllocation;
    private final boolean _lazyColumnMaterialization;
    private final int _realtimeAvgMultiValueCount;
    @Nullable
    private final String _segmentStoreURI;
    @Nullable
    private final String _segmentDirectoryLoader;
    @Nullable
    private final Map<String, Map<String, String>> _instanceTierConfigs;
    private final List<String> _sortedColumns;
    private final ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode;
    private final boolean _hasOpenStructColumns;

    private ImmutableState(@Nullable InstanceDataManagerConfig instanceDataManagerConfig,
        @Nullable TableConfig tableConfig, @Nullable Schema schema) {
      _instanceDataManagerConfig = instanceDataManagerConfig;
      _tableConfig = tableConfig;
      _schema = schema;

      String instanceId = null;
      boolean isRealtimeOffHeapAllocation = false;
      boolean isDirectRealtimeOffHeapAllocation = false;
      int realtimeAvgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
      ReadMode readMode = ReadMode.DEFAULT_MODE;
      SegmentVersion segmentVersion = null;
      String segmentStoreURI = null;
      String segmentDirectoryLoader = null;
      Map<String, Map<String, String>> instanceTierConfigs = null;
      if (instanceDataManagerConfig != null) {
        ReadMode instanceReadMode = instanceDataManagerConfig.getReadMode();
        if (instanceReadMode != null) {
          readMode = instanceReadMode;
        }
        String instanceSegmentVersion = instanceDataManagerConfig.getSegmentFormatVersion();
        if (instanceSegmentVersion != null) {
          segmentVersion = SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
        }
        instanceId = instanceDataManagerConfig.getInstanceId();
        isRealtimeOffHeapAllocation = instanceDataManagerConfig.isRealtimeOffHeapAllocation();
        isDirectRealtimeOffHeapAllocation = instanceDataManagerConfig.isDirectRealtimeOffHeapAllocation();
        String avgMultiValueCount = instanceDataManagerConfig.getAvgMultiValueCount();
        if (avgMultiValueCount != null) {
          realtimeAvgMultiValueCount = Integer.parseInt(avgMultiValueCount);
        }
        segmentStoreURI = instanceDataManagerConfig.getSegmentStoreUri();
        segmentDirectoryLoader = instanceDataManagerConfig.getSegmentDirectoryLoader();
        Map<String, Map<String, String>> tierConfigs = instanceDataManagerConfig.getTierConfigs();
        instanceTierConfigs = tierConfigs != null ? tierConfigs : Map.of();
      }

      List<String> sortedColumns = List.of();
      ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
      boolean hasOpenStructColumns = false;
      if (tableConfig != null) {
        if (schema != null) {
          TimestampIndexUtils.applyTimestampIndex(tableConfig, schema);
          for (ComplexFieldSpec fieldSpec : schema.getComplexFieldSpecs()) {
            if (fieldSpec.getDataType() == DataType.OPEN_STRUCT) {
              hasOpenStructColumns = true;
              break;
            }
          }
        }
        IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
        String tableReadMode = indexingConfig.getLoadMode();
        if (tableReadMode != null) {
          readMode = ReadMode.getEnum(tableReadMode);
        }
        String tableSegmentVersion = indexingConfig.getSegmentFormatVersion();
        if (tableSegmentVersion != null) {
          segmentVersion = SegmentVersion.valueOf(tableSegmentVersion.toLowerCase());
        }
        List<String> tableSortedColumns = indexingConfig.getSortedColumn();
        if (tableSortedColumns != null) {
          sortedColumns = tableSortedColumns;
        }
        String generatorMode = indexingConfig.getColumnMinMaxValueGeneratorMode();
        if (generatorMode != null) {
          columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.valueOf(generatorMode.toUpperCase());
        }
      }

      _instanceId = instanceId;
      _readMode = readMode;
      _segmentVersion = segmentVersion;
      _isRealtimeOffHeapAllocation = isRealtimeOffHeapAllocation;
      _isDirectRealtimeOffHeapAllocation = isDirectRealtimeOffHeapAllocation;
      _lazyColumnMaterialization =
          instanceDataManagerConfig != null && instanceDataManagerConfig.isLazyColumnMaterialization();
      _realtimeAvgMultiValueCount = realtimeAvgMultiValueCount;
      _segmentStoreURI = segmentStoreURI;
      _segmentDirectoryLoader = segmentDirectoryLoader;
      _instanceTierConfigs = instanceTierConfigs;
      _sortedColumns = sortedColumns;
      _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
      _hasOpenStructColumns = hasOpenStructColumns;
    }
  }

  /// Index settings resolved from the table config, segment tier, schema, and known segment columns.
  private static final class ResolvedIndexState {
    private static final ResolvedIndexState EMPTY =
        new ResolvedIndexState(false, null, false, Map.of(), false, null);

    private final boolean _enableDynamicStarTreeCreation;
    @Nullable
    private final List<StarTreeIndexConfig> _starTreeIndexConfigs;
    private final boolean _enableDefaultStarTree;
    private final Map<String, FieldIndexConfigs> _indexConfigsByColName;
    private final boolean _skipSegmentPreprocess;
    @Nullable
    private final MultiColumnTextIndexConfig _multiColTextIndexConfig;

    private ResolvedIndexState(boolean enableDynamicStarTreeCreation,
        @Nullable List<StarTreeIndexConfig> starTreeIndexConfigs, boolean enableDefaultStarTree,
        Map<String, FieldIndexConfigs> indexConfigsByColName, boolean skipSegmentPreprocess,
        @Nullable MultiColumnTextIndexConfig multiColTextIndexConfig) {
      _enableDynamicStarTreeCreation = enableDynamicStarTreeCreation;
      _starTreeIndexConfigs = starTreeIndexConfigs;
      _enableDefaultStarTree = enableDefaultStarTree;
      _indexConfigsByColName = indexConfigsByColName;
      _skipSegmentPreprocess = skipSegmentPreprocess;
      _multiColTextIndexConfig = multiColTextIndexConfig;
    }

    private ResolvedIndexState withIndexConfigsByColName(Map<String, FieldIndexConfigs> indexConfigsByColName) {
      return new ResolvedIndexState(_enableDynamicStarTreeCreation, _starTreeIndexConfigs, _enableDefaultStarTree,
          indexConfigsByColName, _skipSegmentPreprocess, _multiColTextIndexConfig);
    }
  }

  /// NOTE: This step might modify the passed in table config and schema.
  ///
  /// TODO: Revisit the init handling. Currently it doesn't apply tiered config override
  public IndexLoadingConfig(@Nullable InstanceDataManagerConfig instanceDataManagerConfig,
      @Nullable TableConfig tableConfig, @Nullable Schema schema) {
    _immutableState = new ImmutableState(instanceDataManagerConfig, tableConfig, schema);
    if (tableConfig != null) {
      refreshIndexConfigs();
    }
  }

  /// Creates a segment-local wrapper around the already processed table-level config. The wrapper shares the
  /// table config, schema, and resolved index configs until a segment-specific override requires a local copy.
  private IndexLoadingConfig(IndexLoadingConfig source) {
    _immutableState = source._immutableState;
    _readModeOverride = source._readModeOverride;
    _segmentVersionOverride = source._segmentVersionOverride;
    _segmentTier = source._segmentTier;
    _knownColumns = source._knownColumns;
    _tableDataDir = source._tableDataDir;
    _errorOnColumnBuildFailure = source._errorOnColumnBuildFailure;
    _forwardIndexOnly = source._forwardIndexOnly;
    _resolvedIndexState = source._resolvedIndexState;
  }

  @VisibleForTesting
  public IndexLoadingConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig) {
    this(instanceDataManagerConfig, tableConfig, null);
  }

  @VisibleForTesting
  public IndexLoadingConfig(TableConfig tableConfig, @Nullable Schema schema) {
    this(null, tableConfig, schema);
  }

  /// NOTE: Can be used in production code when we want to load a segment as is without any modifications.
  public IndexLoadingConfig() {
    this(null, null, null);
  }

  @Nullable
  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _immutableState._instanceDataManagerConfig;
  }

  @Nullable
  public TableConfig getTableConfig() {
    return _immutableState._tableConfig;
  }

  @Nullable
  public Schema getSchema() {
    return _immutableState._schema;
  }

  public void refreshIndexConfigs() {
    if (_immutableState._tableConfig == null) {
      _resolvedIndexState = ResolvedIndexState.EMPTY;
      return;
    }
    // Accessing the index configs for single-column index is handled by IndexType.getConfig() as defined in index-spi.
    // As the tableConfig is overwritten with tier specific configs, IndexType.getConfig() can access the tier
    // specific index configs transparently.
    TableConfig tableConfig = getTableConfigWithTierOverwrites();
    Schema schema = inferSchema();
    Map<String, FieldIndexConfigs> indexConfigsByColName =
        FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema);
    // Accessing the StarTree index configs is not handled by IndexType.getConfig(), so we manually update them.
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    _resolvedIndexState = new ResolvedIndexState(indexingConfig.isEnableDynamicStarTreeCreation(),
        indexingConfig.getStarTreeIndexConfigs(), indexingConfig.isEnableDefaultStarTree(), indexConfigsByColName,
        indexingConfig.isSkipSegmentPreprocess(), indexingConfig.getMultiColumnTextIndexConfig());
  }

  private ResolvedIndexState getResolvedIndexState() {
    if (_resolvedIndexState == null) {
      refreshIndexConfigs();
    }
    return _resolvedIndexState;
  }

  private TableConfig getTableConfigWithTierOverwrites() {
    return _segmentTier == null || _immutableState._tableConfig == null ? _immutableState._tableConfig
        : TableConfigUtils.overwriteTableConfigForTier(_immutableState._tableConfig, _segmentTier);
  }

  private Schema inferSchema() {
    if (_immutableState._schema != null) {
      return _immutableState._schema;
    }
    Schema schema = new Schema();
    for (String column : getAllKnownColumns()) {
      schema.addField(new DimensionFieldSpec(column, DataType.STRING, true));
    }
    return schema;
  }

  public ReadMode getReadMode() {
    return _readModeOverride != null ? _readModeOverride : _immutableState._readMode;
  }

  public void setReadMode(ReadMode readMode) {
    _readModeOverride = readMode;
  }

  public List<String> getSortedColumns() {
    return unmodifiable(_immutableState._sortedColumns);
  }

  public boolean isEnableDynamicStarTreeCreation() {
    return getResolvedIndexState()._enableDynamicStarTreeCreation;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    return unmodifiable(getResolvedIndexState()._starTreeIndexConfigs);
  }

  @Nullable
  public MultiColumnTextIndexConfig getMultiColTextIndexConfig() {
    return getResolvedIndexState()._multiColTextIndexConfig;
  }

  public boolean isEnableDefaultStarTree() {
    return getResolvedIndexState()._enableDefaultStarTree;
  }

  @Nullable
  public SegmentVersion getSegmentVersion() {
    return _segmentVersionOverride != null ? _segmentVersionOverride : _immutableState._segmentVersion;
  }

  /// For tests only.
  public void setSegmentVersion(SegmentVersion segmentVersion) {
    _segmentVersionOverride = segmentVersion;
  }

  public boolean isRealtimeOffHeapAllocation() {
    return _immutableState._isRealtimeOffHeapAllocation;
  }

  public boolean isDirectRealtimeOffHeapAllocation() {
    return _immutableState._isDirectRealtimeOffHeapAllocation;
  }

  public ColumnMinMaxValueGeneratorMode getColumnMinMaxValueGeneratorMode() {
    return _immutableState._columnMinMaxValueGeneratorMode;
  }

  public String getSegmentStoreURI() {
    return _immutableState._segmentStoreURI;
  }

  public int getRealtimeAvgMultiValueCount() {
    return _immutableState._realtimeAvgMultiValueCount;
  }

  public String getSegmentDirectoryLoader() {
    return StringUtils.isNotBlank(_immutableState._segmentDirectoryLoader) ? _immutableState._segmentDirectoryLoader
        : SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME;
  }

  public String getInstanceId() {
    return _immutableState._instanceId;
  }

  public String getSegmentTier() {
    return _segmentTier;
  }

  public void setSegmentTier(String segmentTier) {
    if (Objects.equals(_segmentTier, segmentTier)) {
      return;
    }
    _segmentTier = segmentTier;
    _resolvedIndexState = null;
  }

  public IndexLoadingConfig withSegmentTier(@Nullable String segmentTier) {
    if (Objects.equals(_segmentTier, segmentTier)) {
      return this;
    }
    return copyWithSegmentTier(segmentTier);
  }

  /// Creates a segment-local mutable wrapper with the given tier. This always creates a wrapper, even when the tier is
  /// unchanged, so callers can safely apply additional segment-specific overrides.
  public IndexLoadingConfig copyWithSegmentTier(@Nullable String segmentTier) {
    IndexLoadingConfig derived = new IndexLoadingConfig(this);
    derived.setSegmentTier(segmentTier);
    return derived;
  }

  public String getTableDataDir() {
    return _tableDataDir;
  }

  public void setTableDataDir(String tableDataDir) {
    _tableDataDir = tableDataDir;
  }

  public boolean isErrorOnColumnBuildFailure() {
    return _errorOnColumnBuildFailure;
  }

  public void setErrorOnColumnBuildFailure(boolean errorOnColumnBuildFailure) {
    _errorOnColumnBuildFailure = errorOnColumnBuildFailure;
  }

  public boolean isForwardIndexOnly() {
    return _forwardIndexOnly;
  }

  public void setForwardIndexOnly(boolean forwardIndexOnly) {
    _forwardIndexOnly = forwardIndexOnly;
  }

  /// Whether immutable segments create physical column readers on first access. This instance setting is shared by
  /// derived segment configs and defaults to eager loading.
  public boolean isLazyColumnMaterialization() {
    return _immutableState._lazyColumnMaterialization;
  }

  public boolean isSkipSegmentPreprocess() {
    return getResolvedIndexState()._skipSegmentPreprocess;
  }

  @Nullable
  public FieldIndexConfigs getFieldIndexConfig(String columnName) {
    return getResolvedIndexState()._indexConfigsByColName.get(columnName);
  }

  public Map<String, FieldIndexConfigs> getFieldIndexConfigByColName() {
    return unmodifiable(getResolvedIndexState()._indexConfigsByColName);
  }

  /// Returns a subset of the columns on the table.
  ///
  /// When [#getSchema()] is defined, the subset is equal the columns on the schema. In other cases, this method
  /// tries its bests to get the columns from other attributes like [#getTableConfig()], which may also not be
  /// defined or may not be complete.
  private Set<String> getAllKnownColumns() {
    assert _immutableState._tableConfig != null && _immutableState._schema == null;
    if (_knownColumns == null) {
      Set<String> knownColumns =
          new HashSet<>(_immutableState._tableConfig.getIndexingConfig().getAllReferencedColumns());
      List<FieldConfig> fieldConfigs = _immutableState._tableConfig.getFieldConfigList();
      if (fieldConfigs != null) {
        for (FieldConfig fieldConfig : fieldConfigs) {
          knownColumns.add(fieldConfig.getName());
        }
      }
      _knownColumns = knownColumns;
    }
    return _knownColumns;
  }

  public Map<String, Map<String, String>> getInstanceTierConfigs() {
    return unmodifiable(_immutableState._instanceTierConfigs);
  }

  private <E> List<E> unmodifiable(List<E> list) {
    return list == null ? null : Collections.unmodifiableList(list);
  }

  private <E> Set<E> unmodifiable(Set<E> set) {
    return set == null ? null : Collections.unmodifiableSet(set);
  }

  private <K, V> Map<K, V> unmodifiable(Map<K, V> map) {
    return map == null ? null : Collections.unmodifiableMap(map);
  }

  public IndexLoadingConfig withOpenStructChildConfigs(SegmentMetadataImpl segmentMetadata) {
    if (!_immutableState._hasOpenStructColumns) {
      return this;
    }
    ResolvedIndexState resolvedIndexState = getResolvedIndexState();
    Map<String, FieldIndexConfigs> indexConfigsByColName = resolvedIndexState._indexConfigsByColName;
    Map<String, FieldIndexConfigs> updatedConfigs = null;
    for (Map.Entry<String, ColumnMetadata> entry : segmentMetadata.getColumnMetadataMap().entrySet()) {
      String childColumn = entry.getKey();
      if (!childColumn.contains(OpenStructNaming.SEPARATOR) || indexConfigsByColName.containsKey(childColumn)) {
        continue;
      }
      if (OpenStructNaming.isSparseColumn(childColumn)) {
        continue;
      }
      String parentColumn = OpenStructNaming.parseParentColumn(childColumn);
      FieldIndexConfigs parentConfigs = indexConfigsByColName.get(parentColumn);
      if (parentConfigs == null) {
        continue;
      }
      IndexConfig osConfig = parentConfigs.getConfig(StandardIndexes.openStruct());
      if (!(osConfig instanceof OpenStructIndexConfig)) {
        continue;
      }
      OpenStructIndexConfig openStructConfig = (OpenStructIndexConfig) osConfig;
      String key = OpenStructNaming.parseKey(childColumn);
      FieldConfig keyFieldConfig = openStructConfig.getValueFieldConfig(key);
      if (keyFieldConfig == null) {
        keyFieldConfig = openStructConfig.getDefaultValueFieldConfig();
      }
      FieldSpec childFieldSpec = entry.getValue().getFieldSpec();
      boolean enableInverted = openStructConfig.shouldEnableInvertedIndexForKey(key);
      FieldIndexConfigs childConfigs = new FieldIndexConfigs.Builder(
          FieldIndexConfigsUtil.fromFieldConfig(keyFieldConfig, childFieldSpec))
          .add(StandardIndexes.inverted(), enableInverted ? IndexConfig.ENABLED : IndexConfig.DISABLED)
          .build();
      if (updatedConfigs == null) {
        updatedConfigs = new HashMap<>(indexConfigsByColName);
      }
      updatedConfigs.put(childColumn, childConfigs);
    }
    if (updatedConfigs == null) {
      return this;
    }
    IndexLoadingConfig derived = new IndexLoadingConfig(this);
    derived._resolvedIndexState = resolvedIndexState.withIndexConfigsByColName(updatedConfigs);
    return derived;
  }

  public void addOpenStructChildConfigs(SegmentMetadataImpl segmentMetadata) {
    _resolvedIndexState = withOpenStructChildConfigs(segmentMetadata)._resolvedIndexState;
  }

  public IndexLoadingConfig withKnownColumns(Set<String> columns) {
    if (_knownColumns != null && _knownColumns.containsAll(columns)) {
      return this;
    }
    IndexLoadingConfig derived = new IndexLoadingConfig(this);
    derived.addKnownColumns(columns);
    return derived;
  }

  public void addKnownColumns(Set<String> columns) {
    if (_knownColumns != null && _knownColumns.containsAll(columns)) {
      return;
    }
    Set<String> knownColumns = _knownColumns != null ? new HashSet<>(_knownColumns) : new HashSet<>();
    knownColumns.addAll(columns);
    _knownColumns = knownColumns;
    _resolvedIndexState = null;
  }
}
