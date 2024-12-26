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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.TimestampIndexUtils;


/**
 * Table level index loading config.
 */
public class IndexLoadingConfig {
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;
  public static final String READ_MODE_KEY = "readMode";

  private final InstanceDataManagerConfig _instanceDataManagerConfig;
  private final TableConfig _tableConfig;
  private final Schema _schema;

  // These fields can be modified after initialization
  // TODO: Revisit them
  private ReadMode _readMode = ReadMode.DEFAULT_MODE;
  private SegmentVersion _segmentVersion;
  private String _segmentTier;
  private Set<String> _knownColumns;
  private String _tableDataDir;
  private boolean _errorOnColumnBuildFailure;

  // Initialized by instance data manager config
  private String _instanceId;
  private boolean _isRealtimeOffHeapAllocation;
  private boolean _isDirectRealtimeOffHeapAllocation;
  private int _realtimeAvgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
  private String _segmentStoreURI;
  private String _segmentDirectoryLoader;
  private Map<String, Map<String, String>> _instanceTierConfigs;

  // Initialized by table config and schema
  private List<String> _sortedColumns = Collections.emptyList();
  private ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
  private boolean _enableDynamicStarTreeCreation;
  private List<StarTreeIndexConfig> _starTreeIndexConfigs;
  private boolean _enableDefaultStarTree;
  private Map<String, FieldIndexConfigs> _indexConfigsByColName = new HashMap<>();

  private boolean _dirty = true;

  /**
   * NOTE: This step might modify the passed in table config and schema.
   *
   * TODO: Revisit the init handling. Currently it doesn't apply tiered config override
   */
  public IndexLoadingConfig(@Nullable InstanceDataManagerConfig instanceDataManagerConfig,
      @Nullable TableConfig tableConfig, @Nullable Schema schema) {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    _tableConfig = tableConfig;
    _schema = schema;
    init();
  }

  @VisibleForTesting
  public IndexLoadingConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig) {
    this(instanceDataManagerConfig, tableConfig, null);
  }

  @VisibleForTesting
  public IndexLoadingConfig(TableConfig tableConfig, @Nullable Schema schema) {
    this(null, tableConfig, schema);
  }

  /**
   * NOTE: Can be used in production code when we want to load a segment as is without any modifications.
   */
  public IndexLoadingConfig() {
    this(null, null, null);
  }

  @Nullable
  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  @Nullable
  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  @Nullable
  public Schema getSchema() {
    return _schema;
  }

  private void init() {
    if (_instanceDataManagerConfig != null) {
      extractFromInstanceConfig();
    }
    if (_tableConfig != null) {
      extractFromTableConfigAndSchema();
    }
  }

  private void extractFromInstanceConfig() {
    _instanceId = _instanceDataManagerConfig.getInstanceId();

    ReadMode instanceReadMode = _instanceDataManagerConfig.getReadMode();
    if (instanceReadMode != null) {
      _readMode = instanceReadMode;
    }

    String instanceSegmentVersion = _instanceDataManagerConfig.getSegmentFormatVersion();
    if (instanceSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
    }

    _isRealtimeOffHeapAllocation = _instanceDataManagerConfig.isRealtimeOffHeapAllocation();
    _isDirectRealtimeOffHeapAllocation = _instanceDataManagerConfig.isDirectRealtimeOffHeapAllocation();

    String avgMultiValueCount = _instanceDataManagerConfig.getAvgMultiValueCount();
    if (avgMultiValueCount != null) {
      _realtimeAvgMultiValueCount = Integer.parseInt(avgMultiValueCount);
    }
    _segmentStoreURI = _instanceDataManagerConfig.getSegmentStoreUri();
    _segmentDirectoryLoader = _instanceDataManagerConfig.getSegmentDirectoryLoader();

    Map<String, Map<String, String>> tierConfigs = _instanceDataManagerConfig.getTierConfigs();
    _instanceTierConfigs = tierConfigs != null ? tierConfigs : Map.of();
  }

  private void extractFromTableConfigAndSchema() {
    if (_schema != null) {
      TimestampIndexUtils.applyTimestampIndex(_tableConfig, _schema);
    }

    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    String tableReadMode = indexingConfig.getLoadMode();
    if (tableReadMode != null) {
      _readMode = ReadMode.getEnum(tableReadMode);
    }

    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (sortedColumns != null) {
      _sortedColumns = sortedColumns;
    }

    String tableSegmentVersion = indexingConfig.getSegmentFormatVersion();
    if (tableSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(tableSegmentVersion.toLowerCase());
    }

    String columnMinMaxValueGeneratorMode = indexingConfig.getColumnMinMaxValueGeneratorMode();
    if (columnMinMaxValueGeneratorMode != null) {
      _columnMinMaxValueGeneratorMode =
          ColumnMinMaxValueGeneratorMode.valueOf(columnMinMaxValueGeneratorMode.toUpperCase());
    }

    refreshIndexConfigs();
  }

  public void refreshIndexConfigs() {
    if (_tableConfig == null) {
      _dirty = false;
      return;
    }
    // Accessing the index configs for single-column index is handled by IndexType.getConfig() as defined in index-spi.
    // As the tableConfig is overwritten with tier specific configs, IndexType.getConfig() can access the tier
    // specific index configs transparently.
    TableConfig tableConfig = getTableConfigWithTierOverwrites();
    Schema schema = inferSchema();
    _indexConfigsByColName = FieldIndexConfigsUtil.createIndexConfigsByColName(tableConfig, schema);
    // Accessing the StarTree index configs is not handled by IndexType.getConfig(), so we manually update them.
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    _enableDynamicStarTreeCreation = indexingConfig.isEnableDynamicStarTreeCreation();
    _starTreeIndexConfigs = indexingConfig.getStarTreeIndexConfigs();
    _enableDefaultStarTree = indexingConfig.isEnableDefaultStarTree();
    _dirty = false;
  }

  private TableConfig getTableConfigWithTierOverwrites() {
    return (_segmentTier == null || _tableConfig == null) ? _tableConfig
        : TableConfigUtils.overwriteTableConfigForTier(_tableConfig, _segmentTier);
  }

  private Schema inferSchema() {
    if (_schema != null) {
      return _schema;
    }
    Schema schema = new Schema();
    for (String column : getAllKnownColumns()) {
      schema.addField(new DimensionFieldSpec(column, FieldSpec.DataType.STRING, true));
    }
    return schema;
  }

  public ReadMode getReadMode() {
    return _readMode;
  }

  public void setReadMode(ReadMode readMode) {
    _readMode = readMode;
  }

  public List<String> getSortedColumns() {
    return unmodifiable(_sortedColumns);
  }

  public boolean isEnableDynamicStarTreeCreation() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return _enableDynamicStarTreeCreation;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return unmodifiable(_starTreeIndexConfigs);
  }

  public boolean isEnableDefaultStarTree() {
    if (_dirty) {
      refreshIndexConfigs();
    }
    return _enableDefaultStarTree;
  }

  @Nullable
  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
  }

  /**
   * For tests only.
   */
  public void setSegmentVersion(SegmentVersion segmentVersion) {
    _segmentVersion = segmentVersion;
  }

  public boolean isRealtimeOffHeapAllocation() {
    return _isRealtimeOffHeapAllocation;
  }

  public boolean isDirectRealtimeOffHeapAllocation() {
    return _isDirectRealtimeOffHeapAllocation;
  }

  public ColumnMinMaxValueGeneratorMode getColumnMinMaxValueGeneratorMode() {
    return _columnMinMaxValueGeneratorMode;
  }

  public String getSegmentStoreURI() {
    return _segmentStoreURI;
  }

  public int getRealtimeAvgMultiValueCount() {
    return _realtimeAvgMultiValueCount;
  }

  public String getSegmentDirectoryLoader() {
    return StringUtils.isNotBlank(_segmentDirectoryLoader) ? _segmentDirectoryLoader
        : SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME;
  }

  public PinotConfiguration getSegmentDirectoryConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(READ_MODE_KEY, _readMode);
    return new PinotConfiguration(props);
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getSegmentTier() {
    return _segmentTier;
  }

  public void setSegmentTier(String segmentTier) {
    _segmentTier = segmentTier;
    _dirty = true;
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

  @Nullable
  public FieldIndexConfigs getFieldIndexConfig(String columnName) {
    if (_indexConfigsByColName == null || _dirty) {
      refreshIndexConfigs();
    }
    return _indexConfigsByColName.get(columnName);
  }

  public Map<String, FieldIndexConfigs> getFieldIndexConfigByColName() {
    if (_indexConfigsByColName == null || _dirty) {
      refreshIndexConfigs();
    }
    return unmodifiable(_indexConfigsByColName);
  }

  /**
   * Returns a subset of the columns on the table.
   *
   * When {@link #getSchema()} is defined, the subset is equal the columns on the schema. In other cases, this method
   * tries its bests to get the columns from other attributes like {@link #getTableConfig()}, which may also not be
   * defined or may not be complete.
   */
  private Set<String> getAllKnownColumns() {
    assert _tableConfig != null && _schema == null;
    if (_knownColumns == null) {
      Set<String> knownColumns = _tableConfig.getIndexingConfig().getAllReferencedColumns();
      List<FieldConfig> fieldConfigs = _tableConfig.getFieldConfigList();
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
    return unmodifiable(_instanceTierConfigs);
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

  public void addKnownColumns(Set<String> columns) {
    if (_knownColumns == null) {
      _knownColumns = new HashSet<>(columns);
    } else {
      _knownColumns.addAll(columns);
    }
    _dirty = true;
  }
}
