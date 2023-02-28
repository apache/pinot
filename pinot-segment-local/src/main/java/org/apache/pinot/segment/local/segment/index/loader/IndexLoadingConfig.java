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
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.segment.local.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.instance.DummyInstanceDataManagerConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;


/**
 * Table level index loading config.
 */
public class IndexLoadingConfig {
  public static final String READ_MODE_KEY = "readMode";
  public static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;

  private final InstanceDataManagerConfig _instanceDataManagerConfig;
  private TableConfig _tableConfig;
  private Schema _schema;
  private String _segmentTier;

  public IndexLoadingConfig(InstanceDataManagerConfig instanceDataManagerConfig, TableConfig tableConfig,
      @Nullable Schema schema) {
    _instanceDataManagerConfig = instanceDataManagerConfig;
    _tableConfig = tableConfig;
    _schema = schema;
  }

  @VisibleForTesting
  public IndexLoadingConfig(TableConfig tableConfig, @Nullable Schema schema) {
    this(new DummyInstanceDataManagerConfig(), tableConfig, schema);
  }

  @VisibleForTesting
  public IndexLoadingConfig(TableConfig tableConfig) {
    this(tableConfig, null);
  }

  public InstanceDataManagerConfig getInstanceDataManagerConfig() {
    return _instanceDataManagerConfig;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public void setTableConfig(TableConfig tableConfig) {
    _tableConfig = tableConfig;
  }

  @Nullable
  public Schema getSchema() {
    return _schema;
  }

  public void setSchema(@Nullable Schema schema) {
    _schema = schema;
  }

  @Nullable
  public String getSegmentTier() {
    return _segmentTier;
  }

  public void setSegmentTier(String segmentTier) {
    _segmentTier = segmentTier;
  }

  public Set<String> getNoDictionaryColumns() {
    List<String> noDictionaryColumns = _tableConfig.getIndexingConfig().getNoDictionaryColumns();
    return noDictionaryColumns != null ? new HashSet<>(noDictionaryColumns) : Collections.emptySet();
  }

  public Map<String, String> getNoDictionaryConfig() {
    Map<String, String> noDictionaryConfig = _tableConfig.getIndexingConfig().getNoDictionaryConfig();
    return noDictionaryConfig != null ? noDictionaryConfig : Collections.emptyMap();
  }

  public Set<String> getVarLengthDictionaryColumns() {
    List<String> varLengthDictionaryColumns = _tableConfig.getIndexingConfig().getVarLengthDictionaryColumns();
    return varLengthDictionaryColumns != null ? new HashSet<>(varLengthDictionaryColumns) : Collections.emptySet();
  }

  public Set<String> getOnHeapDictionaryColumns() {
    List<String> onHeapDictionaryColumns = _tableConfig.getIndexingConfig().getOnHeapDictionaryColumns();
    return onHeapDictionaryColumns != null ? new HashSet<>(onHeapDictionaryColumns) : Collections.emptySet();
  }

  public List<String> getSortedColumns() {
    return _tableConfig.getIndexingConfig().getSortedColumn();
  }

  public Set<String> getInvertedIndexColumns() {
    List<String> invertedIndexColumns = _tableConfig.getIndexingConfig().getInvertedIndexColumns();
    return invertedIndexColumns != null ? new HashSet<>(invertedIndexColumns) : Collections.emptySet();
  }

  public Set<String> getRangeIndexColumns() {
    List<String> rangeIndexColumns = _tableConfig.getIndexingConfig().getRangeIndexColumns();
    return rangeIndexColumns != null ? new HashSet<>(rangeIndexColumns) : Collections.emptySet();
  }

  public int getRangeIndexVersion() {
    return _tableConfig.getIndexingConfig().getRangeIndexVersion();
  }

  public Map<String, BloomFilterConfig> getBloomFilterConfigs() {
    Map<String, BloomFilterConfig> bloomFilterConfigs = new HashMap<>();
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    if (indexingConfig.getBloomFilterColumns() != null) {
      for (String bloomFilterColumn : indexingConfig.getBloomFilterColumns()) {
        bloomFilterConfigs.put(bloomFilterColumn, new BloomFilterConfig(BloomFilterConfig.DEFAULT_FPP, 0, false));
      }
    }
    if (indexingConfig.getBloomFilterConfigs() != null) {
      bloomFilterConfigs.putAll(indexingConfig.getBloomFilterConfigs());
    }
    return bloomFilterConfigs;
  }

  public Set<String> getTextIndexColumns() {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return Collections.emptySet();
    }
    Set<String> textIndexColumns = new HashSet<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.TEXT)) {
        textIndexColumns.add(fieldConfig.getName());
      }
    }
    return textIndexColumns;
  }

  public Set<String> getFSTIndexColumns() {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return Collections.emptySet();
    }
    Set<String> fstIndexColumns = new HashSet<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.FST)) {
        fstIndexColumns.add(fieldConfig.getName());
      }
    }
    return fstIndexColumns;
  }

  public Map<String, FSTType> getFSTTypes() {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return Collections.emptyMap();
    }
    FSTType defaultFstType = _tableConfig.getIndexingConfig().getFSTIndexType();
    if (defaultFstType == null) {
      defaultFstType = FSTType.LUCENE;
    }
    Map<String, FSTType> fstTypes = new HashMap<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      List<FieldConfig.IndexType> indexTypes = fieldConfig.getIndexTypes();
      if (indexTypes.contains(FieldConfig.IndexType.TEXT) || indexTypes.contains(FieldConfig.IndexType.FST)) {
        Map<String, String> fieldConfigProperties = fieldConfig.getProperties();
        if (fieldConfigProperties == null) {
          fstTypes.put(fieldConfig.getName(), defaultFstType);
        } else {
          String fstType = fieldConfigProperties.get(FieldConfig.TEXT_FST_TYPE);
          fstTypes.put(fieldConfig.getName(),
              fstType != null ? FSTType.valueOf(fstType.toUpperCase()) : defaultFstType);
        }
      }
    }
    return fstTypes;
  }

  public Map<String, H3IndexConfig> getH3IndexConfigs() {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return Collections.emptyMap();
    }
    Map<String, H3IndexConfig> h3IndexConfigs = new HashMap<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.H3)) {
        //noinspection ConstantConditions
        h3IndexConfigs.put(fieldConfig.getName(), new H3IndexConfig(fieldConfig.getProperties()));
      }
    }
    return h3IndexConfigs;
  }

  public Map<String, JsonIndexConfig> getJsonIndexConfigs() {
    // Ignore jsonIndexColumns when jsonIndexConfigs is configured
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    if (indexingConfig.getJsonIndexConfigs() != null) {
      return indexingConfig.getJsonIndexConfigs();
    }
    if (indexingConfig.getJsonIndexColumns() != null) {
      Map<String, JsonIndexConfig> jsonIndexConfigs = new HashMap<>();
      for (String jsonIndexColumn : indexingConfig.getJsonIndexColumns()) {
        jsonIndexConfigs.put(jsonIndexColumn, new JsonIndexConfig());
      }
      return jsonIndexConfigs;
    }
    return Collections.emptyMap();
  }

  public Map<String, Map<String, String>> getColumnProperties() {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return Collections.emptyMap();
    }
    Map<String, Map<String, String>> columnProperties = new HashMap<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      columnProperties.put(fieldConfig.getName(), fieldConfig.getProperties());
    }
    return columnProperties;
  }

  /**
   * Populates a map containing column name as key and compression type as value. This map will only contain the
   * compressionType overrides, and it does not correspond to the default value of compressionType (derived using
   * SegmentColumnarIndexCreator.getColumnCompressionType())  used for a column. Note that only RAW forward index
   * columns will be populated in this map.
   *
   * @return a map containing column name as key and compressionType as value.
   */
  public Map<String, ChunkCompressionType> getCompressionConfigs() {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return Collections.emptyMap();
    }
    Map<String, ChunkCompressionType> compressionConfigs = new HashMap<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      if (fieldConfig.getCompressionCodec() != null) {
        compressionConfigs.put(fieldConfig.getName(),
            ChunkCompressionType.valueOf(fieldConfig.getCompressionCodec().name()));
      }
    }
    return compressionConfigs;
  }

  public Set<String> getForwardIndexDisabledColumns() {
    List<FieldConfig> fieldConfigList = _tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      return Collections.emptySet();
    }
    Set<String> forwardIndexDisabledColumns = new HashSet<>();
    for (FieldConfig fieldConfig : fieldConfigList) {
      Map<String, String> fieldConfigProperties = fieldConfig.getProperties();
      if (fieldConfigProperties != null && Boolean.parseBoolean(
          fieldConfigProperties.get(FieldConfig.FORWARD_INDEX_DISABLED))) {
        forwardIndexDisabledColumns.add(fieldConfig.getName());
      }
    }
    return forwardIndexDisabledColumns;
  }

  @Nullable
  public List<StarTreeIndexConfig> getStarTreeIndexConfigs() {
    return _tableConfig.getIndexingConfig().getStarTreeIndexConfigs();
  }

  public boolean isEnableDynamicStarTreeCreation() {
    return _tableConfig.getIndexingConfig().isEnableDynamicStarTreeCreation();
  }

  public boolean isEnableDefaultStarTree() {
    return _tableConfig.getIndexingConfig().isEnableDefaultStarTree();
  }

  @Nullable
  public SegmentVersion getSegmentVersion() {
    String tableSegmentVersion = _tableConfig.getIndexingConfig().getSegmentFormatVersion();
    if (tableSegmentVersion != null) {
      return SegmentVersion.valueOf(tableSegmentVersion.toLowerCase());
    }
    String instanceSegmentVersion = _instanceDataManagerConfig.getSegmentFormatVersion();
    if (instanceSegmentVersion != null) {
      return SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
    }
    return null;
  }

  public boolean isEnableSplitCommit() {
    return _instanceDataManagerConfig.isEnableSplitCommit();
  }

  public boolean isEnableSplitCommitEndWithMetadata() {
    return _instanceDataManagerConfig.isEnableSplitCommitEndWithMetadata();
  }

  public boolean isRealtimeOffHeapAllocation() {
    return _instanceDataManagerConfig.isRealtimeOffHeapAllocation();
  }

  public boolean isDirectRealtimeOffHeapAllocation() {
    return _instanceDataManagerConfig.isDirectRealtimeOffHeapAllocation();
  }

  public ColumnMinMaxValueGeneratorMode getColumnMinMaxValueGeneratorMode() {
    String columnMinMaxValueGeneratorMode = _tableConfig.getIndexingConfig().getColumnMinMaxValueGeneratorMode();
    return columnMinMaxValueGeneratorMode != null ? ColumnMinMaxValueGeneratorMode.valueOf(
        columnMinMaxValueGeneratorMode.toUpperCase()) : ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
  }

  public String getSegmentStoreURI() {
    return _instanceDataManagerConfig.getSegmentStoreUri();
  }

  public int getRealtimeAvgMultiValueCount() {
    String avgMultiValueCount = _instanceDataManagerConfig.getAvgMultiValueCount();
    return avgMultiValueCount != null ? Integer.parseInt(avgMultiValueCount) : DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
  }

  public String getSegmentDirectoryLoader() {
    String segmentDirectoryLoader = _instanceDataManagerConfig.getSegmentDirectoryLoader();
    return segmentDirectoryLoader != null ? segmentDirectoryLoader
        : SegmentDirectoryLoaderRegistry.DEFAULT_SEGMENT_DIRECTORY_LOADER_NAME;
  }

  public PinotConfiguration getSegmentDirectoryConfigs() {
    ReadMode readMode;
    String tableReadMode = _tableConfig.getIndexingConfig().getLoadMode();
    if (tableReadMode != null) {
      readMode = ReadMode.getEnum(tableReadMode);
    } else {
      readMode = _instanceDataManagerConfig.getReadMode();
    }
    Map<String, Object> properties = new HashMap<>();
    properties.put(READ_MODE_KEY, readMode);
    return new PinotConfiguration(properties);
  }

  public String getInstanceId() {
    return _instanceDataManagerConfig.getInstanceId();
  }

  public String getTableDataDir() {
    return _instanceDataManagerConfig.getInstanceDataDir() + "/" + _tableConfig.getTableName();
  }

  public Map<String, Map<String, String>> getInstanceTierConfigs() {
    return TierConfigUtils.getInstanceTierConfigs(_instanceDataManagerConfig);
  }
}
