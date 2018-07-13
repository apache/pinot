/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.manager.config.InstanceDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.index.loader.columnminmaxvalue.ColumnMinMaxValueGeneratorMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Table level index loading config.
 */
public class IndexLoadingConfig {
  private static final int DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT = 2;

  private ReadMode _readMode = ReadMode.DEFAULT_MODE;
  private List<String> _sortedColumns = Collections.emptyList();
  private Set<String> _invertedIndexColumns = new HashSet<>();
  private Set<String> _noDictionaryColumns = new HashSet<>(); // TODO: replace this by _noDictionaryConfig.
  private Map<String, String> _noDictionaryConfig = new HashMap<>();
  private Set<String> _onHeapDictionaryColumns = new HashSet<>();
  private SegmentVersion _segmentVersion;
  // This value will remain true only when the empty constructor is invoked.
  private boolean _enableDefaultColumns = true;
  private ColumnMinMaxValueGeneratorMode _columnMinMaxValueGeneratorMode = ColumnMinMaxValueGeneratorMode.DEFAULT_MODE;
  private int _realtimeAvgMultiValueCount = DEFAULT_REALTIME_AVG_MULTI_VALUE_COUNT;
  private boolean _enableSplitCommit;
  private boolean _isRealtimeOffheapAllocation;
  private boolean _isDirectRealtimeOffheapAllocation;

  public IndexLoadingConfig(@Nonnull InstanceDataManagerConfig instanceDataManagerConfig,
      @Nonnull TableConfig tableConfig) {
    extractFromInstanceConfig(instanceDataManagerConfig);
    extractFromTableConfig(tableConfig);
  }

  private void extractFromTableConfig(@Nonnull TableConfig tableConfig) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    String tableReadMode = indexingConfig.getLoadMode();
    if (tableReadMode != null) {
      _readMode = ReadMode.getEnum(tableReadMode);
    }

    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (sortedColumns != null) {
      _sortedColumns = sortedColumns;
    }

    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    if (invertedIndexColumns != null) {
      _invertedIndexColumns.addAll(invertedIndexColumns);
    }

    List<String> noDictionaryColumns = indexingConfig.getNoDictionaryColumns();
    if (noDictionaryColumns != null) {
      _noDictionaryColumns.addAll(noDictionaryColumns);
    }

    Map<String, String> noDictionaryConfig = indexingConfig.getnoDictionaryConfig();
    if (noDictionaryConfig != null) {
      _noDictionaryConfig.putAll(noDictionaryConfig);
    }

    List<String> onHeapDictionaryColumns = indexingConfig.getOnHeapDictionaryColumns();
    if (onHeapDictionaryColumns != null) {
      _onHeapDictionaryColumns.addAll(onHeapDictionaryColumns);
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
  }

  private void extractFromInstanceConfig(@Nonnull InstanceDataManagerConfig instanceDataManagerConfig) {
    ReadMode instanceReadMode = instanceDataManagerConfig.getReadMode();
    if (instanceReadMode != null) {
      _readMode = instanceReadMode;
    }

    String instanceSegmentVersion = instanceDataManagerConfig.getSegmentFormatVersion();
    if (instanceSegmentVersion != null) {
      _segmentVersion = SegmentVersion.valueOf(instanceSegmentVersion.toLowerCase());
    }

    _enableDefaultColumns = instanceDataManagerConfig.isEnableDefaultColumns();

    _enableSplitCommit = instanceDataManagerConfig.isEnableSplitCommit();

    _isRealtimeOffheapAllocation = instanceDataManagerConfig.isRealtimeOffHeapAllocation();
    _isDirectRealtimeOffheapAllocation = instanceDataManagerConfig.isDirectRealtimeOffheapAllocation();

    String avgMultiValueCount = instanceDataManagerConfig.getAvgMultiValueCount();
    if (avgMultiValueCount != null) {
      _realtimeAvgMultiValueCount = Integer.valueOf(avgMultiValueCount);
    }
  }

  /**
   * For tests only.
   */
  public IndexLoadingConfig() {
  }

  @Nonnull
  public ReadMode getReadMode() {
    return _readMode;
  }

  /**
   * For tests only.
   */
  public void setReadMode(@Nonnull ReadMode readMode) {
    _readMode = readMode;
  }

  @Nonnull
  public List<String> getSortedColumns() {
    return _sortedColumns;
  }

  @Nonnull
  public Set<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  /**
   * For tests only.
   */
  @VisibleForTesting
  public void setInvertedIndexColumns(@Nonnull Set<String> invertedIndexColumns) {
    _invertedIndexColumns = invertedIndexColumns;
  }

  @VisibleForTesting
  public void setOnHeapDictionaryColumns(@Nonnull Set<String> onHeapDictionaryColumns) {
    _onHeapDictionaryColumns = onHeapDictionaryColumns;
  }

  @Nonnull
  public Set<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  @Nonnull
  public Map<String, String> getnoDictionaryConfig() {
    return _noDictionaryConfig;
  }

  @Nonnull
  public Set<String> getOnHeapDictionaryColumns() {
    return _onHeapDictionaryColumns;
  }

  @Nullable
  public SegmentVersion getSegmentVersion() {
    return _segmentVersion;
  }

  /**
   * For tests only.
   */
  public void setSegmentVersion(@Nonnull SegmentVersion segmentVersion) {
    _segmentVersion = segmentVersion;
  }

  public boolean isEnableDefaultColumns() {
    return _enableDefaultColumns;
  }

  public boolean isEnableSplitCommit() {
    return _enableSplitCommit;
  }

  public boolean isRealtimeOffheapAllocation() {
    return _isRealtimeOffheapAllocation;
  }

  public boolean isDirectRealtimeOffheapAllocation() {
    return _isDirectRealtimeOffheapAllocation;
  }

  @Nonnull
  public ColumnMinMaxValueGeneratorMode getColumnMinMaxValueGeneratorMode() {
    return _columnMinMaxValueGeneratorMode;
  }

  /**
   * For tests only.
   */
  public void setColumnMinMaxValueGeneratorMode(ColumnMinMaxValueGeneratorMode columnMinMaxValueGeneratorMode) {
    _columnMinMaxValueGeneratorMode = columnMinMaxValueGeneratorMode;
  }

  public int getRealtimeAvgMultiValueCount() {
    return _realtimeAvgMultiValueCount;
  }
}
