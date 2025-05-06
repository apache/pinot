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
package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;


public class UpsertContext {
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final List<String> _primaryKeyColumns;
  private final HashFunction _hashFunction;
  private final List<String> _comparisonColumns;
  @Nullable
  private final PartialUpsertHandler _partialUpsertHandler;
  @Nullable
  private final String _deleteRecordColumn;
  private final boolean _dropOutOfOrderRecord;
  @Nullable
  private final String _outOfOrderRecordColumn;
  private final boolean _enableSnapshot;
  private final boolean _enablePreload;
  private final double _metadataTTL;
  private final double _deletedKeysTTL;
  private final boolean _enableDeletedKeysCompactionConsistency;
  private final UpsertConfig.ConsistencyMode _consistencyMode;
  private final long _upsertViewRefreshIntervalMs;
  private final long _newSegmentTrackingTimeMs;
  @Nullable
  private final Map<String, String> _metadataManagerConfigs;

  /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
  @Deprecated
  private final boolean _allowPartialUpsertConsumptionDuringCommit;

  /// Can be null if the context is not created from server.
  @Nullable
  private final TableDataManager _tableDataManager;
  private final File _tableIndexDir;

  private UpsertContext(TableConfig tableConfig, Schema schema, List<String> primaryKeyColumns,
      HashFunction hashFunction, List<String> comparisonColumns, @Nullable PartialUpsertHandler partialUpsertHandler,
      @Nullable String deleteRecordColumn, boolean dropOutOfOrderRecord, @Nullable String outOfOrderRecordColumn,
      boolean enableSnapshot, boolean enablePreload, double metadataTTL, double deletedKeysTTL,
      boolean enableDeletedKeysCompactionConsistency, UpsertConfig.ConsistencyMode consistencyMode,
      long upsertViewRefreshIntervalMs, long newSegmentTrackingTimeMs,
      @Nullable Map<String, String> metadataManagerConfigs, boolean allowPartialUpsertConsumptionDuringCommit,
      @Nullable TableDataManager tableDataManager, File tableIndexDir) {
    _tableConfig = tableConfig;
    _schema = schema;
    _primaryKeyColumns = primaryKeyColumns;
    _hashFunction = hashFunction;
    _comparisonColumns = comparisonColumns;
    _partialUpsertHandler = partialUpsertHandler;
    _deleteRecordColumn = deleteRecordColumn;
    _dropOutOfOrderRecord = dropOutOfOrderRecord;
    _outOfOrderRecordColumn = outOfOrderRecordColumn;
    _enableSnapshot = enableSnapshot;
    _enablePreload = enablePreload;
    _metadataTTL = metadataTTL;
    _deletedKeysTTL = deletedKeysTTL;
    _enableDeletedKeysCompactionConsistency = enableDeletedKeysCompactionConsistency;
    _consistencyMode = consistencyMode;
    _upsertViewRefreshIntervalMs = upsertViewRefreshIntervalMs;
    _newSegmentTrackingTimeMs = newSegmentTrackingTimeMs;
    _metadataManagerConfigs = metadataManagerConfigs;
    _allowPartialUpsertConsumptionDuringCommit = allowPartialUpsertConsumptionDuringCommit;
    _tableDataManager = tableDataManager;
    _tableIndexDir = tableIndexDir;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public Schema getSchema() {
    return _schema;
  }

  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  public List<String> getComparisonColumns() {
    return _comparisonColumns;
  }

  @Nullable
  public PartialUpsertHandler getPartialUpsertHandler() {
    return _partialUpsertHandler;
  }

  public UpsertConfig.Mode getUpsertMode() {
    return _partialUpsertHandler == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }

  @Nullable
  public String getDeleteRecordColumn() {
    return _deleteRecordColumn;
  }

  public boolean isDropOutOfOrderRecord() {
    return _dropOutOfOrderRecord;
  }

  @Nullable
  public String getOutOfOrderRecordColumn() {
    return _outOfOrderRecordColumn;
  }

  public boolean isSnapshotEnabled() {
    return _enableSnapshot;
  }

  public boolean isPreloadEnabled() {
    return _enablePreload;
  }

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  public double getDeletedKeysTTL() {
    return _deletedKeysTTL;
  }

  public boolean isEnableDeletedKeysCompactionConsistency() {
    return _enableDeletedKeysCompactionConsistency;
  }

  public UpsertConfig.ConsistencyMode getConsistencyMode() {
    return _consistencyMode;
  }

  public long getUpsertViewRefreshIntervalMs() {
    return _upsertViewRefreshIntervalMs;
  }

  public long getNewSegmentTrackingTimeMs() {
    return _newSegmentTrackingTimeMs;
  }

  @Nullable
  public Map<String, String> getMetadataManagerConfigs() {
    return _metadataManagerConfigs;
  }

  @Deprecated
  public boolean isAllowPartialUpsertConsumptionDuringCommit() {
    return _allowPartialUpsertConsumptionDuringCommit;
  }

  @Nullable
  public TableDataManager getTableDataManager() {
    return _tableDataManager;
  }

  public File getTableIndexDir() {
    return _tableIndexDir;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("primaryKeyColumns", _primaryKeyColumns)
        .append("hashFunction", _hashFunction)
        .append("comparisonColumns", _comparisonColumns)
        .append("mode", getUpsertMode())
        .append("deleteRecordColumn", _deleteRecordColumn)
        .append("dropOutOfOrderRecord", _dropOutOfOrderRecord)
        .append("outOfOrderRecordColumn", _outOfOrderRecordColumn)
        .append("enableSnapshot", _enableSnapshot)
        .append("enablePreload", _enablePreload)
        .append("metadataTTL", _metadataTTL)
        .append("deletedKeysTTL", _deletedKeysTTL)
        .append("enableDeletedKeysCompactionConsistency", _enableDeletedKeysCompactionConsistency)
        .append("consistencyMode", _consistencyMode)
        .append("upsertViewRefreshIntervalMs", _upsertViewRefreshIntervalMs)
        .append("newSegmentTrackingTimeMs", _newSegmentTrackingTimeMs)
        .append("metadataManagerConfigs", _metadataManagerConfigs)
        .append("allowPartialUpsertConsumptionDuringCommit", _allowPartialUpsertConsumptionDuringCommit)
        .append("tableIndexDir", _tableIndexDir)
        .toString();
  }

  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;
    private List<String> _primaryKeyColumns;
    private HashFunction _hashFunction = HashFunction.NONE;
    private List<String> _comparisonColumns;
    private PartialUpsertHandler _partialUpsertHandler;
    private String _deleteRecordColumn;
    private boolean _dropOutOfOrderRecord;
    private String _outOfOrderRecordColumn;
    private boolean _enableSnapshot;
    private boolean _enablePreload;
    private double _metadataTTL;
    private double _deletedKeysTTL;
    private boolean _enableDeletedKeysCompactionConsistency;
    private UpsertConfig.ConsistencyMode _consistencyMode = UpsertConfig.ConsistencyMode.NONE;
    private long _upsertViewRefreshIntervalMs;
    private long _newSegmentTrackingTimeMs;
    private Map<String, String> _metadataManagerConfigs;
    @Deprecated
    private boolean _allowPartialUpsertConsumptionDuringCommit;
    private TableDataManager _tableDataManager;
    private File _tableIndexDir;

    public Builder setTableConfig(TableConfig tableConfig) {
      _tableConfig = tableConfig;
      return this;
    }

    public Builder setSchema(Schema schema) {
      _schema = schema;
      return this;
    }

    public Builder setPrimaryKeyColumns(List<String> primaryKeyColumns) {
      _primaryKeyColumns = primaryKeyColumns;
      return this;
    }

    public Builder setHashFunction(HashFunction hashFunction) {
      _hashFunction = hashFunction;
      return this;
    }

    public Builder setComparisonColumns(List<String> comparisonColumns) {
      _comparisonColumns = comparisonColumns;
      return this;
    }

    public Builder setPartialUpsertHandler(PartialUpsertHandler partialUpsertHandler) {
      _partialUpsertHandler = partialUpsertHandler;
      return this;
    }

    public Builder setDeleteRecordColumn(String deleteRecordColumn) {
      _deleteRecordColumn = deleteRecordColumn;
      return this;
    }

    public Builder setDropOutOfOrderRecord(boolean dropOutOfOrderRecord) {
      _dropOutOfOrderRecord = dropOutOfOrderRecord;
      return this;
    }

    public Builder setOutOfOrderRecordColumn(String outOfOrderRecordColumn) {
      _outOfOrderRecordColumn = outOfOrderRecordColumn;
      return this;
    }

    public Builder setEnableSnapshot(boolean enableSnapshot) {
      _enableSnapshot = enableSnapshot;
      return this;
    }

    public Builder setEnablePreload(boolean enablePreload) {
      _enablePreload = enablePreload;
      return this;
    }

    public Builder setMetadataTTL(double metadataTTL) {
      _metadataTTL = metadataTTL;
      return this;
    }

    public Builder setDeletedKeysTTL(double deletedKeysTTL) {
      _deletedKeysTTL = deletedKeysTTL;
      return this;
    }

    public Builder setEnableDeletedKeysCompactionConsistency(boolean enableDeletedKeysCompactionConsistency) {
      _enableDeletedKeysCompactionConsistency = enableDeletedKeysCompactionConsistency;
      return this;
    }

    public Builder setConsistencyMode(UpsertConfig.ConsistencyMode consistencyMode) {
      _consistencyMode = consistencyMode;
      return this;
    }

    public Builder setUpsertViewRefreshIntervalMs(long upsertViewRefreshIntervalMs) {
      _upsertViewRefreshIntervalMs = upsertViewRefreshIntervalMs;
      return this;
    }

    public Builder setNewSegmentTrackingTimeMs(long newSegmentTrackingTimeMs) {
      _newSegmentTrackingTimeMs = newSegmentTrackingTimeMs;
      return this;
    }

    public Builder setMetadataManagerConfigs(Map<String, String> metadataManagerConfigs) {
      _metadataManagerConfigs = metadataManagerConfigs;
      return this;
    }

    @Deprecated
    public Builder setAllowPartialUpsertConsumptionDuringCommit(boolean allowPartialUpsertConsumptionDuringCommit) {
      _allowPartialUpsertConsumptionDuringCommit = allowPartialUpsertConsumptionDuringCommit;
      return this;
    }

    public Builder setTableDataManager(TableDataManager tableDataManager) {
      _tableDataManager = tableDataManager;
      return this;
    }

    public Builder setTableIndexDir(File tableIndexDir) {
      _tableIndexDir = tableIndexDir;
      return this;
    }

    public UpsertContext build() {
      Preconditions.checkState(_tableConfig != null, "Table config must be set");
      Preconditions.checkState(_schema != null, "Schema must be set");
      Preconditions.checkState(CollectionUtils.isNotEmpty(_primaryKeyColumns), "Primary key columns must be set");
      Preconditions.checkState(_hashFunction != null, "Hash function must be set");
      Preconditions.checkState(CollectionUtils.isNotEmpty(_comparisonColumns), "Comparison columns must be set");
      Preconditions.checkState(_consistencyMode != null, "Consistency mode must be set");
      if (_tableIndexDir == null) {
        Preconditions.checkState(_tableDataManager != null, "Either table data manager or table index dir must be set");
        _tableIndexDir = _tableDataManager.getTableDataDir();
      }
      return new UpsertContext(_tableConfig, _schema, _primaryKeyColumns, _hashFunction, _comparisonColumns,
          _partialUpsertHandler, _deleteRecordColumn, _dropOutOfOrderRecord, _outOfOrderRecordColumn, _enableSnapshot,
          _enablePreload, _metadataTTL, _deletedKeysTTL, _enableDeletedKeysCompactionConsistency, _consistencyMode,
          _upsertViewRefreshIntervalMs, _newSegmentTrackingTimeMs, _metadataManagerConfigs,
          _allowPartialUpsertConsumptionDuringCommit, _tableDataManager, _tableIndexDir);
    }
  }
}
