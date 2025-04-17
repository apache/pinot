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
package org.apache.pinot.segment.local.dedup;

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
import org.apache.pinot.spi.data.Schema;


public class DedupContext {
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final List<String> _primaryKeyColumns;
  private final HashFunction _hashFunction;
  private final double _metadataTTL;
  @Nullable
  private final String _dedupTimeColumn;
  private final boolean _enablePreload;
  private final boolean _ignoreNonDefaultTiers;
  @Nullable
  private final Map<String, String> _metadataManagerConfigs;

  /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
  @Deprecated
  private final boolean _allowDedupConsumptionDuringCommit;

  /// Can be null if the context is not created from server.
  @Nullable
  private final TableDataManager _tableDataManager;
  private final File _tableIndexDir;

  private DedupContext(TableConfig tableConfig, Schema schema, List<String> primaryKeyColumns,
      HashFunction hashFunction, double metadataTTL, @Nullable String dedupTimeColumn, boolean enablePreload,
      boolean ignoreNonDefaultTiers, @Nullable Map<String, String> metadataManagerConfigs,
      boolean allowDedupConsumptionDuringCommit, @Nullable TableDataManager tableDataManager, File tableIndexDir) {
    _tableConfig = tableConfig;
    _schema = schema;
    _primaryKeyColumns = primaryKeyColumns;
    _hashFunction = hashFunction;
    _metadataTTL = metadataTTL;
    _dedupTimeColumn = dedupTimeColumn;
    _enablePreload = enablePreload;
    _ignoreNonDefaultTiers = ignoreNonDefaultTiers;
    _metadataManagerConfigs = metadataManagerConfigs;
    _allowDedupConsumptionDuringCommit = allowDedupConsumptionDuringCommit;
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

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  @Nullable
  public String getDedupTimeColumn() {
    return _dedupTimeColumn;
  }

  public boolean isPreloadEnabled() {
    return _enablePreload;
  }

  public boolean isIgnoreNonDefaultTiers() {
    return _ignoreNonDefaultTiers;
  }

  @Nullable
  public Map<String, String> getMetadataManagerConfigs() {
    return _metadataManagerConfigs;
  }

  @Deprecated
  public boolean isAllowDedupConsumptionDuringCommit() {
    return _allowDedupConsumptionDuringCommit;
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
        .append("metadataTTL", _metadataTTL)
        .append("dedupTimeColumn", _dedupTimeColumn)
        .append("enablePreload", _enablePreload)
        .append("ignoreNonDefaultTiers", _ignoreNonDefaultTiers)
        .append("metadataManagerConfigs", _metadataManagerConfigs)
        .append("allowDedupConsumptionDuringCommit", _allowDedupConsumptionDuringCommit)
        .append("tableIndexDir", _tableIndexDir)
        .toString();
  }

  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;
    private List<String> _primaryKeyColumns;
    private HashFunction _hashFunction = HashFunction.NONE;
    private double _metadataTTL;
    private String _dedupTimeColumn;
    private boolean _enablePreload;
    private boolean _ignoreNonDefaultTiers;
    private Map<String, String> _metadataManagerConfigs;
    @Deprecated
    private boolean _allowDedupConsumptionDuringCommit;
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

    public Builder setMetadataTTL(double metadataTTL) {
      _metadataTTL = metadataTTL;
      return this;
    }

    public Builder setDedupTimeColumn(String dedupTimeColumn) {
      _dedupTimeColumn = dedupTimeColumn;
      return this;
    }

    public Builder setEnablePreload(boolean enablePreload) {
      _enablePreload = enablePreload;
      return this;
    }

    public Builder setIgnoreNonDefaultTiers(boolean ignoreNonDefaultTiers) {
      _ignoreNonDefaultTiers = ignoreNonDefaultTiers;
      return this;
    }

    public Builder setMetadataManagerConfigs(Map<String, String> metadataManagerConfigs) {
      _metadataManagerConfigs = metadataManagerConfigs;
      return this;
    }

    @Deprecated
    public Builder setAllowDedupConsumptionDuringCommit(boolean allowDedupConsumptionDuringCommit) {
      _allowDedupConsumptionDuringCommit = allowDedupConsumptionDuringCommit;
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

    public DedupContext build() {
      Preconditions.checkState(_tableConfig != null, "Table config must be set");
      Preconditions.checkState(_schema != null, "Schema must be set");
      Preconditions.checkState(CollectionUtils.isNotEmpty(_primaryKeyColumns), "Primary key columns must be set");
      Preconditions.checkState(_hashFunction != null, "Hash function must be set");
      if (_tableIndexDir == null) {
        Preconditions.checkState(_tableDataManager != null, "Either table data manager or table index dir must be set");
        _tableIndexDir = _tableDataManager.getTableDataDir();
      }
      return new DedupContext(_tableConfig, _schema, _primaryKeyColumns, _hashFunction, _metadataTTL, _dedupTimeColumn,
          _enablePreload, _ignoreNonDefaultTiers, _metadataManagerConfigs, _allowDedupConsumptionDuringCommit,
          _tableDataManager, _tableIndexDir);
    }
  }
}
