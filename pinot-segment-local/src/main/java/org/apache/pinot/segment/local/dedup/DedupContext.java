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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class DedupContext {
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final List<String> _primaryKeyColumns;
  private final HashFunction _hashFunction;
  private final boolean _enablePreload;
  private final double _metadataTTL;
  private final String _dedupTimeColumn;
  private final File _tableIndexDir;
  private final TableDataManager _tableDataManager;

  private DedupContext(TableConfig tableConfig, Schema schema, List<String> primaryKeyColumns,
      HashFunction hashFunction, boolean enablePreload, double metadataTTL, String dedupTimeColumn, File tableIndexDir,
      TableDataManager tableDataManager) {
    _tableConfig = tableConfig;
    _schema = schema;
    _primaryKeyColumns = primaryKeyColumns;
    _hashFunction = hashFunction;
    _enablePreload = enablePreload;
    _metadataTTL = metadataTTL;
    _dedupTimeColumn = dedupTimeColumn;
    _tableIndexDir = tableIndexDir;
    _tableDataManager = tableDataManager;
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

  public boolean isPreloadEnabled() {
    return _enablePreload;
  }

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  public String getDedupTimeColumn() {
    return _dedupTimeColumn;
  }

  public File getTableIndexDir() {
    return _tableIndexDir;
  }

  public TableDataManager getTableDataManager() {
    return _tableDataManager;
  }

  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;
    private List<String> _primaryKeyColumns;
    private HashFunction _hashFunction;
    private boolean _enablePreload;
    private double _metadataTTL;
    private String _dedupTimeColumn;
    private File _tableIndexDir;
    private TableDataManager _tableDataManager;

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

    public Builder setEnablePreload(boolean enablePreload) {
      _enablePreload = enablePreload;
      return this;
    }

    public Builder setMetadataTTL(double metadataTTL) {
      _metadataTTL = metadataTTL;
      return this;
    }

    public Builder setDedupTimeColumn(String deupTimeColumn) {
      _dedupTimeColumn = deupTimeColumn;
      return this;
    }

    public Builder setTableIndexDir(File tableIndexDir) {
      _tableIndexDir = tableIndexDir;
      return this;
    }

    public Builder setTableDataManager(TableDataManager tableDataManager) {
      _tableDataManager = tableDataManager;
      return this;
    }

    public DedupContext build() {
      Preconditions.checkState(_tableConfig != null, "Table config must be set");
      Preconditions.checkState(_schema != null, "Schema must be set");
      Preconditions.checkState(CollectionUtils.isNotEmpty(_primaryKeyColumns), "Primary key columns must be set");
      Preconditions.checkState(_hashFunction != null, "Hash function must be set");
      Preconditions.checkState(_tableIndexDir != null, "Table index directory must be set");
      return new DedupContext(_tableConfig, _schema, _primaryKeyColumns, _hashFunction, _enablePreload, _metadataTTL,
          _dedupTimeColumn, _tableIndexDir, _tableDataManager);
    }
  }
}
