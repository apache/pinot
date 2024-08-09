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
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class DedupContext {
  private final TableConfig _tableConfig;
  private final Schema _schema;
  private final List<String> _primaryKeyColumns;
  private final HashFunction _hashFunction;
  private final double _metadataTTL;
  private final String _metadataTimeColumn;
  private final ServerMetrics _serverMetrics;

  private DedupContext(TableConfig tableConfig, Schema schema, List<String> primaryKeyColumns,
      HashFunction hashFunction, double metadataTTL, String metadataTimeColumn, ServerMetrics serverMetrics) {
    _tableConfig = tableConfig;
    _schema = schema;
    _primaryKeyColumns = primaryKeyColumns;
    _hashFunction = hashFunction;
    _metadataTTL = metadataTTL;
    _metadataTimeColumn = metadataTimeColumn;
    _serverMetrics = serverMetrics;
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

  public String getMetadataTimeColumn() {
    return _metadataTimeColumn;
  }

  public ServerMetrics getServerMetrics() {
    return _serverMetrics;
  }

  public static class Builder {
    private TableConfig _tableConfig;
    private Schema _schema;
    private List<String> _primaryKeyColumns;
    private HashFunction _hashFunction;
    private double _metadataTTL;
    private String _metadataTimeColumn;
    private ServerMetrics _serverMetrics;

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

    public Builder setMetadataTimeColumn(String metadataTimeColumn) {
      _metadataTimeColumn = metadataTimeColumn;
      return this;
    }

    public Builder setServerMetrics(ServerMetrics serverMetrics) {
      _serverMetrics = serverMetrics;
      return this;
    }

    public DedupContext build() {
      Preconditions.checkState(_tableConfig != null, "Table config must be set");
      Preconditions.checkState(_schema != null, "Schema must be set");
      Preconditions.checkState(CollectionUtils.isNotEmpty(_primaryKeyColumns), "Primary key columns must be set");
      Preconditions.checkState(_hashFunction != null, "Hash function must be set");
      return new DedupContext(_tableConfig, _schema, _primaryKeyColumns, _hashFunction, _metadataTTL,
          _metadataTimeColumn, _serverMetrics);
    }
  }
}
