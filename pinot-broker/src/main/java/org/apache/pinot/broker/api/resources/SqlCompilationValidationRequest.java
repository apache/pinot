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
package org.apache.pinot.broker.api.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;

/// Request body for {@code POST /query/sql/validateQueryCompilation}. Carries a batch of query
/// payloads plus optional static table metadata used to mirror query compilation without executing
/// the query.
/// Thread-safety: not thread-safe because it retains references to mutable Jackson and collection
/// instances; intended for request-scoped deserialization only.
public class SqlCompilationValidationRequest {
  private final List<ObjectNode> _queries;
  private final List<TableConfig> _tableConfigs;
  private final List<Schema> _schemas;
  private final List<LogicalTableConfig> _logicalTableConfigs;
  private final boolean _ignoreCase;

  @JsonCreator
  public SqlCompilationValidationRequest(@JsonProperty("queries") @Nullable List<ObjectNode> queries,
      @JsonProperty("tableConfigs") @Nullable List<TableConfig> tableConfigs,
      @JsonProperty("schemas") @Nullable List<Schema> schemas,
      @JsonProperty("logicalTableConfigs") @Nullable List<LogicalTableConfig> logicalTableConfigs,
      @JsonProperty("ignoreCase") boolean ignoreCase) {
    _queries = queries;
    _tableConfigs = tableConfigs;
    _schemas = schemas;
    _logicalTableConfigs = logicalTableConfigs;
    _ignoreCase = ignoreCase;
  }

  @Nullable
  public List<ObjectNode> getQueries() {
    return _queries;
  }

  @Nullable
  public List<TableConfig> getTableConfigs() {
    return _tableConfigs;
  }

  @Nullable
  public List<Schema> getSchemas() {
    return _schemas;
  }

  @Nullable
  public List<LogicalTableConfig> getLogicalTableConfigs() {
    return _logicalTableConfigs;
  }

  public boolean isIgnoreCase() {
    return _ignoreCase;
  }
}
