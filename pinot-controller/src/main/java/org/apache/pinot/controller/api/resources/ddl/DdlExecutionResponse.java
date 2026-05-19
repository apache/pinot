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
package org.apache.pinot.controller.api.resources.ddl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.sql.ddl.compile.DdlOperation;


/// Response body for `POST /sql/ddl`.
///
/// The shape of the response varies by operation. [JsonInclude.Include#NON_NULL] keeps
/// the wire payload focused on the fields that actually apply to the operation that ran.
///
/// - CREATE_TABLE: `tableName, tableType, schema, tableConfig, ifNotExists, warnings, dryRun`
/// - DROP_TABLE: `tableName, tableType, deletedTables, ifExists, dryRun`
/// - SHOW_TABLES: `tableNames`
/// - SHOW_CREATE_TABLE: `tableName, tableType, ddl`
///
/// `dryRun` is emitted only for operations that have dry-run semantics (CREATE, DROP); it is
/// absent from SHOW responses where the concept does not apply.
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DdlExecutionResponse {
  private DdlOperation _operation;
  /// Boxed Boolean so that CREATE / DROP responses can carry the dry-run flag while SHOW
  /// TABLES and SHOW CREATE TABLE responses elide it via {@link JsonInclude.Include#NON_NULL}
  /// — those operations have no dry-run semantics and a serialized `"dryRun": false` on them
  /// is meaningless to the caller.
  private Boolean _dryRun;
  private String _databaseName;
  private String _tableName;
  private String _tableType;
  private Boolean _ifNotExists;
  private Boolean _ifExists;
  private JsonNode _schema;
  private JsonNode _tableConfig;
  private List<String> _warnings;
  private List<String> _deletedTables;
  private List<String> _tableNames;
  private String _ddl;
  private String _message;

  public DdlOperation getOperation() {
    return _operation;
  }

  public DdlExecutionResponse setOperation(DdlOperation operation) {
    _operation = operation;
    return this;
  }

  public Boolean isDryRun() {
    return _dryRun;
  }

  public DdlExecutionResponse setDryRun(boolean dryRun) {
    _dryRun = dryRun;
    return this;
  }

  @Nullable
  public String getDatabaseName() {
    return _databaseName;
  }

  public DdlExecutionResponse setDatabaseName(@Nullable String databaseName) {
    _databaseName = databaseName;
    return this;
  }

  @Nullable
  public String getTableName() {
    return _tableName;
  }

  public DdlExecutionResponse setTableName(@Nullable String tableName) {
    _tableName = tableName;
    return this;
  }

  @Nullable
  public String getTableType() {
    return _tableType;
  }

  public DdlExecutionResponse setTableType(@Nullable String tableType) {
    _tableType = tableType;
    return this;
  }

  @Nullable
  public Boolean getIfNotExists() {
    return _ifNotExists;
  }

  public DdlExecutionResponse setIfNotExists(@Nullable Boolean ifNotExists) {
    _ifNotExists = ifNotExists;
    return this;
  }

  @Nullable
  public Boolean getIfExists() {
    return _ifExists;
  }

  public DdlExecutionResponse setIfExists(@Nullable Boolean ifExists) {
    _ifExists = ifExists;
    return this;
  }

  @Nullable
  public JsonNode getSchema() {
    return _schema;
  }

  public DdlExecutionResponse setSchema(@Nullable JsonNode schema) {
    _schema = schema;
    return this;
  }

  @Nullable
  public JsonNode getTableConfig() {
    return _tableConfig;
  }

  public DdlExecutionResponse setTableConfig(@Nullable JsonNode tableConfig) {
    _tableConfig = tableConfig;
    return this;
  }

  @Nullable
  public List<String> getWarnings() {
    return _warnings;
  }

  public DdlExecutionResponse setWarnings(@Nullable List<String> warnings) {
    _warnings = warnings == null || warnings.isEmpty() ? null : warnings;
    return this;
  }

  @Nullable
  public List<String> getDeletedTables() {
    return _deletedTables;
  }

  public DdlExecutionResponse setDeletedTables(@Nullable List<String> deletedTables) {
    _deletedTables = deletedTables;
    return this;
  }

  @Nullable
  public List<String> getTableNames() {
    return _tableNames;
  }

  public DdlExecutionResponse setTableNames(@Nullable List<String> tableNames) {
    _tableNames = tableNames;
    return this;
  }

  @Nullable
  public String getDdl() {
    return _ddl;
  }

  /// Canonical CREATE TABLE statement returned by `SHOW CREATE TABLE`.
  public DdlExecutionResponse setDdl(@Nullable String ddl) {
    _ddl = ddl;
    return this;
  }

  @Nullable
  public String getMessage() {
    return _message;
  }

  public DdlExecutionResponse setMessage(@Nullable String message) {
    _message = message;
    return this;
  }
}
