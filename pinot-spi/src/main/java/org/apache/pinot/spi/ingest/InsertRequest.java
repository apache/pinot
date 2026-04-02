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
package org.apache.pinot.spi.ingest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Immutable data transfer object representing an INSERT INTO request.
 *
 * <p>Carries all information needed to execute a single INSERT statement, including the data
 * payload (rows or file URI), target table, idempotency keys, and consistency preferences.
 *
 * <p>Instances are created via the {@link Builder} or deserialized from JSON. Immutable and
 * therefore thread-safe.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class InsertRequest {
  private final String _statementId;
  private final String _requestId;
  private final String _payloadHash;
  private final String _tableName;
  private final TableType _tableType;
  private final InsertType _insertType;
  private final List<GenericRow> _rows;
  private final String _fileUri;
  private final Map<String, String> _options;
  private final InsertConsistencyMode _consistencyMode;

  @JsonCreator
  public InsertRequest(
      @JsonProperty("statementId") String statementId,
      @JsonProperty("requestId") String requestId,
      @JsonProperty("payloadHash") String payloadHash,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("tableType") TableType tableType,
      @JsonProperty("insertType") InsertType insertType,
      @JsonProperty("rows") List<GenericRow> rows,
      @JsonProperty("fileUri") String fileUri,
      @JsonProperty("options") Map<String, String> options,
      @JsonProperty("consistencyMode") InsertConsistencyMode consistencyMode) {
    _statementId = statementId != null ? statementId : UUID.randomUUID().toString();
    _requestId = requestId;
    _payloadHash = payloadHash;
    _tableName = tableName;
    _tableType = tableType;
    _insertType = insertType;
    _rows = rows != null ? Collections.unmodifiableList(new ArrayList<>(rows)) : Collections.emptyList();
    _fileUri = fileUri;
    _options = options != null ? Collections.unmodifiableMap(new HashMap<>(options)) : Collections.emptyMap();
    _consistencyMode = consistencyMode != null ? consistencyMode : InsertConsistencyMode.WAIT_FOR_ACCEPT;
  }

  private InsertRequest(Builder builder) {
    _statementId = builder._statementId != null ? builder._statementId : UUID.randomUUID().toString();
    _requestId = builder._requestId;
    _payloadHash = builder._payloadHash;
    _tableName = builder._tableName;
    _tableType = builder._tableType;
    _insertType = builder._insertType;
    _rows = builder._rows != null
        ? Collections.unmodifiableList(new ArrayList<>(builder._rows)) : Collections.emptyList();
    _fileUri = builder._fileUri;
    _options = builder._options != null
        ? Collections.unmodifiableMap(new HashMap<>(builder._options)) : Collections.emptyMap();
    _consistencyMode = builder._consistencyMode != null ? builder._consistencyMode
        : InsertConsistencyMode.WAIT_FOR_ACCEPT;
  }

  @JsonProperty("statementId")
  public String getStatementId() {
    return _statementId;
  }

  @JsonProperty("requestId")
  public String getRequestId() {
    return _requestId;
  }

  @JsonProperty("payloadHash")
  public String getPayloadHash() {
    return _payloadHash;
  }

  @JsonProperty("tableName")
  public String getTableName() {
    return _tableName;
  }

  @JsonProperty("tableType")
  public TableType getTableType() {
    return _tableType;
  }

  @JsonProperty("insertType")
  public InsertType getInsertType() {
    return _insertType;
  }

  @JsonProperty("rows")
  public List<GenericRow> getRows() {
    return _rows;
  }

  @JsonProperty("fileUri")
  public String getFileUri() {
    return _fileUri;
  }

  @JsonProperty("options")
  public Map<String, String> getOptions() {
    return _options;
  }

  @JsonProperty("consistencyMode")
  public InsertConsistencyMode getConsistencyMode() {
    return _consistencyMode;
  }

  /**
   * Returns a copy of this request with the table name and type resolved to the given physical
   * table name (e.g. {@code "myTable_REALTIME"}). The type suffix is parsed from the name.
   *
   * @param tableNameWithType the fully-qualified table name including type suffix
   * @return a new InsertRequest with the resolved table name and type
   */
  public InsertRequest withResolvedTable(String tableNameWithType) {
    TableType resolvedType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    return new InsertRequest(_statementId, _requestId, _payloadHash, tableNameWithType, resolvedType,
        _insertType, _rows != null ? new ArrayList<>(_rows) : null, _fileUri,
        _options != null ? new HashMap<>(_options) : null, _consistencyMode);
  }

  /**
   * Builder for constructing {@link InsertRequest} instances.
   */
  public static class Builder {
    private String _statementId;
    private String _requestId;
    private String _payloadHash;
    private String _tableName;
    private TableType _tableType;
    private InsertType _insertType;
    private List<GenericRow> _rows;
    private String _fileUri;
    private Map<String, String> _options;
    private InsertConsistencyMode _consistencyMode;

    public Builder setStatementId(String statementId) {
      _statementId = statementId;
      return this;
    }

    public Builder setRequestId(String requestId) {
      _requestId = requestId;
      return this;
    }

    public Builder setPayloadHash(String payloadHash) {
      _payloadHash = payloadHash;
      return this;
    }

    public Builder setTableName(String tableName) {
      _tableName = tableName;
      return this;
    }

    public Builder setTableType(TableType tableType) {
      _tableType = tableType;
      return this;
    }

    public Builder setInsertType(InsertType insertType) {
      _insertType = insertType;
      return this;
    }

    public Builder setRows(List<GenericRow> rows) {
      _rows = rows;
      return this;
    }

    public Builder setFileUri(String fileUri) {
      _fileUri = fileUri;
      return this;
    }

    public Builder setOptions(Map<String, String> options) {
      _options = options;
      return this;
    }

    public Builder setConsistencyMode(InsertConsistencyMode consistencyMode) {
      _consistencyMode = consistencyMode;
      return this;
    }

    public InsertRequest build() {
      return new InsertRequest(this);
    }
  }
}
