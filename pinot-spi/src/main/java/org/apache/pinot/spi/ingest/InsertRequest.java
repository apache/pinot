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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Immutable data transfer object representing an INSERT INTO request.
///
/// Carries all information needed to execute a single INSERT statement, including the data
/// payload (rows or file URI), target table, idempotency keys, and consistency preferences.
///
/// Instances are created via the {@link Builder} or deserialized from JSON. Immutable and
/// therefore thread-safe.
@JsonIgnoreProperties(ignoreUnknown = true)
@InterfaceStability.Evolving
public class InsertRequest {
  /// Both statementId and requestId are embedded verbatim in ZooKeeper znode names, segment names
  /// (`insert_<statementId>_...`), and deep-store paths. A `/` would create nested znodes and break
  /// listing/pruning; other special characters break segment-name parsing. Restrict both to a strict
  /// character set and bounded length, enforced on every construction path (wire and builder).
  public static final Pattern ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,128}");

  /// Option keys that carry routing/idempotency control rather than payload data. They are excluded
  /// from the payload hash so that e.g. retrying with the same requestId hashes identically.
  public static final String OPTION_REQUEST_ID = "requestId";
  public static final String OPTION_TABLE_TYPE = "tableType";

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
    /// Mirror Builder.build()'s invariants on the wire-deserialized path so a malformed JSON body
    /// is rejected at construction with a clear message rather than silently propagating a null
    /// through the coordinator and surfacing as an NPE later. Wording is intentionally identical
    /// to the Builder.build() throws so log-greps catch both paths.
    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException("tableName is required for InsertRequest");
    }
    if (insertType == null) {
      throw new IllegalArgumentException("insertType is required for InsertRequest");
    }
    if (insertType == InsertType.FILE && (fileUri == null || fileUri.isEmpty())) {
      throw new IllegalArgumentException("fileUri is required for FILE insert");
    }
    validateId("statementId", statementId);
    validateId("requestId", requestId);
    _statementId = statementId != null ? statementId : UUID.randomUUID().toString();
    _requestId = requestId;
    _payloadHash = payloadHash;
    _tableName = tableName;
    _tableType = tableType;
    _insertType = insertType;
    _rows = rows != null ? Collections.unmodifiableList(new ArrayList<>(rows)) : List.of();
    _fileUri = fileUri;
    _options = options != null ? Collections.unmodifiableMap(new HashMap<>(options)) : Map.of();
    _consistencyMode = consistencyMode != null ? consistencyMode : InsertConsistencyMode.WAIT_FOR_ACCEPT;
  }

  private InsertRequest(Builder builder) {
    validateId("statementId", builder._statementId);
    validateId("requestId", builder._requestId);
    _statementId = builder._statementId != null ? builder._statementId : UUID.randomUUID().toString();
    _requestId = builder._requestId;
    _payloadHash = builder._payloadHash;
    _tableName = builder._tableName;
    _tableType = builder._tableType;
    _insertType = builder._insertType;
    _rows = builder._rows != null
        ? Collections.unmodifiableList(new ArrayList<>(builder._rows)) : List.of();
    _fileUri = builder._fileUri;
    _options = builder._options != null
        ? Collections.unmodifiableMap(new HashMap<>(builder._options)) : Map.of();
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

  /// Returns a copy of this request with the table name and type resolved to the given physical
  /// table name (e.g. `"myTable_REALTIME"`). The type suffix is parsed from the name.
  ///
  /// @param tableNameWithType the fully-qualified table name including type suffix
  /// @return a new InsertRequest with the resolved table name and type
  public InsertRequest withResolvedTable(String tableNameWithType) {
    TableType resolvedType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    /// Pass _rows and _options directly: they are already unmodifiable views (set in the
    /// constructor). The target constructor wraps with unmodifiable + copy, so an extra ArrayList
    /// copy here would be wasted work — for a 10K-row INSERT it allocates a redundant 10K-entry
    /// ArrayList. The constructor's own copy ensures the new instance has its own backing.
    return new InsertRequest(_statementId, _requestId, _payloadHash, tableNameWithType, resolvedType,
        _insertType, _rows, _fileUri, _options, _consistencyMode);
  }

  /// Returns a copy of this request with a freshly generated (server-authoritative) statementId.
  /// The statementId becomes a ZK znode name, the segment-name prefix, and a deep-store path
  /// component, so trust boundaries (e.g. the controller REST endpoint) must never accept a
  /// client-chosen value. Idempotent retries are correlated via requestId, not statementId.
  public InsertRequest withServerGeneratedStatementId() {
    return new InsertRequest(UUID.randomUUID().toString(), _requestId, _payloadHash, _tableName, _tableType,
        _insertType, _rows, _fileUri, _options, _consistencyMode);
  }

  private static void validateId(String name, @Nullable String id) {
    if (id != null && !ID_PATTERN.matcher(id).matches()) {
      throw new IllegalArgumentException(
          name + " must match " + ID_PATTERN.pattern() + " (it is used in ZK paths and segment names); got: " + id);
    }
  }

  /// Computes a stable SHA-256 hash of this request's data payload: table name, insert type, row
  /// contents (field names and values, sorted by field name), file URI, and options excluding the
  /// control keys {@code requestId} and {@code tableType}.
  ///
  /// The hash is computed by the coordinator from the request it actually received — never trusted
  /// from the client — and pairs with requestId for idempotency conflict detection: a retry with the
  /// same requestId and the same payload returns the original result, while a reused requestId with
  /// a different payload is rejected.
  ///
  /// Values are canonicalized before hashing (numerics via BigDecimal plain string, byte[] as raw
  /// bytes, everything else via toString) so that logically identical payloads hash identically
  /// regardless of the numeric boxing the deserializer chose.
  public String computePayloadHash() {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(_tableName.getBytes(StandardCharsets.UTF_8));
      digest.update((byte) 0);
      digest.update(_insertType.name().getBytes(StandardCharsets.UTF_8));
      digest.update((byte) 0);
      if (_fileUri != null) {
        digest.update(_fileUri.getBytes(StandardCharsets.UTF_8));
      }
      digest.update((byte) 1);
      for (GenericRow row : _rows) {
        /// Sort field names so the hash is independent of map iteration order.
        TreeMap<String, Object> sortedFields = new TreeMap<>(row.getFieldToValueMap());
        for (Map.Entry<String, Object> entry : sortedFields.entrySet()) {
          digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) 0);
          hashValue(digest, entry.getValue());
          digest.update((byte) 0);
        }
        digest.update((byte) 2);
      }
      if (!_options.isEmpty()) {
        /// Case-insensitive ordering + control-key exclusion mirror the case-insensitive option
        /// handling on the SQL parse path (OPTION(requestId=...) can arrive in any case).
        TreeMap<String, String> canonical = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        canonical.putAll(_options);
        canonical.remove(OPTION_REQUEST_ID);
        canonical.remove(OPTION_TABLE_TYPE);
        for (Map.Entry<String, String> entry : canonical.entrySet()) {
          digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
          digest.update((byte) 0);
          if (entry.getValue() != null) {
            digest.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
          }
          digest.update((byte) 0);
        }
      }
      StringBuilder hex = new StringBuilder();
      for (byte b : digest.digest()) {
        hex.append(String.format("%02x", b));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }

  private static void hashValue(MessageDigest digest, @Nullable Object val) {
    if (val == null) {
      digest.update((byte) 'N');
    } else if (val instanceof Long || val instanceof Integer || val instanceof Short || val instanceof Byte) {
      digest.update((byte) 'D');
      digest.update(canonicalizeBigDecimal(BigDecimal.valueOf(((Number) val).longValue()))
          .getBytes(StandardCharsets.UTF_8));
    } else if (val instanceof BigDecimal) {
      digest.update((byte) 'D');
      digest.update(canonicalizeBigDecimal((BigDecimal) val).getBytes(StandardCharsets.UTF_8));
    } else if (val instanceof BigInteger) {
      digest.update((byte) 'D');
      digest.update(canonicalizeBigDecimal(new BigDecimal((BigInteger) val)).getBytes(StandardCharsets.UTF_8));
    } else if (val instanceof Double || val instanceof Float) {
      /// NaN/Infinity cannot pass through BigDecimal; hash stable sentinels instead.
      double d = ((Number) val).doubleValue();
      digest.update((byte) 'D');
      if (Double.isNaN(d)) {
        digest.update((byte) 'X');
      } else if (Double.isInfinite(d)) {
        digest.update(d > 0 ? (byte) 'P' : (byte) 'M');
      } else {
        /// Use the type's own toString so Float 0.1f and Double 0.1 don't diverge via widening.
        String canonical = val instanceof Float ? Float.toString((Float) val) : Double.toString((Double) val);
        digest.update(canonicalizeBigDecimal(new BigDecimal(canonical)).getBytes(StandardCharsets.UTF_8));
      }
    } else if (val instanceof Boolean) {
      digest.update((byte) 'B');
      digest.update((Boolean) val ? (byte) 1 : (byte) 0);
    } else if (val instanceof byte[]) {
      digest.update((byte) 'X');
      digest.update((byte[]) val);
    } else {
      digest.update((byte) 'S');
      digest.update(val.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  /// Canonicalizes a BigDecimal so equivalent values produce identical strings. Zero is
  /// special-cased because stripTrailingZeros() returns the denormalized form 0E-1.
  private static String canonicalizeBigDecimal(BigDecimal bd) {
    if (bd.signum() == 0) {
      return "0";
    }
    return bd.stripTrailingZeros().toPlainString();
  }

  /// Builder for constructing {@link InsertRequest} instances.
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
      if (_tableName == null || _tableName.isEmpty()) {
        throw new IllegalArgumentException("tableName is required for InsertRequest");
      }
      if (_insertType == null) {
        throw new IllegalArgumentException("insertType is required for InsertRequest");
      }
      if (_insertType == InsertType.FILE && (_fileUri == null || _fileUri.isEmpty())) {
        throw new IllegalArgumentException("fileUri is required for FILE insert");
      }
      return new InsertRequest(this);
    }
  }
}
