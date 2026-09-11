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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Versioned response for the edit-capable `GET /tables/{tableName}` representation.
///
/// The table configs are deliberately nested below an envelope that is not a valid [TableConfig]. Clients retaining
/// redaction markers must submit this complete response to the matching PUT endpoint; a bare table config containing a
/// marker is rejected.
@JsonPropertyOrder({RedactedTableConfigResponse.RESPONSE_TYPE_KEY,
    RedactedTableConfigResponse.RESPONSE_VERSION_KEY, RedactedTableConfigResponse.TABLE_NAME_KEY,
    RedactedTableConfigResponse.BASE_VERSIONS_KEY, RedactedTableConfigResponse.CONFIGS_KEY})
@JsonIgnoreProperties(ignoreUnknown = true)
public final class RedactedTableConfigResponse {
  public static final String RESPONSE_TYPE_KEY = "responseType";
  public static final String RESPONSE_VERSION_KEY = "responseVersion";
  public static final String TABLE_NAME_KEY = "tableName";
  public static final String BASE_VERSIONS_KEY = "baseVersions";
  public static final String CONFIGS_KEY = "configs";
  public static final String SCHEMA_VERSION_KEY = "schema";
  public static final String RESPONSE_TYPE = "redactedTableConfig";
  public static final int RESPONSE_VERSION = 1;
  public static final String MEDIA_TYPE = "application/vnd.apache.pinot.redacted-table-config-v1+json";

  private final TableIdentity _tableName;
  private final Map<String, Integer> _baseVersions;
  private final Map<String, TableConfig> _configs;

  @JsonCreator
  public RedactedTableConfigResponse(
      @JsonProperty(value = RESPONSE_TYPE_KEY, required = true) String responseType,
      @JsonProperty(value = RESPONSE_VERSION_KEY, required = true) int responseVersion,
      @JsonProperty(value = TABLE_NAME_KEY, required = true) TableIdentity tableName,
      @JsonProperty(value = BASE_VERSIONS_KEY, required = true) Map<String, Integer> baseVersions,
      @JsonProperty(value = CONFIGS_KEY, required = true) Map<String, TableConfig> configs) {
    Preconditions.checkArgument(RESPONSE_TYPE.equals(responseType), "Unsupported table config response type");
    Preconditions.checkArgument(responseVersion == RESPONSE_VERSION, "Unsupported table config response version");
    _tableName = Preconditions.checkNotNull(tableName, "Table name must not be null");
    Preconditions.checkNotNull(configs, "Table configs must not be null");
    Preconditions.checkArgument(!configs.isEmpty(), "Table configs must not be empty");
    Preconditions.checkNotNull(baseVersions, "Table config base versions must not be null");
    Map<String, Integer> configVersions = new LinkedHashMap<>(baseVersions);
    Integer schemaVersion = configVersions.remove(SCHEMA_VERSION_KEY);
    Preconditions.checkArgument(schemaVersion != null && schemaVersion >= 0,
        "Table config schema base version must not be null or negative");
    Preconditions.checkArgument(configVersions.keySet().equals(configs.keySet()),
        "Table config base versions must exactly match the schema and config types");
    for (Integer baseVersion : configVersions.values()) {
      Preconditions.checkArgument(baseVersion != null && baseVersion >= 0,
          "Table config base versions must not be null or negative");
    }
    for (Map.Entry<String, TableConfig> entry : configs.entrySet()) {
      TableConfig config = Preconditions.checkNotNull(entry.getValue(), "Table config must not be null");
      Preconditions.checkArgument(entry.getKey().equals(config.getTableType().name()),
          "Table config key must match its table type");
      Preconditions.checkArgument(
          _tableName.getRaw().equals(TableNameBuilder.extractRawTableName(config.getTableName())),
          "Table config must match the response table name");
    }
    if (_tableName.getType() != null) {
      Preconditions.checkArgument(configs.size() == 1, "A typed response must contain exactly one table config");
      Preconditions.checkArgument(configs.containsKey(_tableName.getType().name()),
          "Table config must match the response table type");
    }
    _baseVersions = Collections.unmodifiableMap(new LinkedHashMap<>(baseVersions));
    _configs = Collections.unmodifiableMap(new LinkedHashMap<>(configs));
  }

  public RedactedTableConfigResponse(Map<String, TableConfig> configs, Map<String, Integer> baseVersions) {
    this(RESPONSE_TYPE, RESPONSE_VERSION, tableIdentity(configs), baseVersions, configs);
  }

  @JsonProperty(RESPONSE_TYPE_KEY)
  public String getResponseType() {
    return RESPONSE_TYPE;
  }

  @JsonProperty(RESPONSE_VERSION_KEY)
  public int getResponseVersion() {
    return RESPONSE_VERSION;
  }

  @JsonProperty(TABLE_NAME_KEY)
  public TableIdentity getTableName() {
    return _tableName;
  }

  @JsonProperty(BASE_VERSIONS_KEY)
  public Map<String, Integer> getBaseVersions() {
    return _baseVersions;
  }

  @JsonProperty(CONFIGS_KEY)
  public Map<String, TableConfig> getConfigs() {
    return _configs;
  }

  public static boolean isEnvelope(JsonNode node) {
    return node.isObject() && node.has(RESPONSE_TYPE_KEY);
  }

  private static TableIdentity tableIdentity(Map<String, TableConfig> configs) {
    Preconditions.checkNotNull(configs, "Table configs must not be null");
    Preconditions.checkArgument(!configs.isEmpty(), "Table configs must not be empty");
    TableConfig firstConfig = configs.values().iterator().next();
    String rawTableName = TableNameBuilder.extractRawTableName(firstConfig.getTableName());
    return new TableIdentity(rawTableName, configs.size() == 1 ? firstConfig.getTableType() : null);
  }

  /// Object-valued identity that makes this envelope incompatible with the legacy string-valued `tableName` field.
  public static final class TableIdentity {
    private final String _raw;
    @Nullable
    private final TableType _type;

    @JsonCreator
    public TableIdentity(@JsonProperty(value = "raw", required = true) String raw,
        @JsonProperty("type") @Nullable TableType type) {
      _raw = Preconditions.checkNotNull(raw, "Raw table name must not be null");
      _type = type;
    }

    public String getRaw() {
      return _raw;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public TableType getType() {
      return _type;
    }
  }
}
