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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.TableConfigs;


/// Versioned response for the edit-capable `GET /tableConfigs/{tableName}` representation.
///
/// The [TableConfigs] value is deliberately nested below an envelope that cannot be deserialized as a valid legacy
/// [TableConfigs] request. Clients retaining redaction markers must submit the complete response to the matching PUT
/// endpoint.
@JsonPropertyOrder({RedactedTableConfigsResponse.RESPONSE_TYPE_KEY,
    RedactedTableConfigsResponse.RESPONSE_VERSION_KEY, RedactedTableConfigsResponse.TABLE_NAME_KEY,
    RedactedTableConfigsResponse.BASE_VERSIONS_KEY, RedactedTableConfigsResponse.CONFIGS_KEY})
@JsonIgnoreProperties(ignoreUnknown = true)
public final class RedactedTableConfigsResponse {
  public static final String RESPONSE_TYPE_KEY = "responseType";
  public static final String RESPONSE_VERSION_KEY = "responseVersion";
  public static final String TABLE_NAME_KEY = "tableName";
  public static final String BASE_VERSIONS_KEY = "baseVersions";
  public static final String CONFIGS_KEY = "configs";
  public static final String RESPONSE_TYPE = "redactedTableConfigs";
  public static final int RESPONSE_VERSION = 1;
  public static final int ABSENT_VERSION = -1;
  public static final String SCHEMA_VERSION_KEY = "schema";
  public static final String OFFLINE_VERSION_KEY = "offline";
  public static final String REALTIME_VERSION_KEY = "realtime";
  public static final String MEDIA_TYPE = "application/vnd.apache.pinot.redacted-table-configs-v1+json";

  private final TableIdentity _tableName;
  private final Map<String, Integer> _baseVersions;
  private final TableConfigs _configs;

  @JsonCreator
  public RedactedTableConfigsResponse(
      @JsonProperty(value = RESPONSE_TYPE_KEY, required = true) String responseType,
      @JsonProperty(value = RESPONSE_VERSION_KEY, required = true) int responseVersion,
      @JsonProperty(value = TABLE_NAME_KEY, required = true) TableIdentity tableName,
      @JsonProperty(value = BASE_VERSIONS_KEY, required = true) Map<String, Integer> baseVersions,
      @JsonProperty(value = CONFIGS_KEY, required = true) TableConfigs configs) {
    Preconditions.checkArgument(RESPONSE_TYPE.equals(responseType), "Unsupported TableConfigs response type");
    Preconditions.checkArgument(responseVersion == RESPONSE_VERSION, "Unsupported TableConfigs response version");
    _tableName = Preconditions.checkNotNull(tableName, "Table name must not be null");
    _configs = Preconditions.checkNotNull(configs, "TableConfigs must not be null");
    Preconditions.checkArgument(_tableName.getRaw().equals(_configs.getTableName()),
        "TableConfigs must match the response table name");
    Set<String> expectedTypes = new LinkedHashSet<>();
    expectedTypes.add(SCHEMA_VERSION_KEY);
    expectedTypes.add(OFFLINE_VERSION_KEY);
    expectedTypes.add(REALTIME_VERSION_KEY);
    Preconditions.checkNotNull(baseVersions, "TableConfigs base versions must not be null");
    Preconditions.checkArgument(baseVersions.keySet().equals(expectedTypes),
        "TableConfigs base versions must cover schema, offline, and realtime state");
    Integer schemaVersion = baseVersions.get(SCHEMA_VERSION_KEY);
    Preconditions.checkArgument(schemaVersion != null && schemaVersion >= 0,
        "TableConfigs schema base version must not be null or negative");
    validateTableTypeVersion(OFFLINE_VERSION_KEY, baseVersions.get(OFFLINE_VERSION_KEY),
        _configs.getOffline() != null);
    validateTableTypeVersion(REALTIME_VERSION_KEY, baseVersions.get(REALTIME_VERSION_KEY),
        _configs.getRealtime() != null);
    _baseVersions = Collections.unmodifiableMap(new LinkedHashMap<>(baseVersions));
  }

  public RedactedTableConfigsResponse(TableConfigs configs, Map<String, Integer> baseVersions) {
    this(RESPONSE_TYPE, RESPONSE_VERSION, new TableIdentity(configs.getTableName()), baseVersions, configs);
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
  public TableConfigs getConfigs() {
    return _configs;
  }

  public static boolean isEnvelope(JsonNode node) {
    return node.isObject() && node.has(RESPONSE_TYPE_KEY);
  }

  private static void validateTableTypeVersion(String type, Integer version, boolean configIncluded) {
    Preconditions.checkArgument(version != null && version >= ABSENT_VERSION,
        "TableConfigs %s base version must be -1 or non-negative", type);
    Preconditions.checkArgument(configIncluded || version == ABSENT_VERSION,
        "TableConfigs %s config cannot be omitted when it existed in the read snapshot", type);
  }

  /// Object-valued identity that makes this envelope incompatible with the legacy string-valued `tableName` field.
  public static final class TableIdentity {
    private final String _raw;

    @JsonCreator
    public TableIdentity(@JsonProperty(value = "raw", required = true) String raw) {
      _raw = Preconditions.checkNotNull(raw, "Raw table name must not be null");
    }

    public String getRaw() {
      return _raw;
    }
  }
}
