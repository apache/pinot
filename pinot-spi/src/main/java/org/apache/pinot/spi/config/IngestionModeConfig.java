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
package org.apache.pinot.spi.config;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.utils.JsonUtils;

import javax.annotation.Nullable;
import java.util.List;

@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public class IngestionModeConfig {
  // new config key to indicate if a table is for upsert. If this is define as upsert, it would be an upsert table
  // for other value it would be append value for now
  private String _ingestionMode;
  // primary key refers to the column name that is the primary key of this upsert table
  private List<String> _primaryKeys;
  // offset key refers to the column name that we are going to store the offset value to
  private String _offsetKey;
  // validFrom key refers to the column name that we are going to store the validFrom value to
  private String _validFromKey;
  // validFrom key refers to the column name that we are going to store the validFrom value to
  private String _validUntilKey;

  public static final String UPSERT_TABLE_CONFIG_VALUE = "upsert";
  public static final String APPEND_TABLE_CONFIG_VALUE = "append";

  public static final IngestionModeConfig DEFAULT_APPEND_INGESTION_MODE =
      new IngestionModeConfig(APPEND_TABLE_CONFIG_VALUE, null, null, null,
          null);

  @JsonCreator
  public IngestionModeConfig(
      @JsonProperty(value="ingestionMode") @Nullable String ingestionMode,
      @JsonProperty(value="primaryKeys") @Nullable List<String> primaryKeys,
      @JsonProperty(value="offsetKey") @Nullable String offsetKey,
      @JsonProperty(value="validFromKey") @Nullable String validFromKey,
      @JsonProperty(value="validUntilKey") @Nullable String validUntilKey) {
    if (StringUtils.isEmpty(ingestionMode)) {
      _ingestionMode = APPEND_TABLE_CONFIG_VALUE;
    } else {
      _ingestionMode = ingestionMode.toLowerCase();
    }
    if (primaryKeys == null) {
      _primaryKeys = ImmutableList.of();
    } else {
      _primaryKeys = primaryKeys;
    }
    _offsetKey = offsetKey;
    _validFromKey = validFromKey;
    _validUntilKey = validUntilKey;

    if (UPSERT_TABLE_CONFIG_VALUE.equals(_ingestionMode)) {
      Preconditions.checkState(_primaryKeys.size() == 1,
          "pinot upsert require one and only one primary key");
      Preconditions.checkState(StringUtils.isNotEmpty(_offsetKey),
          "pinot upsert require one offset key");
      Preconditions.checkState(StringUtils.isNotEmpty(_validFromKey),
          "pinot upsert require one validFrom key");
      Preconditions.checkState(StringUtils.isNotEmpty(_validUntilKey),
          "pinot upsert require one validUntil key");
    }
  }

  public IngestionModeConfig(String ingestionMode) {
    Preconditions.checkState(!UPSERT_TABLE_CONFIG_VALUE.equals(ingestionMode),
        "upsert ingestion mode should define primary key and offsetKey");
    _ingestionMode = ingestionMode;
  }

  @JsonIgnore
  public boolean isForUpsert() {
    return UPSERT_TABLE_CONFIG_VALUE.equals(_ingestionMode);
  }

  public String getIngestionMode() {
    return _ingestionMode;
  }

  public List<String> getPrimaryKeys() {
    return _primaryKeys;
  }

  @Nullable
  public String getOffsetKey() {
    return _offsetKey;
  }

  @Nullable
  public String getValidFromKey() {
    return _validFromKey;
  }

  @Nullable
  public String getValidUntilKey() {
    return _validUntilKey;
  }

  public JsonNode toJsonNode() {
    return JsonUtils.objectToJsonNode(this);
  }

  public String toJsonString() {
    return toJsonNode().toString();
  }

  @Override
  public int hashCode() {
    return toJsonNode().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof IngestionModeConfig) {
      return toJsonNode().equals(((IngestionModeConfig) obj).toJsonNode());
    }
    return false;
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}
