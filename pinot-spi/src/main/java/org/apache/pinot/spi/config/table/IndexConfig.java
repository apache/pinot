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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.JsonUtils;


/// Base class for index configs.
///
/// Common logic across all indexes: each can be enabled or disabled. Indexes with no extra
/// configuration can use this class directly.
public class IndexConfig extends BaseJsonConfig {
  public static final IndexConfig ENABLED = new IndexConfig((Boolean) null);
  public static final IndexConfig DISABLED = new IndexConfig(true);
  private final Boolean _disabled;

  /// @param disabled whether the config is disabled. `null` and `false` both mean enabled — explicit `false` is
  /// normalized to `null` so the slim form omits the key (every index defaults to enabled and that default is
  /// extremely unlikely to flip).
  @JsonCreator
  public IndexConfig(@JsonProperty("disabled") Boolean disabled) {
    _disabled = Boolean.TRUE.equals(disabled) ? Boolean.TRUE : null;
  }

  public boolean isDisabled() {
    return Boolean.TRUE.equals(_disabled);
  }

  @JsonIgnore
  public boolean isEnabled() {
    return !isDisabled();
  }

  /// Curated Jackson serializer. Annotated with [JsonValue] so Jackson skips default bean introspection and uses
  /// this method as the sole source of truth, emitting only fields that were explicitly configured (non-null).
  /// Subclasses override and call `super.toJsonObject()` first to inherit the `disabled` key handling.
  ///
  /// This mirrors the slim-serialization pattern introduced for `Schema` and `TableConfigs` in apache/pinot#17558
  /// and ensures user-supplied slim index configs do not get fattened with defaults on the first round-trip
  /// through any Pinot `ObjectMapper`.
  @JsonValue
  public ObjectNode toJsonObject() {
    ObjectNode node = JsonUtils.newObjectNode();
    if (Boolean.TRUE.equals(_disabled)) {
      node.put("disabled", true);
    }
    return node;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexConfig that = (IndexConfig) o;
    return Objects.equals(_disabled, that._disabled);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_disabled);
  }
}
