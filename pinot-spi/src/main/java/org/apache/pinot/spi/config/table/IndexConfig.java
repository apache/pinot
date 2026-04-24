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


/**
 * This is the base class used to configure indexes.
 *
 * The common logic between all indexes is that they can be enabled or disabled.
 *
 * Indexes that do not require extra configuration can directly use this class.
 */
public class IndexConfig extends BaseJsonConfig {
  public static final IndexConfig ENABLED = new IndexConfig(false);
  public static final IndexConfig DISABLED = new IndexConfig(true);
  private final boolean _disabled;

  /**
   * @param disabled whether the config is disabled. Null is considered enabled.
   */
  @JsonCreator
  public IndexConfig(@JsonProperty("disabled") Boolean disabled) {
    _disabled = Boolean.TRUE.equals(disabled);
  }

  public boolean isDisabled() {
    return _disabled;
  }

  @JsonIgnore
  public boolean isEnabled() {
    return !_disabled;
  }

  /**
   * Curated Jackson serializer. Annotated with {@link JsonValue} so Jackson skips default bean
   * introspection and uses this method as the sole source of truth, emitting only fields whose
   * value differs from the class default. Subclasses override and call {@code super.toJsonObject()}
   * first to inherit the {@code disabled} key handling.
   *
   * <p>This mirrors the slim-serialization pattern introduced for {@code Schema} and
   * {@code TableConfigs} in apache/pinot#17558 and ensures user-supplied slim index configs do not
   * get fattened with defaults on the first round-trip through any Pinot {@code ObjectMapper}.
   */
  @JsonValue
  public ObjectNode toJsonObject() {
    ObjectNode node = JsonUtils.newObjectNode();
    if (_disabled) {
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
    return _disabled == that._disabled;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_disabled);
  }
}
