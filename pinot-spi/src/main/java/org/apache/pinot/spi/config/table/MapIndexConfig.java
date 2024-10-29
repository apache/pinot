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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;


/**
 * Configs related to the MAP index:
 */
public class MapIndexConfig extends IndexConfig {
  public static final MapIndexConfig DEFAULT = new MapIndexConfig();
  public static final MapIndexConfig DISABLED = new MapIndexConfig(true);

  private final Map<String, Object> _configs;

  public MapIndexConfig() {
    this(false);
  }

  public MapIndexConfig(Boolean disabled) {
    super(disabled);
    _configs = new HashMap<>();
  }

  @JsonCreator
  public MapIndexConfig(@JsonProperty("disabled") @Nullable Boolean disabled,
      @JsonProperty("configs") @Nullable Map<String, Object> configs) {
    super(disabled);
    _configs = configs != null ? configs : new HashMap<>();
  }

  public Map<String, Object> getConfigs() {
    return _configs;
  }

  public void putConfig(String key, Object value) {
    _configs.put(key, value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MapIndexConfig config = (MapIndexConfig) o;
    return _configs.equals(config._configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _configs);
  }
}
