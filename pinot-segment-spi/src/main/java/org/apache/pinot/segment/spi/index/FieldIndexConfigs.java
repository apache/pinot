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

package org.apache.pinot.segment.spi.index;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.pinot.spi.config.table.IndexConfig;


/**
 * FieldIndexConfigs are a map like structure that relates index types with their configuration, providing a type safe
 * interface.
 *
 * This class can be serialized into a JSON object whose keys are the index type ids using Jackson, but cannot be
 * serialized back. A custom Jackson deserializer could be provided if needed.
 */
public class FieldIndexConfigs {

  public static final FieldIndexConfigs EMPTY = new FieldIndexConfigs(new HashMap<>());

  private final Map<IndexType, IndexConfig> _configMap;

  private FieldIndexConfigs(Map<IndexType, IndexConfig> configMap) {
    _configMap = Collections.unmodifiableMap(configMap);
  }

  /**
   * Returns the configuration associated with the given index type, which will be null if there is no configuration for
   * that index type.
   */
  @JsonIgnore
  public <C extends IndexConfig, I extends IndexType<C, ?, ?>> C getConfig(I indexType) {
    IndexConfig config = _configMap.get(indexType);
    if (config == null) {
      return indexType.getDefaultConfig();
    }
    return (C) config;
  }

  /*
  This is used by Jackson when this object is serialized. Each entry of the map will be directly contained in the
  JSON object, with the key name as the key in the JSON object and the result of serializing the key value as the value
  in the JSON object.
   */
  @JsonAnyGetter
  public Map<String, JsonNode> unwrapIndexes() {
    Function<Map.Entry<IndexType, IndexConfig>, JsonNode> serializer =
        entry -> entry.getKey().serialize(entry.getValue());
    return _configMap.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(entry -> entry.getKey().getId(), serializer));
  }

  public static class Builder {
    private final Map<IndexType, IndexConfig> _configMap;

    public Builder() {
      _configMap = new HashMap<>();
    }

    public Builder(FieldIndexConfigs other) {
      _configMap = new HashMap<>(other._configMap);
    }

    public <C extends IndexConfig, I extends IndexType<C, ?, ?>> Builder add(I indexType, C config) {
      _configMap.put(indexType, config);
      return this;
    }

    public Builder addUnsafe(IndexType<?, ?, ?> indexType, IndexConfig config) {
      _configMap.put(indexType, config);
      return this;
    }

    public Builder undeclare(IndexType<?, ?, ?> indexType) {
      _configMap.remove(indexType);
      return this;
    }

    public FieldIndexConfigs build() {
      return new FieldIndexConfigs(_configMap);
    }
  }

  public static class UnrecognizedIndexException extends RuntimeException {
    private final String _indexId;

    public UnrecognizedIndexException(String indexId) {
      super("There is no index type whose identified as " + indexId);
      _indexId = indexId;
    }

    public String getIndexId() {
      return _indexId;
    }
  }
}
