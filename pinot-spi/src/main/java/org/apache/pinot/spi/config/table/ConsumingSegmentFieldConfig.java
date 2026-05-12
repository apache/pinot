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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/// Field-level index profile applied only while a realtime segment is mutable and consuming.
///
/// This config intentionally exposes a narrow subset of [FieldConfig]: `name`, `encodingType`, and `indexes`.
/// Validation rejects all other keys so mutable consuming segment shape cannot diverge from the surrounding
/// ingestion pipeline in unsupported ways.
public class ConsumingSegmentFieldConfig extends BaseJsonConfig {
  public static final String NAME_KEY = "name";
  public static final String ENCODING_TYPE_KEY = "encodingType";
  public static final String INDEXES_KEY = "indexes";

  private final String _name;
  private final FieldConfig.EncodingType _encodingType;
  private final JsonNode _indexes;
  private final Map<String, JsonNode> _unknownProperties = new LinkedHashMap<>();

  @JsonCreator
  public ConsumingSegmentFieldConfig(@JsonProperty(value = NAME_KEY, required = true) String name,
      @JsonProperty(ENCODING_TYPE_KEY) @Nullable FieldConfig.EncodingType encodingType,
      @JsonProperty(INDEXES_KEY) @Nullable JsonNode indexes) {
    _name = name;
    _encodingType = encodingType;
    _indexes = indexes == null ? NullNode.getInstance() : indexes;
  }

  @JsonProperty(NAME_KEY)
  public String getName() {
    return _name;
  }

  @JsonProperty(ENCODING_TYPE_KEY)
  @Nullable
  public FieldConfig.EncodingType getEncodingType() {
    return _encodingType;
  }

  @JsonProperty(INDEXES_KEY)
  public JsonNode getIndexes() {
    return _indexes;
  }

  @JsonAnySetter
  public void setUnknownProperty(String key, JsonNode value) {
    _unknownProperties.put(key, value);
  }

  @JsonIgnore
  public Map<String, JsonNode> getUnknownProperties() {
    return _unknownProperties;
  }

  @JsonIgnore
  public boolean hasConfiguredIndexes() {
    return !_indexes.isNull();
  }
}
