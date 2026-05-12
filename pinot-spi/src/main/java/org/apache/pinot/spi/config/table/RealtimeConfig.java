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
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/// Realtime-specific table config.
///
/// Keep lifecycle-specific realtime controls here instead of overloading storage-tier config. The first supported
/// setting is [#getConsumingSegmentIndexConfig()], which affects only mutable consuming segment construction.
public class RealtimeConfig extends BaseJsonConfig {
  public static final String CONSUMING_SEGMENT_INDEX_CONFIG_KEY = "consumingSegmentIndexConfig";

  private final ConsumingSegmentIndexConfig _consumingSegmentIndexConfig;
  private final Map<String, JsonNode> _unknownProperties = new LinkedHashMap<>();

  @JsonCreator
  public RealtimeConfig(
      @JsonProperty(CONSUMING_SEGMENT_INDEX_CONFIG_KEY) @Nullable ConsumingSegmentIndexConfig
          consumingSegmentIndexConfig) {
    _consumingSegmentIndexConfig = consumingSegmentIndexConfig;
  }

  @JsonProperty(CONSUMING_SEGMENT_INDEX_CONFIG_KEY)
  @Nullable
  public ConsumingSegmentIndexConfig getConsumingSegmentIndexConfig() {
    return _consumingSegmentIndexConfig;
  }

  @JsonAnySetter
  public void setUnknownProperty(String key, JsonNode value) {
    _unknownProperties.put(key, value);
  }

  @JsonIgnore
  public Map<String, JsonNode> getUnknownProperties() {
    return _unknownProperties;
  }
}
