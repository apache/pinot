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
package org.apache.pinot.common.metadata.segment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * The <code>SegmentZKMetadataCustomMapModifier</code> class provides util methods to serialize/de-serialize the segment
 * ZK metadata custom map modifier HTTP header and modify the custom map field in {@link SegmentZKMetadata}.
 */
public class SegmentZKMetadataCustomMapModifier {
  public enum ModifyMode {
    REPLACE,  // Replace the current map
    UPDATE    // Update the current map
  }

  private static final String MAP_MODIFY_MODE_KEY = "mapModifyMode";
  private static final String MAP_KEY = "map";

  private final ModifyMode _modifyMode;
  private final Map<String, String> _map;

  public SegmentZKMetadataCustomMapModifier(@Nonnull ModifyMode modifyMode, @Nullable Map<String, String> map) {
    _modifyMode = modifyMode;
    if (map == null || map.isEmpty()) {
      _map = null;
    } else {
      _map = map;
    }
  }

  public SegmentZKMetadataCustomMapModifier(@Nonnull String jsonString)
      throws IOException {
    JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);
    _modifyMode = ModifyMode.valueOf(jsonNode.get(MAP_MODIFY_MODE_KEY).asText());
    JsonNode jsonMap = jsonNode.get(MAP_KEY);
    if (jsonMap == null || jsonMap.isEmpty()) {
      _map = null;
    } else {
      _map = new HashMap<>();
      Iterator<String> keys = jsonMap.fieldNames();
      while (keys.hasNext()) {
        String key = keys.next();
        _map.put(key, jsonMap.get(key).asText());
      }
    }
  }

  public String toJsonString() {
    ObjectNode objectNode = JsonUtils.newObjectNode();
    objectNode.put(MAP_MODIFY_MODE_KEY, _modifyMode.toString());
    objectNode.set(MAP_KEY, JsonUtils.objectToJsonNode(_map));
    return objectNode.toString();
  }

  public Map<String, String> modifyMap(@Nullable Map<String, String> existingMap) {
    if (_modifyMode == ModifyMode.REPLACE || existingMap == null || existingMap.isEmpty()) {
      if (_map == null) {
        return null;
      } else {
        return new HashMap<>(_map);
      }
    } else {
      // UPDATE modify mode && existing map is not empty
      if (_map != null) {
        existingMap.putAll(_map);
      }
      return existingMap;
    }
  }
}
