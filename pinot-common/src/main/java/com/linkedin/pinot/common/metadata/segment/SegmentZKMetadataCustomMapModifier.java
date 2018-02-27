/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metadata.segment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.json.JSONException;
import org.json.JSONObject;


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

  public SegmentZKMetadataCustomMapModifier(@Nonnull String jsonString) throws JSONException {
    JSONObject jsonObject = new JSONObject(jsonString);
    _modifyMode = ModifyMode.valueOf(jsonObject.getString(MAP_MODIFY_MODE_KEY));
    JSONObject jsonMap = jsonObject.getJSONObject(MAP_KEY);
    if (jsonMap == null || jsonMap.length() == 0) {
      _map = null;
    } else {
      _map = new HashMap<>();
      @SuppressWarnings("unchecked")
      Iterator<String> keys = jsonMap.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        _map.put(key, jsonMap.getString(key));
      }
    }
  }

  public String toJsonString() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(MAP_MODIFY_MODE_KEY, _modifyMode);
    jsonObject.put(MAP_KEY, _map);
    return jsonObject.toString();
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
