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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/// Turns a nested OPEN_STRUCT document into flat keys, so a value buried under an object is
/// addressable as a key of its own.
///
/// OPEN_STRUCT keys a document one level deep: `col['device']` names an entry of the top-level
/// map. A document like `{"device": {"os": "ios"}}` therefore has exactly one key, `device`, whose
/// value is an object -- there is no key that names the `os` inside it, so no column is ever
/// materialized for it and no predicate can reach it without decoding the object per row.
///
/// Flattening makes the **path** the key: `device.os`. That key is materialized, indexed, filtered
/// and projected like any other, with no change to the key/value contract -- `.` is an ordinary
/// character in a key, so `col['device.os']` is already valid syntax.
///
/// The container keeps its own entry, serialized as JSON text, so `col['device']` still returns the
/// whole object after its leaves have been split out.
///
/// ```
/// {"device": {"os": "ios", "ver": 17}}   maxDepth = 2
///
///   device      -> {"os":"ios","ver":17}   (container, JSON text)
///   device.os   -> "ios"
///   device.ver  -> 17
/// ```
///
/// `maxDepth` counts path segments and bounds the recursion: 1 (the default) emits the document
/// unchanged, 2 reaches `a.b`, 3 reaches `a.b.c`. An object sitting at the depth limit is still
/// emitted as JSON text, so no data is dropped by a limit that is too low -- only addressability
/// is. Depth is the only bound: a wide object contributes a key per entry, which is why this is
/// opt-in per column.
///
/// Only objects are split. A list stays a single value, whatever it holds, because its elements
/// have no names to build a path from.
public final class OpenStructKeyFlattener {

  /// Path segment separator, and the character that appears in the resulting key.
  public static final char PATH_SEPARATOR = '.';

  /// `maxDepth` value that leaves a document untouched. Also the default.
  public static final int NO_FLATTENING = 1;

  private OpenStructKeyFlattener() {
  }

  /// Receives one flat entry. `container` is true when `value` is the JSON text of an object that
  /// was (or would have been) recursed into, which callers use to keep those entries out of
  /// automatic dense-key selection -- a column of JSON blobs is not what dense materialization is
  /// for.
  @FunctionalInterface
  public interface EntryConsumer {
    void accept(String path, @Nullable Object value, boolean container);
  }

  /// Walks `document`, emitting every flat key and its value to `sink`. Values are passed through
  /// untouched, nulls included, so the caller keeps its own absent-key rule.
  public static void flatten(Map<String, Object> document, int maxDepth, EntryConsumer sink) {
    if (maxDepth <= NO_FLATTENING) {
      // No allocation, no string building: identical to iterating the map directly.
      for (Map.Entry<String, Object> entry : document.entrySet()) {
        sink.accept(entry.getKey(), entry.getValue(), false);
      }
      return;
    }
    flattenInto(document, null, 1, maxDepth, sink);
  }

  private static void flattenInto(Map<String, Object> map, @Nullable String prefix, int depth, int maxDepth,
      EntryConsumer sink) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        continue;
      }
      String path = prefix == null ? key : prefix + PATH_SEPARATOR + key;
      Object value = entry.getValue();
      if (value instanceof Map) {
        sink.accept(path, toJson(value), true);
        if (depth < maxDepth) {
          @SuppressWarnings("unchecked")
          Map<String, Object> child = (Map<String, Object>) value;
          flattenInto(child, path, depth + 1, maxDepth, sink);
        }
      } else {
        sink.accept(path, value, false);
      }
    }
  }

  /// Serializes a container. Returns null when it cannot be serialized, which callers treat as an
  /// absent key -- the same outcome the value would have had as an uncoercible object, and the
  /// leaves underneath it are emitted either way.
  @Nullable
  private static String toJson(Object container) {
    try {
      return JsonUtils.objectToString(container);
    } catch (JsonProcessingException e) {
      return null;
    }
  }
}
