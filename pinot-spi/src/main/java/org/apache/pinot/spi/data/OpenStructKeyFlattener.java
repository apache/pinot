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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
///
/// `.` being an ordinary key character is also what lets one document produce the same key twice:
/// `{"a.b": 100, "a": {"b": 200}}` has a literal `a.b` and a path `a.b`. A key the document
/// actually carries wins -- the path is not emitted -- at any level, so `{"a": {"b.c": 1, "b":
/// {"c": 2}}}` keys `a.b.c` to `1`. Addressability is what flattening adds; it never shadows data
/// that was already addressable. Two synthesized paths can still collide with each other
/// (`{"a.b": {"c": 1}, "a": {"b.c": 2}}` reaches `a.b.c` two ways, neither of them literal); there
/// the first emission wins and the rest are dropped, so a key is emitted at most once per document
/// whatever the document holds.
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
    flattenInto(new Level(document, null, null), 1, maxDepth, sink, new PathGuard());
  }

  /// One map being walked, with the path that leads to it and the level above. The chain is what a
  /// synthesized path is checked against: an enclosing map holding the rest of the path as a literal
  /// key means the document already has this key, and the path is dropped rather than emitted twice.
  private static final class Level {
    final Map<String, Object> _map;
    @Nullable
    final String _prefix;
    @Nullable
    final Level _parent;

    Level(Map<String, Object> map, @Nullable String prefix, @Nullable Level parent) {
      _map = map;
      _prefix = prefix;
      _parent = parent;
    }
  }

  /// Remembers the synthesized paths already emitted for the current document, so two paths that
  /// collide with each other resolve to the first one. Allocates nothing until a path is emitted,
  /// which is never for a flat document.
  private static final class PathGuard {
    @Nullable
    private Set<String> _emitted;

    boolean firstEmission(String path) {
      if (_emitted == null) {
        _emitted = new HashSet<>();
      }
      return _emitted.add(path);
    }
  }

  private static void flattenInto(Level level, int depth, int maxDepth, EntryConsumer sink, PathGuard guard) {
    boolean synthesized = level._prefix != null;
    for (Map.Entry<String, Object> entry : level._map.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        continue;
      }
      String path = synthesized ? level._prefix + PATH_SEPARATOR + key : key;
      // A literal key of this document, at this or any enclosing level, is the value of `path`; only a
      // path the document does not carry itself is synthesized, and only once.
      boolean emit = !synthesized || (!carriedByEnclosingMap(level, path) && guard.firstEmission(path));
      Object value = entry.getValue();
      if (value instanceof Map) {
        if (emit) {
          sink.accept(path, toJson(value), true);
        }
        if (depth < maxDepth) {
          @SuppressWarnings("unchecked")
          Map<String, Object> child = (Map<String, Object>) value;
          // Recursed into even when the container entry was dropped: the leaves underneath it are
          // their own keys, and a collision on the container says nothing about them.
          flattenInto(new Level(child, path, level), depth + 1, maxDepth, sink, guard);
        }
      } else if (emit) {
        sink.accept(path, value, false);
      }
    }
  }

  /// Whether a map enclosing `level` holds the remainder of `path` as a key of its own, which makes
  /// the path a second name for a value the document already keys directly.
  private static boolean carriedByEnclosingMap(Level level, String path) {
    for (Level enclosing = level._parent; enclosing != null; enclosing = enclosing._parent) {
      String prefix = enclosing._prefix;
      String suffix = prefix == null ? path : path.substring(prefix.length() + 1);
      if (enclosing._map.containsKey(suffix)) {
        return true;
      }
    }
    return false;
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
